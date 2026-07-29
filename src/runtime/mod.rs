use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use reqwest::Client;
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tracing::{debug, warn};
use utoipa::OpenApi;

use crate::{
    config::{self, IntegrationConfig, PostingConfig},
    contract::{
        ErrorBody, ErrorCode, ErrorEnvelope, EventKind, OriginKind, Post, PostOrigin, PostRef,
        ReplyRequest, ReplyResponse, SchemaVersion, Thread, WebhookEvent,
    },
    metrics,
    origin::OriginMatcher,
    posting::PostWriter,
    rate_limit::RateLimiters,
    reading::ThreadReader,
    store::Store,
};

mod auth;
mod replies;
mod responses;
mod status;
mod telemetry;
mod threads;

use replies::integration_reply;
pub(crate) use status::Status;
use threads::integration_thread;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "ptchan-gateway integration API",
        version = "1.0.0",
        description = "Signed, moderation-safe access to ptchan threads and replies. Webhook payloads use the WebhookEvent schema. Webhook delivery is durable and at-least-once; consumers must deduplicate by event_id and tolerate delayed or out-of-order events."
    ),
    paths(threads::integration_thread, replies::integration_reply),
    components(schemas(
        SchemaVersion,
        WebhookEvent,
        EventKind,
        Post,
        Thread,
        PostRef,
        PostOrigin,
        OriginKind,
        ReplyRequest,
        ReplyResponse,
        ErrorEnvelope,
        ErrorBody,
        ErrorCode
    )),
    tags(
        (name = "integration", description = "Signed integration-facing API")
    )
)]
struct ContractApi;

pub(crate) fn contract_openapi() -> utoipa::openapi::OpenApi {
    ContractApi::openapi()
}

#[derive(Clone)]
pub(super) struct AppState {
    pub(super) status: Arc<Status>,
    pub(super) store: Arc<Store>,
    pub(super) thread_reader: ThreadReader,
    pub(super) post_writer: PostWriter,
    pub(super) integrations: Arc<HashMap<String, Arc<IntegrationConfig>>>,
    pub(super) postings: Arc<HashMap<String, Arc<PostingConfig>>>,
    pub(super) origins: OriginMatcher,
    pub(super) rate_limiters: RateLimiters,
}

pub(crate) async fn spawn_http(
    server: HttpServer,
    mut shutdown: watch::Receiver<bool>,
) -> Result<JoinHandle<Result<()>>> {
    metrics::init();
    let listener = TcpListener::bind(config::runtime_addr(&server.addr)?)
        .await
        .context("bind runtime http")?;
    let local_addr = listener.local_addr().context("runtime local addr")?;
    tracing::info!(address = %local_addr, "runtime http listening");
    let rate_limiters = RateLimiters::new(&server.integrations, &server.rate_limit)?;
    let app = router(AppState {
        status: server.status,
        store: server.store,
        thread_reader: server.thread_reader,
        post_writer: server.post_writer,
        integrations: Arc::new(
            server
                .integrations
                .into_iter()
                .map(|integration| (integration.name.clone(), Arc::new(integration)))
                .collect(),
        ),
        postings: Arc::new(
            server
                .postings
                .into_iter()
                .map(|posting| (posting.name.clone(), Arc::new(posting)))
                .collect(),
        ),
        origins: server.origins,
        rate_limiters,
    });
    Ok(tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await
            .context("runtime http server")
    }))
}

pub(crate) struct HttpServer {
    pub(crate) addr: String,
    pub(crate) status: Arc<Status>,
    pub(crate) store: Arc<Store>,
    pub(crate) thread_reader: ThreadReader,
    pub(crate) post_writer: PostWriter,
    pub(crate) integrations: Vec<IntegrationConfig>,
    pub(crate) postings: Vec<PostingConfig>,
    pub(crate) origins: OriginMatcher,
    pub(crate) rate_limit: config::RuntimeRateLimitConfig,
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .route("/integration/v1/openapi.json", get(openapi_handler))
        .route(
            "/integration/v1/threads/:board/:thread_id",
            get(integration_thread),
        )
        .route(
            "/integration/v1/threads/:board/:thread_id/replies",
            post(integration_reply),
        )
        .with_state(state)
}

async fn openapi_handler() -> Json<utoipa::openapi::OpenApi> {
    Json(contract_openapi())
}

async fn healthz() -> impl IntoResponse {
    debug!("health check requested");
    (StatusCode::OK, "ok\n")
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    let auth_healthy = state.status.auth_healthy();
    let upstream_joined = state.status.upstream_joined();
    let upstream_required = state.status.upstream_required();
    let upstream_ready = state.status.ready();
    let storage_ready = state.store.is_ready().await;
    debug!(
        auth_healthy,
        upstream_joined, upstream_required, storage_ready, "readiness requested"
    );
    if upstream_ready && storage_ready {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

async fn metrics_handler() -> Response {
    debug!("metrics requested");
    match metrics::render() {
        Ok(body) => (StatusCode::OK, body).into_response(),
        Err(err) => {
            warn!(error = %err, "metrics render failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub(crate) async fn check_health(addr: &str) -> Result<()> {
    let url = format!("http://{}/healthz", config::runtime_addr(addr)?);
    let response = Client::new()
        .get(url)
        .send()
        .await
        .context("send health check")?;
    if !response.status().is_success() {
        anyhow::bail!("health check status {}", response.status());
    }
    println!("ok");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request},
        response::{IntoResponse, Response},
    };
    use chrono::Utc;
    use hmac::{Hmac, Mac};
    use serde_json::Value;
    use sha2::Sha256;
    use tower::ServiceExt;

    use super::{
        responses::gateway_rate_limited_error, router, telemetry, AppState, Status, StatusCode,
    };
    use crate::{
        config::{
            IntegrationConfig, PostingConfig, PtchanConfig, RateLimitBucketConfig, RateLimitConfig,
            RuntimeRateLimitConfig,
        },
        contract::{ErrorCode, ErrorEnvelope},
        metrics,
        origin::OriginMatcher,
        posting::PostWriter,
        rate_limit::RateLimiters,
        reading::ThreadReader,
        store::Store,
    };

    #[test]
    fn ready_requires_auth_and_joined_socket() {
        let status = Status::default();
        assert!(!status.ready());

        status.set_auth_healthy(true);
        assert!(!status.ready());

        status.set_auth_healthy(false);
        status.set_upstream_joined(true);
        assert!(!status.ready());

        status.set_auth_healthy(true);
        assert!(status.ready());
    }

    #[test]
    fn ready_without_upstream_requirement() {
        let status = Status::new(false);

        assert!(status.ready());
        assert!(!status.upstream_required());
    }

    #[tokio::test]
    async fn oversized_post_preserves_axum_payload_too_large() {
        let request = Request::builder()
            .method("POST")
            .uri("/integration/v1/threads/i/100/replies")
            .body(Body::from(vec![b'x'; 2 * 1024 * 1024 + 1]))
            .unwrap();

        let response = router(test_state().await).oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            response_error_code(response).await,
            ErrorCode::PayloadTooLarge
        );
    }

    #[tokio::test]
    async fn unsigned_read_is_rejected() {
        let request = Request::builder()
            .uri("/integration/v1/threads/i/100")
            .body(Body::empty())
            .unwrap();

        let response = router(test_state().await).oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response_error_code(response).await, ErrorCode::Unauthorized);
    }

    #[tokio::test]
    async fn signed_read_requires_the_reading_capability() {
        let integration = IntegrationConfig {
            name: "assistant".to_string(),
            allowed_boards: Vec::new(),
            reading: false,
            rate_limit: RateLimitConfig::default(),
            secret: "secret".to_string(),
        };
        let mut state = test_state().await;
        state.integrations = Arc::new(HashMap::from([(
            integration.name.clone(),
            Arc::new(integration.clone()),
        )]));
        state.rate_limiters =
            RateLimiters::new(&[integration], &RuntimeRateLimitConfig::default()).unwrap();
        let target = "/integration/v1/threads/i/100?limit=25";
        let timestamp = Utc::now().to_rfc3339();
        let signature = request_signature("secret", &timestamp, &Method::GET, target);
        let request = Request::builder()
            .uri(target)
            .header("x-ptchan-integration", "assistant")
            .header("x-ptchan-timestamp", timestamp)
            .header("x-ptchan-signature", signature)
            .body(Body::empty())
            .unwrap();

        let response = router(state).oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response_error_code(response).await,
            ErrorCode::CapabilityNotEnabled
        );
    }

    #[tokio::test]
    async fn malformed_signed_read_consumes_quota() {
        let integration = one_request_integration(true);
        let mut state = test_state().await;
        state.integrations = Arc::new(HashMap::from([(
            integration.name.clone(),
            Arc::new(integration.clone()),
        )]));
        state.rate_limiters =
            RateLimiters::new(&[integration], &RuntimeRateLimitConfig::default()).unwrap();
        let app = router(state);

        let malformed = signed_request(
            Method::GET,
            "/integration/v1/threads/i/100?limit=invalid",
            &[],
        );
        let response = app.clone().oneshot(malformed).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response_error_code(response).await, ErrorCode::InvalidQuery);

        let valid = signed_request(Method::GET, "/integration/v1/threads/i/100", &[]);
        let response = app.oneshot(valid).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response_error_code(response).await, ErrorCode::RateLimited);
    }

    #[tokio::test]
    async fn malformed_signed_read_paths_consume_quota() {
        for (target, expected_code) in [
            (
                "/integration/v1/threads/bad-board/100",
                ErrorCode::InvalidBoard,
            ),
            (
                "/integration/v1/threads/i/not-a-thread",
                ErrorCode::InvalidThreadId,
            ),
        ] {
            let integration = one_request_integration(true);
            let mut state = test_state().await;
            state.integrations = Arc::new(HashMap::from([(
                integration.name.clone(),
                Arc::new(integration.clone()),
            )]));
            state.rate_limiters =
                RateLimiters::new(&[integration], &RuntimeRateLimitConfig::default()).unwrap();
            let app = router(state);

            let malformed = signed_request(Method::GET, target, &[]);
            let response = app.clone().oneshot(malformed).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(response_error_code(response).await, expected_code);

            let valid = signed_request(Method::GET, "/integration/v1/threads/i/100", &[]);
            let response = app.oneshot(valid).await.unwrap();
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(response_error_code(response).await, ErrorCode::RateLimited);
        }
    }

    #[tokio::test]
    async fn malformed_signed_reply_consumes_quota() {
        let integration = one_request_integration(true);
        let posting = PostingConfig {
            name: integration.name.clone(),
            allowed_boards: Vec::new(),
            display_name: None,
            secret: integration.secret.clone(),
            tripcode_secret: "trip-secret".to_string(),
            public_tripcode: "!!X8NXmAS44=".to_string(),
            post_password: "post-secret".to_string(),
        };
        let mut state = test_state().await;
        state.postings = Arc::new(HashMap::from([(posting.name.clone(), Arc::new(posting))]));
        state.rate_limiters =
            RateLimiters::new(&[integration], &RuntimeRateLimitConfig::default()).unwrap();
        let app = router(state);
        let target = "/integration/v1/threads/i/100/replies";

        let malformed = signed_request(Method::POST, target, b"{");
        let response = app.clone().oneshot(malformed).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response_error_code(response).await, ErrorCode::InvalidJson);

        let valid = signed_request(Method::POST, target, br#"{"message":"hello"}"#);
        let response = app.oneshot(valid).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response_error_code(response).await, ErrorCode::RateLimited);
    }

    async fn test_state() -> AppState {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(&dir.path().join("test.db")).await.unwrap());
        store.migrate().await.unwrap();
        let status = Arc::new(Status::default());
        let cfg = PtchanConfig {
            base_url: "https://ptchan.test".to_string(),
        };
        let thread_reader = ThreadReader::new(&cfg).unwrap();
        let post_writer = PostWriter::new(&cfg).unwrap();

        AppState {
            status,
            store,
            thread_reader,
            post_writer,
            integrations: Arc::new(HashMap::new()),
            postings: Arc::new(HashMap::new()),
            origins: OriginMatcher::new(&[]),
            rate_limiters: RateLimiters::new(&[], &RuntimeRateLimitConfig::default()).unwrap(),
        }
    }

    fn request_signature(secret: &str, timestamp: &str, method: &Method, target: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(method.as_str().as_bytes());
        mac.update(b".");
        mac.update(target.as_bytes());
        format!("hmac-sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn signed_request(method: Method, target: &str, body: &[u8]) -> Request<Body> {
        let timestamp = Utc::now().to_rfc3339();
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(method.as_str().as_bytes());
        mac.update(b".");
        mac.update(target.as_bytes());
        if !body.is_empty() {
            mac.update(b".");
            mac.update(body);
        }
        Request::builder()
            .method(method)
            .uri(target)
            .header("x-ptchan-integration", "assistant")
            .header("x-ptchan-timestamp", timestamp)
            .header(
                "x-ptchan-signature",
                format!("hmac-sha256={}", hex::encode(mac.finalize().into_bytes())),
            )
            .body(Body::from(body.to_vec()))
            .unwrap()
    }

    fn one_request_integration(reading: bool) -> IntegrationConfig {
        let bucket = RateLimitBucketConfig {
            requests: 1,
            window: Duration::from_secs(60),
            burst: 1,
        };
        IntegrationConfig {
            name: "assistant".to_string(),
            allowed_boards: Vec::new(),
            reading,
            rate_limit: RateLimitConfig {
                reading: bucket.clone(),
                posting: bucket,
            },
            secret: "secret".to_string(),
        }
    }

    async fn response_error_code(response: Response) -> ErrorCode {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice::<ErrorEnvelope>(&body)
            .unwrap()
            .error
            .code
    }

    #[tokio::test]
    async fn gateway_rate_limit_response_is_structured_for_integrations() {
        let response = gateway_rate_limited_error().into_response();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice::<Value>(&body).unwrap();
        assert_eq!(body["error"]["code"], "rate_limited");
        assert_eq!(body["error"]["message"], "gateway rate limit exceeded");
        assert_eq!(body["error"]["retryable"], true);
        assert!(body["error"].get("upstream_status").is_none());
    }

    #[test]
    fn gateway_rate_limit_metric_sanitizes_board_label() {
        metrics::init();

        telemetry::record_gateway_rate_limited_request(
            "assistant",
            "bad/board",
            "reading",
            "integration",
        );

        let rendered = metrics::render().unwrap();
        assert!(rendered.contains(
            r#"ptchan_gateway_rate_limited_requests_total{board="invalid",capability="reading",integration="assistant",scope="integration"}"#
        ));
        assert!(!rendered.contains(r#"board="bad/board""#));
    }
}

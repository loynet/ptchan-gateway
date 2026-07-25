use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tracing::{debug, warn};

use crate::{
    config::{self, IntegrationConfig, PostingConfig},
    metrics,
    posting::{self, PostWriter, ReplyError, ReplyRequest},
    rate_limit::RateLimiters,
    reading::{ThreadReader, DEFAULT_THREAD_LIMIT},
    store::Store,
};

type HmacSha256 = Hmac<Sha256>;
const REQUEST_MAX_SKEW_SECONDS: i64 = 5 * 60;

pub(crate) struct Status {
    upstream_joined: std::sync::atomic::AtomicBool,
    auth_healthy: std::sync::atomic::AtomicBool,
    upstream_required: std::sync::atomic::AtomicBool,
}

impl Default for Status {
    fn default() -> Self {
        Self::new(true)
    }
}

impl Status {
    pub(crate) fn new(upstream_required: bool) -> Self {
        metrics::UPSTREAM_REQUIRED.set(i64::from(upstream_required));
        Self {
            upstream_joined: std::sync::atomic::AtomicBool::new(false),
            auth_healthy: std::sync::atomic::AtomicBool::new(false),
            upstream_required: std::sync::atomic::AtomicBool::new(upstream_required),
        }
    }

    pub(crate) fn set_upstream_joined(&self, joined: bool) {
        self.upstream_joined
            .store(joined, std::sync::atomic::Ordering::Relaxed);
        metrics::SOCKET_JOINED.set(i64::from(joined));
    }

    pub(crate) fn set_auth_healthy(&self, healthy: bool) {
        self.auth_healthy
            .store(healthy, std::sync::atomic::Ordering::Relaxed);
        metrics::UPSTREAM_AUTH_HEALTHY.set(i64::from(healthy));
    }

    pub(crate) fn auth_healthy(&self) -> bool {
        self.auth_healthy.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn upstream_joined(&self) -> bool {
        self.upstream_joined
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn upstream_required(&self) -> bool {
        self.upstream_required
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn ready(&self) -> bool {
        !self.upstream_required() || (self.auth_healthy() && self.upstream_joined())
    }
}

#[derive(Clone)]
struct AppState {
    status: Arc<Status>,
    store: Arc<Store>,
    thread_reader: ThreadReader,
    post_writer: PostWriter,
    integrations: Arc<HashMap<String, IntegrationConfig>>,
    postings: Arc<HashMap<String, PostingConfig>>,
    rate_limiters: RateLimiters,
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
                .map(|integration| (integration.name.clone(), integration))
                .collect(),
        ),
        postings: Arc::new(
            server
                .postings
                .into_iter()
                .map(|posting| (posting.name.clone(), posting))
                .collect(),
        ),
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
    pub(crate) rate_limit: config::RuntimeRateLimitConfig,
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
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

async fn healthz() -> impl IntoResponse {
    debug!("health check requested");
    (StatusCode::OK, "ok\n")
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    let auth_healthy = state.status.auth_healthy();
    let upstream_joined = state.status.upstream_joined();
    let upstream_required = state.status.upstream_required();
    let upstream_ready = state.status.ready();
    let storage_ready = state.store.is_ready();
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

#[derive(Deserialize)]
struct ThreadQuery {
    limit: Option<usize>,
}

async fn integration_thread(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path((board, thread_id)): Path<(String, i64)>,
    Query(query): Query<ThreadQuery>,
) -> impl IntoResponse {
    let started_at = Instant::now();
    let Some(integration) = authenticate_integration(&state, &headers, &method, &uri) else {
        record_reading_request("unknown", &board, "unauthorized", started_at.elapsed());
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let integration_name = integration.name.clone();
    if !integration.reading_enabled() || !integration.board_allowed(&board) {
        record_reading_request(&integration_name, &board, "forbidden", started_at.elapsed());
        return StatusCode::FORBIDDEN.into_response();
    }
    if let Err(rejection) = state.rate_limiters.check_reading(&integration_name) {
        record_reading_request(
            &integration_name,
            &board,
            "rate_limited",
            started_at.elapsed(),
        );
        record_gateway_rate_limited_request(
            &integration_name,
            &board,
            "reading",
            rejection.as_str(),
        );
        return gateway_rate_limited_response();
    }

    let limit = query.limit.unwrap_or(DEFAULT_THREAD_LIMIT);
    match state
        .thread_reader
        .fetch_thread(&board, thread_id, limit)
        .await
    {
        Ok(Some(mut thread)) => {
            if let Err(err) = annotate_thread_origins(&state.store, &mut thread) {
                record_reading_request(
                    &integration_name,
                    &board,
                    "store_error",
                    started_at.elapsed(),
                );
                warn!(error = %err, integration = %integration_name, board, thread_id, "integration thread origin annotation failed");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            record_reading_request(&integration_name, &board, "success", started_at.elapsed());
            Json(thread).into_response()
        }
        Ok(None) => {
            record_reading_request(&integration_name, &board, "not_found", started_at.elapsed());
            StatusCode::NOT_FOUND.into_response()
        }
        Err(err) => {
            record_reading_request(
                &integration_name,
                &board,
                "upstream_error",
                started_at.elapsed(),
            );
            warn!(error = %err, integration = %integration_name, board, thread_id, "integration thread reading failed");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

fn annotate_thread_origins(store: &Store, thread: &mut crate::contract::Thread) -> Result<()> {
    for post in &mut thread.posts {
        post.origin = store.produced_post_origin(&post.board, post.thread_id, post.id)?;
    }
    Ok(())
}

async fn integration_reply(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path((board, thread_id)): Path<(String, i64)>,
    body: Bytes,
) -> impl IntoResponse {
    let started_at = Instant::now();
    let input = ReplyInput {
        headers: &headers,
        method: &method,
        uri: &uri,
        board: &board,
        thread_id,
        body: &body,
        started_at,
    };
    let prepared = match prepare_reply(&state, &input) {
        Ok(prepared) => prepared,
        Err(response) => return *response,
    };
    let result = state
        .post_writer
        .reply(prepared.posting, &board, thread_id, &prepared.request)
        .await;
    reply_response(&state, &prepared, &board, thread_id, result, started_at)
}

fn reply_response(
    state: &AppState,
    prepared: &PreparedReply<'_>,
    board: &str,
    thread_id: i64,
    result: std::result::Result<posting::ReplyResponse, ReplyError>,
    started_at: Instant,
) -> Response {
    match result {
        Ok(response) => {
            if let Err(err) = record_accepted_reply(
                &state.store,
                prepared.pending_id,
                &prepared.integration_name,
                board,
                &response,
            ) {
                record_posting_request(
                    &prepared.integration_name,
                    board,
                    "origin_tracking_unavailable",
                    started_at.elapsed(),
                );
                warn!(error = %err, integration = %prepared.integration_name, board, thread_id, post_id = response.post_id, "integration reply accepted but origin tracking failed");
                return posting_error_response(
                    StatusCode::BAD_GATEWAY,
                    PostingErrorBody::new(
                        "origin_tracking_unavailable",
                        "ptchan accepted the reply but the gateway could not record origin; check the thread before retrying",
                    )
                    .retryable(false),
                );
            }
            record_posting_request(
                &prepared.integration_name,
                board,
                "success",
                started_at.elapsed(),
            );
            debug!(
                integration = %prepared.integration_name,
                board,
                thread_id,
                post_id = response.post_id,
                "integration reply accepted"
            );
            Json(response).into_response()
        }
        Err(ReplyError::InvalidRequest(err)) => {
            invalid_reply_response(state, prepared, board, thread_id, err, started_at)
        }
        Err(ReplyError::Upstream(err)) => {
            upstream_reply_response(state, prepared, board, thread_id, err, started_at)
        }
        Err(ReplyError::AcceptedUnknown(err)) => {
            reply_state_unknown_response(prepared, board, thread_id, &err, started_at, true)
        }
        Err(ReplyError::Request(err)) => {
            reply_state_unknown_response(prepared, board, thread_id, &err, started_at, false)
        }
    }
}

fn invalid_reply_response(
    state: &AppState,
    prepared: &PreparedReply<'_>,
    board: &str,
    thread_id: i64,
    err: posting::ReplyValidationError,
    started_at: Instant,
) -> Response {
    record_posting_request(
        &prepared.integration_name,
        board,
        err.code(),
        started_at.elapsed(),
    );
    delete_pending_reply(
        &state.store,
        prepared.pending_id,
        &prepared.integration_name,
        board,
        thread_id,
        "invalid",
    );
    posting_error_response(
        StatusCode::BAD_REQUEST,
        PostingErrorBody::new(err.code(), err.to_string()).retryable(false),
    )
}

fn upstream_reply_response(
    state: &AppState,
    prepared: &PreparedReply<'_>,
    board: &str,
    thread_id: i64,
    err: posting::UpstreamReplyError,
    started_at: Instant,
) -> Response {
    record_posting_request(
        &prepared.integration_name,
        board,
        err.code.as_str(),
        started_at.elapsed(),
    );
    delete_pending_reply(
        &state.store,
        prepared.pending_id,
        &prepared.integration_name,
        board,
        thread_id,
        "rejected",
    );
    posting_error_response(
        posting_status_for_upstream(err.status),
        PostingErrorBody::new(err.code.as_str(), err.message)
            .retryable(err.code.retryable())
            .upstream_status(err.status.as_u16()),
    )
}

fn reply_state_unknown_response(
    prepared: &PreparedReply<'_>,
    board: &str,
    thread_id: i64,
    err: &anyhow::Error,
    started_at: Instant,
    accepted: bool,
) -> Response {
    record_posting_request(
        &prepared.integration_name,
        board,
        "reply_state_unknown",
        started_at.elapsed(),
    );
    if accepted {
        warn!(error = %err, integration = %prepared.integration_name, board, thread_id, "integration reply accepted but response could not be decoded");
        return posting_error_response(
            StatusCode::BAD_GATEWAY,
            PostingErrorBody::new(
                "reply_state_unknown",
                "ptchan may have accepted the reply; check the thread before retrying",
            )
            .retryable(false),
        );
    }
    warn!(error = %err, integration = %prepared.integration_name, board, thread_id, "integration reply result is unknown");
    posting_error_response(
        StatusCode::BAD_GATEWAY,
        PostingErrorBody::new(
            "reply_state_unknown",
            "ptchan reply result is unknown; check the thread before retrying",
        )
        .retryable(false),
    )
}

struct ReplyInput<'a> {
    headers: &'a HeaderMap,
    method: &'a Method,
    uri: &'a Uri,
    board: &'a str,
    thread_id: i64,
    body: &'a [u8],
    started_at: Instant,
}

struct PreparedReply<'a> {
    posting: &'a PostingConfig,
    integration_name: String,
    request: ReplyRequest,
    pending_id: i64,
}

fn prepare_reply<'a>(
    state: &'a AppState,
    input: &ReplyInput<'_>,
) -> std::result::Result<PreparedReply<'a>, Box<Response>> {
    let Some(posting) =
        authenticate_posting(state, input.headers, input.method, input.uri, input.body)
    else {
        record_posting_request(
            "unknown",
            input.board,
            "unauthorized",
            input.started_at.elapsed(),
        );
        return Err(Box::new(StatusCode::UNAUTHORIZED.into_response()));
    };
    let integration_name = posting.name.clone();
    if !config::board_allowed(&posting.allowed_boards, input.board) {
        record_posting_request(
            &integration_name,
            input.board,
            "board_not_allowed",
            input.started_at.elapsed(),
        );
        return Err(Box::new(posting_error_response(
            StatusCode::FORBIDDEN,
            PostingErrorBody::new(
                "board_not_allowed",
                "integration is not allowed to post on this board",
            )
            .retryable(false),
        )));
    }
    if let Err(rejection) = state.rate_limiters.check_posting(&integration_name) {
        record_posting_request(
            &integration_name,
            input.board,
            "rate_limited",
            input.started_at.elapsed(),
        );
        record_gateway_rate_limited_request(
            &integration_name,
            input.board,
            "posting",
            rejection.as_str(),
        );
        return Err(Box::new(gateway_rate_limited_response()));
    }
    let request = serde_json::from_slice::<ReplyRequest>(input.body).map_err(|_| {
        record_posting_request(
            &integration_name,
            input.board,
            "invalid_json",
            input.started_at.elapsed(),
        );
        Box::new(posting_error_response(
            StatusCode::BAD_REQUEST,
            PostingErrorBody::new("invalid_json", "request body must be valid JSON")
                .retryable(false),
        ))
    })?;
    if let Err(err) = posting::validate_reply(input.board, input.thread_id, &request.message) {
        record_posting_request(
            &integration_name,
            input.board,
            err.code(),
            input.started_at.elapsed(),
        );
        return Err(Box::new(posting_error_response(
            StatusCode::BAD_REQUEST,
            PostingErrorBody::new(err.code(), err.to_string()).retryable(false),
        )));
    }
    let pending_id = state
        .store
        .record_pending_produced_post(
            input.board,
            input.thread_id,
            &integration_name,
            &request.message,
            Utc::now(),
        )
        .map_err(|err| {
            record_posting_request(
                &integration_name,
                input.board,
                "origin_tracking_unavailable",
                input.started_at.elapsed(),
            );
            warn!(error = %err, integration = %integration_name, board = input.board, thread_id = input.thread_id, "failed to record pending produced post");
            Box::new(posting_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                PostingErrorBody::new(
                    "origin_tracking_unavailable",
                    "gateway could not prepare origin tracking for this reply",
                )
                .retryable(true),
            ))
        })?;
    Ok(PreparedReply {
        posting,
        integration_name,
        request,
        pending_id,
    })
}

fn record_accepted_reply(
    store: &Store,
    pending_id: i64,
    integration: &str,
    board: &str,
    response: &posting::ReplyResponse,
) -> Result<()> {
    store.record_produced_post(
        &response.board,
        response.thread_id,
        response.post_id,
        integration,
        Utc::now(),
    )?;
    if let Err(err) = store.delete_pending_produced_post(pending_id) {
        warn!(error = %err, integration, board, thread_id = response.thread_id, post_id = response.post_id, "failed to delete pending produced post");
    }
    Ok(())
}

fn delete_pending_reply(
    store: &Store,
    pending_id: i64,
    integration: &str,
    board: &str,
    thread_id: i64,
    reason: &str,
) {
    if let Err(err) = store.delete_pending_produced_post(pending_id) {
        warn!(error = %err, integration, board, thread_id, reason, "failed to delete pending produced post");
    }
}

#[derive(Serialize)]
struct PostingErrorEnvelope {
    error: PostingErrorBody,
}

#[derive(Serialize)]
struct PostingErrorBody {
    code: String,
    message: String,
    retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    upstream_status: Option<u16>,
}

impl PostingErrorBody {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
            upstream_status: None,
        }
    }

    const fn retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    const fn upstream_status(mut self, upstream_status: u16) -> Self {
        self.upstream_status = Some(upstream_status);
        self
    }
}

fn posting_error_response(status: StatusCode, error: PostingErrorBody) -> axum::response::Response {
    (status, Json(PostingErrorEnvelope { error })).into_response()
}

fn posting_status_for_upstream(status: StatusCode) -> StatusCode {
    if status.is_client_error() {
        status
    } else {
        StatusCode::BAD_GATEWAY
    }
}

fn gateway_rate_limited_response() -> Response {
    posting_error_response(
        StatusCode::TOO_MANY_REQUESTS,
        PostingErrorBody::new("rate_limited", "gateway rate limit exceeded").retryable(true),
    )
}

fn record_reading_request(integration: &str, board: &str, result: &str, elapsed: Duration) {
    let board = metric_board(board);
    metrics::READING_REQUESTS
        .with_label_values(&[integration, board, result])
        .inc();
    metrics::READING_REQUEST_SECONDS
        .with_label_values(&[integration, board, result])
        .observe(elapsed.as_secs_f64());
}

fn record_posting_request(integration: &str, board: &str, result: &str, elapsed: Duration) {
    let board = metric_board(board);
    metrics::POSTING_REQUESTS
        .with_label_values(&[integration, board, result])
        .inc();
    metrics::POSTING_REQUEST_SECONDS
        .with_label_values(&[integration, board, result])
        .observe(elapsed.as_secs_f64());
}

fn record_gateway_rate_limited_request(
    integration: &str,
    board: &str,
    capability: &str,
    scope: &str,
) {
    let board = metric_board(board);
    metrics::GATEWAY_RATE_LIMITED_REQUESTS
        .with_label_values(&[integration, board, capability, scope])
        .inc();
}

fn metric_board(board: &str) -> &str {
    if config::valid_board_name(board) {
        board
    } else {
        "invalid"
    }
}

fn authenticate_integration<'a>(
    state: &'a AppState,
    headers: &HeaderMap,
    method: &Method,
    uri: &Uri,
) -> Option<&'a IntegrationConfig> {
    let name = header(headers, "x-ptchan-integration")?;
    let integration = state.integrations.get(name)?;
    verify_request_headers(&integration.secret, headers, method, uri, None).ok()?;
    Some(integration)
}

fn authenticate_posting<'a>(
    state: &'a AppState,
    headers: &HeaderMap,
    method: &Method,
    uri: &Uri,
    body: &[u8],
) -> Option<&'a PostingConfig> {
    let name = header(headers, "x-ptchan-integration")?;
    let posting = state.postings.get(name)?;
    verify_request_headers(&posting.secret, headers, method, uri, Some(body)).ok()?;
    Some(posting)
}

fn verify_request_headers(
    secret: &str,
    headers: &HeaderMap,
    method: &Method,
    uri: &Uri,
    body: Option<&[u8]>,
) -> Result<()> {
    let timestamp = header(headers, "x-ptchan-timestamp").context("missing x-ptchan-timestamp")?;
    let parsed_timestamp = DateTime::parse_from_rfc3339(timestamp)
        .context("x-ptchan-timestamp must be RFC3339")?
        .with_timezone(&Utc);
    let skew = (Utc::now() - parsed_timestamp).num_seconds().abs();
    if skew > REQUEST_MAX_SKEW_SECONDS {
        anyhow::bail!("x-ptchan-timestamp is outside allowed skew");
    }
    let signature = header(headers, "x-ptchan-signature").context("missing x-ptchan-signature")?;
    verify_request_signature(secret, timestamp, method, uri, body, signature)
}

fn verify_request_signature(
    secret: &str,
    timestamp: &str,
    method: &Method,
    uri: &Uri,
    body: Option<&[u8]>,
    signature: &str,
) -> Result<()> {
    let provided = signature
        .strip_prefix("hmac-sha256=")
        .ok_or_else(|| anyhow!("x-ptchan-signature must use hmac-sha256"))?;
    let provided = hex::decode(provided).context("x-ptchan-signature is not hex")?;
    let target = uri
        .path_and_query()
        .map_or_else(|| uri.path(), axum::http::uri::PathAndQuery::as_str);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).context("create hmac")?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(method.as_str().as_bytes());
    mac.update(b".");
    mac.update(target.as_bytes());
    if let Some(body) = body {
        mac.update(b".");
        mac.update(body);
    }
    mac.verify_slice(&provided)
        .context("x-ptchan-signature mismatch")
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
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

    use axum::body::to_bytes;
    use hmac::Mac;
    use serde_json::Value;

    use super::{
        router, verify_request_signature, AppState, HmacSha256, Method, Status, StatusCode, Uri,
    };
    use crate::{
        config::{IntegrationConfig, PtchanConfig, ReadingCapabilityConfig},
        metrics,
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

    #[test]
    fn runtime_http_routes_build() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(&dir.path().join("test.db")).unwrap());
        store.migrate().unwrap();
        let status = Arc::new(Status::default());
        let cfg = PtchanConfig {
            base_url: "https://ptchan.test".to_string(),
            user_agent: "ptchan-gateway-test".to_string(),
            session_refresh_fallback_interval: Duration::from_mins(1),
            socket_reconnect_min: Duration::from_secs(1),
            socket_reconnect_max: Duration::from_secs(2),
        };
        let thread_reader = ThreadReader::new(&cfg).unwrap();
        let post_writer = PostWriter::new(&cfg).unwrap();

        let _app = router(AppState {
            status,
            store,
            thread_reader,
            post_writer,
            integrations: Arc::new(HashMap::new()),
            postings: Arc::new(HashMap::new()),
            rate_limiters: RateLimiters::new(
                &[],
                &crate::config::RuntimeRateLimitConfig::default(),
            )
            .unwrap(),
        });
    }

    #[test]
    fn posting_signature_covers_body() {
        let body = br#"{"message":"hello"}"#;
        let timestamp = "2026-07-19T12:00:00Z";
        let method = Method::POST;
        let uri = "/integration/v1/threads/i/100/replies"
            .parse::<Uri>()
            .unwrap();
        let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(method.as_str().as_bytes());
        mac.update(b".");
        mac.update(uri.path().as_bytes());
        mac.update(b".");
        mac.update(body);
        let signature = format!("hmac-sha256={}", hex::encode(mac.finalize().into_bytes()));

        verify_request_signature("secret", timestamp, &method, &uri, Some(body), &signature)
            .unwrap();
        assert!(verify_request_signature(
            "secret",
            timestamp,
            &method,
            &uri,
            Some(br#"{"message":"bye"}"#),
            &signature
        )
        .is_err());
    }

    #[tokio::test]
    async fn gateway_rate_limit_response_is_structured_for_integrations() {
        let response = super::gateway_rate_limited_response();

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

        super::record_gateway_rate_limited_request(
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

    #[test]
    fn integration_reading_capability_is_explicit() {
        let integration = IntegrationConfig {
            name: "assistant".to_string(),
            allowed_boards: vec!["test".to_string()],
            reading: Some(ReadingCapabilityConfig {}),
            webhook: None,
            posting: None,
            rate_limit: crate::config::RateLimitConfig::default(),
            secret: "secret".to_string(),
        };

        assert!(integration.reading_enabled());
        assert!(integration.board_allowed("test"));
        assert!(!integration.board_allowed("other"));
    }
}

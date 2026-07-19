use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tracing::{debug, warn};

use crate::{
    config::WebhookConfig,
    context::{ThreadReader, DEFAULT_THREAD_LIMIT},
    metrics,
    store::Store,
};

type HmacSha256 = Hmac<Sha256>;
const REQUEST_MAX_SKEW_SECONDS: i64 = 5 * 60;

#[derive(Default)]
pub struct Status {
    upstream_joined: std::sync::atomic::AtomicBool,
    auth_healthy: std::sync::atomic::AtomicBool,
}

impl Status {
    pub fn set_upstream_joined(&self, joined: bool) {
        self.upstream_joined
            .store(joined, std::sync::atomic::Ordering::Relaxed);
        metrics::SOCKET_JOINED.set(i64::from(joined));
    }

    pub fn set_auth_healthy(&self, healthy: bool) {
        self.auth_healthy
            .store(healthy, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn auth_healthy(&self) -> bool {
        self.auth_healthy.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn upstream_joined(&self) -> bool {
        self.upstream_joined
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn ready(&self) -> bool {
        self.auth_healthy() && self.upstream_joined()
    }
}

#[derive(Clone)]
struct AppState {
    status: Arc<Status>,
    store: Arc<Store>,
    thread_reader: ThreadReader,
    consumers: Arc<HashMap<String, WebhookConfig>>,
}

pub async fn spawn_http(
    addr: String,
    status: Arc<Status>,
    store: Arc<Store>,
    thread_reader: ThreadReader,
    consumers: Vec<WebhookConfig>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<JoinHandle<Result<()>>> {
    metrics::init();
    let listener = TcpListener::bind(normalize_addr(&addr)?)
        .await
        .context("bind runtime http")?;
    let local_addr = listener.local_addr().context("runtime local addr")?;
    tracing::info!(address = %local_addr, "runtime http listening");
    let app = router(AppState {
        status,
        store,
        thread_reader,
        consumers: Arc::new(
            consumers
                .into_iter()
                .map(|consumer| (consumer.name.clone(), consumer))
                .collect(),
        ),
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

fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .route(
            "/consumer/v1/threads/:board/:thread_id",
            get(consumer_thread),
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
    let upstream_ready = state.status.ready();
    let storage_ready = state.store.is_ready();
    debug!(
        auth_healthy,
        upstream_joined, storage_ready, "readiness requested"
    );
    if upstream_ready && storage_ready {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

async fn metrics_handler() -> impl IntoResponse {
    debug!("metrics requested");
    (StatusCode::OK, metrics::render())
}

#[derive(Deserialize)]
struct ThreadQuery {
    limit: Option<usize>,
}

async fn consumer_thread(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path((board, thread_id)): Path<(String, i64)>,
    Query(query): Query<ThreadQuery>,
) -> impl IntoResponse {
    let Some(consumer) = authenticate_consumer(&state, &headers, &method, &uri) else {
        metrics::CONTEXT_REQUESTS
            .with_label_values(&["unauthorized"])
            .inc();
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let consumer_name = consumer.name.clone();
    let board_allowed = consumer.allowed_boards.is_empty()
        || consumer
            .allowed_boards
            .iter()
            .any(|allowed_board| allowed_board == &board);
    if !board_allowed {
        metrics::CONTEXT_REQUESTS
            .with_label_values(&["forbidden"])
            .inc();
        return StatusCode::FORBIDDEN.into_response();
    }

    let limit = query.limit.unwrap_or(DEFAULT_THREAD_LIMIT);
    match state
        .thread_reader
        .fetch_thread(&board, thread_id, limit)
        .await
    {
        Ok(Some(thread)) => {
            metrics::CONTEXT_REQUESTS
                .with_label_values(&["success"])
                .inc();
            Json(thread).into_response()
        }
        Ok(None) => {
            metrics::CONTEXT_REQUESTS
                .with_label_values(&["not_found"])
                .inc();
            StatusCode::NOT_FOUND.into_response()
        }
        Err(err) => {
            metrics::CONTEXT_REQUESTS
                .with_label_values(&["failure"])
                .inc();
            warn!(error = %err, consumer = %consumer_name, board, thread_id, "consumer thread context failed");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

fn authenticate_consumer<'a>(
    state: &'a AppState,
    headers: &HeaderMap,
    method: &Method,
    uri: &Uri,
) -> Option<&'a WebhookConfig> {
    let name = header(headers, "x-ptchan-consumer")?;
    let consumer = state.consumers.get(name)?;
    let timestamp = header(headers, "x-ptchan-timestamp")?;
    let timestamp = DateTime::parse_from_rfc3339(timestamp)
        .ok()?
        .with_timezone(&Utc);
    let skew = (Utc::now() - timestamp).num_seconds().abs();
    if skew > REQUEST_MAX_SKEW_SECONDS {
        return None;
    }
    let signature = header(headers, "x-ptchan-signature")?;
    verify_signature(
        &consumer.secret,
        header(headers, "x-ptchan-timestamp")?,
        method,
        uri,
        signature,
    )
    .ok()?;
    Some(consumer)
}

fn verify_signature(
    secret: &str,
    timestamp: &str,
    method: &Method,
    uri: &Uri,
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
    mac.verify_slice(&provided)
        .context("x-ptchan-signature mismatch")
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

pub async fn check_health(addr: &str) -> Result<()> {
    let url = format!("http://{}/healthz", normalize_addr(addr)?);
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

fn normalize_addr(addr: &str) -> Result<SocketAddr> {
    let normalized = if let Some(port) = addr.strip_prefix(':') {
        format!("0.0.0.0:{port}")
    } else {
        addr.to_string()
    };
    normalized
        .parse()
        .with_context(|| format!("parse address {addr}"))
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use std::collections::HashMap;

    use super::{router, AppState, Status};
    use crate::{
        config::PtchanConfig, context::ThreadReader, session::SessionCookie, store::Store,
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
    fn runtime_http_routes_build() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(&dir.path().join("test.db")).unwrap());
        store.migrate().unwrap();
        let status = Arc::new(Status::default());
        let cookie = Arc::new(SessionCookie::new("session=s%3Aabc"));
        let cfg = PtchanConfig {
            base_url: "https://ptchan.test".to_string(),
            user_agent: "ptchan-gateway-test".to_string(),
            session_refresh_fallback_interval: Duration::from_secs(60),
            socket_reconnect_min: Duration::from_secs(1),
            socket_reconnect_max: Duration::from_secs(2),
        };
        let thread_reader = ThreadReader::new(&cfg, cookie).unwrap();

        let _app = router(AppState {
            status,
            store,
            thread_reader,
            consumers: Arc::new(HashMap::new()),
        });
    }
}

use std::{env, net::SocketAddr};

use anyhow::{anyhow, Context, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderName, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
use tracing::{debug, info, warn};

type HmacSha256 = Hmac<Sha256>;
const DEFAULT_CONTEXT_LIMIT: usize = 50;

#[derive(Clone)]
struct AppState {
    webhook_secret: String,
    log_body: bool,
    context: Option<ContextClient>,
}

#[derive(Clone)]
struct ContextClient {
    base_url: String,
    consumer: String,
    secret: String,
    limit: usize,
    client: Client,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG").unwrap_or_else(|_| "webhook_consumer=info,tower_http=warn".into()),
        )
        .init();

    let addr = env::var("CONSUMER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8081".into())
        .parse::<SocketAddr>()
        .context("parse CONSUMER_ADDR")?;
    let webhook_secret =
        env::var("PTCHAN_CONSUMER_SECRET").context("PTCHAN_CONSUMER_SECRET is unset")?;
    let log_body = env_flag("CONSUMER_LOG_BODY");
    let context = context_client(&webhook_secret);
    let context_enabled = context.is_some();

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/internal/ptchan/events", post(handle_event))
        .with_state(AppState {
            webhook_secret,
            log_body,
            context,
        });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, log_body, context_enabled, "example consumer listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    debug!(headers = %safe_header_summary(&headers), body_bytes = body.len(), "webhook request received");
    match verify_request(&state.webhook_secret, &headers, &body) {
        Ok(VerifiedRequest {
            event_id,
            timestamp,
        }) => match serde_json::from_slice::<Value>(&body) {
            Ok(event) => {
                let kind = event
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let board = event
                    .pointer("/post/board")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let post_id = event
                    .pointer("/post/post_id")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                let attachment_count = event
                    .pointer("/post/attachment_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let message_bytes = event
                    .pointer("/post/message")
                    .and_then(Value::as_str)
                    .map_or(0, str::len);
                let donor = event.pointer("/post/donor").and_then(Value::as_bool);
                let has_poster_fingerprint = event.pointer("/post/poster_fingerprint").is_some();
                info!(
                    %event_id,
                    %timestamp,
                    kind,
                    board,
                    post_id,
                    attachment_count,
                    message_bytes,
                    ?donor,
                    has_poster_fingerprint,
                    "accepted ptchan event"
                );
                if state.log_body {
                    debug!(body = %String::from_utf8_lossy(&body), "accepted sanitized body");
                }
                if let Some(context) = &state.context {
                    if let Some(thread_id) =
                        event.pointer("/post/thread_id").and_then(Value::as_i64)
                    {
                        if let Err(err) = context.fetch_thread(board, thread_id).await {
                            warn!(error = %err, board, thread_id, "thread context fetch failed");
                        }
                    }
                }
                StatusCode::NO_CONTENT
            }
            Err(err) => {
                warn!(error = %err, "invalid event json");
                StatusCode::BAD_REQUEST
            }
        },
        Err(err) => {
            warn!(error = %err, "rejected webhook request");
            StatusCode::UNAUTHORIZED
        }
    }
}

impl ContextClient {
    async fn fetch_thread(&self, board: &str, thread_id: i64) -> Result<()> {
        let path = format!(
            "/consumer/v1/threads/{board}/{thread_id}?limit={}",
            self.limit
        );
        let timestamp = Utc::now().to_rfc3339();
        let signature = context_signature(&self.secret, &timestamp, "GET", &path)?;
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .get(url)
            .header("x-ptchan-consumer", &self.consumer)
            .header("x-ptchan-timestamp", &timestamp)
            .header("x-ptchan-signature", signature)
            .send()
            .await
            .context("send context request")?;
        let status = response.status();
        if !status.is_success() {
            return Err(anyhow!("context status {status}"));
        }
        let body = response.text().await.context("read context response")?;
        let context = serde_json::from_str::<Value>(&body).context("decode context response")?;
        let posts = context
            .pointer("/posts")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let truncated = context
            .pointer("/truncated")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        info!(
            board,
            thread_id, posts, truncated, "thread context accepted"
        );
        debug!(body_bytes = body.len(), "thread context response received");
        Ok(())
    }
}

struct VerifiedRequest {
    event_id: String,
    timestamp: DateTime<Utc>,
}

fn verify_request(secret: &str, headers: &HeaderMap, body: &[u8]) -> Result<VerifiedRequest> {
    let event_id = required_header(headers, "x-ptchan-event-id")?.to_string();
    let timestamp = required_header(headers, "x-ptchan-timestamp")?;
    let signature = required_header(headers, "x-ptchan-signature")?;
    let parsed_timestamp = DateTime::parse_from_rfc3339(timestamp)
        .context("x-ptchan-timestamp must be RFC3339")?
        .with_timezone(&Utc);

    let provided = signature
        .strip_prefix("hmac-sha256=")
        .ok_or_else(|| anyhow!("x-ptchan-signature must use hmac-sha256"))?;
    let provided = hex::decode(provided).context("x-ptchan-signature is not hex")?;

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).context("create hmac")?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    mac.verify_slice(&provided)
        .context("x-ptchan-signature mismatch")?;

    Ok(VerifiedRequest {
        event_id,
        timestamp: parsed_timestamp,
    })
}

fn context_signature(secret: &str, timestamp: &str, method: &str, path: &str) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).context("create hmac")?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(method.as_bytes());
    mac.update(b".");
    mac.update(path.as_bytes());
    Ok(format!(
        "hmac-sha256={}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

fn required_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str> {
    headers
        .get(name)
        .ok_or_else(|| anyhow!("missing {name}"))?
        .to_str()
        .with_context(|| format!("{name} is not valid header text"))
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn context_client(webhook_secret: &str) -> Option<ContextClient> {
    let base_url = env::var("PTCHAN_GATEWAY_URL").ok()?;
    let base_url = base_url.trim().trim_end_matches('/').to_string();
    if base_url.is_empty() {
        return None;
    }
    Some(ContextClient {
        base_url,
        consumer: env::var("PTCHAN_CONSUMER_NAME").unwrap_or_else(|_| "example".to_string()),
        secret: webhook_secret.to_string(),
        limit: env_usize("PTCHAN_CONTEXT_LIMIT", DEFAULT_CONTEXT_LIMIT),
        client: Client::new(),
    })
}

fn safe_header_summary(headers: &HeaderMap) -> String {
    let mut names = headers
        .keys()
        .map(HeaderName::as_str)
        .filter(|name| {
            !matches!(
                *name,
                "cookie" | "set-cookie" | "authorization" | "x-ptchan-signature"
            )
        })
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.join(",")
}

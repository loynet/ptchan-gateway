use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::Url;
use rust_socketio::{ClientBuilder, Event, Payload, RawClient, TransportType};
use serde_json::{json, Value};
use tokio::{
    sync::{watch, Notify},
    time,
};
use tracing::{debug, error, info, warn};

use crate::{
    config::{PtchanConfig, WebhookConfig},
    event, metrics,
    runtime::Status,
    session::SessionCookie,
    store::{EventDelivery, Store},
};

const ROOM: &str = "globalmanage-recent-hashed";

#[derive(Clone)]
pub(crate) struct Supervisor {
    pub(crate) cfg: PtchanConfig,
    pub(crate) cookie: Arc<SessionCookie>,
    pub(crate) store: Arc<Store>,
    pub(crate) webhooks: Vec<WebhookConfig>,
    pub(crate) fingerprint_secret: Option<String>,
    pub(crate) delivery_wakeup: Arc<Notify>,
    pub(crate) status: Arc<Status>,
}

pub(crate) async fn supervise(supervisor: Supervisor, mut shutdown: watch::Receiver<bool>) {
    let mut delay = supervisor.cfg.socket_reconnect_min;
    loop {
        if *shutdown.borrow() {
            supervisor.status.set_upstream_joined(false);
            return;
        }
        if !supervisor.status.auth_healthy() {
            supervisor.status.set_upstream_joined(false);
            tokio::select! {
                _ = shutdown.changed() => {}
                () = time::sleep(supervisor.cfg.socket_reconnect_min) => {}
            }
            continue;
        }
        metrics::SOCKET_RECONNECTS.inc();
        let stop_socket = Arc::new(AtomicBool::new(false));
        let once_supervisor = supervisor.clone();
        let once_stop = stop_socket.clone();
        debug!(delay = ?delay, room = ROOM, "starting socket connection attempt");
        let mut handle =
            tokio::task::spawn_blocking(move || run_socket_once(once_supervisor, once_stop));

        let result = tokio::select! {
            result = &mut handle => result,
            _ = shutdown.changed() => {
                stop_socket.store(true, Ordering::Relaxed);
                supervisor.status.set_upstream_joined(false);
                if let Err(err) = handle.await {
                    error!(error = %err, "socket task failed during shutdown");
                }
                return;
            }
        };

        supervisor.status.set_upstream_joined(false);
        match result {
            Ok(Ok(joined)) => {
                if joined {
                    delay = supervisor.cfg.socket_reconnect_min;
                } else {
                    metrics::SOCKET_JOIN_FAILURES.inc();
                    warn!(room = ROOM, "socket connection ended before room join");
                }
                info!("socket connection ended");
            }
            Ok(Err(err)) => warn!(error = %err, "socket connection failed"),
            Err(err) => error!(error = %err, "socket task panicked"),
        }

        tokio::select! {
            _ = shutdown.changed() => {}
            () = time::sleep(delay) => {}
        }
        delay = std::cmp::min(delay.saturating_mul(2), supervisor.cfg.socket_reconnect_max);
    }
}

#[allow(clippy::needless_pass_by_value)]
fn run_socket_once(supervisor: Supervisor, stop: Arc<AtomicBool>) -> Result<bool> {
    let closed = Arc::new(AtomicBool::new(false));
    let joined = Arc::new(AtomicBool::new(false));
    let close_flag = closed.clone();
    let joined_flag = joined.clone();
    let joined_status = supervisor.status.clone();
    let base_url = supervisor.cfg.base_url.clone();
    let origin = socket_origin(&supervisor.cfg.base_url)?;
    let event_store = supervisor.store.clone();
    let event_webhooks = supervisor.webhooks;
    let event_fingerprint_secret = supervisor.fingerprint_secret;
    let event_wakeup = supervisor.delivery_wakeup;

    debug!(room = ROOM, base_url = %supervisor.cfg.base_url, "connecting socket");
    let client = ClientBuilder::new(supervisor.cfg.base_url.clone())
        // Let our supervisor reconnect so each attempt gets the latest session cookie.
        .reconnect(false)
        .transport_type(TransportType::Websocket)
        .opening_header("user-agent", supervisor.cfg.user_agent.clone())
        .opening_header("origin", origin)
        .opening_header("cookie", supervisor.cookie.get())
        .on(Event::Connect, |_payload: Payload, socket: RawClient| {
            if let Err(err) = socket.emit("room", json!(ROOM)) {
                warn!(error = %err, room = ROOM, "socket room join emit failed");
            } else {
                info!(room = ROOM, "socket connected; room join emitted");
            }
        })
        .on("message", move |payload: Payload, _socket: RawClient| {
            if message_is_joined(&payload) {
                joined_flag.store(true, Ordering::Relaxed);
                joined_status.set_upstream_joined(true);
                info!(room = ROOM, "socket room joined");
            }
        })
        .on("newPost", move |payload: Payload, _socket: RawClient| {
            handle_new_post(
                &base_url,
                payload,
                &event_store,
                &event_webhooks,
                event_fingerprint_secret.as_deref(),
                &event_wakeup,
            );
        })
        .on("error", |payload: Payload, _socket: RawClient| {
            warn!(payload = ?safe_payload_debug(&payload), "socket error");
        })
        .on("close", move |_payload: Payload, _socket: RawClient| {
            close_flag.store(true, Ordering::Relaxed);
            info!("socket closed");
        })
        .connect()
        .context("connect socket")?;

    while !closed.load(Ordering::Relaxed)
        && !stop.load(Ordering::Relaxed)
        && supervisor.status.auth_healthy()
    {
        thread::sleep(Duration::from_secs(1));
    }
    if stop.load(Ordering::Relaxed) || !supervisor.status.auth_healthy() {
        let _ = client.disconnect();
    }
    Ok(joined.load(Ordering::Relaxed))
}

fn socket_origin(base_url: &str) -> Result<String> {
    let url = Url::parse(base_url).context("parse ptchan base url for socket origin")?;
    let scheme = url.scheme();
    let host = url
        .host_str()
        .context("ptchan base url must include a host")?;
    let Some(port) = url.port() else {
        return Ok(format!("{scheme}://{host}"));
    };
    Ok(format!("{scheme}://{host}:{port}"))
}

fn handle_new_post(
    base_url: &str,
    payload: Payload,
    store: &Store,
    webhooks: &[WebhookConfig],
    fingerprint_secret: Option<&str>,
    delivery_wakeup: &Notify,
) {
    let Some(value) = payload_first_json(payload) else {
        metrics::SOCKET_EVENTS
            .with_label_values(&["parse_error"])
            .inc();
        warn!("newPost payload did not contain JSON object");
        return;
    };
    debug!(shape = %json_shape(&value), "socket newPost received");
    match event::gateway_event(base_url, value, Utc::now()) {
        Ok(built) => {
            let deliveries = match event_deliveries(&built, webhooks, fingerprint_secret) {
                Ok(deliveries) => deliveries,
                Err(err) => {
                    metrics::SOCKET_EVENTS
                        .with_label_values(&["store_error"])
                        .inc();
                    warn!(error = %err, "failed to prepare webhook deliveries");
                    return;
                }
            };
            if deliveries.is_empty() {
                metrics::SOCKET_EVENTS
                    .with_label_values(&["no_allowed_webhooks"])
                    .inc();
                debug!(
                    event_id = %built.event.event_id,
                    board = %built.event.post.board,
                    "socket event skipped; no webhook allowed for board"
                );
                return;
            }
            match store.create_event(&built.event, &built.payload, &deliveries) {
                Ok(true) => {
                    metrics::SOCKET_EVENTS.with_label_values(&["queued"]).inc();
                    info!(
                        event_id = %built.event.event_id,
                        kind = %built.event.kind,
                        board = %built.event.post.board,
                        thread_id = built.event.post.thread_id,
                        post_id = built.event.post.id,
                        attachment_count = built.event.post.attachment_count,
                        references = built.event.post.references.len(),
                        referenced_by = built.event.post.referenced_by.len(),
                        webhook_count = deliveries.len(),
                        fingerprint_source = built.poster_identity.is_some(),
                        "event queued"
                    );
                    delivery_wakeup.notify_one();
                }
                Ok(false) => {
                    metrics::SOCKET_EVENTS
                        .with_label_values(&["duplicate"])
                        .inc();
                    debug!(event_id = %built.event.event_id, "duplicate socket event ignored");
                }
                Err(err) => {
                    metrics::SOCKET_EVENTS
                        .with_label_values(&["store_error"])
                        .inc();
                    warn!(error = %err, "failed to store socket event");
                }
            }
        }
        Err(err) => {
            metrics::SOCKET_EVENTS
                .with_label_values(&["redacted_or_invalid"])
                .inc();
            warn!(error = %err, "socket event rejected");
        }
    }
}

fn event_deliveries(
    built: &event::BuiltEvent,
    webhooks: &[WebhookConfig],
    fingerprint_secret: Option<&str>,
) -> Result<Vec<EventDelivery>> {
    webhooks
        .iter()
        .filter(|webhook| webhook_allowed_for_board(webhook, &built.event.post.board))
        .map(|webhook| {
            let mut event = built.event.clone();
            if webhook.include_poster_fingerprint {
                let secret =
                    fingerprint_secret.context("poster fingerprint secret is not loaded")?;
                event.post.poster_fingerprint = event::poster_fingerprint(
                    secret,
                    &webhook.name,
                    built.poster_identity.as_deref(),
                )?;
            }
            Ok(EventDelivery {
                webhook: webhook.name.clone(),
                payload: event::encode_event(&event)?,
            })
        })
        .collect()
}

fn webhook_allowed_for_board(webhook: &WebhookConfig, board: &str) -> bool {
    webhook.allowed_boards.is_empty()
        || webhook
            .allowed_boards
            .iter()
            .any(|allowed_board| allowed_board == board)
}

fn payload_first_json(payload: Payload) -> Option<Value> {
    match payload {
        Payload::Text(values) => values.into_iter().next(),
        _ => None,
    }
}

fn message_is_joined(payload: &Payload) -> bool {
    match payload {
        Payload::Text(values) => values.iter().any(|v| v.as_str() == Some("joined")),
        _ => false,
    }
}

#[allow(clippy::match_wildcard_for_single_variants)]
fn safe_payload_debug(payload: &Payload) -> String {
    match payload {
        Payload::Text(values) => {
            let Some(value) = values.first() else {
                return "text_values=0".to_string();
            };
            match value {
                Value::String(text) => format!("text={}", capped(text)),
                Value::Number(number) => format!("number={number}"),
                Value::Bool(value) => format!("bool={value}"),
                Value::Null => "null".to_string(),
                Value::Array(_) | Value::Object(_) => format!("text_values={}", values.len()),
            }
        }
        Payload::Binary(bytes) => format!("binary_len={}", bytes.len()),
        _ => "unknown_payload".to_string(),
    }
}

fn capped(value: &str) -> String {
    const MAX: usize = 200;
    if value.len() <= MAX {
        value.to_string()
    } else {
        format!("{}...", value.chars().take(MAX).collect::<String>())
    }
}

fn json_shape(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().map(String::as_str).collect::<Vec<_>>();
            keys.sort_unstable();
            format!("object keys=[{}]", keys.join(","))
        }
        Value::Array(values) => format!("array len={}", values.len()),
        Value::String(_) => "string".to_string(),
        Value::Number(_) => "number".to_string(),
        Value::Bool(_) => "bool".to_string(),
        Value::Null => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::{EventKind, Post, WebhookEvent};

    #[test]
    fn event_deliveries_respects_allowed_boards() {
        let built = built_event("test");
        let webhooks = vec![
            webhook("all", Vec::new()),
            webhook("test-only", vec!["test".to_string()]),
            webhook("other-only", vec!["other".to_string()]),
        ];

        let deliveries = event_deliveries(&built, &webhooks, None).unwrap();

        let names = deliveries
            .iter()
            .map(|delivery| delivery.webhook.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["all", "test-only"]);
    }

    #[test]
    fn event_deliveries_allows_empty_board_lists() {
        let built = built_event("test");
        let webhooks = vec![webhook("all", Vec::new())];

        let deliveries = event_deliveries(&built, &webhooks, None).unwrap();

        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].webhook, "all");
    }

    fn built_event(board: &str) -> event::BuiltEvent {
        let observed_at = Utc::now();
        event::BuiltEvent {
            event: WebhookEvent {
                event_id: format!("ptchan:post.created:{board}:101"),
                kind: EventKind::PostCreated,
                source: "ptchan".to_string(),
                observed_at,
                post: Post {
                    board: board.to_string(),
                    thread_id: 100,
                    id: 101,
                    url: format!("https://ptchan.test/{board}/thread/100.html#101"),
                    date: observed_at,
                    subject: None,
                    message: Some("body".to_string()),
                    name: None,
                    tripcode: None,
                    capcode: None,
                    donor: None,
                    country: None,
                    poster_fingerprint: None,
                    attachment_count: 0,
                    references: Vec::new(),
                    referenced_by: Vec::new(),
                },
            },
            payload: br"{}".to_vec(),
            poster_identity: None,
        }
    }

    fn webhook(name: &str, allowed_boards: Vec<String>) -> WebhookConfig {
        WebhookConfig {
            name: name.to_string(),
            url: "http://127.0.0.1:8081/events".to_string(),
            allowed_boards,
            include_poster_fingerprint: false,
            secret: "secret".to_string(),
            timeout: Duration::from_secs(5),
        }
    }
}

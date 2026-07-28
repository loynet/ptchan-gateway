use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use reqwest::Url;
use serde_json::{json, Value};
use tokio::{
    sync::{watch, Notify},
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        http::header::{COOKIE, ORIGIN, USER_AGENT},
        Message,
    },
};
use tracing::{debug, info, warn};

use crate::{
    config::{PtchanConfig, WebhookConfig},
    event, metrics,
    runtime::Status,
    session::SessionCookie,
    store::{EventDelivery, Store},
};

const ROOM: &str = "globalmanage-recent-hashed";
const PENDING_ORIGIN_WINDOW_SECONDS: i64 = 60;

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
        metrics::SOCKET_CONNECTION_ATTEMPTS.inc();
        let once_supervisor = supervisor.clone();
        debug!(delay = ?delay, room = ROOM, "starting socket connection attempt");

        let result = tokio::select! {
            result = run_socket_once(once_supervisor) => result,
            _ = shutdown.changed() => {
                supervisor.status.set_upstream_joined(false);
                return;
            }
        };

        supervisor.status.set_upstream_joined(false);
        match result {
            Ok(joined) => {
                if joined {
                    delay = supervisor.cfg.socket_reconnect_min;
                } else {
                    metrics::SOCKET_JOIN_FAILURES.inc();
                    warn!(room = ROOM, "socket connection ended before room join");
                }
                info!("socket connection ended");
            }
            Err(err) => warn!(error = %err, "socket connection failed"),
        }

        tokio::select! {
            _ = shutdown.changed() => {}
            () = time::sleep(delay) => {}
        }
        delay = std::cmp::min(delay.saturating_mul(2), supervisor.cfg.socket_reconnect_max);
    }
}

#[allow(clippy::needless_pass_by_value)]
async fn run_socket_once(supervisor: Supervisor) -> Result<bool> {
    let base_url = supervisor.cfg.base_url.clone();
    let origin = socket_origin(&supervisor.cfg.base_url)?;
    let socket_url = socket_url(&supervisor.cfg.base_url)?;
    let mut request = socket_url
        .into_client_request()
        .context("build socket request")?;
    let headers = request.headers_mut();
    headers.insert(
        USER_AGENT,
        supervisor
            .cfg
            .user_agent
            .parse()
            .context("build socket user-agent header")?,
    );
    headers.insert(
        ORIGIN,
        origin.parse().context("build socket origin header")?,
    );
    headers.insert(
        COOKIE,
        supervisor
            .cookie
            .get()
            .parse()
            .context("build socket cookie header")?,
    );

    debug!(room = ROOM, base_url = %supervisor.cfg.base_url, "connecting socket");
    let (mut socket, _response) = connect_async(request).await.context("connect socket")?;
    let _connection_guard = SocketConnectionGuard::new();
    let mut joined = false;

    while supervisor.status.auth_healthy() {
        let message = tokio::select! {
            message = socket.next() => message,
            () = time::sleep(Duration::from_secs(1)) => continue,
        };
        let Some(message) = message else {
            info!("socket closed");
            break;
        };
        let message = message.context("read socket message")?;
        match message {
            Message::Text(text) => {
                match handle_socket_text(&text, &mut socket, &supervisor, &base_url, &mut joined)
                    .await?
                {
                    SocketTextResult::Continue => {}
                    SocketTextResult::RoomJoinEmitted => {
                        info!(room = ROOM, "socket connected; room join emitted");
                    }
                    SocketTextResult::Closed => {
                        info!("socket closed");
                        break;
                    }
                }
            }
            Message::Binary(_) => debug!("socket binary message ignored"),
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .context("send websocket pong")?,
            Message::Close(frame) => {
                if let Some(frame) = frame {
                    info!(
                        close_code = %frame.code,
                        close_reason = %capped(&frame.reason),
                        "socket closed"
                    );
                } else {
                    info!("socket closed");
                }
                break;
            }
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    let _ = socket.close(None).await;
    Ok(joined)
}

enum SocketTextResult {
    Continue,
    RoomJoinEmitted,
    Closed,
}

struct SocketConnectionGuard {
    started_at: Instant,
}

impl SocketConnectionGuard {
    fn new() -> Self {
        metrics::SOCKET_ACTIVE_CONNECTIONS.inc();
        Self {
            started_at: Instant::now(),
        }
    }
}

impl Drop for SocketConnectionGuard {
    fn drop(&mut self) {
        metrics::SOCKET_ACTIVE_CONNECTIONS.dec();
        metrics::SOCKET_CONNECTION_SECONDS.observe(self.started_at.elapsed().as_secs_f64());
    }
}

async fn handle_socket_text<S>(
    text: &str,
    socket: &mut S,
    supervisor: &Supervisor,
    base_url: &str,
    joined: &mut bool,
) -> Result<SocketTextResult>
where
    S: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let Some((engine_packet, rest)) = text.split_at_checked(1) else {
        warn!("socket packet was empty");
        return Ok(SocketTextResult::Continue);
    };
    match engine_packet {
        "0" => {
            socket
                .send(Message::Text("40".into()))
                .await
                .context("send socket namespace connect")?;
        }
        "2" => {
            socket
                .send(Message::Text("3".into()))
                .await
                .context("send engine.io pong")?;
        }
        "4" => return handle_socketio_text(rest, socket, supervisor, base_url, joined).await,
        "1" => {
            info!("socket engine close packet received");
            return Ok(SocketTextResult::Closed);
        }
        _ => {
            debug!(packet = engine_packet, "socket engine packet ignored");
        }
    }
    Ok(SocketTextResult::Continue)
}

async fn handle_socketio_text<S>(
    text: &str,
    socket: &mut S,
    supervisor: &Supervisor,
    base_url: &str,
    joined: &mut bool,
) -> Result<SocketTextResult>
where
    S: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let Some((packet, payload)) = text.split_at_checked(1) else {
        warn!("socket.io packet was empty");
        return Ok(SocketTextResult::Continue);
    };
    match packet {
        "0" => {
            socket
                .send(Message::Text(format!("42{}", json!(["room", ROOM])).into()))
                .await
                .context("send socket room join")?;
            Ok(SocketTextResult::RoomJoinEmitted)
        }
        "1" => {
            info!(
                payload_bytes = payload.len(),
                "socket.io disconnect packet received"
            );
            Ok(SocketTextResult::Closed)
        }
        "2" => {
            handle_socket_event(payload, supervisor, base_url, joined).await?;
            Ok(SocketTextResult::Continue)
        }
        "4" => {
            warn!(payload_bytes = payload.len(), "socket error");
            Ok(SocketTextResult::Continue)
        }
        _ => {
            debug!(packet, "socket.io packet ignored");
            Ok(SocketTextResult::Continue)
        }
    }
}

async fn handle_socket_event(
    payload: &str,
    supervisor: &Supervisor,
    base_url: &str,
    joined: &mut bool,
) -> Result<()> {
    let values = match serde_json::from_str::<Vec<Value>>(payload) {
        Ok(values) => values,
        Err(err) => {
            warn!(error = %err, payload_bytes = payload.len(), "socket event parse failed");
            return Ok(());
        }
    };
    let Some(event) = values.first().and_then(Value::as_str) else {
        warn!(payload = %event_values_debug(&values), "socket event missing name");
        return Ok(());
    };
    let event_payload = &values[1..];
    match event {
        "message" => {
            if message_is_joined(event_payload) {
                *joined = true;
                supervisor.status.set_upstream_joined(true);
                metrics::observe_now(&metrics::SOCKET_LAST_JOIN_TIMESTAMP_SECONDS);
                info!(room = ROOM, "socket room joined");
            }
        }
        "newPost" => {
            handle_new_post(
                base_url.to_string(),
                event_payload.to_vec(),
                supervisor.store.clone(),
                supervisor.webhooks.clone(),
                supervisor.fingerprint_secret.clone(),
                supervisor.delivery_wakeup.clone(),
            )
            .await?;
        }
        _ => debug!(event, payload = %event_values_debug(event_payload), "socket event ignored"),
    }
    Ok(())
}

fn socket_url(base_url: &str) -> Result<String> {
    let mut url = Url::parse(base_url).context("parse ptchan base url for socket")?;
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        other => anyhow::bail!("unsupported ptchan socket scheme {other}"),
    };
    url.set_scheme(scheme)
        .map_err(|()| anyhow::anyhow!("set ptchan socket scheme"))?;
    url.set_path("/socket.io/");
    url.set_query(Some("EIO=4&transport=websocket"));
    Ok(url.to_string())
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

async fn handle_new_post(
    base_url: String,
    payload: Vec<Value>,
    store: Arc<Store>,
    webhooks: Vec<WebhookConfig>,
    fingerprint_secret: Option<String>,
    delivery_wakeup: Arc<Notify>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        handle_new_post_blocking(
            &base_url,
            &payload,
            &store,
            &webhooks,
            fingerprint_secret.as_deref(),
            &delivery_wakeup,
        );
    })
    .await
    .context("socket event processing task panicked")
}

fn handle_new_post_blocking(
    base_url: &str,
    payload: &[Value],
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
        Ok(mut built) => {
            match store.produced_post_origin_or_claim_pending(
                &built.event.post.board,
                built.event.post.thread_id,
                built.event.post.id,
                built.event.post.message.as_deref(),
                Utc::now() - chrono::Duration::seconds(PENDING_ORIGIN_WINDOW_SECONDS),
                Utc::now(),
            ) {
                Ok(origin) => built.event.post.origin = origin,
                Err(err) => {
                    metrics::SOCKET_EVENTS
                        .with_label_values(&["store_error"])
                        .inc();
                    warn!(error = %err, "failed to load produced post origin");
                    return;
                }
            }
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
                    metrics::observe_now(&metrics::SOCKET_LAST_EVENT_TIMESTAMP_SECONDS);
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

fn payload_first_json(payload: &[Value]) -> Option<Value> {
    payload.first()?.as_object()?;
    payload.first().cloned()
}

fn message_is_joined(payload: &[Value]) -> bool {
    payload.iter().any(|v| v.as_str() == Some("joined"))
}

fn event_values_debug(values: &[Value]) -> String {
    let Some(value) = values.first() else {
        return "values=0".to_string();
    };
    match value {
        Value::String(text) => format!("string_len={}", text.len()),
        Value::Number(number) => format!("number={number}"),
        Value::Bool(value) => format!("bool={value}"),
        Value::Null => "null".to_string(),
        Value::Array(_) | Value::Object(_) => format!("values={}", values.len()),
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
    use crate::contract::{EventKind, Post, WebhookEvent};
    use std::time::Duration;

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

    #[test]
    fn socket_url_uses_engineio_websocket_endpoint() {
        assert_eq!(
            socket_url("https://ptchan.test").unwrap(),
            "wss://ptchan.test/socket.io/?EIO=4&transport=websocket"
        );
        assert_eq!(
            socket_url("http://ptchan.test/base").unwrap(),
            "ws://ptchan.test/socket.io/?EIO=4&transport=websocket"
        );
    }

    #[test]
    fn socket_payload_helpers_read_expected_values() {
        let object = json!({"postId": 123});
        let values = vec![object.clone()];

        assert_eq!(payload_first_json(&values), Some(object));
        assert!(message_is_joined(&[json!("ignored"), json!("joined")]));
        assert!(!message_is_joined(&[json!("not joined")]));
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
                    origin: None,
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

use std::{
    cmp,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
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
    config::{self, PtchanConfig, WebhookConfig},
    metrics,
    origin::OriginMatcher,
    runtime::Status,
    session::SessionCookie,
    store::Store,
};

mod ingest;
mod protocol;

const ROOM: &str = "globalmanage-recent-hashed";
const RECONNECT_MIN: Duration = Duration::from_secs(3);
const RECONNECT_MAX: Duration = Duration::from_secs(60);
pub(crate) struct Supervisor {
    pub(crate) cfg: PtchanConfig,
    pub(crate) cookie: Arc<SessionCookie>,
    pub(crate) store: Arc<Store>,
    pub(crate) webhooks: Vec<WebhookConfig>,
    pub(crate) origins: OriginMatcher,
    pub(crate) fingerprint_secret: Option<String>,
    pub(crate) delivery_wakeup: Arc<Notify>,
    pub(crate) status: Arc<Status>,
}

pub(crate) async fn supervise(supervisor: Supervisor, mut shutdown: watch::Receiver<bool>) {
    let mut delay = RECONNECT_MIN;
    loop {
        if *shutdown.borrow() {
            supervisor.status.set_upstream_joined(false);
            return;
        }
        if !supervisor.status.auth_healthy() {
            supervisor.status.set_upstream_joined(false);
            tokio::select! {
                _ = shutdown.changed() => {}
                () = time::sleep(RECONNECT_MIN) => {}
            }
            continue;
        }
        metrics::SOCKET_CONNECTION_ATTEMPTS.inc();
        debug!(delay = ?delay, room = ROOM, "starting socket connection attempt");

        let result = tokio::select! {
            result = run_socket_once(&supervisor) => result,
            _ = shutdown.changed() => {
                supervisor.status.set_upstream_joined(false);
                return;
            }
        };

        supervisor.status.set_upstream_joined(false);
        match result {
            Ok(joined) => {
                if joined {
                    delay = RECONNECT_MIN;
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
        delay = cmp::min(delay.saturating_mul(2), RECONNECT_MAX);
    }
}

async fn run_socket_once(supervisor: &Supervisor) -> Result<bool> {
    let base_url = supervisor.cfg.base_url.clone();
    let origin = socket_origin(&supervisor.cfg.base_url)?;
    let socket_url = socket_url(&supervisor.cfg.base_url)?;
    let mut request = socket_url
        .into_client_request()
        .context("build socket request")?;
    let headers = request.headers_mut();
    headers.insert(
        USER_AGENT,
        config::gateway_user_agent()
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
                match handle_socket_text(&text, &mut socket, supervisor, &base_url, &mut joined)
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
    let packet = match protocol::decode(text) {
        Ok(packet) => packet,
        Err(err) => {
            warn!(error = %err, payload_bytes = text.len(), "socket protocol packet rejected");
            return Ok(SocketTextResult::Continue);
        }
    };
    match packet {
        protocol::Packet::EngineOpen => {
            socket
                .send(Message::Text("40".into()))
                .await
                .context("send socket namespace connect")?;
        }
        protocol::Packet::EnginePing => {
            socket
                .send(Message::Text("3".into()))
                .await
                .context("send engine.io pong")?;
        }
        protocol::Packet::EngineClose => {
            info!("socket engine close packet received");
            return Ok(SocketTextResult::Closed);
        }
        protocol::Packet::SocketConnected => {
            socket
                .send(Message::Text(format!("42{}", json!(["room", ROOM])).into()))
                .await
                .context("send socket room join")?;
            return Ok(SocketTextResult::RoomJoinEmitted);
        }
        protocol::Packet::SocketDisconnected { payload_bytes } => {
            info!(payload_bytes, "socket.io disconnect packet received");
            return Ok(SocketTextResult::Closed);
        }
        protocol::Packet::SocketEvent { name, payload } => {
            handle_socket_event(&name, &payload, supervisor, base_url, joined).await;
        }
        protocol::Packet::SocketError { payload_bytes } => {
            warn!(payload_bytes, "socket error");
        }
        protocol::Packet::Ignored { layer, code } => {
            debug!(layer, code, "socket protocol packet ignored");
        }
    }
    Ok(SocketTextResult::Continue)
}

async fn handle_socket_event(
    event: &str,
    payload: &[Value],
    supervisor: &Supervisor,
    base_url: &str,
    joined: &mut bool,
) {
    match event {
        "message" => {
            if message_is_joined(payload) {
                *joined = true;
                supervisor.status.set_upstream_joined(true);
                metrics::observe_now(&metrics::SOCKET_LAST_JOIN_TIMESTAMP_SECONDS);
                info!(room = ROOM, "socket room joined");
            }
        }
        "newPost" => {
            ingest::new_post(
                base_url,
                payload,
                &supervisor.store,
                &supervisor.webhooks,
                &supervisor.origins,
                supervisor.fingerprint_secret.as_deref(),
                &supervisor.delivery_wakeup,
            )
            .await;
        }
        _ => debug!(event, payload = %event_values_debug(payload), "socket event ignored"),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn recognizes_room_join_message() {
        assert!(message_is_joined(&[json!("ignored"), json!("joined")]));
        assert!(!message_is_joined(&[json!("not joined")]));
    }
}

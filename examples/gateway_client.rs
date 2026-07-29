use std::{env, io, net::SocketAddr};

use anyhow::{anyhow, Context, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Router,
};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::{Client, Response};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use tokio::net::TcpListener;
use tracing::{info, warn};

type HmacSha256 = Hmac<Sha256>;

struct GatewayClient {
    base_url: String,
    integration: String,
    secret: String,
    client: Client,
}

enum Command {
    Health,
    Read {
        board: String,
        thread_id: i64,
        limit: usize,
    },
    Post {
        board: String,
        thread_id: i64,
        message: String,
        sage: bool,
    },
    Listen {
        addr: SocketAddr,
    },
}

#[derive(Clone)]
struct ListenerState {
    secret: String,
}

#[derive(Deserialize)]
struct Webhook {
    schema_version: String,
    event_id: String,
    kind: String,
    post: EventPost,
}

#[derive(Deserialize)]
struct EventPost {
    board: String,
    thread_id: i64,
    post_id: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let command = parse_command()?;
    let client = GatewayClient::from_env(!matches!(command, Command::Health))?;

    match command {
        Command::Health => client.health().await,
        Command::Read {
            board,
            thread_id,
            limit,
        } => client.read_thread(&board, thread_id, limit).await,
        Command::Post {
            board,
            thread_id,
            message,
            sage,
        } => client.post_reply(&board, thread_id, &message, sage).await,
        Command::Listen { addr } => listen(addr, client.secret).await,
    }
}

impl GatewayClient {
    fn from_env(require_secret: bool) -> Result<Self> {
        let integration =
            env::var("PTCHAN_INTEGRATION_NAME").unwrap_or_else(|_| "example".to_string());
        let secret = if require_secret {
            integration_secret(&integration)?
        } else {
            String::new()
        };
        let base_url = env::var("PTCHAN_GATEWAY_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string())
            .trim_end_matches('/')
            .to_string();
        if base_url.is_empty() {
            anyhow::bail!("PTCHAN_GATEWAY_URL must not be empty");
        }
        Ok(Self {
            base_url,
            integration,
            secret,
            client: Client::new(),
        })
    }

    async fn health(&self) -> Result<()> {
        let response = self
            .client
            .get(format!("{}/healthz", self.base_url))
            .send()
            .await
            .context("send health request")?;
        let status = response.status();
        print_json(&json!({ "status": status.as_u16(), "ok": status.is_success() }))?;
        ensure_success(status)
    }

    async fn read_thread(&self, board: &str, thread_id: i64, limit: usize) -> Result<()> {
        let path = format!("/integration/v1/threads/{board}/{thread_id}?limit={limit}");
        let response = self.signed_request("GET", &path, None)?.send().await?;
        print_response(response).await
    }

    async fn post_reply(
        &self,
        board: &str,
        thread_id: i64,
        message: &str,
        sage: bool,
    ) -> Result<()> {
        let path = format!("/integration/v1/threads/{board}/{thread_id}/replies");
        let body = serde_json::to_vec(&json!({ "message": message, "sage": sage }))
            .context("encode reply request")?;
        let response = self
            .signed_request("POST", &path, Some(&body))?
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;
        print_response(response).await
    }

    fn signed_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<reqwest::RequestBuilder> {
        let timestamp = Utc::now().to_rfc3339();
        let signature = signature(&self.secret, &timestamp, method, path, body)?;
        let request = match method {
            "GET" => self.client.get(format!("{}{}", self.base_url, path)),
            "POST" => self.client.post(format!("{}{}", self.base_url, path)),
            _ => anyhow::bail!("unsupported method {method}"),
        };
        Ok(request
            .header("x-ptchan-integration", &self.integration)
            .header("x-ptchan-timestamp", timestamp)
            .header("x-ptchan-signature", signature))
    }
}

async fn listen(addr: SocketAddr, secret: String) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "gateway_client=info".to_string()))
        .init();
    let app = Router::new()
        .route("/internal/ptchan/events", post(receive_event))
        .with_state(ListenerState { secret });
    let listener = TcpListener::bind(addr)
        .await
        .context("bind webhook listener")?;
    info!(%addr, "webhook listener started");
    axum::serve(listener, app).await.context("serve webhooks")
}

async fn receive_event(
    State(state): State<ListenerState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    match verify_event(&state.secret, &headers, &body) {
        Ok(event) => {
            info!(
                event_id = event.event_id,
                kind = event.kind,
                board = event.post.board,
                thread_id = event.post.thread_id,
                post_id = event.post.post_id,
                "webhook accepted"
            );
            StatusCode::NO_CONTENT
        }
        Err(err) => {
            warn!(error = %err, "webhook rejected");
            StatusCode::UNAUTHORIZED
        }
    }
}

fn verify_event(secret: &str, headers: &HeaderMap, body: &[u8]) -> Result<Webhook> {
    let event_id = required_header(headers, "x-ptchan-event-id")?;
    let timestamp = required_header(headers, "x-ptchan-timestamp")?;
    let signature = required_header(headers, "x-ptchan-signature")?;
    DateTime::parse_from_rfc3339(timestamp).context("x-ptchan-timestamp must be RFC3339")?;

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

    let event = serde_json::from_slice::<Webhook>(body).context("decode webhook event")?;
    if event.schema_version != "1" {
        anyhow::bail!(
            "unsupported webhook schema version {}",
            event.schema_version
        );
    }
    if event.event_id != event_id {
        anyhow::bail!("event ID header does not match body");
    }
    Ok(event)
}

fn signature(
    secret: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).context("create hmac")?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(method.as_bytes());
    mac.update(b".");
    mac.update(path.as_bytes());
    if let Some(body) = body {
        mac.update(b".");
        mac.update(body);
    }
    Ok(format!(
        "hmac-sha256={}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

async fn print_response(response: Response) -> Result<()> {
    let status = response.status();
    let text = response.text().await.context("read response")?;
    let body = serde_json::from_str::<Value>(&text).unwrap_or_else(|_| json!({ "body": text }));
    print_json(&json!({
        "status": status.as_u16(),
        "ok": status.is_success(),
        "body": body
    }))?;
    ensure_success(status)
}

fn print_json(value: &Value) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).context("encode json output")?
    );
    Ok(())
}

fn ensure_success(status: StatusCode) -> Result<()> {
    if status.is_success() {
        Ok(())
    } else {
        anyhow::bail!("gateway returned HTTP {status}");
    }
}

fn required_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str> {
    headers
        .get(name)
        .ok_or_else(|| anyhow!("missing {name}"))?
        .to_str()
        .with_context(|| format!("{name} is not valid header text"))
}

fn integration_secret(integration: &str) -> Result<String> {
    env::var("PTCHAN_INTEGRATION_SECRET")
        .or_else(|_| {
            env::var(format!(
                "PTCHAN_INTEGRATION_{}_SECRET",
                env_safe_name(integration)
            ))
        })
        .context("integration secret is unset")
}

fn env_safe_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn parse_command() -> Result<Command> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let Some(command) = args.first().map(String::as_str) else {
        return Err(anyhow!(usage()));
    };
    match command {
        "health" if args.len() == 1 => Ok(Command::Health),
        "read" if matches!(args.len(), 3 | 4) => Ok(Command::Read {
            board: args[1].clone(),
            thread_id: args[2].parse().context("parse thread_id")?,
            limit: args
                .get(3)
                .map_or(Ok(50), |limit| limit.parse().context("parse limit"))?,
        }),
        "post" if args.len() >= 4 => {
            let sage = args[3..].iter().any(|arg| arg == "--sage");
            let message = if args[3..].iter().any(|arg| arg == "--stdin") {
                io::read_to_string(io::stdin()).context("read reply from stdin")?
            } else {
                args[3..]
                    .iter()
                    .filter(|arg| arg.as_str() != "--sage")
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" ")
            };
            if message.is_empty() {
                anyhow::bail!("post message must not be empty");
            }
            Ok(Command::Post {
                board: args[1].clone(),
                thread_id: args[2].parse().context("parse thread_id")?,
                message,
                sage,
            })
        }
        "listen" if args.len() <= 2 => Ok(Command::Listen {
            addr: args
                .get(1)
                .map_or("127.0.0.1:8081", String::as_str)
                .parse()
                .context("parse listen address")?,
        }),
        _ => Err(anyhow!(usage())),
    }
}

fn usage() -> &'static str {
    "usage:
  cargo run --example gateway_client -- health
  cargo run --example gateway_client -- read <board> <thread_id> [limit]
  cargo run --example gateway_client -- post <board> <thread_id> <message> [--sage]
  cargo run --example gateway_client -- post <board> <thread_id> --stdin [--sage]
  cargo run --example gateway_client -- listen [address]"
}

use std::{collections::HashMap, env, fs, net::SocketAddr};

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
use serde_json::{json, Value};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
struct AppState {
    secret: String,
    print_headers: bool,
    read_after_receive: Option<GatewayClient>,
}

#[derive(Clone)]
struct GatewayClient {
    base_url: String,
    integration: String,
    secret: String,
    limit: usize,
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
        print_headers: bool,
        read_after_receive: bool,
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let command = command_from_args(&args)?;
    let dev_env = DevEnv::load();
    match command {
        Command::Health => gateway_client_without_secret(&dev_env)?.health().await,
        Command::Read {
            board,
            thread_id,
            limit,
        } => {
            gateway_client(&dev_env)?
                .read_thread(&board, thread_id, limit)
                .await
        }
        Command::Post {
            board,
            thread_id,
            message,
            sage,
        } => {
            gateway_client(&dev_env)?
                .post_reply(&board, thread_id, &message, sage)
                .await
        }
        Command::Listen {
            addr,
            print_headers,
            read_after_receive,
            limit,
        } => listen(&dev_env, addr, print_headers, read_after_receive, limit).await,
    }
}

impl GatewayClient {
    async fn health(&self) -> Result<()> {
        let url = format!("{}/healthz", self.base_url);
        let response = self.client.get(url).send().await.context("send health")?;
        let status = response.status();
        print_json(&json!({ "status": status.as_u16(), "ok": status.is_success() }))?;
        ensure_success(status)
    }

    async fn read_thread(&self, board: &str, thread_id: i64, limit: usize) -> Result<()> {
        let path = format!("/integration/v1/threads/{board}/{thread_id}?limit={limit}");
        let timestamp = Utc::now().to_rfc3339();
        let signature = gateway_signature(&self.secret, &timestamp, "GET", &path, None)?;
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .header("x-ptchan-integration", &self.integration)
            .header("x-ptchan-timestamp", timestamp)
            .header("x-ptchan-signature", signature)
            .send()
            .await
            .context("send thread read")?;
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
        let timestamp = Utc::now().to_rfc3339();
        let signature = gateway_signature(&self.secret, &timestamp, "POST", &path, Some(&body))?;
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .header("content-type", "application/json")
            .header("x-ptchan-integration", &self.integration)
            .header("x-ptchan-timestamp", timestamp)
            .header("x-ptchan-signature", signature)
            .body(body)
            .send()
            .await
            .context("send reply")?;
        print_response(response).await
    }
}

async fn listen(
    dev_env: &DevEnv,
    addr: SocketAddr,
    print_headers: bool,
    read_after_receive: bool,
    limit: usize,
) -> Result<()> {
    let secret = integration_secret(dev_env)?;
    let read_client = if read_after_receive {
        Some(gateway_client_with_secret(dev_env, &secret, limit)?)
    } else {
        None
    };
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/internal/ptchan/events", post(handle_event))
        .with_state(AppState {
            secret,
            print_headers,
            read_after_receive: read_client,
        });
    eprintln!("listening on http://{addr}/internal/ptchan/events");
    if read_after_receive {
        eprintln!("thread read limit: {limit}");
    }
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("bind listener")?;
    axum::serve(listener, app).await.context("serve listener")
}

async fn handle_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    match verify_webhook(&state.secret, &headers, &body) {
        Ok(verified) => match serde_json::from_slice::<Value>(&body) {
            Ok(mut event) => {
                if state.print_headers {
                    add_header_summary(&mut event, &headers);
                }
                if let Err(err) = print_json(&event) {
                    eprintln!("failed to print event: {err:#}");
                }
                if let Some(client) = &state.read_after_receive {
                    if let Err(err) = read_received_thread(client, &event).await {
                        eprintln!("thread read failed: {err:#}");
                    }
                }
                eprintln!("accepted {}", verified.event_id);
                StatusCode::NO_CONTENT
            }
            Err(err) => {
                eprintln!("invalid event json: {err:#}");
                StatusCode::BAD_REQUEST
            }
        },
        Err(err) => {
            eprintln!("rejected webhook: {err:#}");
            StatusCode::UNAUTHORIZED
        }
    }
}

async fn read_received_thread(client: &GatewayClient, event: &Value) -> Result<()> {
    let board = event
        .pointer("/post/board")
        .and_then(Value::as_str)
        .context("event missing post.board")?;
    let thread_id = event
        .pointer("/post/thread_id")
        .and_then(Value::as_i64)
        .context("event missing post.thread_id")?;
    client.read_thread(board, thread_id, client.limit).await
}

fn command_from_args(args: &[String]) -> Result<Command> {
    let Some(command) = args.first().map(String::as_str) else {
        return Err(usage_error());
    };
    match command {
        "health" => Ok(Command::Health),
        "read" => read_command(&args[1..]),
        "post" => post_command(&args[1..]),
        "listen" => listen_command(&args[1..]),
        "help" | "--help" | "-h" => Err(usage_error()),
        other => Err(anyhow!("unknown command {other}\n\n{}", usage())),
    }
}

fn read_command(args: &[String]) -> Result<Command> {
    if args.len() < 2 {
        return Err(anyhow!("read requires <board> <thread_id>\n\n{}", usage()));
    }
    Ok(Command::Read {
        board: args[0].clone(),
        thread_id: args[1].parse().context("parse thread_id")?,
        limit: option_usize(args, "--limit", 50)?,
    })
}

fn post_command(args: &[String]) -> Result<Command> {
    if args.len() < 2 {
        return Err(anyhow!("post requires <board> <thread_id>\n\n{}", usage()));
    }
    let message = if flag(args, "--stdin") {
        std::io::read_to_string(std::io::stdin()).context("read stdin")?
    } else if let Some(path) = option_value(args, "--message-file") {
        fs::read_to_string(path).with_context(|| format!("read {path}"))?
    } else {
        option_value(args, "--message")
            .map(str::to_string)
            .ok_or_else(|| anyhow!("post requires --message, --message-file, or --stdin"))?
    };
    Ok(Command::Post {
        board: args[0].clone(),
        thread_id: args[1].parse().context("parse thread_id")?,
        message,
        sage: flag(args, "--sage"),
    })
}

fn listen_command(args: &[String]) -> Result<Command> {
    Ok(Command::Listen {
        addr: option_value(args, "--addr")
            .unwrap_or("127.0.0.1:8081")
            .parse()
            .context("parse --addr")?,
        print_headers: flag(args, "--print-headers"),
        read_after_receive: flag(args, "--read-after-receive"),
        limit: option_usize(args, "--limit", 50)?,
    })
}

fn gateway_client(dev_env: &DevEnv) -> Result<GatewayClient> {
    let secret = integration_secret(dev_env)?;
    gateway_client_with_secret(dev_env, &secret, 50)
}

fn gateway_client_without_secret(dev_env: &DevEnv) -> Result<GatewayClient> {
    Ok(GatewayClient {
        base_url: gateway_url(dev_env)?,
        integration: integration_name(dev_env),
        secret: String::new(),
        limit: 50,
        client: Client::new(),
    })
}

fn gateway_client_with_secret(
    dev_env: &DevEnv,
    secret: &str,
    limit: usize,
) -> Result<GatewayClient> {
    Ok(GatewayClient {
        base_url: gateway_url(dev_env)?,
        integration: integration_name(dev_env),
        secret: secret.to_string(),
        limit,
        client: Client::new(),
    })
}

fn gateway_url(dev_env: &DevEnv) -> Result<String> {
    let base_url = dev_env
        .var("PTCHAN_GATEWAY_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string())
        .trim_end_matches('/')
        .to_string();
    if base_url.is_empty() {
        anyhow::bail!("PTCHAN_GATEWAY_URL must not be empty");
    }
    Ok(base_url)
}

fn integration_name(dev_env: &DevEnv) -> String {
    dev_env
        .var("PTCHAN_INTEGRATION_NAME")
        .unwrap_or_else(|_| "example".to_string())
}

fn integration_secret(dev_env: &DevEnv) -> Result<String> {
    if let Ok(secret) = dev_env.var("PTCHAN_INTEGRATION_SECRET") {
        return Ok(secret);
    }
    let name = integration_name(dev_env);
    let env_name = format!("PTCHAN_INTEGRATION_{}_SECRET", env_safe_name(&name));
    dev_env.var(&env_name).with_context(|| {
        format!("set PTCHAN_INTEGRATION_SECRET or {env_name} in the environment or .env.dev")
    })
}

fn gateway_signature(
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

fn verify_webhook(secret: &str, headers: &HeaderMap, body: &[u8]) -> Result<VerifiedWebhook> {
    let event_id = required_header(headers, "x-ptchan-event-id")?.to_string();
    let timestamp = required_header(headers, "x-ptchan-timestamp")?;
    let signature = required_header(headers, "x-ptchan-signature")?;
    let _timestamp = DateTime::parse_from_rfc3339(timestamp)
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
    Ok(VerifiedWebhook { event_id })
}

struct VerifiedWebhook {
    event_id: String,
}

async fn print_response(response: reqwest::Response) -> Result<()> {
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

fn ensure_success(status: reqwest::StatusCode) -> Result<()> {
    if status.is_success() {
        Ok(())
    } else {
        anyhow::bail!("gateway returned HTTP {status}");
    }
}

fn print_json(value: &Value) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).context("encode json output")?
    );
    Ok(())
}

fn add_header_summary(event: &mut Value, headers: &HeaderMap) {
    let Value::Object(object) = event else {
        return;
    };
    object.insert("_headers".to_string(), json!(safe_header_names(headers)));
}

fn safe_header_names(headers: &HeaderMap) -> Vec<&str> {
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
    names
}

fn required_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str> {
    headers
        .get(name)
        .ok_or_else(|| anyhow!("missing {name}"))?
        .to_str()
        .with_context(|| format!("{name} is not valid header text"))
}

fn option_value<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|window| window[0] == name)
        .map(|window| window[1].as_str())
}

fn option_usize(args: &[String], name: &str, default: usize) -> Result<usize> {
    option_value(args, name).map_or(Ok(default), |value| {
        value.parse().with_context(|| format!("parse {name} value"))
    })
}

fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|arg| arg == name)
}

struct DevEnv {
    values: HashMap<String, String>,
}

impl DevEnv {
    fn load() -> Self {
        let values = fs::read_to_string(".env.dev")
            .ok()
            .map_or_else(HashMap::new, |raw| parse_env_file(&raw));
        Self { values }
    }

    fn var(&self, name: &str) -> std::result::Result<String, env::VarError> {
        env::var(name).or_else(|err| self.values.get(name).cloned().ok_or(err))
    }
}

fn parse_env_file(raw: &str) -> HashMap<String, String> {
    raw.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (name, value) = line.split_once('=')?;
            Some((name.trim().to_string(), unquote_env_value(value.trim())))
        })
        .collect()
}

fn unquote_env_value(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
        .to_string()
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

fn usage_error() -> anyhow::Error {
    anyhow!(usage())
}

fn usage() -> &'static str {
    "usage:
  cargo run --example gateway_client -- health
  cargo run --example gateway_client -- read <board> <thread_id> [--limit N]
  cargo run --example gateway_client -- post <board> <thread_id> (--message TEXT | --message-file PATH | --stdin) [--sage]
  cargo run --example gateway_client -- listen [--addr HOST:PORT] [--print-headers] [--read-after-receive] [--limit N]

env:
  PTCHAN_GATEWAY_URL=http://127.0.0.1:8080
  PTCHAN_INTEGRATION_NAME=example
  PTCHAN_INTEGRATION_SECRET=change-me

The client also reads .env.dev and can use PTCHAN_INTEGRATION_<NAME>_SECRET."
}

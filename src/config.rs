use std::{collections::HashSet, env, fs, time::Duration};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Clone, Deserialize)]
pub struct Config {
    pub ptchan: PtchanConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub webhook: Vec<WebhookConfig>,
    #[serde(skip)]
    pub fingerprint_secret: Option<String>,
}

#[derive(Clone, Deserialize)]
pub struct PtchanConfig {
    pub base_url: String,
    #[serde(default = "gateway_user_agent")]
    pub user_agent: String,
    #[serde(
        default = "default_refresh_fallback_interval",
        deserialize_with = "duration_from_str"
    )]
    pub session_refresh_fallback_interval: Duration,
    #[serde(
        default = "default_reconnect_min",
        deserialize_with = "duration_from_str"
    )]
    pub socket_reconnect_min: Duration,
    #[serde(
        default = "default_reconnect_max",
        deserialize_with = "duration_from_str"
    )]
    pub socket_reconnect_max: Duration,
}

#[derive(Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_http_addr")]
    pub http_addr: String,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
}

#[derive(Clone, Deserialize)]
pub struct StorageConfig {
    pub sqlite_path: String,
    #[serde(
        default = "default_event_retention",
        deserialize_with = "duration_from_str"
    )]
    pub event_retention: Duration,
}

#[derive(Clone, Deserialize)]
pub struct WebhookConfig {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub allowed_boards: Vec<String>,
    #[serde(default)]
    pub include_poster_fingerprint: bool,
    #[serde(skip)]
    pub secret: String,
    #[serde(
        default = "default_webhook_timeout",
        deserialize_with = "duration_from_str"
    )]
    pub timeout: Duration,
}

impl Config {
    pub fn load_from_env() -> Result<Self> {
        let path = env::var("CONFIG_FILE").unwrap_or_else(|_| "config/dev.toml".to_string());
        let raw = fs::read_to_string(&path).with_context(|| format!("read {path}"))?;
        let mut cfg: Config = toml::from_str(&raw).with_context(|| format!("parse {path}"))?;
        if let Ok(sqlite_path) = env::var("SQLITE_PATH") {
            if !sqlite_path.trim().is_empty() {
                cfg.storage.sqlite_path = sqlite_path;
            }
        }
        for wh in &mut cfg.webhook {
            let env_name = webhook_secret_env(&wh.name);
            wh.secret = env::var(&env_name).with_context(|| {
                format!("webhook {} secret env {} is not set", wh.name, env_name)
            })?;
            if wh.secret.trim().is_empty() {
                return Err(anyhow!(
                    "webhook {} secret env {} is empty",
                    wh.name,
                    env_name
                ));
            }
        }
        if cfg.webhook.iter().any(|wh| wh.include_poster_fingerprint) {
            let secret = env::var("PTCHAN_FINGERPRINT_SECRET")
                .context("fingerprint env PTCHAN_FINGERPRINT_SECRET is not set")?;
            if secret.trim().is_empty() {
                return Err(anyhow!(
                    "fingerprint env PTCHAN_FINGERPRINT_SECRET is empty"
                ));
            }
            cfg.fingerprint_secret = Some(secret);
        }
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.ptchan.base_url.trim().is_empty() {
            return Err(anyhow!("ptchan.base_url is required"));
        }
        reqwest::Url::parse(&self.ptchan.base_url)
            .context("ptchan.base_url must be an absolute URL")?;
        if self.ptchan.user_agent.trim().is_empty() {
            return Err(anyhow!("ptchan.user_agent is required"));
        }
        if self.storage.sqlite_path.trim().is_empty() {
            return Err(anyhow!("storage.sqlite_path is required"));
        }
        if self.storage.event_retention.is_zero() {
            return Err(anyhow!("storage.event_retention must be greater than zero"));
        }
        let mut names = HashSet::new();
        for wh in &self.webhook {
            if wh.name.trim().is_empty() {
                return Err(anyhow!("webhook.name is required"));
            }
            if !names.insert(wh.name.as_str()) {
                return Err(anyhow!("duplicate webhook name {}", wh.name));
            }
            reqwest::Url::parse(&wh.url)
                .with_context(|| format!("webhook {} url must be absolute", wh.name))?;
            for board in &wh.allowed_boards {
                if !valid_board_name(board) {
                    return Err(anyhow!(
                        "webhook {} allowed board {} is invalid",
                        wh.name,
                        board
                    ));
                }
            }
        }
        Ok(())
    }
}

pub fn ptchan_session_cookie() -> Result<String> {
    let cookie = env::var("PTCHAN_SESSION_COOKIE")
        .context("ptchan session cookie env PTCHAN_SESSION_COOKIE is not set")?;
    if cookie.trim().is_empty() {
        return Err(anyhow!(
            "ptchan session cookie env PTCHAN_SESSION_COOKIE is empty"
        ));
    }
    Ok(cookie)
}

fn webhook_secret_env(name: &str) -> String {
    let normalized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("PTCHAN_WEBHOOK_{normalized}_SECRET")
}

pub fn valid_board_name(board: &str) -> bool {
    !board.is_empty()
        && board.len() <= 32
        && board
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

pub fn init_logging(cfg: &LoggingConfig) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.level))
        .context("parse log level")?;
    match cfg.format.as_str() {
        "json" => fmt().json().with_env_filter(filter).init(),
        "text" => fmt().with_env_filter(filter).init(),
        other => return Err(anyhow!("unsupported log format {other}; use text or json")),
    }
    Ok(())
}

fn duration_from_str<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    humantime::parse_duration(&value).map_err(serde::de::Error::custom)
}

fn default_refresh_fallback_interval() -> Duration {
    Duration::from_secs(12 * 60 * 60)
}
pub(crate) fn gateway_user_agent() -> String {
    format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
}
fn default_reconnect_min() -> Duration {
    Duration::from_secs(3)
}
fn default_reconnect_max() -> Duration {
    Duration::from_secs(60)
}
fn default_http_addr() -> String {
    "0.0.0.0:8080".to_string()
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "json".to_string()
}
fn default_webhook_timeout() -> Duration {
    Duration::from_secs(10)
}
fn default_event_retention() -> Duration {
    Duration::from_secs(14 * 24 * 60 * 60)
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            http_addr: default_http_addr(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

use std::{env, net::SocketAddr, time::Duration};

use anyhow::{anyhow, Context, Result};
use serde::{de::Error as DeError, Deserialize, Deserializer};
use tracing_subscriber::{fmt, EnvFilter};

mod file;
mod validation;

pub(crate) use file::load_from_env;

const POSTING_NAME_MAX_CHARS: usize = 25;

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) ptchan: PtchanConfig,
    pub(crate) runtime: RuntimeConfig,
    pub(crate) storage: StorageConfig,
    pub(crate) integrations: Vec<IntegrationConfig>,
    pub(crate) webhooks: Vec<WebhookConfig>,
    pub(crate) postings: Vec<PostingConfig>,
    pub(crate) fingerprint_secret: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PtchanConfig {
    pub(crate) base_url: String,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RuntimeConfig {
    pub(crate) http_addr: String,
    pub(crate) logging: LoggingConfig,
    pub(crate) rate_limit: RuntimeRateLimitConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            http_addr: default_http_addr(),
            logging: LoggingConfig::default(),
            rate_limit: RuntimeRateLimitConfig::default(),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct LoggingConfig {
    pub(crate) level: String,
    pub(crate) format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: LogFormat::Json,
        }
    }
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogFormat {
    #[default]
    Json,
    Text,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RuntimeRateLimitConfig {
    pub(crate) reading: RateLimitBucketConfig,
    pub(crate) posting: RateLimitBucketConfig,
}

impl Default for RuntimeRateLimitConfig {
    fn default() -> Self {
        Self {
            reading: default_global_reading_rate_limit(),
            posting: default_global_posting_rate_limit(),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageConfig {
    pub(crate) sqlite_path: String,
    #[serde(
        default = "default_event_retention",
        deserialize_with = "duration_from_str"
    )]
    pub(crate) event_retention: Duration,
}

#[derive(Clone)]
pub(crate) struct IntegrationConfig {
    pub(crate) name: String,
    pub(crate) allowed_boards: Vec<String>,
    pub(crate) reading: bool,
    pub(crate) rate_limit: RateLimitConfig,
    pub(crate) secret: String,
}

impl IntegrationConfig {
    pub(crate) fn board_allowed(&self, board: &str) -> bool {
        board_allowed(&self.allowed_boards, board)
    }
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RateLimitConfig {
    pub(crate) reading: RateLimitBucketConfig,
    pub(crate) posting: RateLimitBucketConfig,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            reading: RateLimitBucketConfig::default(),
            posting: default_posting_rate_limit(),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RateLimitBucketConfig {
    pub(crate) requests: u32,
    #[serde(deserialize_with = "duration_from_str")]
    pub(crate) window: Duration,
    pub(crate) burst: u32,
}

impl Default for RateLimitBucketConfig {
    fn default() -> Self {
        Self {
            requests: default_reading_rate_limit_requests(),
            window: default_rate_limit_window(),
            burst: default_reading_rate_limit_burst(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct WebhookConfig {
    pub(crate) name: String,
    pub(crate) url: String,
    pub(crate) allowed_boards: Vec<String>,
    pub(crate) include_poster_fingerprint: bool,
    pub(crate) secret: String,
}

impl WebhookConfig {
    pub(crate) fn board_allowed(&self, board: &str) -> bool {
        board_allowed(&self.allowed_boards, board)
    }
}

#[derive(Clone)]
pub(crate) struct PostingConfig {
    pub(crate) name: String,
    pub(crate) allowed_boards: Vec<String>,
    pub(crate) display_name: Option<String>,
    pub(crate) secret: String,
    pub(crate) tripcode_secret: String,
    pub(crate) public_tripcode: String,
    pub(crate) post_password: String,
}

impl PostingConfig {
    pub(crate) fn board_allowed(&self, board: &str) -> bool {
        board_allowed(&self.allowed_boards, board)
    }

    pub(crate) fn form_name(&self) -> String {
        let name = self.display_name.as_deref().unwrap_or(&self.name).trim();
        format!("{name}##{}", self.tripcode_secret)
    }
}

pub(crate) fn ptchan_session_cookie() -> Result<String> {
    required_env(
        "PTCHAN_SESSION_COOKIE",
        "ptchan session cookie env PTCHAN_SESSION_COOKIE",
    )
}

fn required_env(name: &str, description: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{description} is not set"))?;
    if value.trim().is_empty() {
        return Err(anyhow!("{description} is empty"));
    }
    Ok(value)
}

fn integration_secret_env(name: &str) -> String {
    format!("PTCHAN_INTEGRATION_{}_SECRET", env_safe_name(name))
}

fn integration_tripcode_env(name: &str) -> String {
    format!("PTCHAN_INTEGRATION_{}_TRIPCODE", env_safe_name(name))
}

fn integration_post_password_env(name: &str) -> String {
    format!("PTCHAN_INTEGRATION_{}_POST_PASSWORD", env_safe_name(name))
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

fn board_allowed(allowed_boards: &[String], board: &str) -> bool {
    allowed_boards.is_empty()
        || allowed_boards
            .iter()
            .any(|allowed_board| allowed_board == board)
}

fn valid_public_tripcode(tripcode: &str) -> bool {
    let Some(encoded) = tripcode
        .strip_prefix("!!")
        .and_then(|value| value.strip_suffix('='))
    else {
        return false;
    };
    encoded.len() == 9
        && encoded
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
}

pub(crate) fn valid_board_name(board: &str) -> bool {
    !board.is_empty()
        && board.len() <= 32
        && board
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn valid_integration_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub(crate) fn init_logging(cfg: &LoggingConfig) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.level))
        .context("parse log level")?;
    match cfg.format {
        LogFormat::Json => fmt().json().with_env_filter(filter).init(),
        LogFormat::Text => fmt().with_env_filter(filter).init(),
    }
    Ok(())
}

fn duration_from_str<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    humantime::parse_duration(&value).map_err(DeError::custom)
}

pub(crate) fn runtime_addr(addr: &str) -> Result<SocketAddr> {
    let normalized = if let Some(port) = addr.strip_prefix(':') {
        format!("0.0.0.0:{port}")
    } else {
        addr.to_string()
    };
    normalized
        .parse()
        .with_context(|| format!("parse address {addr}"))
}

pub(crate) fn gateway_user_agent() -> String {
    format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
}

fn default_http_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_reading_rate_limit_requests() -> u32 {
    120
}

fn default_rate_limit_window() -> Duration {
    Duration::from_mins(1)
}

fn default_reading_rate_limit_burst() -> u32 {
    30
}

fn default_posting_rate_limit() -> RateLimitBucketConfig {
    RateLimitBucketConfig {
        requests: 30,
        window: default_rate_limit_window(),
        burst: 5,
    }
}

fn default_global_reading_rate_limit() -> RateLimitBucketConfig {
    RateLimitBucketConfig {
        requests: 1_000,
        window: default_rate_limit_window(),
        burst: 200,
    }
}

fn default_global_posting_rate_limit() -> RateLimitBucketConfig {
    RateLimitBucketConfig {
        requests: 100,
        window: default_rate_limit_window(),
        burst: 20,
    }
}

fn default_event_retention() -> Duration {
    Duration::from_hours(14 * 24)
}

#[cfg(test)]
mod tests;

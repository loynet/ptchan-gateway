use std::{collections::HashSet, env, fs, net::SocketAddr, str::FromStr, time::Duration};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Clone, Deserialize)]
pub(crate) struct Config {
    pub(crate) ptchan: PtchanConfig,
    #[serde(default)]
    pub(crate) runtime: RuntimeConfig,
    pub(crate) storage: StorageConfig,
    #[serde(default)]
    pub(crate) webhook: Vec<WebhookConfig>,
    #[serde(skip)]
    pub(crate) fingerprint_secret: Option<String>,
}

#[derive(Clone, Deserialize)]
pub(crate) struct PtchanConfig {
    pub(crate) base_url: String,
    #[serde(default = "gateway_user_agent")]
    pub(crate) user_agent: String,
    #[serde(
        default = "default_refresh_fallback_interval",
        deserialize_with = "duration_from_str"
    )]
    pub(crate) session_refresh_fallback_interval: Duration,
    #[serde(
        default = "default_reconnect_min",
        deserialize_with = "duration_from_str"
    )]
    pub(crate) socket_reconnect_min: Duration,
    #[serde(
        default = "default_reconnect_max",
        deserialize_with = "duration_from_str"
    )]
    pub(crate) socket_reconnect_max: Duration,
}

#[derive(Clone, Deserialize)]
pub(crate) struct RuntimeConfig {
    #[serde(default = "default_http_addr")]
    pub(crate) http_addr: String,
    #[serde(default)]
    pub(crate) logging: LoggingConfig,
}

#[derive(Clone, Deserialize)]
pub(crate) struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub(crate) level: String,
    #[serde(default, deserialize_with = "log_format_from_str")]
    pub(crate) format: LogFormat,
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
            format: LogFormat::Json,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) enum LogFormat {
    #[default]
    Json,
    Text,
}

impl FromStr for LogFormat {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "json" => Ok(Self::Json),
            "text" => Ok(Self::Text),
            other => Err(anyhow!("unsupported log format {other}; use text or json")),
        }
    }
}

#[derive(Clone, Deserialize)]
pub(crate) struct StorageConfig {
    pub(crate) sqlite_path: String,
    #[serde(
        default = "default_event_retention",
        deserialize_with = "duration_from_str"
    )]
    pub(crate) event_retention: Duration,
}

#[derive(Clone, Deserialize)]
pub(crate) struct WebhookConfig {
    pub(crate) name: String,
    pub(crate) url: String,
    #[serde(default)]
    pub(crate) allowed_boards: Vec<String>,
    #[serde(default)]
    pub(crate) include_poster_fingerprint: bool,
    #[serde(skip)]
    pub(crate) secret: String,
    #[serde(
        default = "default_webhook_timeout",
        deserialize_with = "duration_from_str"
    )]
    pub(crate) timeout: Duration,
}

impl Config {
    pub(crate) fn load_from_env() -> Result<Self> {
        let path = env::var("CONFIG_FILE").unwrap_or_else(|_| "config/dev.toml".to_string());
        let raw = fs::read_to_string(&path).with_context(|| format!("read {path}"))?;
        let mut cfg: Config = toml::from_str(&raw).with_context(|| format!("parse {path}"))?;
        if let Ok(sqlite_path) = env::var("SQLITE_PATH") {
            if !sqlite_path.trim().is_empty() {
                cfg.storage.sqlite_path = sqlite_path;
            }
        }
        cfg.validate().context("validate config")?;
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

    pub(crate) fn validate(&self) -> Result<()> {
        if self.ptchan.base_url.trim().is_empty() {
            return Err(anyhow!("ptchan.base_url is required"));
        }
        reqwest::Url::parse(&self.ptchan.base_url)
            .context("ptchan.base_url must be an absolute URL")?;
        if self.ptchan.user_agent.trim().is_empty() {
            return Err(anyhow!("ptchan.user_agent is required"));
        }
        if self.ptchan.session_refresh_fallback_interval.is_zero() {
            return Err(anyhow!(
                "ptchan.session_refresh_fallback_interval must be greater than zero"
            ));
        }
        if self.ptchan.socket_reconnect_min.is_zero() {
            return Err(anyhow!(
                "ptchan.socket_reconnect_min must be greater than zero"
            ));
        }
        if self.ptchan.socket_reconnect_max < self.ptchan.socket_reconnect_min {
            return Err(anyhow!(
                "ptchan.socket_reconnect_max must be greater than or equal to ptchan.socket_reconnect_min"
            ));
        }
        runtime_addr(&self.runtime.http_addr).context("runtime.http_addr is invalid")?;
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
            if wh.timeout.is_zero() {
                return Err(anyhow!(
                    "webhook {} timeout must be greater than zero",
                    wh.name
                ));
            }
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

pub(crate) fn ptchan_session_cookie() -> Result<String> {
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

pub(crate) fn valid_board_name(board: &str) -> bool {
    !board.is_empty()
        && board.len() <= 32
        && board
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
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

fn log_format_from_str<'de, D>(deserializer: D) -> std::result::Result<LogFormat, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value.parse().map_err(serde::de::Error::custom)
}

fn duration_from_str<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    humantime::parse_duration(&value).map_err(serde::de::Error::custom)
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

fn default_refresh_fallback_interval() -> Duration {
    Duration::from_hours(12)
}
pub(crate) fn gateway_user_agent() -> String {
    format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
}

fn default_reconnect_min() -> Duration {
    Duration::from_secs(3)
}
fn default_reconnect_max() -> Duration {
    Duration::from_mins(1)
}
fn default_http_addr() -> String {
    "0.0.0.0:8080".to_string()
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_webhook_timeout() -> Duration {
    Duration::from_secs(10)
}
fn default_event_retention() -> Duration {
    Duration::from_hours(14 * 24)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_runtime_address() {
        let mut cfg = valid_config();
        cfg.runtime.http_addr = "not-an-address".to_string();

        let err = cfg.validate().unwrap_err();

        assert!(err.to_string().contains("runtime.http_addr is invalid"));
    }

    #[test]
    fn validates_reconnect_range() {
        let mut cfg = valid_config();
        cfg.ptchan.socket_reconnect_min = Duration::from_secs(10);
        cfg.ptchan.socket_reconnect_max = Duration::from_secs(3);

        let err = cfg.validate().unwrap_err();

        assert!(err
            .to_string()
            .contains("ptchan.socket_reconnect_max must be greater than or equal"));
    }

    #[test]
    fn validates_webhook_timeout() {
        let mut cfg = valid_config();
        cfg.webhook[0].timeout = Duration::ZERO;

        let err = cfg.validate().unwrap_err();

        assert!(err
            .to_string()
            .contains("webhook example timeout must be greater than zero"));
    }

    #[test]
    fn parses_log_format() {
        assert!(matches!(
            "json".parse::<LogFormat>().unwrap(),
            LogFormat::Json
        ));
        assert!(matches!(
            "text".parse::<LogFormat>().unwrap(),
            LogFormat::Text
        ));
        assert!("pretty".parse::<LogFormat>().is_err());
    }

    #[test]
    fn defaults_runtime_and_logging_sections() {
        let raw = r#"
[ptchan]
base_url = "https://ptchan.test"

[storage]
sqlite_path = "data/test.db"
"#;

        let cfg = toml::from_str::<Config>(raw).unwrap();

        assert_eq!(cfg.runtime.http_addr, "0.0.0.0:8080");
        assert_eq!(cfg.runtime.logging.level, "info");
        assert!(matches!(cfg.runtime.logging.format, LogFormat::Json));
    }

    fn valid_config() -> Config {
        Config {
            ptchan: PtchanConfig {
                base_url: "https://ptchan.test".to_string(),
                user_agent: "ptchan-gateway-test".to_string(),
                session_refresh_fallback_interval: Duration::from_hours(12),
                socket_reconnect_min: Duration::from_secs(3),
                socket_reconnect_max: Duration::from_mins(1),
            },
            runtime: RuntimeConfig {
                http_addr: "127.0.0.1:8080".to_string(),
                logging: LoggingConfig {
                    level: "info".to_string(),
                    format: LogFormat::Json,
                },
            },
            storage: StorageConfig {
                sqlite_path: "data/test.db".to_string(),
                event_retention: Duration::from_hours(14 * 24),
            },
            webhook: vec![WebhookConfig {
                name: "example".to_string(),
                url: "http://127.0.0.1:8081/events".to_string(),
                allowed_boards: Vec::new(),
                include_poster_fingerprint: false,
                secret: String::new(),
                timeout: Duration::from_secs(10),
            }],
            fingerprint_secret: None,
        }
    }
}

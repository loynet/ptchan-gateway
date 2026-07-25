use std::{collections::HashSet, env, fs, net::SocketAddr, str::FromStr, time::Duration};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use tracing_subscriber::{fmt, EnvFilter};

const POSTING_NAME_MAX_CHARS: usize = 25;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub(crate) ptchan: PtchanConfig,
    #[serde(default)]
    pub(crate) runtime: RuntimeConfig,
    pub(crate) storage: StorageConfig,
    #[serde(default)]
    pub(crate) integration: Vec<IntegrationConfig>,
    #[serde(skip)]
    pub(crate) webhook: Vec<WebhookConfig>,
    #[serde(skip)]
    pub(crate) posting: Vec<PostingConfig>,
    #[serde(skip)]
    pub(crate) fingerprint_secret: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeConfig {
    #[serde(default = "default_http_addr")]
    pub(crate) http_addr: String,
    #[serde(default)]
    pub(crate) logging: LoggingConfig,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub(crate) struct StorageConfig {
    pub(crate) sqlite_path: String,
    #[serde(
        default = "default_event_retention",
        deserialize_with = "duration_from_str"
    )]
    pub(crate) event_retention: Duration,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IntegrationConfig {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) allowed_boards: Vec<String>,
    #[serde(default)]
    pub(crate) reading: Option<ReadingCapabilityConfig>,
    #[serde(default)]
    pub(crate) webhook: Option<WebhookCapabilityConfig>,
    #[serde(default)]
    pub(crate) posting: Option<PostingCapabilityConfig>,
    #[serde(skip)]
    pub(crate) secret: String,
}

impl IntegrationConfig {
    pub(crate) fn reading_enabled(&self) -> bool {
        self.reading.as_ref().is_some_and(|reading| reading.enabled)
    }

    pub(crate) fn board_allowed(&self, board: &str) -> bool {
        board_allowed(&self.allowed_boards, board)
    }
}

pub(crate) fn board_allowed(allowed_boards: &[String], board: &str) -> bool {
    allowed_boards.is_empty()
        || allowed_boards
            .iter()
            .any(|allowed_board| allowed_board == board)
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadingCapabilityConfig {
    #[serde(default = "default_enabled")]
    pub(crate) enabled: bool,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebhookCapabilityConfig {
    pub(crate) url: String,
    #[serde(default)]
    pub(crate) include_poster_fingerprint: bool,
    #[serde(
        default = "default_webhook_timeout",
        deserialize_with = "duration_from_str"
    )]
    pub(crate) timeout: Duration,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostingCapabilityConfig {
    #[serde(default)]
    pub(crate) display_name: Option<String>,
    #[serde(default)]
    pub(crate) use_tripcode: bool,
    #[serde(default = "default_secure_tripcode")]
    pub(crate) secure_tripcode: bool,
    #[serde(default)]
    pub(crate) use_post_password: bool,
    #[serde(skip)]
    pub(crate) tripcode: Option<String>,
    #[serde(skip)]
    pub(crate) post_password: Option<String>,
    #[serde(
        default = "default_posting_timeout",
        deserialize_with = "duration_from_str"
    )]
    pub(crate) timeout: Duration,
}

#[derive(Clone)]
pub(crate) struct WebhookConfig {
    pub(crate) name: String,
    pub(crate) url: String,
    pub(crate) allowed_boards: Vec<String>,
    pub(crate) include_poster_fingerprint: bool,
    pub(crate) secret: String,
    pub(crate) timeout: Duration,
}

#[derive(Clone)]
pub(crate) struct PostingConfig {
    pub(crate) name: String,
    pub(crate) allowed_boards: Vec<String>,
    pub(crate) display_name: Option<String>,
    pub(crate) secure_tripcode: bool,
    pub(crate) secret: String,
    pub(crate) tripcode: Option<String>,
    pub(crate) post_password: Option<String>,
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
        for integration in &mut cfg.integration {
            let env_name = integration_secret_env(&integration.name);
            integration.secret = env::var(&env_name).with_context(|| {
                format!(
                    "integration {} secret env {} is not set",
                    integration.name, env_name
                )
            })?;
            if integration.secret.trim().is_empty() {
                return Err(anyhow!(
                    "integration {} secret env {} is empty",
                    integration.name,
                    env_name
                ));
            }
            if let Some(posting) = &mut integration.posting {
                if posting.use_tripcode {
                    let env_name = integration_tripcode_env(&integration.name);
                    let tripcode = env::var(&env_name).with_context(|| {
                        format!(
                            "integration {} tripcode env {} is not set",
                            integration.name, env_name
                        )
                    })?;
                    if tripcode.trim().is_empty() {
                        return Err(anyhow!(
                            "integration {} tripcode env {} is empty",
                            integration.name,
                            env_name
                        ));
                    }
                    posting.tripcode = Some(tripcode);
                }
                if posting.use_post_password {
                    let env_name = integration_post_password_env(&integration.name);
                    let post_password = env::var(&env_name).with_context(|| {
                        format!(
                            "integration {} post password env {} is not set",
                            integration.name, env_name
                        )
                    })?;
                    if post_password.trim().is_empty() {
                        return Err(anyhow!(
                            "integration {} post password env {} is empty",
                            integration.name,
                            env_name
                        ));
                    }
                    posting.post_password = Some(post_password);
                }
            }
        }
        cfg.validate().context("validate loaded config")?;
        cfg.webhook = cfg.webhooks();
        cfg.posting = cfg.postings();
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

    fn webhooks(&self) -> Vec<WebhookConfig> {
        self.integration
            .iter()
            .filter_map(|integration| {
                let webhook = integration.webhook.as_ref()?;
                Some(WebhookConfig {
                    name: integration.name.clone(),
                    url: webhook.url.clone(),
                    allowed_boards: integration.allowed_boards.clone(),
                    include_poster_fingerprint: webhook.include_poster_fingerprint,
                    secret: integration.secret.clone(),
                    timeout: webhook.timeout,
                })
            })
            .collect()
    }

    fn postings(&self) -> Vec<PostingConfig> {
        self.integration
            .iter()
            .filter_map(|integration| {
                let posting = integration.posting.as_ref()?;
                Some(PostingConfig {
                    name: integration.name.clone(),
                    allowed_boards: integration.allowed_boards.clone(),
                    display_name: posting.display_name.clone(),
                    secure_tripcode: posting.secure_tripcode,
                    secret: integration.secret.clone(),
                    tripcode: posting.tripcode.clone(),
                    post_password: posting.post_password.clone(),
                    timeout: posting.timeout,
                })
            })
            .collect()
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
        let mut env_names = HashSet::new();
        for integration in &self.integration {
            if integration.name.trim().is_empty() {
                return Err(anyhow!("integration.name is required"));
            }
            if !valid_integration_name(&integration.name) {
                return Err(anyhow!(
                    "integration name {} is invalid; use 1-64 ASCII letters, digits, underscores, or hyphens",
                    integration.name
                ));
            }
            if !names.insert(integration.name.as_str()) {
                return Err(anyhow!("duplicate integration name {}", integration.name));
            }
            let env_name = env_safe_name(&integration.name);
            if !env_names.insert(env_name) {
                return Err(anyhow!(
                    "integration name {} conflicts with another integration environment name",
                    integration.name
                ));
            }
            if integration.reading.is_none()
                && integration.webhook.is_none()
                && integration.posting.is_none()
            {
                return Err(anyhow!(
                    "integration {} must enable at least one capability",
                    integration.name
                ));
            }
            for board in &integration.allowed_boards {
                if !valid_board_name(board) {
                    return Err(anyhow!(
                        "integration {} allowed board {} is invalid",
                        integration.name,
                        board
                    ));
                }
            }
            if let Some(reading) = &integration.reading {
                if !reading.enabled {
                    return Err(anyhow!(
                        "integration {} reading.enabled must be true or the reading section must be omitted",
                        integration.name
                    ));
                }
            }
            if let Some(webhook) = &integration.webhook {
                reqwest::Url::parse(&webhook.url).with_context(|| {
                    format!(
                        "integration {} webhook url must be absolute",
                        integration.name
                    )
                })?;
                if webhook.timeout.is_zero() {
                    return Err(anyhow!(
                        "integration {} webhook timeout must be greater than zero",
                        integration.name
                    ));
                }
            }
            if let Some(posting) = &integration.posting {
                validate_posting(&integration.name, posting)?;
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

fn posting_form_name(integration_name: &str, posting: &PostingCapabilityConfig) -> Option<String> {
    let name = posting
        .display_name
        .as_deref()
        .unwrap_or(integration_name)
        .trim();
    match posting.tripcode.as_deref() {
        Some(tripcode) if posting.secure_tripcode => Some(format!("{name}##{tripcode}")),
        Some(tripcode) => Some(format!("{name}#{tripcode}")),
        None if name.is_empty() => None,
        None => Some(name.to_string()),
    }
}

fn validate_posting(integration_name: &str, posting: &PostingCapabilityConfig) -> Result<()> {
    if posting.timeout.is_zero() {
        return Err(anyhow!(
            "integration {integration_name} posting timeout must be greater than zero"
        ));
    }
    if matches!(posting.display_name.as_deref(), Some(name) if name.trim().is_empty()) {
        return Err(anyhow!(
            "integration {integration_name} posting display_name must not be empty"
        ));
    }
    let Some(name) = posting_form_name(integration_name, posting) else {
        return Ok(());
    };
    let name_len = name.chars().count();
    if name_len > POSTING_NAME_MAX_CHARS {
        return Err(anyhow!(
            "integration {integration_name} posting name is {name_len} characters; ptchan allows {POSTING_NAME_MAX_CHARS} or less"
        ));
    }
    Ok(())
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
fn default_posting_timeout() -> Duration {
    Duration::from_secs(15)
}
const fn default_enabled() -> bool {
    true
}
const fn default_secure_tripcode() -> bool {
    true
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
        cfg.integration[0].webhook.as_mut().unwrap().timeout = Duration::ZERO;

        let err = cfg.validate().unwrap_err();

        assert!(err
            .to_string()
            .contains("integration example webhook timeout must be greater than zero"));
    }

    #[test]
    fn validates_posting_name_length_after_tripcode() {
        let mut cfg = valid_config();
        cfg.integration[0].posting = Some(PostingCapabilityConfig {
            display_name: Some("ptchan-gateway".to_string()),
            use_tripcode: true,
            secure_tripcode: true,
            use_post_password: false,
            tripcode: Some("this-is-an-example-ok".to_string()),
            post_password: None,
            timeout: Duration::from_secs(15),
        });

        let err = cfg.validate().unwrap_err();

        assert!(err
            .to_string()
            .contains("integration example posting name is"));
    }

    #[test]
    fn validates_integration_names_for_env_and_metrics() {
        let mut cfg = valid_config();
        cfg.integration[0].name = "bad name".to_string();

        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("integration name bad name is invalid"));
    }

    #[test]
    fn rejects_integration_env_name_collisions() {
        let mut cfg = valid_config();
        cfg.integration.push(IntegrationConfig {
            name: "example-test".to_string(),
            allowed_boards: Vec::new(),
            reading: Some(ReadingCapabilityConfig { enabled: true }),
            webhook: None,
            posting: None,
            secret: String::new(),
        });
        cfg.integration[0].name = "example_test".to_string();

        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("conflicts with another integration environment name"));
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
            integration: vec![IntegrationConfig {
                name: "example".to_string(),
                allowed_boards: Vec::new(),
                reading: Some(ReadingCapabilityConfig { enabled: true }),
                webhook: Some(WebhookCapabilityConfig {
                    url: "http://127.0.0.1:8081/events".to_string(),
                    include_poster_fingerprint: false,
                    timeout: Duration::from_secs(10),
                }),
                posting: None,
                secret: String::new(),
            }],
            webhook: Vec::new(),
            posting: Vec::new(),
            fingerprint_secret: None,
        }
    }
}

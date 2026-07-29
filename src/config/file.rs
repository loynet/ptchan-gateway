use std::{env, fs};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use super::{
    integration_post_password_env, integration_secret_env, integration_tripcode_env, required_env,
    validation, Config, IntegrationConfig, PostingConfig, PtchanConfig, RateLimitConfig,
    RuntimeConfig, StorageConfig, WebhookConfig, POSTING_NAME_MAX_CHARS,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileConfig {
    pub(super) ptchan: PtchanConfig,
    #[serde(default)]
    pub(super) runtime: RuntimeConfig,
    pub(super) storage: StorageConfig,
    #[serde(default, rename = "integration")]
    pub(super) integrations: Vec<FileIntegration>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileIntegration {
    pub(super) name: String,
    #[serde(default)]
    pub(super) allowed_boards: Vec<String>,
    #[serde(default)]
    pub(super) reading: Option<ReadingCapability>,
    #[serde(default)]
    pub(super) webhook: Option<FileWebhook>,
    #[serde(default)]
    pub(super) posting: Option<FilePosting>,
    #[serde(default)]
    pub(super) rate_limit: RateLimitConfig,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReadingCapability {}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileWebhook {
    pub(super) url: String,
    #[serde(default)]
    pub(super) include_poster_fingerprint: bool,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FilePosting {
    #[serde(default)]
    pub(super) display_name: Option<String>,
    pub(super) public_tripcode: String,
}

pub(crate) fn load_from_env() -> Result<Config> {
    let path = env::var("CONFIG_FILE").unwrap_or_else(|_| "config/dev.toml".to_string());
    let raw = fs::read_to_string(&path).with_context(|| format!("read {path}"))?;
    let mut file = parse(&raw).with_context(|| format!("parse {path}"))?;
    if let Ok(sqlite_path) = env::var("SQLITE_PATH") {
        if !sqlite_path.trim().is_empty() {
            file.storage.sqlite_path = sqlite_path;
        }
    }
    resolve(file, required_env)
}

pub(super) fn parse(raw: &str) -> Result<FileConfig, toml::de::Error> {
    toml::from_str(raw)
}

pub(super) fn resolve(
    file: FileConfig,
    get_secret: impl Fn(&str, &str) -> Result<String>,
) -> Result<Config> {
    validation::validate(&file).context("validate config")?;

    let mut integrations = Vec::with_capacity(file.integrations.len());
    let mut webhooks = Vec::new();
    let mut postings = Vec::new();

    for integration in file.integrations {
        let secret_env = integration_secret_env(&integration.name);
        let secret = get_secret(
            &secret_env,
            &format!("integration {} secret env {}", integration.name, secret_env),
        )?;

        if let Some(webhook) = integration.webhook {
            webhooks.push(WebhookConfig {
                name: integration.name.clone(),
                url: webhook.url,
                allowed_boards: integration.allowed_boards.clone(),
                include_poster_fingerprint: webhook.include_poster_fingerprint,
                secret: secret.clone(),
            });
        }

        if let Some(posting) = integration.posting {
            let tripcode_env = integration_tripcode_env(&integration.name);
            let tripcode_secret = get_secret(
                &tripcode_env,
                &format!(
                    "integration {} tripcode env {}",
                    integration.name, tripcode_env
                ),
            )?;
            let password_env = integration_post_password_env(&integration.name);
            let post_password = get_secret(
                &password_env,
                &format!(
                    "integration {} post password env {}",
                    integration.name, password_env
                ),
            )?;
            let posting = PostingConfig {
                name: integration.name.clone(),
                allowed_boards: integration.allowed_boards.clone(),
                display_name: posting.display_name,
                secret: secret.clone(),
                tripcode_secret,
                public_tripcode: posting.public_tripcode,
                post_password,
            };
            let name_len = posting.form_name().chars().count();
            if name_len > POSTING_NAME_MAX_CHARS {
                return Err(anyhow!(
                    "integration {} posting name is {name_len} characters; ptchan allows {POSTING_NAME_MAX_CHARS} or less",
                    integration.name
                ));
            }
            postings.push(posting);
        }

        integrations.push(IntegrationConfig {
            name: integration.name,
            allowed_boards: integration.allowed_boards,
            reading: integration.reading.is_some(),
            rate_limit: integration.rate_limit,
            secret,
        });
    }

    let fingerprint_secret = if webhooks
        .iter()
        .any(|webhook| webhook.include_poster_fingerprint)
    {
        Some(get_secret(
            "PTCHAN_FINGERPRINT_SECRET",
            "fingerprint env PTCHAN_FINGERPRINT_SECRET",
        )?)
    } else {
        None
    };

    Ok(Config {
        ptchan: file.ptchan,
        runtime: file.runtime,
        storage: file.storage,
        integrations,
        webhooks,
        postings,
        fingerprint_secret,
    })
}

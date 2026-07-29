use std::collections::HashSet;

use anyhow::{anyhow, Context, Result};
use reqwest::Url;

use super::{
    env_safe_name,
    file::{FileConfig, FilePosting},
    runtime_addr, valid_board_name, valid_integration_name, valid_public_tripcode,
    RateLimitBucketConfig, RateLimitConfig,
};

pub(super) fn validate(config: &FileConfig) -> Result<()> {
    if config.ptchan.base_url.trim().is_empty() {
        return Err(anyhow!("ptchan.base_url is required"));
    }
    Url::parse(&config.ptchan.base_url).context("ptchan.base_url must be an absolute URL")?;
    runtime_addr(&config.runtime.http_addr).context("runtime.http_addr is invalid")?;
    if config.storage.sqlite_path.trim().is_empty() {
        return Err(anyhow!("storage.sqlite_path is required"));
    }
    if config.storage.event_retention.is_zero() {
        return Err(anyhow!("storage.event_retention must be greater than zero"));
    }
    validate_rate_limit_bucket("runtime", "reading", &config.runtime.rate_limit.reading)?;
    validate_rate_limit_bucket("runtime", "posting", &config.runtime.rate_limit.posting)?;

    let mut names = HashSet::new();
    let mut env_names = HashSet::new();
    let mut public_tripcodes = HashSet::new();
    for integration in &config.integrations {
        if !valid_integration_name(&integration.name) {
            return Err(anyhow!(
                "integration name {} is invalid; use 1-64 ASCII letters, digits, underscores, or hyphens",
                integration.name
            ));
        }
        if !names.insert(integration.name.as_str()) {
            return Err(anyhow!("duplicate integration name {}", integration.name));
        }
        if !env_names.insert(env_safe_name(&integration.name)) {
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
        if let Some(webhook) = &integration.webhook {
            Url::parse(&webhook.url).with_context(|| {
                format!(
                    "integration {} webhook url must be absolute",
                    integration.name
                )
            })?;
        }
        if let Some(posting) = &integration.posting {
            validate_posting(&integration.name, posting)?;
            if !public_tripcodes.insert(posting.public_tripcode.as_str()) {
                return Err(anyhow!(
                    "integration {} posting public_tripcode conflicts with another integration",
                    integration.name
                ));
            }
        }
        validate_rate_limit(&integration.name, &integration.rate_limit)?;
    }
    Ok(())
}

fn validate_posting(integration_name: &str, posting: &FilePosting) -> Result<()> {
    if matches!(posting.display_name.as_deref(), Some(name) if name.trim().is_empty()) {
        return Err(anyhow!(
            "integration {integration_name} posting display_name must not be empty"
        ));
    }
    if !valid_public_tripcode(&posting.public_tripcode) {
        return Err(anyhow!(
            "integration {integration_name} posting public_tripcode must be the 12-character public secure tripcode emitted by ptchan, including its !! prefix"
        ));
    }
    Ok(())
}

fn validate_rate_limit(integration_name: &str, rate_limit: &RateLimitConfig) -> Result<()> {
    let owner = format!("integration {integration_name}");
    validate_rate_limit_bucket(&owner, "reading", &rate_limit.reading)?;
    validate_rate_limit_bucket(&owner, "posting", &rate_limit.posting)
}

fn validate_rate_limit_bucket(
    owner: &str,
    capability: &str,
    rate_limit: &RateLimitBucketConfig,
) -> Result<()> {
    if rate_limit.requests == 0 {
        return Err(anyhow!(
            "{owner} rate_limit.{capability} requests must be greater than zero"
        ));
    }
    if rate_limit.window.is_zero() {
        return Err(anyhow!(
            "{owner} rate_limit.{capability} window must be greater than zero"
        ));
    }
    if rate_limit.burst == 0 || rate_limit.burst > rate_limit.requests {
        return Err(anyhow!(
            "{owner} rate_limit.{capability} burst must be positive and no greater than requests"
        ));
    }
    if (rate_limit.window / rate_limit.requests).is_zero() {
        return Err(anyhow!(
            "{owner} rate_limit.{capability} window is too small for the configured request count"
        ));
    }
    Ok(())
}

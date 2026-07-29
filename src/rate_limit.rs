use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use anyhow::{anyhow, Context, Result};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};

use crate::config::{IntegrationConfig, RateLimitBucketConfig, RuntimeRateLimitConfig};

#[derive(Clone)]
pub(crate) struct RateLimiters {
    integrations: Arc<HashMap<String, CapabilityRateLimiters>>,
    global: CapabilityRateLimiters,
}

#[derive(Clone)]
struct CapabilityRateLimiters {
    reading: Arc<DefaultDirectRateLimiter>,
    posting: Arc<DefaultDirectRateLimiter>,
}

pub(crate) enum RateLimitRejection {
    Integration,
    Global,
}

impl RateLimitRejection {
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Integration => "integration",
            Self::Global => "global",
        }
    }
}

impl RateLimiters {
    pub(crate) fn new(
        integrations: &[IntegrationConfig],
        global: &RuntimeRateLimitConfig,
    ) -> Result<Self> {
        let integrations = integrations
            .iter()
            .map(|integration| {
                let limiters = CapabilityRateLimiters::new(
                    &integration.rate_limit.reading,
                    &integration.rate_limit.posting,
                )
                .with_context(|| {
                    format!("create rate limiters for integration {}", integration.name)
                })?;
                Ok((integration.name.clone(), limiters))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(Self {
            integrations: Arc::new(integrations),
            global: CapabilityRateLimiters::new(&global.reading, &global.posting)
                .context("create global rate limiters")?,
        })
    }

    pub(crate) fn check_reading(&self, integration: &str) -> Result<(), RateLimitRejection> {
        self.check(integration, Capability::Reading)
    }

    pub(crate) fn check_posting(&self, integration: &str) -> Result<(), RateLimitRejection> {
        self.check(integration, Capability::Posting)
    }

    fn check(&self, integration: &str, capability: Capability) -> Result<(), RateLimitRejection> {
        if self
            .integrations
            .get(integration)
            .is_some_and(|limiters| limiters.check(capability).is_err())
        {
            return Err(RateLimitRejection::Integration);
        }
        if self.global.check(capability).is_err() {
            return Err(RateLimitRejection::Global);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Capability {
    Reading,
    Posting,
}

impl CapabilityRateLimiters {
    fn new(reading: &RateLimitBucketConfig, posting: &RateLimitBucketConfig) -> Result<Self> {
        Ok(Self {
            reading: Arc::new(limiter(reading).context("create reading rate limiter")?),
            posting: Arc::new(limiter(posting).context("create posting rate limiter")?),
        })
    }

    fn check(&self, capability: Capability) -> Result<(), ()> {
        let limiter = match capability {
            Capability::Reading => &self.reading,
            Capability::Posting => &self.posting,
        };
        limiter.check().map_err(|_| ())
    }
}

fn limiter(cfg: &RateLimitBucketConfig) -> Result<DefaultDirectRateLimiter> {
    let requests = NonZeroU32::new(cfg.requests)
        .ok_or_else(|| anyhow!("rate limit requests must be greater than zero"))?;
    let burst = NonZeroU32::new(cfg.burst)
        .ok_or_else(|| anyhow!("rate limit burst must be greater than zero"))?;
    let replenish_1_per = cfg.window / requests.get();
    let quota = Quota::with_period(replenish_1_per)
        .ok_or_else(|| anyhow!("rate limit window per request is too small"))?
        .allow_burst(burst);
    Ok(RateLimiter::direct(quota))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::RateLimitConfig;

    #[test]
    fn limits_each_integration_independently() {
        let limiters = RateLimiters::new(
            &[integration("alpha"), integration("beta")],
            &runtime_limits(),
        )
        .unwrap();

        assert!(limiters.check_reading("alpha").is_ok());
        assert!(matches!(
            limiters.check_reading("alpha"),
            Err(RateLimitRejection::Integration)
        ));
        assert!(limiters.check_reading("beta").is_ok());
    }

    #[test]
    fn limits_reading_and_posting_independently() {
        let limiters = RateLimiters::new(&[integration("alpha")], &runtime_limits()).unwrap();

        assert!(limiters.check_reading("alpha").is_ok());
        assert!(limiters.check_reading("alpha").is_err());
        assert!(limiters.check_posting("alpha").is_ok());
    }

    #[test]
    fn applies_global_limits_across_integrations() {
        let limiters = RateLimiters::new(
            &[integration("alpha"), integration("beta")],
            &RuntimeRateLimitConfig {
                reading: RateLimitBucketConfig {
                    requests: 1,
                    window: Duration::from_secs(60),
                    burst: 1,
                },
                posting: RateLimitBucketConfig {
                    requests: 10,
                    window: Duration::from_secs(60),
                    burst: 10,
                },
            },
        )
        .unwrap();

        assert!(limiters.check_reading("alpha").is_ok());
        assert!(matches!(
            limiters.check_reading("beta"),
            Err(RateLimitRejection::Global)
        ));
    }

    #[test]
    fn unknown_integrations_are_not_limited() {
        let limiters = RateLimiters::new(&[], &runtime_limits()).unwrap();

        assert!(limiters.check_reading("unknown").is_ok());
        assert!(limiters.check_posting("unknown").is_ok());
    }

    fn integration(name: &str) -> IntegrationConfig {
        IntegrationConfig {
            name: name.to_string(),
            allowed_boards: Vec::new(),
            reading: true,
            rate_limit: RateLimitConfig {
                reading: RateLimitBucketConfig {
                    requests: 1,
                    window: Duration::from_secs(60),
                    burst: 1,
                },
                posting: RateLimitBucketConfig {
                    requests: 1,
                    window: Duration::from_secs(60),
                    burst: 1,
                },
            },
            secret: "secret".to_string(),
        }
    }

    fn runtime_limits() -> RuntimeRateLimitConfig {
        RuntimeRateLimitConfig {
            reading: RateLimitBucketConfig {
                requests: 10,
                window: Duration::from_secs(60),
                burst: 10,
            },
            posting: RateLimitBucketConfig {
                requests: 10,
                window: Duration::from_secs(60),
                burst: 10,
            },
        }
    }
}

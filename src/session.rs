use std::{
    cmp,
    sync::{Arc, PoisonError, RwLock},
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use cookie_store::{CookieExpiration, CookieStore, RawCookie};
use reqwest::{header::HeaderValue, Client, Url};
use serde_json::Value;
use tokio::{sync::watch, time};
use tracing::{debug, info, warn};

use crate::{
    config::{self, PtchanConfig},
    metrics,
    runtime::Status,
};

const SESSION_REFRESH_RETRY_INTERVAL: Duration = Duration::from_mins(1);
const SESSION_REFRESH_MAX_SAFETY_MARGIN: Duration = Duration::from_hours(1);

pub(crate) struct SessionCookie {
    store: RwLock<CookieStore>,
    url: Url,
}

impl SessionCookie {
    pub(crate) fn new(value: &str, base_url: &str) -> Result<Self> {
        let url = Url::parse(base_url).context("parse ptchan base url for cookie store")?;
        let mut store = CookieStore::new();
        for cookie in RawCookie::split_parse(value) {
            let cookie = cookie.context("parse management Cookie header")?;
            store
                .insert_raw(&cookie, &url)
                .context("store management cookie")?;
        }
        if store.get_request_values(&url).next().is_none() {
            anyhow::bail!("management Cookie header did not contain any cookies");
        }
        Ok(Self {
            store: RwLock::new(store),
            url,
        })
    }

    pub(crate) fn get(&self) -> String {
        self.store
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get_request_values(&self.url)
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.store
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .matches(&self.url)
            .into_iter()
            .filter_map(|cookie| match cookie.expires {
                CookieExpiration::AtUtc(expires_at) => {
                    DateTime::from_timestamp(expires_at.unix_timestamp(), 0)
                }
                CookieExpiration::SessionEnd => None,
            })
            .min()
    }

    fn merge(&self, updates: &[HeaderValue]) -> Result<bool> {
        let mut store = self.store.write().unwrap_or_else(PoisonError::into_inner);
        for value in updates {
            store
                .parse(
                    value
                        .to_str()
                        .context("ptchan Set-Cookie header was not text")?,
                    &self.url,
                )
                .context("parse ptchan Set-Cookie header")?;
        }
        Ok(!updates.is_empty())
    }
}

pub(crate) async fn refresh_loop(
    cfg: PtchanConfig,
    cookie: Arc<SessionCookie>,
    status: Arc<Status>,
    mut shutdown: watch::Receiver<bool>,
) {
    let client = match Client::builder()
        .user_agent(config::gateway_user_agent())
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            status.set_auth_healthy(false);
            warn!(error = %err, "failed to build ptchan refresh client");
            return;
        }
    };
    loop {
        if *shutdown.borrow() {
            return;
        }
        let sleep_for = match refresh_once(&client, &cfg, &cookie).await {
            Ok(updated) => {
                let expires_at = cookie.expires_at();
                match next_refresh_delay(&cookie, Utc::now()) {
                    Ok(sleep_for) => {
                        status.set_auth_healthy(true);
                        if let Some(expires_at) = expires_at {
                            metrics::SESSION_EXPIRES_AT_SECONDS.set(expires_at.timestamp());
                        }
                        metrics::SESSION_REFRESH
                            .with_label_values(&["success"])
                            .inc();
                        if updated {
                            info!(?expires_at, ?sleep_for, "ptchan session cookie refreshed");
                        } else {
                            info!(?expires_at, ?sleep_for, "ptchan session refresh ok");
                        }
                        sleep_for
                    }
                    Err(err) => refresh_failed(&status, &cookie, &err),
                }
            }
            Err(err) => refresh_failed(&status, &cookie, &err),
        };
        tokio::select! {
            _ = shutdown.changed() => {}
            () = time::sleep(sleep_for) => {}
        }
    }
}

fn refresh_failed(status: &Status, cookie: &SessionCookie, err: &anyhow::Error) -> Duration {
    status.set_auth_healthy(false);
    metrics::SESSION_EXPIRES_AT_SECONDS.set(0);
    metrics::SESSION_REFRESH
        .with_label_values(&["failure"])
        .inc();
    warn!(
        error = %err,
        auth_healthy = false,
        retry_in = ?SESSION_REFRESH_RETRY_INTERVAL,
        expires_at = ?cookie.expires_at(),
        "ptchan session refresh failed"
    );
    SESSION_REFRESH_RETRY_INTERVAL
}

async fn refresh_once(
    client: &Client,
    cfg: &PtchanConfig,
    cookie: &SessionCookie,
) -> anyhow::Result<bool> {
    let url = format!(
        "{}/globalmanage/recent.json",
        cfg.base_url.trim_end_matches('/')
    );
    let response = client
        .get(url)
        .header("accept", "application/json")
        .header("cookie", cookie.get())
        .send()
        .await?;
    let status = response.status();
    debug!(%status, "ptchan session refresh response received");
    if !status.is_success() {
        anyhow::bail!("refresh status {status}");
    }
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let body = response
        .text()
        .await
        .context("read refresh response body")?;
    let body = serde_json::from_str::<Value>(&body)
        .context("refresh response was not valid management JSON")?;
    validate_recent_json(&body).context("refresh response was not management recent JSON")?;

    if !set_cookies.is_empty() {
        debug!(
            set_cookie_count = set_cookies.len(),
            "ptchan session cookie update accepted"
        );
        let cookie_changed = cookie.merge(&set_cookies)?;
        ensure_cookie_has_expiry(cookie)?;
        return Ok(cookie_changed);
    }
    ensure_cookie_has_expiry(cookie)?;
    Ok(false)
}

fn ensure_cookie_has_expiry(cookie: &SessionCookie) -> anyhow::Result<()> {
    if cookie.expires_at().is_some() {
        Ok(())
    } else {
        anyhow::bail!("validated management session has no known expiry")
    }
}

fn validate_recent_json(value: &Value) -> anyhow::Result<()> {
    value
        .as_array()
        .context("management recent response was not an array")?;
    Ok(())
}

fn next_refresh_delay(cookie: &SessionCookie, now: DateTime<Utc>) -> anyhow::Result<Duration> {
    let expires_at = cookie
        .expires_at()
        .context("validated management session has no known expiry")?;
    let remaining = expires_at
        .signed_duration_since(now)
        .to_std()
        .unwrap_or(Duration::ZERO);
    let safety_margin = cmp::min(remaining / 5, SESSION_REFRESH_MAX_SAFETY_MARGIN);
    Ok(remaining.saturating_sub(safety_margin))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn set_cookie_attributes_are_not_sent_as_cookies() {
        let cookie = session_cookie("session=old");
        cookie
            .merge(&[HeaderValue::from_static(
                "session=s%3Aabc; Path=/; Expires=Mon, 22 Jul 2030 16:28:48 GMT; HttpOnly; Secure; SameSite=Lax",
            )])
            .unwrap();

        assert_eq!(cookie.get(), "session=s%3Aabc");
        assert_eq!(
            cookie.expires_at(),
            Some(Utc.with_ymd_and_hms(2030, 7, 22, 16, 28, 48).unwrap())
        );
    }

    #[test]
    fn max_age_overrides_expires() {
        let cookie = session_cookie("session=old");
        let before = Utc::now();
        cookie
            .merge(&[HeaderValue::from_static(
                "session=s%3Aabc; Expires=Mon, 22 Jul 2030 16:28:48 GMT; Max-Age=120; Path=/",
            )])
            .unwrap();
        let after = Utc::now();
        let expires_at = cookie.expires_at().unwrap();

        assert!(expires_at >= before + chrono::Duration::seconds(119));
        assert!(expires_at <= after + chrono::Duration::seconds(121));
    }

    #[test]
    fn refresh_cookie_merge_preserves_existing_cookies() {
        let cookie = session_cookie("session=s%3Aold; aux=keep");
        let changed = cookie
            .merge(&[HeaderValue::from_static(
                "theme=dark; Path=/; HttpOnly; SameSite=Lax",
            )])
            .unwrap();

        assert!(changed);
        assert_eq!(
            cookie_pairs(&cookie),
            ["aux=keep", "session=s%3Aold", "theme=dark"]
        );
    }

    #[test]
    fn refresh_cookie_merge_replaces_cookie_by_name() {
        let cookie = session_cookie("session=s%3Aold; aux=keep");
        let changed = cookie
            .merge(&[HeaderValue::from_static(
                "session=s%3Anew; Path=/; Expires=Mon, 22 Jul 2030 12:00:00 GMT; HttpOnly",
            )])
            .unwrap();

        assert!(changed);
        assert_eq!(cookie_pairs(&cookie), ["aux=keep", "session=s%3Anew"]);
        assert_eq!(
            cookie.expires_at(),
            Some(Utc.with_ymd_and_hms(2030, 7, 22, 12, 0, 0).unwrap())
        );
    }

    #[test]
    fn refresh_cookie_merge_removes_expired_cookie_by_name() {
        let cookie = session_cookie("session=s%3Aold; aux=remove");
        let changed = cookie
            .merge(&[HeaderValue::from_static(
                "aux=deleted; Path=/; Expires=Wed, 01 Jan 2020 00:00:00 GMT",
            )])
            .unwrap();

        assert!(changed);
        assert_eq!(cookie.get(), "session=s%3Aold");
    }

    #[test]
    fn refresh_delay_uses_cookie_expiry_before_fallback() {
        let now = Utc.with_ymd_and_hms(2030, 7, 19, 12, 0, 0).unwrap();
        let cookie = session_cookie("session=s%3Aabc");
        cookie
            .merge(&[HeaderValue::from_static(
                "session=s%3Aabc; Path=/; Expires=Mon, 22 Jul 2030 12:00:00 GMT",
            )])
            .unwrap();

        assert_eq!(
            next_refresh_delay(&cookie, now).unwrap(),
            Duration::from_hours((3 * 24) - 1)
        );
    }

    #[test]
    fn refresh_delay_rejects_cookies_without_expiry() {
        let now = Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap();
        let cookie = session_cookie("session=s%3Aabc");

        assert!(next_refresh_delay(&cookie, now).is_err());
        assert!(ensure_cookie_has_expiry(&cookie).is_err());
    }

    #[test]
    fn recent_json_validation_accepts_recent_post_arrays() {
        validate_recent_json(&json!([
            {
                "board": "test",
                "postId": 123
            }
        ]))
        .unwrap();
        validate_recent_json(&json!([])).unwrap();
    }

    #[test]
    fn recent_json_validation_rejects_login_or_wrong_shape() {
        assert!(validate_recent_json(&json!({"login": true})).is_err());
    }

    fn session_cookie(value: &str) -> SessionCookie {
        SessionCookie::new(value, "https://ptchan.test").unwrap()
    }

    fn cookie_pairs(cookie: &SessionCookie) -> Vec<String> {
        let mut pairs = cookie
            .get()
            .split("; ")
            .map(str::to_string)
            .collect::<Vec<_>>();
        pairs.sort();
        pairs
    }
}

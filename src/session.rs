use std::{
    cmp,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::Value;
use tokio::{sync::watch, time};
use tracing::{debug, info, warn};

use crate::{config::PtchanConfig, metrics, runtime::Status};

const SESSION_REFRESH_RETRY_INTERVAL: Duration = Duration::from_mins(1);
const SESSION_REFRESH_MAX_SAFETY_MARGIN: Duration = Duration::from_hours(1);

pub(crate) struct SessionCookie {
    cookies: RwLock<Vec<StoredCookie>>,
}

#[derive(Clone)]
struct StoredCookie {
    name: String,
    pair: String,
    expires_at: Option<DateTime<Utc>>,
}

impl SessionCookie {
    pub(crate) fn new(value: &str) -> Self {
        let parsed = parse_cookie_header(value, Utc::now());
        Self {
            cookies: RwLock::new(
                parsed
                    .pairs
                    .into_iter()
                    .map(|pair| StoredCookie {
                        name: pair.name,
                        pair: pair.value,
                        expires_at: parsed.expires_at,
                    })
                    .collect(),
            ),
        }
    }

    pub(crate) fn get(&self) -> String {
        self.cookies
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|cookie| cookie.pair.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    }

    fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.cookies
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(|cookie| cookie.expires_at)
            .min()
    }

    fn merge(&self, updates: Vec<ParsedCookieHeader>, now: DateTime<Utc>) -> bool {
        let mut cookies = self
            .cookies
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut changed = false;
        for update in updates {
            for pair in update.pairs {
                let position = cookies
                    .iter()
                    .position(|cookie| cookie.name.eq_ignore_ascii_case(&pair.name));
                if update
                    .expires_at
                    .is_some_and(|expires_at| expires_at <= now)
                {
                    if let Some(position) = position {
                        cookies.remove(position);
                        changed = true;
                    }
                    continue;
                }
                let cookie = StoredCookie {
                    name: pair.name,
                    pair: pair.value,
                    expires_at: update.expires_at,
                };
                if let Some(position) = position {
                    if cookies[position].pair != cookie.pair
                        || cookies[position].expires_at != cookie.expires_at
                    {
                        cookies[position] = cookie;
                        changed = true;
                    }
                } else {
                    cookies.push(cookie);
                    changed = true;
                }
            }
        }
        changed
    }
}

struct ParsedCookieHeader {
    value: String,
    pairs: Vec<ParsedCookie>,
    expires_at: Option<DateTime<Utc>>,
}

struct ParsedCookie {
    name: String,
    value: String,
}

fn parse_cookie_header(value: &str, now: DateTime<Utc>) -> ParsedCookieHeader {
    let mut pairs = Vec::new();
    let mut max_age_expires_at = None;
    let mut expires_at = None;
    for part in value
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let Some((name, value)) = part.split_once('=') else {
            continue;
        };
        match name.to_ascii_lowercase().as_str() {
            "domain" | "path" | "priority" | "samesite" => {}
            "expires" => expires_at = parse_cookie_expires(value),
            "max-age" => max_age_expires_at = parse_cookie_max_age(value, now),
            _ => pairs.push(ParsedCookie {
                name: name.to_string(),
                value: part.to_string(),
            }),
        }
    }
    let value = pairs
        .iter()
        .map(|pair| pair.value.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    ParsedCookieHeader {
        value,
        pairs,
        expires_at: max_age_expires_at.or(expires_at),
    }
}

fn parse_cookie_expires(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|date| date.with_timezone(&Utc))
}

fn parse_cookie_max_age(value: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let seconds = value.parse::<i64>().ok()?;
    Some(now + chrono::Duration::seconds(seconds))
}

pub(crate) async fn refresh_loop(
    cfg: PtchanConfig,
    cookie: Arc<SessionCookie>,
    status: Arc<Status>,
    mut shutdown: watch::Receiver<bool>,
) {
    let client = match Client::builder().user_agent(cfg.user_agent.clone()).build() {
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
        .map(std::borrow::ToOwned::to_owned)
        .collect::<Vec<_>>();
    let body = response
        .text()
        .await
        .context("read refresh response body")?;
    let body = serde_json::from_str::<Value>(&body)
        .context("refresh response was not valid management JSON")?;
    validate_recent_json(&body).context("refresh response was not management recent JSON")?;

    let mut updates = Vec::new();
    let now = Utc::now();
    for value in &set_cookies {
        let value = value.to_str()?;
        let parsed = parse_cookie_header(value, now);
        if !parsed.value.is_empty() {
            updates.push(parsed);
        }
    }
    if !updates.is_empty() {
        debug!(
            set_cookie_count = updates.len(),
            "ptchan session cookie update accepted"
        );
        let cookie_changed = cookie.merge(updates, now);
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
    fn strips_set_cookie_attributes_and_keeps_expiry() {
        let now = Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap();
        let parsed = parse_cookie_header(
            "session=s%3Aabc; Path=/; Expires=Wed, 22 Jul 2026 16:28:48 GMT; HttpOnly; Secure; SameSite=Lax",
            now,
        );

        assert_eq!(parsed.value, "session=s%3Aabc");
        assert_eq!(
            parsed.expires_at,
            Some(Utc.with_ymd_and_hms(2026, 7, 22, 16, 28, 48).unwrap())
        );
    }

    #[test]
    fn max_age_sets_expiry_relative_to_refresh_time() {
        let now = Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap();
        let parsed = parse_cookie_header(
            "session=s%3Aabc; Expires=Wed, 22 Jul 2026 16:28:48 GMT; Max-Age=120; Path=/",
            now,
        );

        assert_eq!(parsed.value, "session=s%3Aabc");
        assert_eq!(
            parsed.expires_at,
            Some(Utc.with_ymd_and_hms(2026, 7, 19, 12, 2, 0).unwrap())
        );
    }

    #[test]
    fn refresh_cookie_merge_preserves_existing_cookies() {
        let now = Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap();
        let cookie = SessionCookie::new("session=s%3Aold; aux=keep");
        let changed = cookie.merge(
            vec![parse_cookie_header(
                "theme=dark; Path=/; HttpOnly; SameSite=Lax",
                now,
            )],
            now,
        );

        assert!(changed);
        assert_eq!(cookie.get(), "session=s%3Aold; aux=keep; theme=dark");
    }

    #[test]
    fn refresh_cookie_merge_replaces_cookie_by_name() {
        let now = Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap();
        let cookie = SessionCookie::new("session=s%3Aold; aux=keep");
        let changed = cookie.merge(
            vec![parse_cookie_header(
                "session=s%3Anew; Path=/; Expires=Wed, 22 Jul 2026 12:00:00 GMT; HttpOnly",
                now,
            )],
            now,
        );

        assert!(changed);
        assert_eq!(cookie.get(), "session=s%3Anew; aux=keep");
        assert_eq!(
            cookie.expires_at(),
            Some(Utc.with_ymd_and_hms(2026, 7, 22, 12, 0, 0).unwrap())
        );
    }

    #[test]
    fn refresh_cookie_merge_removes_expired_cookie_by_name() {
        let now = Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap();
        let cookie = SessionCookie::new("session=s%3Aold; aux=remove");
        let changed = cookie.merge(
            vec![parse_cookie_header(
                "aux=deleted; Path=/; Expires=Wed, 01 Jan 2020 00:00:00 GMT",
                now,
            )],
            now,
        );

        assert!(changed);
        assert_eq!(cookie.get(), "session=s%3Aold");
    }

    #[test]
    fn refresh_delay_uses_cookie_expiry_before_fallback() {
        let now = Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap();
        let cookie = SessionCookie::new("session=s%3Aabc; Expires=Wed, 22 Jul 2026 12:00:00 GMT");

        assert_eq!(
            next_refresh_delay(&cookie, now).unwrap(),
            Duration::from_hours((3 * 24) - 1)
        );
    }

    #[test]
    fn refresh_delay_rejects_cookies_without_expiry() {
        let now = Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap();
        let cookie = SessionCookie::new("session=s%3Aabc");

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
}

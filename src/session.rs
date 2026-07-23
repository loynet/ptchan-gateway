use std::{
    cmp,
    sync::{Arc, RwLock},
    time::Duration,
};

use chrono::{DateTime, Utc};
use reqwest::Client;
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
            .expect("cookie lock poisoned")
            .iter()
            .map(|cookie| cookie.pair.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    }

    fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.cookies
            .read()
            .expect("cookie lock poisoned")
            .iter()
            .filter_map(|cookie| cookie.expires_at)
            .min()
    }

    fn is_usable_at(&self, now: DateTime<Utc>) -> bool {
        self.expires_at().is_none_or(|expires_at| expires_at > now)
    }

    fn merge(&self, updates: Vec<ParsedCookieHeader>, now: DateTime<Utc>) -> bool {
        let mut cookies = self.cookies.write().expect("cookie lock poisoned");
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
                status.set_auth_healthy(true);
                metrics::SESSION_REFRESH
                    .with_label_values(&["success"])
                    .inc();
                let sleep_for =
                    next_refresh_delay(&cookie, cfg.session_refresh_fallback_interval, Utc::now());
                let expires_at = cookie.expires_at();
                if updated {
                    info!(?expires_at, ?sleep_for, "ptchan session cookie refreshed");
                } else {
                    info!(?expires_at, ?sleep_for, "ptchan session refresh ok");
                }
                sleep_for
            }
            Err(err) => {
                let auth_healthy = status.auth_healthy() && cookie.is_usable_at(Utc::now());
                status.set_auth_healthy(auth_healthy);
                metrics::SESSION_REFRESH
                    .with_label_values(&["failure"])
                    .inc();
                warn!(
                    error = %err,
                    auth_healthy,
                    retry_in = ?SESSION_REFRESH_RETRY_INTERVAL,
                    expires_at = ?cookie.expires_at(),
                    "ptchan session refresh failed"
                );
                SESSION_REFRESH_RETRY_INTERVAL
            }
        };
        tokio::select! {
            _ = shutdown.changed() => {}
            () = time::sleep(sleep_for) => {}
        }
    }
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
    let set_cookies = response.headers().get_all("set-cookie");
    let mut updates = Vec::new();
    let now = Utc::now();
    for value in set_cookies {
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
        return Ok(cookie.merge(updates, now));
    }
    Ok(false)
}

fn next_refresh_delay(cookie: &SessionCookie, fallback: Duration, now: DateTime<Utc>) -> Duration {
    let Some(expires_at) = cookie.expires_at() else {
        return fallback;
    };
    let remaining = expires_at
        .signed_duration_since(now)
        .to_std()
        .unwrap_or(Duration::ZERO);
    let safety_margin = cmp::min(remaining / 5, SESSION_REFRESH_MAX_SAFETY_MARGIN);
    remaining.saturating_sub(safety_margin)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

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
            next_refresh_delay(&cookie, Duration::from_hours(12), now),
            Duration::from_hours((3 * 24) - 1)
        );
    }

    #[test]
    fn refresh_delay_falls_back_without_expiry() {
        let now = Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap();
        let cookie = SessionCookie::new("session=s%3Aabc");

        assert_eq!(
            next_refresh_delay(&cookie, Duration::from_hours(12), now),
            Duration::from_hours(12)
        );
    }
}

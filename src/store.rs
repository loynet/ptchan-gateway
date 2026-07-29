use std::{fs, path::Path, time::Duration};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio_rusqlite::{
    rusqlite::{self, params, OptionalExtension},
    Connection,
};

use crate::{contract::WebhookEvent, metrics};

#[derive(Clone)]
pub(crate) struct Store {
    connection: Connection,
}

#[derive(Debug)]
pub(crate) struct PendingDelivery {
    pub(crate) event_id: String,
    pub(crate) webhook: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) attempts: i64,
}

#[derive(Clone)]
pub(crate) struct EventDelivery {
    pub(crate) webhook: String,
    pub(crate) payload: Vec<u8>,
}

impl Store {
    pub(crate) async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        let connection = Connection::open(path)
            .await
            .with_context(|| format!("open sqlite {}", path.display()))?;
        let store = Self { connection };
        store
            .call(|conn| {
                conn.pragma_update(None, "journal_mode", "WAL")
                    .context("enable sqlite wal")?;
                conn.pragma_update(None, "foreign_keys", "ON")
                    .context("enable sqlite foreign keys")?;
                Ok(())
            })
            .await?;
        Ok(store)
    }

    pub(crate) async fn migrate(&self) -> Result<()> {
        self.call(|conn| {
            conn.execute_batch(
                r"
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    board TEXT NOT NULL,
                    thread_id INTEGER NOT NULL,
                    post_id INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS deliveries (
                    event_id TEXT NOT NULL,
                    webhook TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT NOT NULL,
                    payload BLOB,
                    last_error TEXT,
                    delivered_at TEXT,
                    PRIMARY KEY (event_id, webhook),
                    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS deliveries_pending_idx
                    ON deliveries(status, next_attempt_at);

                DROP TABLE IF EXISTS pending_produced_posts;
                DROP TABLE IF EXISTS produced_posts;
                ",
            )
            .context("create schema")?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn create_event(
        &self,
        event: &WebhookEvent,
        payload: &[u8],
        deliveries: &[EventDelivery],
    ) -> Result<bool> {
        let event_id = event.event_id.clone();
        let kind = event.kind.as_str().to_string();
        let board = event.post.board.clone();
        let thread_id = event.post.thread_id;
        let post_id = event.post.id;
        let payload = payload.to_vec();
        let created_at = event.observed_at.to_rfc3339();
        let deliveries = deliveries.to_vec();
        self.call(move |conn| {
            let tx = conn.transaction().context("begin event transaction")?;
            let inserted = tx
                .execute(
                    "INSERT OR IGNORE INTO events (id, kind, board, thread_id, post_id, payload, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        event_id,
                        kind,
                        board,
                        thread_id,
                        post_id,
                        payload,
                        created_at,
                    ],
                )
                .context("insert event")?;
            if inserted == 1 {
                for delivery in deliveries {
                    tx.execute(
                        "INSERT INTO deliveries (event_id, webhook, status, attempts, next_attempt_at, payload)
                         VALUES (?1, ?2, 'pending', 0, ?3, ?4)",
                        params![
                            event_id,
                            delivery.webhook,
                            created_at,
                            &delivery.payload,
                        ],
                    )
                    .with_context(|| {
                        format!("insert delivery for webhook {}", delivery.webhook)
                    })?;
                }
            }
            tx.commit().context("commit event transaction")?;
            Ok(inserted == 1)
        })
        .await
    }

    pub(crate) async fn pending_deliveries(
        &self,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<PendingDelivery>> {
        self.call(move |conn| {
            let limit = i64::try_from(limit).context("pending delivery limit is too large")?;
            let mut stmt = conn
                .prepare(
                    "SELECT d.event_id, d.webhook, COALESCE(d.payload, e.payload), d.attempts
                     FROM deliveries d
                     JOIN events e ON e.id = d.event_id
                     WHERE d.status = 'pending' AND d.next_attempt_at <= ?1
                     ORDER BY d.next_attempt_at, d.event_id
                     LIMIT ?2",
                )
                .context("prepare pending deliveries")?;
            let rows = stmt
                .query_map(params![now.to_rfc3339(), limit], |row| {
                    Ok(PendingDelivery {
                        event_id: row.get(0)?,
                        webhook: row.get(1)?,
                        payload: row.get(2)?,
                        attempts: row.get(3)?,
                    })
                })
                .context("query pending deliveries")?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .context("collect pending deliveries")
        })
        .await
    }

    pub(crate) async fn mark_delivered(
        &self,
        event_id: &str,
        webhook: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let event_id = event_id.to_string();
        let webhook = webhook.to_string();
        self.call(move |conn| {
            conn.execute(
                "UPDATE deliveries SET status = 'delivered', delivered_at = ?1 WHERE event_id = ?2 AND webhook = ?3",
                params![now.to_rfc3339(), event_id, webhook],
            )
            .context("mark delivery delivered")?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn mark_failed(
        &self,
        event_id: &str,
        webhook: &str,
        error: &str,
        attempts: i64,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<()> {
        let event_id = event_id.to_string();
        let webhook = webhook.to_string();
        let error = truncate(error, 500);
        self.call(move |conn| {
            conn.execute(
                "UPDATE deliveries
                 SET attempts = ?1, next_attempt_at = ?2, last_error = ?3
                 WHERE event_id = ?4 AND webhook = ?5",
                params![
                    attempts,
                    next_attempt_at.to_rfc3339(),
                    error,
                    event_id,
                    webhook
                ],
            )
            .context("mark delivery failed")?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn pending_count(&self) -> Result<i64> {
        self.call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM deliveries WHERE status = 'pending'",
                [],
                |row| row.get(0),
            )
            .context("count pending deliveries")
        })
        .await
    }

    pub(crate) async fn pending_counts_by_webhook(&self) -> Result<Vec<(String, i64)>> {
        self.call(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT webhook, COUNT(*)
                     FROM deliveries
                     WHERE status = 'pending'
                     GROUP BY webhook",
                )
                .context("prepare pending delivery counts by webhook")?;
            let rows = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .context("query pending delivery counts by webhook")?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .context("collect pending delivery counts by webhook")
        })
        .await
    }

    pub(crate) async fn oldest_pending_age(&self, now: DateTime<Utc>) -> Result<Option<Duration>> {
        self.call(move |conn| {
            let oldest: Option<String> = conn
                .query_row(
                    "SELECT MIN(e.created_at)
                     FROM deliveries d
                     JOIN events e ON e.id = d.event_id
                     WHERE d.status = 'pending'",
                    [],
                    |row| row.get(0),
                )
                .context("query oldest pending delivery")?;
            let Some(oldest) = oldest else {
                return Ok(None);
            };
            let oldest = DateTime::parse_from_rfc3339(&oldest)
                .context("parse oldest pending event time")?
                .with_timezone(&Utc);
            Ok(Some(
                now.signed_duration_since(oldest)
                    .to_std()
                    .unwrap_or(Duration::ZERO),
            ))
        })
        .await
    }

    pub(crate) async fn next_delivery_delay(&self, now: DateTime<Utc>) -> Result<Option<Duration>> {
        self.call(move |conn| {
            let next: Option<String> = conn
                .query_row(
                    "SELECT next_attempt_at FROM deliveries WHERE status = 'pending' ORDER BY next_attempt_at LIMIT 1",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .context("query next pending delivery")?;
            let Some(next) = next else {
                return Ok(None);
            };
            let next = DateTime::parse_from_rfc3339(&next)
                .context("parse next pending delivery time")?
                .with_timezone(&Utc);
            Ok(Some(
                next.signed_duration_since(now)
                    .to_std()
                    .unwrap_or(Duration::ZERO),
            ))
        })
        .await
    }

    pub(crate) async fn prune_delivered_events(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.call(move |conn| {
            conn.execute(
                "DELETE FROM events
                 WHERE created_at < ?1
                 AND NOT EXISTS (
                     SELECT 1 FROM deliveries
                     WHERE deliveries.event_id = events.id
                     AND deliveries.status != 'delivered'
                 )",
                params![cutoff.to_rfc3339()],
            )
            .context("prune delivered events")
        })
        .await
    }

    pub(crate) async fn is_ready(&self) -> bool {
        self.call(|conn| {
            let value: Option<i64> = conn
                .query_row("SELECT 1", [], |row| row.get(0))
                .optional()?;
            Ok(value == Some(1))
        })
        .await
        .unwrap_or(false)
    }

    async fn call<T>(
        &self,
        operation: impl FnOnce(&mut tokio_rusqlite::rusqlite::Connection) -> Result<T> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        match self.connection.call_raw(operation).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(err)) => {
                metrics::SQLITE_ERRORS.inc();
                Err(err)
            }
            Err(err) => {
                metrics::SQLITE_ERRORS.inc();
                Err(anyhow::anyhow!(err))
            }
        }
    }
}

pub(crate) fn delivery_backoff(attempts: i64) -> Duration {
    let exp = u32::try_from(attempts.clamp(1, 8)).unwrap_or(8);
    Duration::from_secs((2_u64.pow(exp)).min(300))
}

fn truncate(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Duration as ChronoDuration;

    use super::*;
    use crate::contract::{EventKind, Post, SchemaVersion};

    #[tokio::test]
    async fn delivery_lifecycle_is_durable_and_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).await.unwrap();
        store.migrate().await.unwrap();
        let now = Utc::now();
        let event = event("ptchan:post.created:i:101", now);
        let deliveries = vec![
            delivery("one", br#"{"integration":"one"}"#),
            delivery("two", br#"{"integration":"two"}"#),
        ];

        assert!(store
            .create_event(&event, b"{}", &deliveries)
            .await
            .unwrap());
        assert!(!store
            .create_event(&event, b"{}", &deliveries)
            .await
            .unwrap());
        assert_eq!(store.pending_count().await.unwrap(), 2);
        assert_eq!(
            store
                .oldest_pending_age(now + ChronoDuration::seconds(10))
                .await
                .unwrap(),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            store.pending_counts_by_webhook().await.unwrap(),
            [("one".to_string(), 1), ("two".to_string(), 1)]
        );

        let pending = store.pending_deliveries(10, now).await.unwrap();
        let payloads = pending
            .iter()
            .map(|delivery| (delivery.webhook.as_str(), delivery.payload.as_slice()))
            .collect::<HashMap<_, _>>();
        assert_eq!(payloads["one"], br#"{"integration":"one"}"#);
        assert_eq!(payloads["two"], br#"{"integration":"two"}"#);

        let retry_at = now + ChronoDuration::seconds(60);
        store
            .mark_failed(&event.event_id, "one", "temporary failure", 1, retry_at)
            .await
            .unwrap();
        store
            .mark_delivered(&event.event_id, "two", now)
            .await
            .unwrap();

        assert!(store
            .pending_deliveries(10, retry_at - ChronoDuration::seconds(1))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store.next_delivery_delay(now).await.unwrap(),
            Some(Duration::from_secs(60))
        );
        let retry = store.pending_deliveries(10, retry_at).await.unwrap();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].webhook, "one");
        assert_eq!(retry[0].attempts, 1);

        store
            .mark_delivered(&event.event_id, "one", retry_at)
            .await
            .unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(store.oldest_pending_age(retry_at).await.unwrap(), None);
        assert_eq!(store.next_delivery_delay(retry_at).await.unwrap(), None);
    }

    #[tokio::test]
    async fn retention_removes_only_old_fully_delivered_events() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).await.unwrap();
        store.migrate().await.unwrap();
        let now = Utc::now();
        let old_delivered = event("ptchan:post.created:i:201", now - ChronoDuration::days(30));
        let old_pending = event("ptchan:post.created:i:202", now - ChronoDuration::days(30));
        let recent_delivered = event("ptchan:post.created:i:203", now);
        let deliveries = [delivery("hook", b"{}")];

        for event in [&old_delivered, &old_pending, &recent_delivered] {
            assert!(store.create_event(event, b"{}", &deliveries).await.unwrap());
        }
        for event in [&old_delivered, &recent_delivered] {
            store
                .mark_delivered(&event.event_id, "hook", now)
                .await
                .unwrap();
        }

        let deleted = store
            .prune_delivered_events(now - ChronoDuration::days(14))
            .await
            .unwrap();

        assert_eq!(deleted, 1);
        assert_eq!(store.pending_count().await.unwrap(), 1);
    }

    #[test]
    fn delivery_backoff_is_bounded() {
        assert_eq!(delivery_backoff(0), Duration::from_secs(2));
        assert_eq!(delivery_backoff(3), Duration::from_secs(8));
        assert_eq!(delivery_backoff(8), Duration::from_secs(256));
        assert_eq!(delivery_backoff(100), Duration::from_secs(256));
    }

    fn event(event_id: &str, observed_at: DateTime<Utc>) -> WebhookEvent {
        WebhookEvent {
            schema_version: SchemaVersion::V1,
            event_id: event_id.to_string(),
            kind: EventKind::PostCreated,
            source: "ptchan".to_string(),
            observed_at,
            post: Post {
                board: "i".to_string(),
                thread_id: 100,
                id: 101,
                url: "https://ptchan.test/i/thread/100.html#101".to_string(),
                date: observed_at,
                subject: None,
                message: Some("body".to_string()),
                name: None,
                tripcode: None,
                capcode: None,
                donor: None,
                country: None,
                poster_fingerprint: None,
                origin: None,
                attachment_count: 0,
                references: Vec::new(),
                referenced_by: Vec::new(),
            },
        }
    }

    fn delivery(webhook: &str, payload: &[u8]) -> EventDelivery {
        EventDelivery {
            webhook: webhook.to_string(),
            payload: payload.to_vec(),
        }
    }
}

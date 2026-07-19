use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

use crate::metrics;

#[derive(Clone)]
pub struct Store {
    inner: Arc<Mutex<Connection>>,
}

#[derive(Debug)]
pub struct PendingDelivery {
    pub event_id: String,
    pub webhook: String,
    pub payload: Vec<u8>,
    pub attempts: i64,
}

pub struct EventDelivery {
    pub webhook: String,
    pub payload: Vec<u8>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let conn =
            Connection::open(path).with_context(|| format!("open sqlite {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("enable sqlite wal")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("enable sqlite foreign keys")?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn migrate(&self) -> Result<()> {
        self.with_conn(|conn| {
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
                ",
            )
            .context("create schema")?;
            Ok(())
        })
    }

    pub fn create_event(
        &self,
        event: &crate::consumer::WebhookEvent,
        payload: &[u8],
        deliveries: &[EventDelivery],
    ) -> Result<bool> {
        self.with_conn(|conn| {
            let tx = conn.transaction().context("begin event transaction")?;
            let inserted = tx
                .execute(
                    "INSERT OR IGNORE INTO events (id, kind, board, thread_id, post_id, payload, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        event.event_id,
                        event.kind.as_str(),
                        event.post.board,
                        event.post.thread_id,
                        event.post.id,
                        payload,
                        event.observed_at.to_rfc3339(),
                    ],
                )
                .context("insert event")?;
            if inserted == 1 {
                for delivery in deliveries {
                    tx.execute(
                        "INSERT INTO deliveries (event_id, webhook, status, attempts, next_attempt_at, payload)
                         VALUES (?1, ?2, 'pending', 0, ?3, ?4)",
                        params![
                            event.event_id,
                            delivery.webhook,
                            event.observed_at.to_rfc3339(),
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
    }

    pub fn pending_deliveries(
        &self,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<PendingDelivery>> {
        self.with_conn(|conn| {
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
    }

    pub fn mark_delivered(&self, event_id: &str, webhook: &str, now: DateTime<Utc>) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE deliveries SET status = 'delivered', delivered_at = ?1 WHERE event_id = ?2 AND webhook = ?3",
                params![now.to_rfc3339(), event_id, webhook],
            )
            .context("mark delivery delivered")?;
            Ok(())
        })
    }

    pub fn mark_failed(
        &self,
        event_id: &str,
        webhook: &str,
        error: &str,
        attempts: i64,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE deliveries
                 SET attempts = ?1, next_attempt_at = ?2, last_error = ?3
                 WHERE event_id = ?4 AND webhook = ?5",
                params![
                    attempts,
                    next_attempt_at.to_rfc3339(),
                    truncate(error, 500),
                    event_id,
                    webhook
                ],
            )
            .context("mark delivery failed")?;
            Ok(())
        })
    }

    pub fn pending_count(&self) -> Result<i64> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM deliveries WHERE status = 'pending'",
                [],
                |row| row.get(0),
            )
            .context("count pending deliveries")
        })
    }

    pub fn next_delivery_delay(&self, now: DateTime<Utc>) -> Result<Option<Duration>> {
        self.with_conn(|conn| {
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
    }

    pub fn prune_delivered_events(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.with_conn(|conn| {
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
    }

    pub fn is_ready(&self) -> bool {
        self.with_conn(|conn| {
            let value: Option<i64> = conn
                .query_row("SELECT 1", [], |row| row.get(0))
                .optional()?;
            Ok(value == Some(1))
        })
        .unwrap_or(false)
    }

    fn with_conn<T>(&self, f: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        let mut conn = self.inner.lock().expect("sqlite mutex poisoned");
        match f(&mut conn) {
            Ok(value) => Ok(value),
            Err(err) => {
                metrics::SQLITE_ERRORS.inc();
                Err(err)
            }
        }
    }
}

pub fn delivery_backoff(attempts: i64) -> Duration {
    let exp = u32::try_from(attempts.clamp(1, 8)).unwrap_or(8);
    Duration::from_secs((2_u64.pow(exp)).min(300))
}

fn truncate(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::{EventKind, Post, WebhookEvent};
    use chrono::{Duration as ChronoDuration, Utc};

    #[test]
    fn dedupes_events_and_creates_deliveries_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).unwrap();
        store.migrate().unwrap();
        let event = event("ptchan:post.created:i:101", Utc::now());
        let deliveries = deliveries("martie", br"{}".to_vec());

        assert!(store.create_event(&event, br"{}", &deliveries).unwrap());
        assert!(!store.create_event(&event, br"{}", &deliveries).unwrap());
        assert_eq!(store.pending_deliveries(10, Utc::now()).unwrap().len(), 1);
        assert_eq!(
            store.next_delivery_delay(Utc::now()).unwrap(),
            Some(Duration::ZERO)
        );

        store
            .mark_delivered(&event.event_id, "martie", Utc::now())
            .unwrap();
        assert_eq!(store.next_delivery_delay(Utc::now()).unwrap(), None);
    }

    #[test]
    fn prunes_only_old_fully_delivered_events() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).unwrap();
        store.migrate().unwrap();
        let deliveries = deliveries("martie", br"{}".to_vec());
        let now = Utc::now();
        let old_delivered = event("ptchan:post.created:i:201", now - ChronoDuration::days(30));
        let old_pending = event("ptchan:post.created:i:202", now - ChronoDuration::days(30));
        let recent_delivered = event("ptchan:post.created:i:203", now);

        assert!(store
            .create_event(&old_delivered, br"{}", &deliveries)
            .unwrap());
        assert!(store
            .create_event(&old_pending, br"{}", &deliveries)
            .unwrap());
        assert!(store
            .create_event(&recent_delivered, br"{}", &deliveries)
            .unwrap());
        store
            .mark_delivered(&old_delivered.event_id, "martie", now)
            .unwrap();
        store
            .mark_delivered(&recent_delivered.event_id, "martie", now)
            .unwrap();

        let deleted = store
            .prune_delivered_events(now - ChronoDuration::days(14))
            .unwrap();

        assert_eq!(deleted, 1);
        assert_eq!(event_count(&store).unwrap(), 2);
        assert_eq!(store.pending_deliveries(10, now).unwrap().len(), 1);
    }

    #[test]
    fn stores_payload_per_delivery() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).unwrap();
        store.migrate().unwrap();
        let event = event("ptchan:post.created:i:301", Utc::now());
        let deliveries = deliveries("martie", br#"{"consumer":"martie"}"#.to_vec());

        assert!(store.create_event(&event, br"{}", &deliveries).unwrap());
        let pending = store.pending_deliveries(10, Utc::now()).unwrap();

        assert_eq!(pending[0].payload, br#"{"consumer":"martie"}"#);
    }

    fn event(event_id: &str, observed_at: DateTime<Utc>) -> WebhookEvent {
        WebhookEvent {
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
                attachment_count: 0,
                references: Vec::new(),
                referenced_by: Vec::new(),
            },
        }
    }

    fn event_count(store: &Store) -> Result<i64> {
        store.with_conn(|conn| {
            conn.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
                .context("count events")
        })
    }

    fn deliveries(webhook: &str, payload: Vec<u8>) -> Vec<EventDelivery> {
        vec![EventDelivery {
            webhook: webhook.to_string(),
            payload,
        }]
    }
}

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::{stream, StreamExt};
use hmac::{Hmac, KeyInit, Mac};
use reqwest::Client;
use sha2::Sha256;
use tokio::{
    sync::{watch, Notify},
    time,
};
use tracing::{debug, info, trace, warn};

use crate::{
    config::{self, WebhookConfig},
    metrics, store,
};

type HmacSha256 = Hmac<Sha256>;
const IDLE_SWEEP_INTERVAL: Duration = Duration::from_mins(1);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
const DELIVERY_CONCURRENCY: usize = 16;

pub(crate) async fn delivery_loop(
    webhooks: Vec<WebhookConfig>,
    store: Arc<store::Store>,
    wakeup: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) {
    let client = match Client::builder()
        .user_agent(config::gateway_user_agent())
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            warn!(error = %err, "failed to build webhook client");
            return;
        }
    };
    let endpoints = webhooks
        .into_iter()
        .map(|w| (w.name.clone(), w))
        .collect::<HashMap<_, _>>();
    loop {
        if *shutdown.borrow() {
            return;
        }
        deliver_pending(&client, &endpoints, &store).await;
        let sleep_for = match store.next_delivery_delay(Utc::now()).await {
            Ok(Some(delay)) => delay,
            Ok(None) => IDLE_SWEEP_INTERVAL,
            Err(err) => {
                warn!(error = %err, "failed to load next webhook delivery time");
                IDLE_SWEEP_INTERVAL
            }
        };
        trace!(?sleep_for, "webhook delivery loop sleeping");
        tokio::select! {
            _ = shutdown.changed() => {}
            () = wakeup.notified() => {}
            () = time::sleep(sleep_for) => {}
        }
    }
}

async fn deliver_pending(
    client: &Client,
    endpoints: &HashMap<String, WebhookConfig>,
    store: &store::Store,
) {
    match store.pending_count().await {
        Ok(count) => {
            metrics::WEBHOOK_PENDING.set(count);
            if count == 0 {
                trace!(pending = count, "webhook pending count loaded");
            } else {
                debug!(pending = count, "webhook pending count loaded");
            }
        }
        Err(err) => warn!(error = %err, "failed to count pending deliveries"),
    }
    update_pending_by_webhook(endpoints, store).await;
    let deliveries = match store.pending_deliveries(50, Utc::now()).await {
        Ok(deliveries) => deliveries,
        Err(err) => {
            warn!(error = %err, "failed to load pending deliveries");
            return;
        }
    };
    if !deliveries.is_empty() {
        debug!(
            delivery_count = deliveries.len(),
            "webhook deliveries loaded"
        );
    }
    stream::iter(deliveries)
        .for_each_concurrent(DELIVERY_CONCURRENCY, |delivery| async move {
            deliver_one(client, endpoints, store, delivery).await;
        })
        .await;
    update_oldest_pending_age(store).await;
}

async fn deliver_one(
    client: &Client,
    endpoints: &HashMap<String, WebhookConfig>,
    store: &store::Store,
    delivery: store::PendingDelivery,
) {
    let Some(endpoint) = endpoints.get(&delivery.webhook) else {
        mark_failed(store, &delivery, "webhook is not configured").await;
        return;
    };
    let started_at = Instant::now();
    match deliver(client, endpoint, &delivery.event_id, &delivery.payload).await {
        Ok(()) => {
            metrics::WEBHOOK_DELIVERIES
                .with_label_values(&[delivery.webhook.as_str(), "success"])
                .inc();
            metrics::WEBHOOK_DELIVERY_SECONDS
                .with_label_values(&[delivery.webhook.as_str(), "success"])
                .observe(started_at.elapsed().as_secs_f64());
            if let Err(err) = store
                .mark_delivered(&delivery.event_id, &delivery.webhook, Utc::now())
                .await
            {
                warn!(event_id = %delivery.event_id, webhook = %delivery.webhook, error = %err, "failed to mark delivered");
            } else {
                info!(event_id = %delivery.event_id, webhook = %delivery.webhook, "webhook delivered");
            }
        }
        Err(err) => {
            metrics::WEBHOOK_DELIVERIES
                .with_label_values(&[delivery.webhook.as_str(), "failure"])
                .inc();
            metrics::WEBHOOK_DELIVERY_SECONDS
                .with_label_values(&[delivery.webhook.as_str(), "failure"])
                .observe(started_at.elapsed().as_secs_f64());
            mark_failed(store, &delivery, &err.to_string()).await;
        }
    }
}

async fn update_pending_by_webhook(
    endpoints: &HashMap<String, WebhookConfig>,
    store: &store::Store,
) {
    for webhook in endpoints.keys() {
        metrics::WEBHOOK_PENDING_BY_WEBHOOK
            .with_label_values(&[webhook])
            .set(0);
    }
    match store.pending_counts_by_webhook().await {
        Ok(counts) => {
            for (webhook, count) in counts {
                metrics::WEBHOOK_PENDING_BY_WEBHOOK
                    .with_label_values(&[webhook.as_str()])
                    .set(count);
            }
        }
        Err(err) => warn!(error = %err, "failed to count pending deliveries by webhook"),
    }
}

async fn update_oldest_pending_age(store: &store::Store) {
    match store.oldest_pending_age(Utc::now()).await {
        Ok(Some(age)) => metrics::WEBHOOK_OLDEST_PENDING_SECONDS.set(age.as_secs_f64()),
        Ok(None) => metrics::WEBHOOK_OLDEST_PENDING_SECONDS.set(0.0),
        Err(err) => warn!(error = %err, "failed to load oldest pending webhook age"),
    }
}

async fn deliver(
    client: &Client,
    endpoint: &WebhookConfig,
    event_id: &str,
    payload: &[u8],
) -> Result<()> {
    let timestamp = Utc::now().to_rfc3339();
    debug!(
        event_id = %event_id,
        webhook = %endpoint.name,
        url = %endpoint.url,
        payload_bytes = payload.len(),
        "sending webhook"
    );
    let response = delivery_request(client, endpoint, event_id, &timestamp, payload)?
        .send()
        .await
        .context("send webhook request")?;
    let status = response.status();
    debug!(event_id = %event_id, webhook = %endpoint.name, %status, "webhook response received");
    if !status.is_success() {
        return Err(anyhow!("webhook status {status}"));
    }
    Ok(())
}

fn delivery_request(
    client: &Client,
    endpoint: &WebhookConfig,
    event_id: &str,
    timestamp: &str,
    payload: &[u8],
) -> Result<reqwest::RequestBuilder> {
    Ok(client
        .post(&endpoint.url)
        .timeout(DELIVERY_TIMEOUT)
        .header("content-type", "application/json")
        .header("x-ptchan-event-id", event_id)
        .header("x-ptchan-timestamp", timestamp)
        .header(
            "x-ptchan-signature",
            signature(&endpoint.secret, timestamp, payload)?,
        )
        .body(payload.to_vec()))
}

fn signature(secret: &str, timestamp: &str, payload: &[u8]) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).context("create hmac")?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(payload);
    Ok(format!(
        "hmac-sha256={}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

async fn mark_failed(store: &store::Store, delivery: &store::PendingDelivery, error: &str) {
    let attempts = delivery.attempts + 1;
    let backoff = i64::try_from(store::delivery_backoff(attempts).as_secs()).unwrap_or(300);
    let next = Utc::now() + ChronoDuration::seconds(backoff);
    if let Err(err) = store
        .mark_failed(&delivery.event_id, &delivery.webhook, error, attempts, next)
        .await
    {
        warn!(event_id = %delivery.event_id, webhook = %delivery.webhook, error = %err, "failed to mark delivery failed");
    } else {
        warn!(event_id = %delivery.event_id, webhook = %delivery.webhook, attempts, error, "webhook delivery failed");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{extract::State, http::StatusCode, routing::post, Router};
    use chrono::Utc;
    use tokio::{net::TcpListener, sync::Notify};

    use super::*;
    use crate::{
        contract::{EventKind, Post, SchemaVersion, WebhookEvent},
        store::{EventDelivery, Store},
    };

    #[test]
    fn signature_uses_timestamp_dot_body() {
        let got = signature("secret", "2026-07-19T12:00:00Z", br#"{"event_id":"evt"}"#).unwrap();
        assert_eq!(
            got,
            "hmac-sha256=8faafae26d51e8b9d92f3409289dad718b74edeb5cec3ac73bf73972b80b875b"
        );
    }

    #[test]
    fn delivery_builds_the_signed_contract() {
        let endpoint = WebhookConfig {
            name: "example".to_string(),
            url: "https://integration.test/events".to_string(),
            allowed_boards: Vec::new(),
            include_poster_fingerprint: false,
            secret: "secret".to_string(),
        };
        let payload = br#"{"event_id":"event-1"}"#;
        let timestamp = "2026-07-19T12:00:00Z";

        let request = delivery_request(&Client::new(), &endpoint, "event-1", timestamp, payload)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(request.url().as_str(), endpoint.url);
        assert_eq!(
            request.headers().get("x-ptchan-event-id").unwrap(),
            "event-1"
        );
        assert_eq!(
            request.headers().get("content-type").unwrap(),
            "application/json"
        );
        assert_eq!(
            request.headers().get("x-ptchan-timestamp").unwrap(),
            timestamp
        );
        assert_eq!(
            request.headers().get("x-ptchan-signature").unwrap(),
            &signature("secret", timestamp, payload).unwrap()
        );
        assert_eq!(
            request.body().unwrap().as_bytes().unwrap(),
            payload.as_slice()
        );
    }

    #[tokio::test]
    async fn slow_webhook_does_not_block_ready_delivery() {
        let release_slow = Arc::new(Notify::new());
        let app = Router::new()
            .route(
                "/slow",
                post(|State(release_slow): State<Arc<Notify>>| async move {
                    release_slow.notified().await;
                    StatusCode::NO_CONTENT
                }),
            )
            .route(
                "/fast",
                post(|State(release_slow): State<Arc<Notify>>| async move {
                    release_slow.notify_one();
                    StatusCode::NO_CONTENT
                }),
            )
            .with_state(release_slow);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).await.unwrap();
        store.migrate().await.unwrap();
        let now = Utc::now();
        store
            .create_event(
                &event("a-slow", 101, now),
                b"{}",
                &[EventDelivery {
                    webhook: "slow".to_string(),
                    payload: b"{}".to_vec(),
                }],
            )
            .await
            .unwrap();
        store
            .create_event(
                &event("b-fast", 102, now),
                b"{}",
                &[EventDelivery {
                    webhook: "fast".to_string(),
                    payload: b"{}".to_vec(),
                }],
            )
            .await
            .unwrap();
        let endpoints = HashMap::from([
            (
                "slow".to_string(),
                webhook("slow", format!("http://{addr}/slow")),
            ),
            (
                "fast".to_string(),
                webhook("fast", format!("http://{addr}/fast")),
            ),
        ]);

        time::timeout(
            Duration::from_secs(3),
            deliver_pending(&Client::new(), &endpoints, &store),
        )
        .await
        .expect("deliveries should run concurrently");

        assert_eq!(store.pending_count().await.unwrap(), 0);
        server.abort();
    }

    fn webhook(name: &str, url: String) -> WebhookConfig {
        WebhookConfig {
            name: name.to_string(),
            url,
            allowed_boards: Vec::new(),
            include_poster_fingerprint: false,
            secret: "secret".to_string(),
        }
    }

    fn event(event_id: &str, post_id: i64, observed_at: chrono::DateTime<Utc>) -> WebhookEvent {
        WebhookEvent {
            schema_version: SchemaVersion::V1,
            event_id: event_id.to_string(),
            kind: EventKind::PostCreated,
            source: "ptchan".to_string(),
            observed_at,
            post: Post {
                board: "test".to_string(),
                thread_id: 100,
                id: post_id,
                url: format!("https://ptchan.test/test/thread/100.html#{post_id}"),
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
}

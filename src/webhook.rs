use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use hmac::{Hmac, Mac};
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
const IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

pub async fn delivery_loop(
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
        let sleep_for = match store.next_delivery_delay(Utc::now()) {
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
    match store.pending_count() {
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
    let deliveries = match store.pending_deliveries(50, Utc::now()) {
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
    for delivery in deliveries {
        let Some(endpoint) = endpoints.get(&delivery.webhook) else {
            mark_failed(store, &delivery, "webhook is not configured");
            continue;
        };
        match deliver(client, endpoint, &delivery.event_id, &delivery.payload).await {
            Ok(()) => {
                metrics::WEBHOOK_DELIVERIES
                    .with_label_values(&[delivery.webhook.as_str(), "success"])
                    .inc();
                if let Err(err) =
                    store.mark_delivered(&delivery.event_id, &delivery.webhook, Utc::now())
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
                mark_failed(store, &delivery, &err.to_string());
            }
        }
    }
}

async fn deliver(
    client: &Client,
    endpoint: &WebhookConfig,
    event_id: &str,
    payload: &[u8],
) -> Result<()> {
    let timestamp = Utc::now().to_rfc3339();
    let signature = signature(&endpoint.secret, &timestamp, payload)?;
    debug!(
        event_id = %event_id,
        webhook = %endpoint.name,
        url = %endpoint.url,
        payload_bytes = payload.len(),
        "sending webhook"
    );
    let response = client
        .post(&endpoint.url)
        .timeout(endpoint.timeout)
        .header("content-type", "application/json")
        .header("x-ptchan-event-id", event_id)
        .header("x-ptchan-timestamp", timestamp)
        .header("x-ptchan-signature", signature)
        .body(payload.to_vec())
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

fn mark_failed(store: &store::Store, delivery: &store::PendingDelivery, error: &str) {
    let attempts = delivery.attempts + 1;
    let next = Utc::now() + chrono::Duration::from_std(store::delivery_backoff(attempts)).unwrap();
    if let Err(err) =
        store.mark_failed(&delivery.event_id, &delivery.webhook, error, attempts, next)
    {
        warn!(event_id = %delivery.event_id, webhook = %delivery.webhook, error = %err, "failed to mark delivery failed");
    } else {
        warn!(event_id = %delivery.event_id, webhook = %delivery.webhook, attempts, error, "webhook delivery failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_uses_timestamp_dot_body() {
        let got = signature("secret", "2026-07-19T12:00:00Z", br#"{"event_id":"evt"}"#).unwrap();
        assert_eq!(
            got,
            "hmac-sha256=8faafae26d51e8b9d92f3409289dad718b74edeb5cec3ac73bf73972b80b875b"
        );
    }
}

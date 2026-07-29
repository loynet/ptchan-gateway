use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::{
    config::WebhookConfig,
    event, metrics,
    origin::OriginMatcher,
    store::{EventDelivery, Store},
};

pub(super) async fn new_post(
    base_url: &str,
    payload: &[Value],
    store: &Store,
    webhooks: &[WebhookConfig],
    origins: &OriginMatcher,
    fingerprint_secret: Option<&str>,
    delivery_wakeup: &Notify,
) {
    let Some(value) = payload.first().filter(|value| value.is_object()).cloned() else {
        metrics::SOCKET_EVENTS
            .with_label_values(&["parse_error"])
            .inc();
        warn!("newPost payload did not contain JSON object");
        return;
    };
    debug!(shape = %json_shape(&value), "socket newPost received");
    let mut built = match event::gateway_event(base_url, value, Utc::now()) {
        Ok(built) => built,
        Err(err) => {
            metrics::SOCKET_EVENTS
                .with_label_values(&["redacted_or_invalid"])
                .inc();
            warn!(error = %err, "socket event rejected");
            return;
        }
    };

    origins.annotate_post(&mut built.event.post);
    built.payload = match event::encode_event(&built.event) {
        Ok(payload) => payload,
        Err(err) => {
            metrics::SOCKET_EVENTS
                .with_label_values(&["redacted_or_invalid"])
                .inc();
            warn!(error = %err, "failed to encode attributed socket event");
            return;
        }
    };
    let deliveries = match event_deliveries(&built, webhooks, fingerprint_secret) {
        Ok(deliveries) => deliveries,
        Err(err) => {
            metrics::SOCKET_EVENTS
                .with_label_values(&["store_error"])
                .inc();
            warn!(error = %err, "failed to prepare webhook deliveries");
            return;
        }
    };
    if deliveries.is_empty() {
        metrics::SOCKET_EVENTS
            .with_label_values(&["no_allowed_webhooks"])
            .inc();
        debug!(
            event_id = %built.event.event_id,
            board = %built.event.post.board,
            "socket event skipped; no webhook allowed for board"
        );
        return;
    }

    match store
        .create_event(&built.event, &built.payload, &deliveries)
        .await
    {
        Ok(true) => {
            metrics::SOCKET_EVENTS.with_label_values(&["queued"]).inc();
            metrics::observe_now(&metrics::SOCKET_LAST_EVENT_TIMESTAMP_SECONDS);
            info!(
                event_id = %built.event.event_id,
                kind = %built.event.kind,
                board = %built.event.post.board,
                thread_id = built.event.post.thread_id,
                post_id = built.event.post.id,
                attachment_count = built.event.post.attachment_count,
                references = built.event.post.references.len(),
                referenced_by = built.event.post.referenced_by.len(),
                webhook_count = deliveries.len(),
                fingerprint_source = built.poster_identity.is_some(),
                "event queued"
            );
            delivery_wakeup.notify_one();
        }
        Ok(false) => {
            metrics::SOCKET_EVENTS
                .with_label_values(&["duplicate"])
                .inc();
            debug!(event_id = %built.event.event_id, "duplicate socket event ignored");
        }
        Err(err) => {
            metrics::SOCKET_EVENTS
                .with_label_values(&["store_error"])
                .inc();
            warn!(error = %err, "failed to store socket event");
        }
    }
}

fn event_deliveries(
    built: &event::BuiltEvent,
    webhooks: &[WebhookConfig],
    fingerprint_secret: Option<&str>,
) -> Result<Vec<EventDelivery>> {
    webhooks
        .iter()
        .filter(|webhook| webhook.board_allowed(&built.event.post.board))
        .map(|webhook| {
            let mut event = built.event.clone();
            if webhook.include_poster_fingerprint {
                let secret =
                    fingerprint_secret.context("poster fingerprint secret is not loaded")?;
                event.post.poster_fingerprint = event::poster_fingerprint(
                    secret,
                    &webhook.name,
                    built.poster_identity.as_deref(),
                )?;
            }
            Ok(EventDelivery {
                webhook: webhook.name.clone(),
                payload: event::encode_event(&event)?,
            })
        })
        .collect()
}

fn json_shape(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().map(String::as_str).collect::<Vec<_>>();
            keys.sort_unstable();
            format!("object keys=[{}]", keys.join(","))
        }
        Value::Array(values) => format!("array len={}", values.len()),
        Value::String(_) => "string".to_string(),
        Value::Number(_) => "number".to_string(),
        Value::Bool(_) => "bool".to_string(),
        Value::Null => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::contract::{EventKind, Post, SchemaVersion, WebhookEvent};

    #[test]
    fn event_deliveries_respects_allowed_boards() {
        let built = built_event("test");
        let webhooks = vec![
            webhook("all", Vec::new()),
            webhook("test-only", vec!["test".to_string()]),
            webhook("other-only", vec!["other".to_string()]),
        ];

        let deliveries = event_deliveries(&built, &webhooks, None).unwrap();
        let names = deliveries
            .iter()
            .map(|delivery| delivery.webhook.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["all", "test-only"]);
    }

    fn built_event(board: &str) -> event::BuiltEvent {
        let observed_at = Utc::now();
        event::BuiltEvent {
            event: WebhookEvent {
                schema_version: SchemaVersion::V1,
                event_id: format!("ptchan:post.created:{board}:101"),
                kind: EventKind::PostCreated,
                source: "ptchan".to_string(),
                observed_at,
                post: Post {
                    board: board.to_string(),
                    thread_id: 100,
                    id: 101,
                    url: format!("https://ptchan.test/{board}/thread/100.html#101"),
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
            },
            payload: br"{}".to_vec(),
            poster_identity: None,
        }
    }

    fn webhook(name: &str, allowed_boards: Vec<String>) -> WebhookConfig {
        WebhookConfig {
            name: name.to_string(),
            url: "http://127.0.0.1:8081/events".to_string(),
            allowed_boards,
            include_poster_fingerprint: false,
            secret: "secret".to_string(),
        }
    }
}

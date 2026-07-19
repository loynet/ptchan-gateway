use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

use crate::{consumer, upstream};

type HmacSha256 = Hmac<Sha256>;

pub fn gateway_event(
    base_url: &str,
    mut value: Value,
    observed_at: DateTime<Utc>,
) -> Result<BuiltEvent> {
    let poster_identity = poster_identity_source(&value);
    drop_upstream_ip_fields(&mut value);
    reject_sensitive_fields(&value)?;
    let post: upstream::Post = serde_json::from_value(value).context("decode upstream newPost")?;
    let kind = if post.thread.is_some() {
        consumer::EventKind::PostCreated
    } else {
        consumer::EventKind::ThreadCreated
    };
    let event_id = format!("ptchan:{}:{}:{}", kind.as_str(), post.board, post.id);
    let post = consumer_post(base_url, post);
    let event = consumer::WebhookEvent {
        event_id,
        kind,
        source: "ptchan".to_string(),
        observed_at,
        post,
    };
    let payload = encode_event(&event)?;
    Ok(BuiltEvent {
        event,
        payload,
        poster_identity,
    })
}

pub fn consumer_post_from_value(base_url: &str, mut value: Value) -> Result<consumer::Post> {
    drop_upstream_ip_fields(&mut value);
    reject_sensitive_fields(&value)?;
    let post: upstream::Post = serde_json::from_value(value).context("decode upstream post")?;
    Ok(consumer_post(base_url, post))
}

fn consumer_post(base_url: &str, post: upstream::Post) -> consumer::Post {
    let thread_id = post.thread.unwrap_or(post.id);
    let board = post.board;
    let url = format!(
        "{}/{}/thread/{}.html#{}",
        base_url.trim_end_matches('/'),
        board,
        thread_id,
        post.id
    );
    consumer::Post {
        board: board.clone(),
        thread_id,
        id: post.id,
        url,
        date: post.date,
        subject: clean(post.subject),
        message: clean(post.message.or(post.nomarkup)),
        name: clean(post.name),
        tripcode: clean(post.tripcode),
        capcode: clean(post.capcode),
        donor: post.donor,
        country: clean(post.country),
        poster_fingerprint: None,
        attachment_count: post.files.len(),
        references: post_refs(post.quotes, &board, thread_id),
        referenced_by: post_refs(post.backlinks, &board, thread_id),
    }
}

#[derive(Debug)]
pub struct BuiltEvent {
    pub event: consumer::WebhookEvent,
    pub payload: Vec<u8>,
    pub poster_identity: Option<String>,
}

pub fn encode_event(event: &consumer::WebhookEvent) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(event).context("encode gateway event")?;
    assert_no_sensitive_json(&payload)?;
    Ok(payload)
}

pub fn poster_fingerprint(
    secret: &str,
    scope: &str,
    source: Option<&str>,
) -> Result<Option<String>> {
    let Some(source) = source else {
        return Ok(None);
    };
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).context("create fingerprint hmac")?;
    mac.update(scope.as_bytes());
    mac.update(b"\0");
    mac.update(source.as_bytes());
    Ok(Some(format!(
        "hmac-sha256:{}",
        hex::encode(mac.finalize().into_bytes())
    )))
}

fn post_refs(values: Vec<Value>, board: &str, thread_id: i64) -> Vec<consumer::PostRef> {
    values
        .into_iter()
        .filter_map(|value| post_ref(value, board, thread_id))
        .collect()
}

fn post_ref(
    value: Value,
    fallback_board: &str,
    fallback_thread_id: i64,
) -> Option<consumer::PostRef> {
    match value {
        Value::Number(number) => number.as_i64().map(|post_id| consumer::PostRef {
            board: fallback_board.to_string(),
            thread_id: fallback_thread_id,
            id: post_id,
        }),
        Value::String(text) => text.parse::<i64>().ok().map(|post_id| consumer::PostRef {
            board: fallback_board.to_string(),
            thread_id: fallback_thread_id,
            id: post_id,
        }),
        Value::Object(map) => {
            let post_id = map
                .get("postId")
                .or_else(|| map.get("post_id"))
                .and_then(Value::as_i64)?;
            let board = map
                .get("board")
                .and_then(Value::as_str)
                .and_then(|value| clean(Some(value.to_string())))
                .unwrap_or_else(|| fallback_board.to_string());
            let thread_id = map
                .get("thread")
                .or_else(|| map.get("thread_id"))
                .and_then(Value::as_i64)
                .unwrap_or(fallback_thread_id);
            Some(consumer::PostRef {
                board,
                thread_id,
                id: post_id,
            })
        }
        Value::Array(_) | Value::Bool(_) | Value::Null => None,
    }
}

fn clean(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn poster_identity_source(value: &Value) -> Option<String> {
    value
        .pointer("/ip/cloak")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn drop_upstream_ip_fields(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if map.remove("ip").is_some() {
                metrics::REDACTION_DROPS.inc();
            }
            for child in map.values_mut() {
                drop_upstream_ip_fields(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                drop_upstream_ip_fields(child);
            }
        }
        _ => {}
    }
}

fn reject_sensitive_fields(value: &Value) -> Result<()> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if matches!(key.as_str(), "raw" | "cloak" | "session" | "permissions") {
                    metrics::REDACTION_DROPS.inc();
                    return Err(anyhow!("upstream payload contains sensitive field {key}"));
                }
                reject_sensitive_fields(child)?;
            }
        }
        Value::Array(values) => {
            for child in values {
                reject_sensitive_fields(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn assert_no_sensitive_json(payload: &[u8]) -> Result<()> {
    let value: Value = serde_json::from_slice(payload).context("decode encoded event payload")?;
    reject_sensitive_fields(&value)
}

use crate::metrics;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::EventKind;
    use serde_json::json;

    #[test]
    fn builds_sanitized_thread_event() {
        let payload = json!({
            "_id": "mongo",
            "date": "2026-07-19T12:00:00.000Z",
            "name": " anon ",
            "country": "PT",
            "board": "i",
            "subject": "hello",
            "message": "body",
            "postId": 100,
            "files": [{
                "filename": "a.jpg",
                "originalFilename": "orig.jpg",
                "mimetype": "image/jpeg",
                "size": 123,
                "width": 640,
                "height": 480,
                "hash": "public-file-hash"
            }]
        });
        let built = gateway_event(
            "https://ptchan.test",
            payload,
            "2026-07-19T12:00:01Z".parse().unwrap(),
        )
        .unwrap();

        assert_eq!(built.event.event_id, "ptchan:thread.created:i:100");
        assert_eq!(built.event.kind, EventKind::ThreadCreated);
        assert_eq!(built.event.post.thread_id, 100);
        assert_eq!(built.event.post.name.as_deref(), Some("anon"));
        assert_eq!(built.event.post.donor, None);
        assert_eq!(built.event.post.attachment_count, 1);
        let text = String::from_utf8(built.payload).unwrap();
        assert!(!text.contains("\"ip\""));
        assert!(!text.contains("\"cloak\""));
        assert!(!text.contains("\"raw\""));
        assert!(!text.contains("originalFilename"));
        assert!(!text.contains("public-file-hash"));
    }

    #[test]
    fn drops_ip_fields_before_forwarding() {
        let payload = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "i",
            "thread": 100,
            "postId": 101,
            "message": "reply",
            "ip": { "cloak": "hash", "raw": null, "type": "ip" }
        });
        let built = gateway_event("https://ptchan.test", payload, Utc::now()).unwrap();
        let text = String::from_utf8(built.payload).unwrap();
        assert!(!text.contains("\"ip\""));
        assert!(!text.contains("\"cloak\""));
        assert!(!text.contains("\"raw\""));
        assert_eq!(built.poster_identity.as_deref(), Some("hash"));
    }

    #[test]
    fn derives_poster_fingerprint_without_exposing_cloak() {
        let payload = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "i",
            "thread": 100,
            "postId": 101,
            "message": "reply",
            "ip": { "cloak": "upstream-cloak", "raw": null, "type": "ip" }
        });
        let built = gateway_event("https://ptchan.test", payload, Utc::now()).unwrap();
        let fingerprint = poster_fingerprint(
            "gateway-secret",
            "consumer-a",
            built.poster_identity.as_deref(),
        )
        .unwrap()
        .unwrap();

        assert!(fingerprint.starts_with("hmac-sha256:"));
        assert!(!fingerprint.contains("upstream-cloak"));
    }

    #[test]
    fn maps_post_references_to_consumer_contract() {
        let payload = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "i",
            "thread": 100,
            "postId": 101,
            "message": "reply",
            "quotes": [
                99,
                { "board": "i", "thread": 100, "postId": 98, "ignored": "value" },
                { "ignored": "missing id" }
            ],
            "backlinks": ["102"]
        });
        let built = gateway_event("https://ptchan.test", payload, Utc::now()).unwrap();

        assert_eq!(built.event.post.references.len(), 2);
        assert_eq!(built.event.post.references[0].board, "i");
        assert_eq!(built.event.post.references[0].thread_id, 100);
        assert_eq!(built.event.post.references[0].id, 99);
        assert_eq!(built.event.post.references[1].board, "i");
        assert_eq!(built.event.post.references[1].thread_id, 100);
        assert_eq!(built.event.post.references[1].id, 98);
        assert_eq!(built.event.post.referenced_by[0].board, "i");
        assert_eq!(built.event.post.referenced_by[0].thread_id, 100);
        assert_eq!(built.event.post.referenced_by[0].id, 102);
    }

    #[test]
    fn preserves_public_donor_flag() {
        let payload = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "i",
            "thread": 100,
            "postId": 101,
            "message": "reply",
            "donor": true
        });
        let built = gateway_event("https://ptchan.test", payload, Utc::now()).unwrap();
        let text = String::from_utf8(built.payload).unwrap();

        assert_eq!(built.event.post.donor, Some(true));
        assert!(text.contains("\"donor\":true"));
    }

    #[test]
    fn rejects_sensitive_fields_outside_ip_envelope() {
        let payload = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "i",
            "thread": 100,
            "postId": 101,
            "message": "reply",
            "cloak": "hash"
        });
        let err = gateway_event("https://ptchan.test", payload, Utc::now()).unwrap_err();
        assert!(err.to_string().contains("sensitive field cloak"));
    }
}

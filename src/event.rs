use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

use crate::{consumer, upstream};

type HmacSha256 = Hmac<Sha256>;

pub(crate) fn gateway_event(
    base_url: &str,
    value: Value,
    observed_at: DateTime<Utc>,
) -> Result<BuiltEvent> {
    let decoded = upstream::DecodedPost::try_from(value)
        .map_err(|err| anyhow!("decode upstream newPost: {err}"))?;
    let poster_identity = decoded.poster_identity;
    let post = decoded.post;
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

pub(crate) fn consumer_post_from_value(base_url: &str, value: Value) -> Result<consumer::Post> {
    let post = upstream::DecodedPost::try_from(value)
        .map_err(|err| anyhow!("decode upstream post: {err}"))?;
    Ok(consumer_post(base_url, post.post))
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
        message: consumer_message(post.nomarkup, post.message),
        name: clean(post.name),
        tripcode: clean(post.tripcode),
        capcode: clean(post.capcode),
        donor: post.donor,
        country: country_code(post.country),
        poster_fingerprint: None,
        attachment_count: post.files.len(),
        references: post_refs(post.quotes, &board, thread_id),
        referenced_by: post_refs(post.backlinks, &board, thread_id),
    }
}

#[derive(Debug)]
pub(crate) struct BuiltEvent {
    pub(crate) event: consumer::WebhookEvent,
    pub(crate) payload: Vec<u8>,
    pub(crate) poster_identity: Option<String>,
}

pub(crate) fn encode_event(event: &consumer::WebhookEvent) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(event).context("encode gateway event")?;
    upstream::assert_consumer_safe(&payload)?;
    Ok(payload)
}

pub(crate) fn poster_fingerprint(
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

fn consumer_message(nomarkup: Option<String>, message: Option<String>) -> Option<String> {
    // Upstream `message` is rendered HTML. `nomarkup` is the readable post text
    // consumers need; fall back only when upstream does not provide it.
    clean(nomarkup).or_else(|| clean(message))
}

fn country_code(country: Option<upstream::Country>) -> Option<String> {
    country.and_then(|country| clean(country.code))
}

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
            "country": { "code": "PT", "name": "Portugal" },
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
        assert_eq!(built.event.post.country.as_deref(), Some("PT"));
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
    fn prefers_plain_nomarkup_message_for_consumers() {
        let payload = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "i",
            "thread": 303_822,
            "postId": 303_921,
            "message": "<a class=\"quote\" href=\"/i/thread/303822.html#303918\">&gt;&gt;303918</a>",
            "nomarkup": ">>303918"
        });

        let post = consumer_post_from_value("https://ptchan.test", payload).unwrap();

        assert_eq!(post.message.as_deref(), Some(">>303918"));
    }

    #[test]
    fn falls_back_to_rendered_message_when_nomarkup_is_missing_or_empty() {
        let missing = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "i",
            "thread": 100,
            "postId": 101,
            "message": "rendered body"
        });
        let empty = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "i",
            "thread": 100,
            "postId": 102,
            "message": "rendered body",
            "nomarkup": " "
        });

        let missing_post = consumer_post_from_value("https://ptchan.test", missing).unwrap();
        let empty_post = consumer_post_from_value("https://ptchan.test", empty).unwrap();

        assert_eq!(missing_post.message.as_deref(), Some("rendered body"));
        assert_eq!(empty_post.message.as_deref(), Some("rendered body"));
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
    fn accepts_posts_without_country() {
        let payload = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "pn",
            "thread": 100,
            "postId": 101,
            "message": "reply"
        });
        let post = consumer_post_from_value("https://ptchan.test", payload).unwrap();

        assert_eq!(post.country, None);
    }

    #[test]
    fn accepts_null_country() {
        let payload = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "pn",
            "thread": 100,
            "postId": 101,
            "message": "reply",
            "country": null
        });
        let post = consumer_post_from_value("https://ptchan.test", payload).unwrap();

        assert_eq!(post.country, None);
    }
}

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use crate::metrics;

#[derive(Debug, Deserialize)]
pub(crate) struct Post {
    pub(crate) date: DateTime<Utc>,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) country: Option<Country>,
    pub(crate) board: String,
    #[serde(default)]
    pub(crate) tripcode: Option<String>,
    #[serde(default)]
    pub(crate) capcode: Option<String>,
    #[serde(default)]
    pub(crate) donor: Option<bool>,
    #[serde(default)]
    pub(crate) subject: Option<String>,
    #[serde(default)]
    pub(crate) message: Option<String>,
    #[serde(default)]
    pub(crate) nomarkup: Option<String>,
    #[serde(default)]
    pub(crate) thread: Option<i64>,
    #[serde(rename = "postId")]
    pub(crate) id: i64,
    #[serde(default)]
    pub(crate) files: Vec<Value>,
    #[serde(default)]
    pub(crate) quotes: Vec<Value>,
    #[serde(default)]
    pub(crate) backlinks: Vec<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Country {
    #[serde(default)]
    pub(crate) code: Option<String>,
}

#[derive(Debug)]
pub(crate) struct DecodedPost {
    pub(crate) post: Post,
    pub(crate) poster_identity: Option<String>,
}

impl TryFrom<Value> for DecodedPost {
    type Error = anyhow::Error;

    fn try_from(mut value: Value) -> Result<Self> {
        let poster_identity = poster_identity_source(&value);
        drop_ip_fields(&mut value);
        reject_sensitive_fields(&value)?;
        let post = post_from_value(&value)?;
        Ok(Self {
            post,
            poster_identity,
        })
    }
}

pub(crate) fn assert_consumer_safe(payload: &[u8]) -> Result<()> {
    let value = serde_json::from_slice(payload).context("decode encoded event payload")?;
    reject_sensitive_fields(&value)
}

fn post_from_value(value: &Value) -> Result<Post> {
    let text = value.to_string();
    let mut deserializer = serde_json::Deserializer::from_str(&text);
    serde_path_to_error::deserialize(&mut deserializer)
        .map_err(|err| anyhow!("decode upstream post at {}: {}", err.path(), err.inner()))
}

fn poster_identity_source(value: &Value) -> Option<String> {
    value
        .pointer("/ip/cloak")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn drop_ip_fields(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if map.remove("ip").is_some() {
                metrics::REDACTION_DROPS.inc();
            }
            for child in map.values_mut() {
                drop_ip_fields(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                drop_ip_fields(child);
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn drops_ip_fields_and_keeps_fingerprint_source() {
        let payload = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "i",
            "thread": 100,
            "postId": 101,
            "ip": { "cloak": " hash ", "raw": null, "type": "ip" }
        });

        let decoded = DecodedPost::try_from(payload).unwrap();

        assert_eq!(decoded.poster_identity.as_deref(), Some("hash"));
        assert_eq!(decoded.post.id, 101);
    }

    #[test]
    fn rejects_sensitive_fields_outside_ip_envelope() {
        let payload = json!({
            "date": "2026-07-19T12:00:00.000Z",
            "board": "i",
            "thread": 100,
            "postId": 101,
            "cloak": "hash"
        });

        let err = DecodedPost::try_from(payload).unwrap_err();

        assert!(err.to_string().contains("sensitive field cloak"));
    }

    #[test]
    fn checks_encoded_consumer_payloads() {
        let payload = br#"{"event_id":"x","session":"secret"}"#;

        let err = assert_consumer_safe(payload).unwrap_err();

        assert!(err.to_string().contains("sensitive field session"));
    }
}

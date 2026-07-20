use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

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

pub(crate) fn post_from_value(value: &Value) -> Result<Post> {
    let text = value.to_string();
    let mut deserializer = serde_json::Deserializer::from_str(&text);
    serde_path_to_error::deserialize(&mut deserializer)
        .map_err(|err| anyhow!("decode upstream post at {}: {}", err.path(), err.inner()))
}

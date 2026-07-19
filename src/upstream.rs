use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub(crate) struct Post {
    pub(crate) date: DateTime<Utc>,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) country: Option<String>,
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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub event_id: String,
    pub kind: EventKind,
    pub source: String,
    pub observed_at: DateTime<Utc>,
    pub post: Post,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventKind {
    #[serde(rename = "thread.created")]
    ThreadCreated,
    #[serde(rename = "post.created")]
    PostCreated,
}

impl EventKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ThreadCreated => "thread.created",
            Self::PostCreated => "post.created",
        }
    }
}

impl fmt::Display for EventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Post {
    pub board: String,
    pub thread_id: i64,
    #[serde(rename = "post_id")]
    pub id: i64,
    pub url: String,
    pub date: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tripcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub donor: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poster_fingerprint: Option<String>,
    #[serde(default)]
    pub attachment_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<PostRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub referenced_by: Vec<PostRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Thread {
    pub board: String,
    #[serde(rename = "thread_id")]
    pub id: i64,
    pub posts: Vec<Post>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostRef {
    pub board: String,
    #[serde(rename = "thread_id")]
    pub thread_id: i64,
    #[serde(rename = "post_id")]
    pub id: i64,
}

use chrono::{DateTime, Utc};
use serde::Serialize;
use std::fmt;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct WebhookEvent {
    pub(crate) event_id: String,
    pub(crate) kind: EventKind,
    pub(crate) source: String,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) post: Post,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) enum EventKind {
    #[serde(rename = "thread.created")]
    ThreadCreated,
    #[serde(rename = "post.created")]
    PostCreated,
}

impl EventKind {
    pub(crate) const fn as_str(self) -> &'static str {
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

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Post {
    pub(crate) board: String,
    pub(crate) thread_id: i64,
    #[serde(rename = "post_id")]
    pub(crate) id: i64,
    pub(crate) url: String,
    pub(crate) date: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tripcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) capcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) donor: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) poster_fingerprint: Option<String>,
    pub(crate) attachment_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) references: Vec<PostRef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) referenced_by: Vec<PostRef>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Thread {
    pub(crate) board: String,
    #[serde(rename = "thread_id")]
    pub(crate) id: i64,
    pub(crate) posts: Vec<Post>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PostRef {
    pub(crate) board: String,
    #[serde(rename = "thread_id")]
    pub(crate) thread_id: i64,
    #[serde(rename = "post_id")]
    pub(crate) id: i64,
}

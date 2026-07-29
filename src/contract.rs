use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

pub(crate) mod artifacts;

/// Version of the integration-facing JSON schema.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, ToSchema)]
pub(crate) enum SchemaVersion {
    /// Initial public contract.
    #[serde(rename = "1")]
    V1,
}

/// Durable event delivered to one configured integration webhook.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, ToSchema)]
pub(crate) struct WebhookEvent {
    /// Schema used to encode this event.
    pub(crate) schema_version: SchemaVersion,
    /// Stable idempotency key, also sent as `x-ptchan-event-id`.
    pub(crate) event_id: String,
    /// Public event kind.
    pub(crate) kind: EventKind,
    /// Upstream service that produced the event.
    pub(crate) source: String,
    /// Time at which the gateway accepted the upstream event.
    pub(crate) observed_at: DateTime<Utc>,
    /// Sanitized public post.
    pub(crate) post: Post,
}

/// Public post event kind.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, ToSchema)]
pub(crate) enum EventKind {
    /// A new thread was created.
    #[serde(rename = "thread.created")]
    ThreadCreated,
    /// A reply was added to an existing thread.
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

/// Gateway-known producer of a post.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, ToSchema)]
pub(crate) struct PostOrigin {
    /// Producer category.
    pub(crate) kind: OriginKind,
    /// Configured integration name.
    pub(crate) name: String,
}

/// Supported producer categories.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, ToSchema)]
pub(crate) enum OriginKind {
    /// A configured gateway integration.
    #[serde(rename = "integration")]
    Integration,
}

/// Moderation-safe public post returned to integrations.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, ToSchema)]
pub(crate) struct Post {
    /// ptchan board name.
    pub(crate) board: String,
    /// Post ID of the containing thread.
    pub(crate) thread_id: i64,
    /// Post ID.
    #[serde(rename = "post_id")]
    pub(crate) id: i64,
    /// Canonical public post URL.
    pub(crate) url: String,
    /// Upstream post timestamp.
    pub(crate) date: DateTime<Utc>,
    /// Public subject, omitted when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subject: Option<String>,
    /// Plain public message, omitted when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) message: Option<String>,
    /// Public display name, omitted when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    /// Public tripcode emitted by ptchan, never the tripcode secret.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tripcode: Option<String>,
    /// Public capcode, omitted when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) capcode: Option<String>,
    /// Public donor marker, omitted when upstream does not provide it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) donor: Option<bool>,
    /// Public country code, omitted when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) country: Option<String>,
    /// Optional integration-scoped pseudonym; never an upstream cloak.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) poster_fingerprint: Option<String>,
    /// Integration identity inferred from an exact configured public-tripcode match.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) origin: Option<PostOrigin>,
    /// Number of public attachments; attachment metadata is not exposed.
    pub(crate) attachment_count: usize,
    /// Posts quoted by this post.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) references: Vec<PostRef>,
    /// Posts that quote this post.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) referenced_by: Vec<PostRef>,
}

/// Sanitized thread returned by the signed reading endpoint.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, ToSchema)]
pub(crate) struct Thread {
    /// ptchan board name.
    pub(crate) board: String,
    /// Thread post ID.
    #[serde(rename = "thread_id")]
    pub(crate) id: i64,
    /// Selected posts in chronological order.
    pub(crate) posts: Vec<Post>,
    /// Whether older posts were omitted by the requested limit.
    pub(crate) truncated: bool,
}

/// Complete coordinates for a quoted post.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, ToSchema)]
pub(crate) struct PostRef {
    /// ptchan board name.
    pub(crate) board: String,
    /// Containing thread post ID.
    #[serde(rename = "thread_id")]
    pub(crate) thread_id: i64,
    /// Referenced post ID.
    #[serde(rename = "post_id")]
    pub(crate) id: i64,
}

/// Signed request body for creating a public reply.
#[derive(Debug, Deserialize, JsonSchema, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplyRequest {
    /// Public post body, limited to 8,000 UTF-8 bytes.
    pub(crate) message: String,
    /// Whether to submit `sage` through the public posting form.
    #[serde(default)]
    pub(crate) sage: bool,
}

/// Coordinates returned after ptchan accepts and identifies a reply.
#[derive(Debug, Deserialize, JsonSchema, Serialize, ToSchema)]
pub(crate) struct ReplyResponse {
    /// ptchan board name.
    pub(crate) board: String,
    /// Containing thread post ID.
    pub(crate) thread_id: i64,
    /// Created post ID.
    #[serde(rename = "post_id")]
    pub(crate) post_id: i64,
    /// Canonical public post URL.
    pub(crate) url: String,
    /// Integration that submitted the reply.
    pub(crate) origin: PostOrigin,
}

/// Stable machine-readable integration API error code.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ErrorCode {
    Unauthorized,
    CapabilityNotEnabled,
    InvalidJson,
    InvalidQuery,
    InvalidBoard,
    InvalidThreadId,
    MissingMessage,
    MessageTooLong,
    InvalidMessage,
    BoardNotAllowed,
    RateLimited,
    ThreadNotFound,
    UpstreamUnavailable,
    CaptchaRequired,
    BlockBypassRequired,
    ThreadLocked,
    ThreadReplyLimit,
    BoardLocked,
    Rejected,
    ReplyStateUnknown,
    PayloadTooLarge,
}

impl ErrorCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::CapabilityNotEnabled => "capability_not_enabled",
            Self::InvalidJson => "invalid_json",
            Self::InvalidQuery => "invalid_query",
            Self::InvalidBoard => "invalid_board",
            Self::InvalidThreadId => "invalid_thread_id",
            Self::MissingMessage => "missing_message",
            Self::MessageTooLong => "message_too_long",
            Self::InvalidMessage => "invalid_message",
            Self::BoardNotAllowed => "board_not_allowed",
            Self::RateLimited => "rate_limited",
            Self::ThreadNotFound => "thread_not_found",
            Self::UpstreamUnavailable => "upstream_unavailable",
            Self::CaptchaRequired => "captcha_required",
            Self::BlockBypassRequired => "block_bypass_required",
            Self::ThreadLocked => "thread_locked",
            Self::ThreadReplyLimit => "thread_reply_limit",
            Self::BoardLocked => "board_locked",
            Self::Rejected => "rejected",
            Self::ReplyStateUnknown => "reply_state_unknown",
            Self::PayloadTooLarge => "payload_too_large",
        }
    }

    pub(crate) const fn retryable(self) -> bool {
        matches!(self, Self::RateLimited | Self::UpstreamUnavailable)
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Uniform JSON body returned by integration API failures.
#[derive(Debug, Deserialize, JsonSchema, Serialize, ToSchema)]
pub(crate) struct ErrorEnvelope {
    pub(crate) error: ErrorBody,
}

/// Stable error details for programmatic handling and operator diagnostics.
#[derive(Debug, Deserialize, JsonSchema, Serialize, ToSchema)]
pub(crate) struct ErrorBody {
    /// Stable code suitable for branching.
    pub(crate) code: ErrorCode,
    /// Human-readable, non-sensitive explanation.
    pub(crate) message: String,
    /// Whether an automatic retry can be safe. `reply_state_unknown` is never retryable.
    pub(crate) retryable: bool,
    /// Upstream HTTP status when ptchan returned a classified rejection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) upstream_status: Option<u16>,
}

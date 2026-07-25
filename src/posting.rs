use std::{error::Error, fmt};

use anyhow::{anyhow, Context, Result};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{self, PostingConfig, PtchanConfig};

pub(crate) const MAX_REPLY_MESSAGE_BYTES: usize = 8_000;

#[derive(Clone)]
pub(crate) struct PostWriter {
    base_url: String,
    client: Client,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplyRequest {
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) sage: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReplyResponse {
    pub(crate) board: String,
    pub(crate) thread_id: i64,
    #[serde(rename = "post_id")]
    pub(crate) post_id: i64,
    pub(crate) url: String,
    pub(crate) origin: crate::contract::PostOrigin,
}

impl PostWriter {
    pub(crate) fn new(cfg: &PtchanConfig) -> Result<Self> {
        let client = Client::builder()
            .user_agent(cfg.user_agent.clone())
            .build()
            .context("build posting client")?;
        Ok(Self {
            base_url: cfg.base_url.clone(),
            client,
        })
    }

    pub(crate) async fn reply(
        &self,
        posting: &PostingConfig,
        board: &str,
        thread_id: i64,
        request: &ReplyRequest,
    ) -> std::result::Result<ReplyResponse, ReplyError> {
        validate_reply(board, thread_id, &request.message).map_err(ReplyError::InvalidRequest)?;
        let url = posting_url(&self.base_url, board);
        let referer = thread_url(&self.base_url, board, thread_id);
        let form = reply_form(posting, thread_id, request);

        let response = self
            .client
            .post(url)
            .timeout(posting.timeout)
            .header("accept", "application/json")
            .header("referer", referer)
            .header("x-using-xhr", "true")
            .form(&form)
            .send()
            .await
            .map_err(|err| ReplyError::Request(anyhow!(err).context("send ptchan reply")))?;
        let status = response.status();
        let body = match response.text().await {
            Ok(body) => body,
            Err(err) if status.is_success() => {
                return Err(ReplyError::Request(
                    anyhow!(err).context("read ptchan reply response"),
                ));
            }
            Err(err) => {
                return Err(ReplyError::Upstream(
                    UpstreamReplyError::from_body_read_error(status, &err),
                ));
            }
        };
        if !status.is_success() {
            return Err(ReplyError::Upstream(UpstreamReplyError::from_response(
                status, &body,
            )));
        }
        let created = serde_json::from_str::<UpstreamReply>(&body).map_err(|err| {
            ReplyError::AcceptedUnknown(anyhow!(err).context("decode ptchan reply response"))
        })?;
        let post_id = created.post_id.ok_or_else(|| {
            ReplyError::AcceptedUnknown(anyhow!("ptchan reply response did not include postId"))
        })?;
        Ok(ReplyResponse {
            board: board.to_string(),
            thread_id,
            post_id,
            url: format!(
                "{}#{}",
                thread_url(&self.base_url, board, thread_id),
                post_id
            ),
            origin: crate::contract::PostOrigin {
                kind: crate::contract::OriginKind::Integration,
                name: posting.name.clone(),
            },
        })
    }
}

#[derive(Debug)]
pub(crate) enum ReplyError {
    InvalidRequest(ReplyValidationError),
    Upstream(UpstreamReplyError),
    AcceptedUnknown(anyhow::Error),
    Request(anyhow::Error),
}

impl fmt::Display for ReplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(err) => write!(f, "{err}"),
            Self::Upstream(err) => write!(f, "{err}"),
            Self::AcceptedUnknown(err) | Self::Request(err) => write!(f, "{err}"),
        }
    }
}

impl Error for ReplyError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplyValidationError {
    InvalidBoard,
    InvalidThreadId,
    MissingMessage,
    MessageTooLong,
    InvalidMessage,
}

impl ReplyValidationError {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::InvalidBoard => "invalid_board",
            Self::InvalidThreadId => "invalid_thread_id",
            Self::MissingMessage => "missing_message",
            Self::MessageTooLong => "message_too_long",
            Self::InvalidMessage => "invalid_message",
        }
    }
}

impl fmt::Display for ReplyValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidBoard => "board is invalid",
            Self::InvalidThreadId => "thread_id must be positive",
            Self::MissingMessage => "message is required",
            Self::MessageTooLong => "message is too long",
            Self::InvalidMessage => "message contains unsupported control characters",
        };
        f.write_str(message)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UpstreamReplyErrorCode {
    CaptchaRequired,
    BlockBypassRequired,
    RateLimited,
    ThreadNotFound,
    ThreadLocked,
    ThreadReplyLimit,
    BoardLocked,
    Rejected,
}

impl UpstreamReplyErrorCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::CaptchaRequired => "captcha_required",
            Self::BlockBypassRequired => "block_bypass_required",
            Self::RateLimited => "rate_limited",
            Self::ThreadNotFound => "thread_not_found",
            Self::ThreadLocked => "thread_locked",
            Self::ThreadReplyLimit => "thread_reply_limit",
            Self::BoardLocked => "board_locked",
            Self::Rejected => "rejected",
        }
    }

    pub(crate) const fn retryable(self) -> bool {
        matches!(self, Self::RateLimited)
    }
}

#[derive(Debug)]
pub(crate) struct UpstreamReplyError {
    pub(crate) status: StatusCode,
    pub(crate) code: UpstreamReplyErrorCode,
    pub(crate) message: String,
}

impl UpstreamReplyError {
    fn from_response(status: StatusCode, body: &str) -> Self {
        let message = upstream_error_message(body)
            .unwrap_or_else(|| format!("ptchan rejected the reply with status {status}"));
        let code = classify_upstream_error(status, &message, body);
        Self {
            status,
            code,
            message,
        }
    }

    fn from_body_read_error(status: StatusCode, err: &reqwest::Error) -> Self {
        let message = format!(
            "ptchan rejected the reply with status {status}; response body could not be read"
        );
        let code = classify_upstream_error(status, &message, &err.to_string());
        Self {
            status,
            code,
            message,
        }
    }
}

impl fmt::Display for UpstreamReplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ptchan reply rejected: {} ({})",
            self.message, self.status
        )
    }
}

pub(crate) fn validate_reply(
    board: &str,
    thread_id: i64,
    message: &str,
) -> std::result::Result<(), ReplyValidationError> {
    if !config::valid_board_name(board) {
        return Err(ReplyValidationError::InvalidBoard);
    }
    if thread_id <= 0 {
        return Err(ReplyValidationError::InvalidThreadId);
    }
    let trimmed = message.trim();
    if trimmed.is_empty() {
        return Err(ReplyValidationError::MissingMessage);
    }
    if message.len() > MAX_REPLY_MESSAGE_BYTES {
        return Err(ReplyValidationError::MessageTooLong);
    }
    if message
        .chars()
        .any(|ch| ch.is_control() && !matches!(ch, '\n' | '\r' | '\t'))
    {
        return Err(ReplyValidationError::InvalidMessage);
    }
    Ok(())
}

fn posting_url(base_url: &str, board: &str) -> String {
    format!(
        "{}/forms/board/{}/post",
        base_url.trim_end_matches('/'),
        board
    )
}

fn thread_url(base_url: &str, board: &str, thread_id: i64) -> String {
    format!(
        "{}/{}/thread/{}.html",
        base_url.trim_end_matches('/'),
        board,
        thread_id
    )
}

fn reply_form(
    posting: &PostingConfig,
    thread_id: i64,
    request: &ReplyRequest,
) -> Vec<(String, String)> {
    let mut form = vec![
        ("thread".to_string(), thread_id.to_string()),
        ("message".to_string(), request.message.clone()),
    ];
    if request.sage {
        form.push(("email".to_string(), "sage".to_string()));
    }
    if let Some(name) = posting_name(posting) {
        form.push(("name".to_string(), name));
    }
    if let Some(password) = posting.post_password.as_deref() {
        form.push(("postpassword".to_string(), password.to_string()));
    }
    form
}

fn upstream_error_message(body: &str) -> Option<String> {
    let value = serde_json::from_str::<Value>(body).ok()?;
    if let Some(message) = value.get("message").and_then(Value::as_str) {
        let message = message.trim();
        if !message.is_empty() {
            return Some(message.to_string());
        }
    }
    if let Some(errors) = value.get("errors").and_then(Value::as_array) {
        let messages = errors
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|message| !message.is_empty())
            .collect::<Vec<_>>();
        if !messages.is_empty() {
            return Some(messages.join("; "));
        }
    }
    None
}

fn classify_upstream_error(
    status: StatusCode,
    message: &str,
    body: &str,
) -> UpstreamReplyErrorCode {
    let haystack = format!("{message}\n{body}").to_ascii_lowercase();
    if haystack.contains("captcha") {
        return UpstreamReplyErrorCode::CaptchaRequired;
    }
    if haystack.contains("block bypass") || haystack.contains("bypass_minimal") {
        return UpstreamReplyErrorCode::BlockBypassRequired;
    }
    if status == StatusCode::TOO_MANY_REQUESTS
        || haystack.contains("flood")
        || haystack.contains("wait before")
    {
        return UpstreamReplyErrorCode::RateLimited;
    }
    if haystack.contains("thread does not exist") {
        return UpstreamReplyErrorCode::ThreadNotFound;
    }
    if haystack.contains("thread locked") {
        return UpstreamReplyErrorCode::ThreadLocked;
    }
    if haystack.contains("reply limit") {
        return UpstreamReplyErrorCode::ThreadReplyLimit;
    }
    if haystack.contains("board locked") || haystack.contains("thread creation locked") {
        return UpstreamReplyErrorCode::BoardLocked;
    }
    UpstreamReplyErrorCode::Rejected
}

fn posting_name(posting: &PostingConfig) -> Option<String> {
    let name = posting
        .display_name
        .as_deref()
        .unwrap_or(&posting.name)
        .trim();
    match posting.tripcode.as_deref() {
        Some(tripcode) if posting.secure_tripcode => Some(format!("{name}##{tripcode}")),
        Some(tripcode) => Some(format!("{name}#{tripcode}")),
        None if name.is_empty() => None,
        None => Some(name.to_string()),
    }
}

#[derive(Deserialize)]
struct UpstreamReply {
    #[serde(rename = "postId")]
    post_id: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn validates_reply_shape() {
        assert!(validate_reply("i", 100, "hello").is_ok());
        assert!(validate_reply("../i", 100, "hello").is_err());
        assert!(validate_reply("i", 0, "hello").is_err());
        assert!(validate_reply("i", 100, " ").is_err());
        assert!(matches!(
            validate_reply("i", 100, "hello\0world"),
            Err(ReplyValidationError::InvalidMessage)
        ));
    }

    #[test]
    fn rejects_unknown_reply_request_fields() {
        let err = serde_json::from_str::<ReplyRequest>(
            r#"{"message":"hello","name":"staff","postpassword":"pw"}"#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn keeps_producer_message_as_single_form_value() {
        let posting = PostingConfig {
            name: "agent".to_string(),
            allowed_boards: vec!["i".to_string()],
            display_name: Some("Agent".to_string()),
            secure_tripcode: true,
            secret: "integration-secret".to_string(),
            tripcode: Some("trip-secret".to_string()),
            post_password: Some("post-secret".to_string()),
            timeout: Duration::from_secs(15),
        };
        let request = ReplyRequest {
            message: "hello&thread=999&name=staff\r\npostpassword=hijack".to_string(),
            sage: true,
        };

        let form = reply_form(&posting, 100, &request);

        assert_eq!(
            form,
            vec![
                ("thread".to_string(), "100".to_string()),
                (
                    "message".to_string(),
                    "hello&thread=999&name=staff\r\npostpassword=hijack".to_string()
                ),
                ("email".to_string(), "sage".to_string()),
                ("name".to_string(), "Agent##trip-secret".to_string()),
                ("postpassword".to_string(), "post-secret".to_string()),
            ]
        );
    }

    #[test]
    fn builds_public_posting_and_thread_urls() {
        assert_eq!(
            posting_url("https://ptchan.org/", "cc99"),
            "https://ptchan.org/forms/board/cc99/post"
        );
        assert_eq!(
            thread_url("https://ptchan.org/", "cc99", 397),
            "https://ptchan.org/cc99/thread/397.html"
        );
    }

    #[test]
    fn builds_name_with_configured_tripcode_secret() {
        let posting = PostingConfig {
            name: "agent".to_string(),
            allowed_boards: vec!["i".to_string()],
            display_name: Some("Agent".to_string()),
            secure_tripcode: true,
            secret: "integration-secret".to_string(),
            tripcode: Some("trip-secret".to_string()),
            post_password: None,
            timeout: Duration::from_secs(15),
        };

        assert_eq!(
            posting_name(&posting).as_deref(),
            Some("Agent##trip-secret")
        );
    }

    #[test]
    fn can_build_legacy_tripcode_name() {
        let posting = PostingConfig {
            name: "agent".to_string(),
            allowed_boards: vec!["i".to_string()],
            display_name: Some("Agent".to_string()),
            secure_tripcode: false,
            secret: "integration-secret".to_string(),
            tripcode: Some("trip-secret".to_string()),
            post_password: None,
            timeout: Duration::from_secs(15),
        };

        assert_eq!(posting_name(&posting).as_deref(), Some("Agent#trip-secret"));
    }

    #[test]
    fn extracts_jschan_error_messages() {
        let body =
            r#"{"title":"Forbidden","message":"Please complete a block bypass to continue"}"#;
        let err = UpstreamReplyError::from_response(StatusCode::FORBIDDEN, body);

        assert_eq!(err.code, UpstreamReplyErrorCode::BlockBypassRequired);
        assert_eq!(err.message, "Please complete a block bypass to continue");
    }

    #[test]
    fn classifies_captcha_errors_from_jschan_body() {
        let body = r#"{"title":"Forbidden","message":"Captcha failed"}"#;
        let err = UpstreamReplyError::from_response(StatusCode::FORBIDDEN, body);

        assert_eq!(err.code, UpstreamReplyErrorCode::CaptchaRequired);
        assert!(!err.code.retryable());
    }

    #[test]
    fn joins_jschan_validation_errors() {
        let body = r#"{"title":"Bad request","errors":["Thread locked","Message must be 10 characters or less"]}"#;
        let err = UpstreamReplyError::from_response(StatusCode::BAD_REQUEST, body);

        assert_eq!(err.code, UpstreamReplyErrorCode::ThreadLocked);
        assert_eq!(
            err.message,
            "Thread locked; Message must be 10 characters or less"
        );
    }
}

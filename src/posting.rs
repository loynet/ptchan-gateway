use std::{error::Error, fmt};

use anyhow::{anyhow, Context, Result};
use reqwest::{Client, StatusCode};
use serde::Deserialize;

use crate::{
    config::{self, PostingConfig, PtchanConfig},
    contract::{ErrorCode, OriginKind, PostOrigin, ReplyRequest, ReplyResponse},
};

pub(crate) const MAX_REPLY_MESSAGE_BYTES: usize = 8_000;
const REPLY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

#[derive(Clone)]
pub(crate) struct PostWriter {
    base_url: String,
    client: Client,
}

impl PostWriter {
    pub(crate) fn new(cfg: &PtchanConfig) -> Result<Self> {
        let client = Client::builder()
            .user_agent(config::gateway_user_agent())
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
        let response = self
            .reply_request(posting, board, thread_id, request)
            .send()
            .await
            .map_err(|err| ReplyError::StateUnknown {
                source: anyhow!(err).context("send ptchan reply"),
                accepted: false,
            })?;
        let status = response.status();
        let body = match response.text().await {
            Ok(body) => body,
            Err(err) if status.is_success() => {
                return Err(ReplyError::StateUnknown {
                    source: anyhow!(err).context("read ptchan reply response"),
                    accepted: true,
                });
            }
            Err(_) => {
                return Err(ReplyError::Upstream(
                    UpstreamReplyError::from_unreadable_response(status),
                ));
            }
        };
        if !status.is_success() {
            return Err(ReplyError::Upstream(UpstreamReplyError::from_response(
                status, &body,
            )));
        }
        let created = serde_json::from_str::<UpstreamReply>(&body).map_err(|err| {
            ReplyError::StateUnknown {
                source: anyhow!(err).context("decode ptchan reply response"),
                accepted: true,
            }
        })?;
        let post_id = created.post_id.ok_or_else(|| ReplyError::StateUnknown {
            source: anyhow!("ptchan reply response did not include postId"),
            accepted: true,
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
            origin: PostOrigin {
                kind: OriginKind::Integration,
                name: posting.name.clone(),
            },
        })
    }

    fn reply_request(
        &self,
        posting: &PostingConfig,
        board: &str,
        thread_id: i64,
        request: &ReplyRequest,
    ) -> reqwest::RequestBuilder {
        self.client
            .post(posting_url(&self.base_url, board))
            .timeout(REPLY_TIMEOUT)
            .header("accept", "application/json")
            .header("referer", thread_url(&self.base_url, board, thread_id))
            .header("x-using-xhr", "true")
            .form(&reply_form(posting, thread_id, request))
    }
}

#[derive(Debug)]
pub(crate) enum ReplyError {
    Upstream(UpstreamReplyError),
    StateUnknown {
        source: anyhow::Error,
        accepted: bool,
    },
}

impl fmt::Display for ReplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Upstream(err) => write!(f, "{err}"),
            Self::StateUnknown { source, .. } => write!(f, "{source}"),
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
    pub(crate) const fn code(self) -> ErrorCode {
        match self {
            Self::InvalidBoard => ErrorCode::InvalidBoard,
            Self::InvalidThreadId => ErrorCode::InvalidThreadId,
            Self::MissingMessage => ErrorCode::MissingMessage,
            Self::MessageTooLong => ErrorCode::MessageTooLong,
            Self::InvalidMessage => ErrorCode::InvalidMessage,
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

#[derive(Debug)]
pub(crate) struct UpstreamReplyError {
    pub(crate) status: StatusCode,
    pub(crate) code: ErrorCode,
}

impl UpstreamReplyError {
    fn from_response(status: StatusCode, body: &str) -> Self {
        Self {
            status,
            code: classify_upstream_error(status, body),
        }
    }

    fn from_unreadable_response(status: StatusCode) -> Self {
        Self {
            status,
            code: classify_upstream_error(status, ""),
        }
    }

    pub(crate) const fn public_message(&self) -> &'static str {
        match self.code {
            ErrorCode::CaptchaRequired => "ptchan requires captcha verification",
            ErrorCode::BlockBypassRequired => "ptchan requires a block bypass",
            ErrorCode::RateLimited => "ptchan rate limit exceeded",
            ErrorCode::ThreadNotFound => "thread was not found",
            ErrorCode::ThreadLocked => "thread is locked",
            ErrorCode::ThreadReplyLimit => "thread reply limit reached",
            ErrorCode::BoardLocked => "board is locked",
            _ => "ptchan rejected the reply",
        }
    }
}

impl fmt::Display for UpstreamReplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ptchan reply rejected: {} ({})", self.code, self.status)
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
    form.push(("name".to_string(), posting.form_name()));
    form.push(("postpassword".to_string(), posting.post_password.clone()));
    form
}

fn classify_upstream_error(status: StatusCode, body: &str) -> ErrorCode {
    let haystack = body.to_ascii_lowercase();
    if haystack.contains("captcha") {
        return ErrorCode::CaptchaRequired;
    }
    if haystack.contains("block bypass") || haystack.contains("bypass_minimal") {
        return ErrorCode::BlockBypassRequired;
    }
    if status == StatusCode::TOO_MANY_REQUESTS
        || haystack.contains("flood")
        || haystack.contains("wait before")
    {
        return ErrorCode::RateLimited;
    }
    if haystack.contains("thread does not exist") {
        return ErrorCode::ThreadNotFound;
    }
    if haystack.contains("thread locked") {
        return ErrorCode::ThreadLocked;
    }
    if haystack.contains("reply limit") {
        return ErrorCode::ThreadReplyLimit;
    }
    if haystack.contains("board locked") || haystack.contains("thread creation locked") {
        return ErrorCode::BoardLocked;
    }
    ErrorCode::Rejected
}

#[derive(Deserialize)]
struct UpstreamReply {
    #[serde(rename = "postId")]
    post_id: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn builds_only_the_public_reply_request() {
        let writer = PostWriter::new(&PtchanConfig {
            base_url: "https://ptchan.test".to_string(),
        })
        .unwrap();
        let request = ReplyRequest {
            message: "hello&thread=999&name=staff\r\npostpassword=hijack".to_string(),
            sage: true,
        };

        let request = writer
            .reply_request(&posting(), "i", 100, &request)
            .build()
            .unwrap();

        assert_eq!(
            request.url().as_str(),
            "https://ptchan.test/forms/board/i/post"
        );
        assert_eq!(
            request.headers().get("referer").unwrap(),
            "https://ptchan.test/i/thread/100.html"
        );
        assert!(request.headers().get("cookie").is_none());
        assert_eq!(
            request.body().unwrap().as_bytes().unwrap(),
            b"thread=100&message=hello%26thread%3D999%26name%3Dstaff%0D%0Apostpassword%3Dhijack&email=sage&name=Agent%23%23trip-secret&postpassword=post-secret"
        );
    }

    #[test]
    fn classifies_known_jschan_rejections() {
        let cases = [
            (
                StatusCode::FORBIDDEN,
                r#"{"message":"Captcha failed"}"#,
                ErrorCode::CaptchaRequired,
            ),
            (
                StatusCode::FORBIDDEN,
                r#"{"message":"Please complete a block bypass"}"#,
                ErrorCode::BlockBypassRequired,
            ),
            (
                StatusCode::TOO_MANY_REQUESTS,
                r#"{"message":"Flood detected"}"#,
                ErrorCode::RateLimited,
            ),
            (
                StatusCode::NOT_FOUND,
                r#"{"message":"Thread does not exist"}"#,
                ErrorCode::ThreadNotFound,
            ),
            (
                StatusCode::BAD_REQUEST,
                r#"{"errors":["Thread locked","Message is too long"]}"#,
                ErrorCode::ThreadLocked,
            ),
            (
                StatusCode::BAD_REQUEST,
                r#"{"message":"Thread reply limit reached"}"#,
                ErrorCode::ThreadReplyLimit,
            ),
            (
                StatusCode::FORBIDDEN,
                r#"{"message":"Board locked"}"#,
                ErrorCode::BoardLocked,
            ),
            (
                StatusCode::BAD_REQUEST,
                r#"{"message":"Unknown rejection"}"#,
                ErrorCode::Rejected,
            ),
        ];

        for (status, body, expected) in cases {
            let err = UpstreamReplyError::from_response(status, body);

            assert_eq!(err.code, expected, "{body}");
            assert_eq!(err.code.retryable(), expected == ErrorCode::RateLimited);
            assert!(!err.public_message().contains("Unknown rejection"));
        }

        let rejected = UpstreamReplyError::from_response(
            StatusCode::BAD_REQUEST,
            r#"{"message":"private upstream detail"}"#,
        );
        assert_eq!(rejected.public_message(), "ptchan rejected the reply");
        assert!(!rejected.to_string().contains("private upstream detail"));
    }

    fn posting() -> PostingConfig {
        PostingConfig {
            name: "agent".to_string(),
            allowed_boards: vec!["i".to_string()],
            display_name: Some("Agent".to_string()),
            secret: "integration-secret".to_string(),
            tripcode_secret: "trip-secret".to_string(),
            public_tripcode: "!!X8NXmAS44=".to_string(),
            post_password: "post-secret".to_string(),
        }
    }
}

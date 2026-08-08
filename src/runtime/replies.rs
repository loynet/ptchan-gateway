use std::{sync::Arc, time::Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, FromRequestParts, Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use tracing::{debug, warn};

use crate::{
    config::PostingConfig,
    contract::{ErrorCode, ErrorEnvelope, ReplyRequest, ReplyResponse},
    posting::{self, ReplyError},
};

use super::{
    auth::authenticate_posting,
    responses::{gateway_rate_limited_error, posting_status_for_upstream, ApiError},
    telemetry::{
        accept_posting_request, record_gateway_rate_limited_request, reject_posting_request,
    },
    AppState,
};

#[utoipa::path(
    post,
    path = "/integration/v1/threads/{board}/{thread_id}/replies",
    tag = "integration",
    params(
        ("board" = String, Path, description = "ptchan board name"),
        ("thread_id" = i64, Path, minimum = 1, description = "Thread post ID"),
        ("x-ptchan-integration" = String, Header, description = "Configured integration name"),
        ("x-ptchan-timestamp" = String, Header, description = "RFC 3339 signing timestamp within five minutes of gateway time"),
        ("x-ptchan-signature" = String, Header, description = "HMAC-SHA256 signature over <timestamp>.<method>.<path-and-query>.<exact JSON body>")
    ),
    request_body(
        content = ReplyRequest,
        content_type = "application/json",
        description = "Gateway-owned public reply fields"
    ),
    responses(
        (status = 200, description = "Reply accepted and post coordinates decoded", body = ReplyResponse),
        (status = 400, description = "Invalid JSON or reply fields", body = ErrorEnvelope),
        (status = 401, description = "Authentication failed", body = ErrorEnvelope),
        (status = 403, description = "Posting capability or board access denied", body = ErrorEnvelope),
        (status = 404, description = "Thread not found", body = ErrorEnvelope),
        (status = 413, description = "Request body exceeds the gateway body limit", body = ErrorEnvelope),
        (status = 429, description = "Gateway or upstream rate limit exceeded", body = ErrorEnvelope),
        (status = 502, description = "Upstream rejected the reply or its final state is unknown", body = ErrorEnvelope)
    )
)]
pub(super) async fn integration_reply(
    State(state): State<AppState>,
    Path((board, thread_id)): Path<(String, String)>,
    reply: SignedReply,
) -> Response {
    let thread_id = thread_id.parse::<i64>().unwrap_or_default();
    match PreparedReply::new(board, thread_id, reply) {
        Ok(reply) => reply.submit(&state).await,
        Err(err) => err.into_response(),
    }
}

struct PreparedReply {
    posting: Arc<PostingConfig>,
    body: ReplyRequest,
    board: String,
    thread_id: i64,
    started_at: Instant,
}

impl PreparedReply {
    fn new(
        board: String,
        thread_id: i64,
        reply: SignedReply,
    ) -> std::result::Result<Self, ApiError> {
        let integration = &reply.posting.name;
        if let Err(err) = posting::validate_reply(&board, thread_id, &reply.body.message) {
            reject_posting_request(
                integration,
                &board,
                thread_id,
                err.code().as_str(),
                StatusCode::BAD_REQUEST,
                reply.started_at,
            );
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                err.code(),
                err.to_string(),
            ));
        }
        Ok(Self {
            posting: reply.posting,
            body: reply.body,
            board,
            thread_id,
            started_at: reply.started_at,
        })
    }

    async fn submit(self, state: &AppState) -> Response {
        match state
            .post_writer
            .reply(&self.posting, &self.board, self.thread_id, &self.body)
            .await
        {
            Ok(response) => self.accepted(response),
            Err(ReplyError::Upstream(err)) => self.rejected(&err),
            Err(ReplyError::StateUnknown { source, accepted }) => {
                self.state_unknown(&source, accepted)
            }
        }
    }

    fn accepted(self, response: ReplyResponse) -> Response {
        let integration = &self.posting.name;
        accept_posting_request(
            integration,
            &self.board,
            self.thread_id,
            "success",
            StatusCode::OK,
            self.started_at,
        );
        debug!(
            integration,
            board = self.board,
            thread_id = self.thread_id,
            post_id = response.post_id,
            "integration reply accepted"
        );
        Json(response).into_response()
    }

    fn rejected(self, err: &posting::UpstreamReplyError) -> Response {
        let integration = &self.posting.name;
        let status = posting_status_for_upstream(err.status);
        reject_posting_request(
            integration,
            &self.board,
            self.thread_id,
            err.code.as_str(),
            status,
            self.started_at,
        );
        ApiError::new(status, err.code, err.public_message())
            .retryable(err.code.retryable())
            .upstream_status(err.status.as_u16())
            .into_response()
    }

    fn state_unknown(self, source: &anyhow::Error, accepted: bool) -> Response {
        let integration = &self.posting.name;
        reject_posting_request(
            integration,
            &self.board,
            self.thread_id,
            "reply_state_unknown",
            StatusCode::BAD_GATEWAY,
            self.started_at,
        );
        let message = if accepted {
            warn!(error = %source, integration, board = self.board, thread_id = self.thread_id, "integration reply accepted but response could not be decoded");
            "ptchan may have accepted the reply; check the thread before retrying"
        } else {
            warn!(error = %source, integration, board = self.board, thread_id = self.thread_id, "integration reply result is unknown");
            "ptchan reply result is unknown; check the thread before retrying"
        };
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            ErrorCode::ReplyStateUnknown,
            message,
        )
        .retryable(false)
        .into_response()
    }
}

pub(super) struct SignedReply {
    posting: Arc<PostingConfig>,
    body: ReplyRequest,
    started_at: Instant,
}

impl FromRequest<AppState> for SignedReply {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &AppState,
    ) -> std::result::Result<Self, Self::Rejection> {
        let started_at = Instant::now();
        let (mut parts, body) = request.into_parts();
        let Path((board, thread_id)) =
            Path::<(String, String)>::from_request_parts(&mut parts, state)
                .await
                .map_err(IntoResponse::into_response)?;
        let thread_id = thread_id.parse::<i64>().unwrap_or_default();
        let headers = parts.headers.clone();
        let method = parts.method.clone();
        let uri = parts.uri.clone();
        let body = Bytes::from_request(Request::from_parts(parts, body), state)
            .await
            .map_err(|rejection| {
                let status = rejection.status();
                let (code, message) = if status == StatusCode::PAYLOAD_TOO_LARGE {
                    (
                        ErrorCode::PayloadTooLarge,
                        "request body exceeds the gateway body limit",
                    )
                } else {
                    (ErrorCode::InvalidJson, "request body could not be read")
                };
                ApiError::new(status, code, message).into_response()
            })?;
        let posting = match authenticate_posting(state, &headers, &method, &uri, &body) {
            Ok(posting) => posting,
            Err(err) => {
                reject_posting_request(
                    err.label(),
                    &board,
                    thread_id,
                    "unauthorized",
                    StatusCode::UNAUTHORIZED,
                    started_at,
                );
                return Err(err.into_response());
            }
        };
        if !posting.board_allowed(&board) {
            reject_posting_request(
                &posting.name,
                &board,
                thread_id,
                ErrorCode::BoardNotAllowed.as_str(),
                StatusCode::FORBIDDEN,
                started_at,
            );
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                ErrorCode::BoardNotAllowed,
                "integration is not allowed to post on this board",
            )
            .into_response());
        }
        if let Err(rejection) = state.rate_limiters.check_posting(&posting.name) {
            reject_posting_request(
                &posting.name,
                &board,
                thread_id,
                ErrorCode::RateLimited.as_str(),
                StatusCode::TOO_MANY_REQUESTS,
                started_at,
            );
            record_gateway_rate_limited_request(
                &posting.name,
                &board,
                "posting",
                rejection.as_str(),
            );
            return Err(gateway_rate_limited_error().into_response());
        }
        let body = serde_json::from_slice::<ReplyRequest>(&body).map_err(|_| {
            reject_posting_request(
                &posting.name,
                &board,
                thread_id,
                "invalid_json",
                StatusCode::BAD_REQUEST,
                started_at,
            );
            ApiError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidJson,
                "request body must be valid JSON",
            )
            .into_response()
        })?;
        Ok(Self {
            posting,
            body,
            started_at,
        })
    }
}

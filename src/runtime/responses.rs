use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::contract::{ErrorBody, ErrorCode, ErrorEnvelope};

pub(super) struct ApiError {
    status: StatusCode,
    body: ErrorBody,
}

impl ApiError {
    pub(super) fn new(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ErrorBody {
                code,
                message: message.into(),
                retryable: code.retryable(),
                upstream_status: None,
            },
        }
    }

    pub(super) fn retryable(mut self, retryable: bool) -> Self {
        self.body.retryable = retryable;
        self
    }

    pub(super) fn upstream_status(mut self, upstream_status: u16) -> Self {
        self.body.upstream_status = Some(upstream_status);
        self
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(ErrorEnvelope { error: self.body })).into_response()
    }
}

pub(super) fn posting_status_for_upstream(status: StatusCode) -> StatusCode {
    if status.is_client_error() {
        status
    } else {
        StatusCode::BAD_GATEWAY
    }
}

pub(super) fn gateway_rate_limited_error() -> ApiError {
    ApiError::new(
        StatusCode::TOO_MANY_REQUESTS,
        ErrorCode::RateLimited,
        "gateway rate limit exceeded",
    )
}

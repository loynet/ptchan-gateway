use axum::{
    extract::{rejection::QueryRejection, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use tracing::warn;

use crate::{
    config,
    contract::{ErrorCode, ErrorEnvelope, Thread},
    reading::DEFAULT_THREAD_LIMIT,
};

use super::{
    auth::VerifiedReading,
    responses::{gateway_rate_limited_error, ApiError},
    telemetry::{
        accept_reading_request, record_gateway_rate_limited_request, reject_reading_request,
    },
    AppState,
};

#[derive(Deserialize)]
pub(super) struct ThreadQuery {
    limit: Option<usize>,
}

#[utoipa::path(
    get,
    path = "/integration/v1/threads/{board}/{thread_id}",
    tag = "integration",
    params(
        ("board" = String, Path, description = "ptchan board name"),
        ("thread_id" = i64, Path, minimum = 1, description = "Thread post ID"),
        ("limit" = Option<usize>, Query, minimum = 1, maximum = 200, description = "Most recent posts to return; defaults to 50 and values above 200 are capped"),
        ("x-ptchan-integration" = String, Header, description = "Configured integration name"),
        ("x-ptchan-timestamp" = String, Header, description = "RFC 3339 signing timestamp within five minutes of gateway time"),
        ("x-ptchan-signature" = String, Header, description = "HMAC-SHA256 signature over <timestamp>.<method>.<path-and-query>")
    ),
    responses(
        (status = 200, description = "Sanitized thread in chronological order", body = Thread),
        (status = 400, description = "Invalid path or query value", body = ErrorEnvelope),
        (status = 401, description = "Authentication failed", body = ErrorEnvelope),
        (status = 403, description = "Reading capability or board access denied", body = ErrorEnvelope),
        (status = 404, description = "Thread not found", body = ErrorEnvelope),
        (status = 429, description = "Gateway rate limit exceeded", body = ErrorEnvelope),
        (status = 502, description = "ptchan could not provide the thread", body = ErrorEnvelope)
    )
)]
pub(super) async fn integration_thread(
    State(state): State<AppState>,
    Path((board, thread_id)): Path<(String, String)>,
    query: Result<Query<ThreadQuery>, QueryRejection>,
    auth: VerifiedReading,
) -> Response {
    match PreparedRead::new(&state, board, &thread_id, query, &auth) {
        Ok(read) => read.fetch(&state).await,
        Err(err) => err.into_response(),
    }
}

struct PreparedRead {
    integration: String,
    board: String,
    thread_id: i64,
    limit: usize,
    started_at: std::time::Instant,
}

impl PreparedRead {
    fn new(
        state: &AppState,
        board: String,
        thread_id: &str,
        query: Result<Query<ThreadQuery>, QueryRejection>,
        auth: &VerifiedReading,
    ) -> Result<Self, ApiError> {
        let started_at = auth.started_at;
        let integration = auth.integration.name.clone();
        let thread_id = thread_id.parse::<i64>().unwrap_or_default();
        if !auth.integration.reading {
            return Err(read_error(
                &integration,
                &board,
                thread_id,
                ErrorCode::CapabilityNotEnabled,
                StatusCode::FORBIDDEN,
                "integration does not have the reading capability",
                started_at,
            ));
        }
        if !auth.integration.board_allowed(&board) {
            return Err(read_error(
                &integration,
                &board,
                thread_id,
                ErrorCode::BoardNotAllowed,
                StatusCode::FORBIDDEN,
                "integration is not allowed to read this board",
                started_at,
            ));
        }
        if let Err(rejection) = state.rate_limiters.check_reading(&integration) {
            reject_reading_request(
                &integration,
                &board,
                thread_id,
                ErrorCode::RateLimited.as_str(),
                StatusCode::TOO_MANY_REQUESTS,
                started_at,
            );
            record_gateway_rate_limited_request(
                &integration,
                &board,
                "reading",
                rejection.as_str(),
            );
            return Err(gateway_rate_limited_error());
        }
        if !config::valid_board_name(&board) {
            return Err(read_error(
                &integration,
                &board,
                thread_id,
                ErrorCode::InvalidBoard,
                StatusCode::BAD_REQUEST,
                "board is invalid",
                started_at,
            ));
        }
        if thread_id <= 0 {
            return Err(read_error(
                &integration,
                &board,
                thread_id,
                ErrorCode::InvalidThreadId,
                StatusCode::BAD_REQUEST,
                "thread_id must be positive",
                started_at,
            ));
        }
        let Ok(Query(query)) = query else {
            return Err(read_error(
                &integration,
                &board,
                thread_id,
                ErrorCode::InvalidQuery,
                StatusCode::BAD_REQUEST,
                "query parameters are invalid",
                started_at,
            ));
        };
        Ok(Self {
            integration,
            board,
            thread_id,
            limit: query.limit.unwrap_or(DEFAULT_THREAD_LIMIT),
            started_at,
        })
    }

    async fn fetch(self, state: &AppState) -> Response {
        match state
            .thread_reader
            .fetch_thread(&self.board, self.thread_id, self.limit)
            .await
        {
            Ok(Some(mut thread)) => {
                state.origins.annotate_thread(&mut thread);
                accept_reading_request(
                    &self.integration,
                    &self.board,
                    self.thread_id,
                    "success",
                    StatusCode::OK,
                    self.started_at,
                );
                Json(thread).into_response()
            }
            Ok(None) => read_error(
                &self.integration,
                &self.board,
                self.thread_id,
                ErrorCode::ThreadNotFound,
                StatusCode::NOT_FOUND,
                "thread was not found",
                self.started_at,
            )
            .into_response(),
            Err(err) => {
                warn!(error = %err, integration = %self.integration, board = %self.board, thread_id = self.thread_id, "integration thread reading failed");
                read_error(
                    &self.integration,
                    &self.board,
                    self.thread_id,
                    ErrorCode::UpstreamUnavailable,
                    StatusCode::BAD_GATEWAY,
                    "ptchan thread reading failed",
                    self.started_at,
                )
                .into_response()
            }
        }
    }
}

fn read_error(
    integration: &str,
    board: &str,
    thread_id: i64,
    code: ErrorCode,
    status: StatusCode,
    message: &str,
    started_at: std::time::Instant,
) -> ApiError {
    reject_reading_request(
        integration,
        board,
        thread_id,
        code.as_str(),
        status,
        started_at,
    );
    ApiError::new(status, code, message)
}

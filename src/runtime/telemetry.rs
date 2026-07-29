use std::time::{Duration, Instant};

use axum::http::StatusCode;
use tracing::{info, warn};

use crate::{config, metrics};

pub(super) fn record_reading_request(
    integration: &str,
    board: &str,
    result: &str,
    elapsed: Duration,
) {
    let board = metric_board(board);
    metrics::READING_REQUESTS
        .with_label_values(&[integration, board, result])
        .inc();
    metrics::READING_REQUEST_SECONDS
        .with_label_values(&[integration, board, result])
        .observe(elapsed.as_secs_f64());
}

fn record_posting_request(integration: &str, board: &str, result: &str, elapsed: Duration) {
    let board = metric_board(board);
    metrics::POSTING_REQUESTS
        .with_label_values(&[integration, board, result])
        .inc();
    metrics::POSTING_REQUEST_SECONDS
        .with_label_values(&[integration, board, result])
        .observe(elapsed.as_secs_f64());
}

pub(super) fn accept_reading_request(
    integration: &str,
    board: &str,
    thread_id: i64,
    result: &str,
    status: StatusCode,
    started_at: Instant,
) {
    let elapsed = started_at.elapsed();
    record_reading_request(integration, board, result, elapsed);
    log_gateway_request_accepted(
        "reading",
        integration,
        board,
        thread_id,
        result,
        status,
        elapsed,
    );
}

pub(super) fn reject_reading_request(
    integration: &str,
    board: &str,
    thread_id: i64,
    result: &str,
    status: StatusCode,
    started_at: Instant,
) {
    let elapsed = started_at.elapsed();
    record_reading_request(integration, board, result, elapsed);
    log_gateway_request_rejected(
        "reading",
        integration,
        board,
        thread_id,
        result,
        status,
        elapsed,
    );
}

pub(super) fn accept_posting_request(
    integration: &str,
    board: &str,
    thread_id: i64,
    result: &str,
    status: StatusCode,
    started_at: Instant,
) {
    let elapsed = started_at.elapsed();
    record_posting_request(integration, board, result, elapsed);
    log_gateway_request_accepted(
        "posting",
        integration,
        board,
        thread_id,
        result,
        status,
        elapsed,
    );
}

pub(super) fn reject_posting_request(
    integration: &str,
    board: &str,
    thread_id: i64,
    result: &str,
    status: StatusCode,
    started_at: Instant,
) {
    let elapsed = started_at.elapsed();
    record_posting_request(integration, board, result, elapsed);
    log_gateway_request_rejected(
        "posting",
        integration,
        board,
        thread_id,
        result,
        status,
        elapsed,
    );
}

pub(super) fn record_gateway_rate_limited_request(
    integration: &str,
    board: &str,
    capability: &str,
    scope: &str,
) {
    let board = metric_board(board);
    metrics::GATEWAY_RATE_LIMITED_REQUESTS
        .with_label_values(&[integration, board, capability, scope])
        .inc();
}

fn log_gateway_request_accepted(
    capability: &str,
    integration: &str,
    board: &str,
    thread_id: i64,
    result: &str,
    status: StatusCode,
    elapsed: Duration,
) {
    info!(
        integration,
        board = metric_board(board),
        capability,
        thread_id,
        result,
        status = status.as_u16(),
        elapsed_ms = elapsed.as_millis(),
        "integration api request accepted"
    );
}

fn log_gateway_request_rejected(
    capability: &str,
    integration: &str,
    board: &str,
    thread_id: i64,
    result: &str,
    status: StatusCode,
    elapsed: Duration,
) {
    warn!(
        integration,
        board = metric_board(board),
        capability,
        thread_id,
        result,
        status = status.as_u16(),
        elapsed_ms = elapsed.as_millis(),
        "integration api request rejected"
    );
}

fn metric_board(board: &str) -> &str {
    if config::valid_board_name(board) {
        board
    } else {
        "invalid"
    }
}

use std::sync::LazyLock;

use anyhow::{Context, Result};
use prometheus::{
    Encoder, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, TextEncoder,
};

pub(crate) static SOCKET_CONNECTION_ATTEMPTS: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "ptchan_socket_connection_attempts_total",
        "Socket connection attempts"
    )
    .unwrap()
});
pub(crate) static SOCKET_JOIN_FAILURES: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "ptchan_socket_join_failures_total",
        "Socket room join failures"
    )
    .unwrap()
});
pub(crate) static SOCKET_JOINED: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "ptchan_socket_joined",
        "Whether the hashed global room is joined"
    )
    .unwrap()
});
pub(crate) static UPSTREAM_AUTH_HEALTHY: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "ptchan_upstream_auth_healthy",
        "Whether the upstream management session is currently usable"
    )
    .unwrap()
});
pub(crate) static UPSTREAM_REQUIRED: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "ptchan_upstream_required",
        "Whether runtime readiness requires upstream socket/auth health"
    )
    .unwrap()
});
pub(crate) static SOCKET_EVENTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "ptchan_socket_events_total",
        "Socket events handled",
        &["result"]
    )
    .unwrap()
});
pub(crate) static SESSION_REFRESH: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "ptchan_session_refresh_total",
        "Session refresh attempts",
        &["result"]
    )
    .unwrap()
});
pub(crate) static WEBHOOK_DELIVERIES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "ptchan_webhook_deliveries_total",
        "Webhook delivery attempts",
        &["webhook", "result"]
    )
    .unwrap()
});
pub(crate) static WEBHOOK_PENDING: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!("ptchan_webhook_pending", "Pending webhook deliveries").unwrap()
});
pub(crate) static WEBHOOK_PENDING_BY_WEBHOOK: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    prometheus::register_int_gauge_vec!(
        "ptchan_webhook_pending_by_webhook",
        "Pending webhook deliveries by configured webhook",
        &["webhook"]
    )
    .unwrap()
});
pub(crate) static WEBHOOK_DELIVERY_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus::register_histogram_vec!(
        "ptchan_webhook_delivery_seconds",
        "Webhook delivery request latency",
        &["webhook", "result"],
        vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
    )
    .unwrap()
});
pub(crate) static READING_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "ptchan_reading_requests_total",
        "Integration reading requests",
        &["integration", "board", "result"]
    )
    .unwrap()
});
pub(crate) static READING_REQUEST_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus::register_histogram_vec!(
        "ptchan_reading_request_seconds",
        "Integration reading request latency",
        &["integration", "board", "result"],
        vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    .unwrap()
});
pub(crate) static POSTING_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "ptchan_posting_requests_total",
        "Integration posting requests",
        &["integration", "board", "result"]
    )
    .unwrap()
});
pub(crate) static POSTING_REQUEST_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus::register_histogram_vec!(
        "ptchan_posting_request_seconds",
        "Integration posting request latency",
        &["integration", "board", "result"],
        vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
    )
    .unwrap()
});
pub(crate) static SQLITE_ERRORS: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!("ptchan_sqlite_errors_total", "SQLite operation failures")
        .unwrap()
});
pub(crate) static REDACTION_DROPS: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "ptchan_redaction_drops_total",
        "Payloads dropped by redaction checks"
    )
    .unwrap()
});

pub(crate) fn init() {
    LazyLock::force(&SOCKET_CONNECTION_ATTEMPTS);
    LazyLock::force(&SOCKET_JOIN_FAILURES);
    LazyLock::force(&SOCKET_JOINED);
    LazyLock::force(&UPSTREAM_AUTH_HEALTHY);
    LazyLock::force(&UPSTREAM_REQUIRED);
    LazyLock::force(&SOCKET_EVENTS);
    LazyLock::force(&SESSION_REFRESH);
    LazyLock::force(&WEBHOOK_DELIVERIES);
    LazyLock::force(&WEBHOOK_PENDING);
    LazyLock::force(&WEBHOOK_PENDING_BY_WEBHOOK);
    LazyLock::force(&WEBHOOK_DELIVERY_SECONDS);
    LazyLock::force(&READING_REQUESTS);
    LazyLock::force(&READING_REQUEST_SECONDS);
    LazyLock::force(&POSTING_REQUESTS);
    LazyLock::force(&POSTING_REQUEST_SECONDS);
    LazyLock::force(&SQLITE_ERRORS);
    LazyLock::force(&REDACTION_DROPS);
}

pub(crate) fn render() -> Result<String> {
    init();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .context("encode prometheus metrics")?;
    String::from_utf8(buffer).context("prometheus metrics were not utf8")
}

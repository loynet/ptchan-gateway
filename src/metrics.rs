use std::sync::LazyLock;

use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, TextEncoder};

pub static SOCKET_RECONNECTS: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "ptchan_socket_reconnects_total",
        "Socket reconnect attempts"
    )
    .unwrap()
});
pub static SOCKET_JOIN_FAILURES: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "ptchan_socket_join_failures_total",
        "Socket room join failures"
    )
    .unwrap()
});
pub static SOCKET_JOINED: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "ptchan_socket_joined",
        "Whether the hashed global room is joined"
    )
    .unwrap()
});
pub static SOCKET_EVENTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "ptchan_socket_events_total",
        "Socket events handled",
        &["result"]
    )
    .unwrap()
});
pub static SESSION_REFRESH: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "ptchan_session_refresh_total",
        "Session refresh attempts",
        &["result"]
    )
    .unwrap()
});
pub static WEBHOOK_DELIVERIES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "ptchan_webhook_deliveries_total",
        "Webhook delivery attempts",
        &["webhook", "result"]
    )
    .unwrap()
});
pub static WEBHOOK_PENDING: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!("ptchan_webhook_pending", "Pending webhook deliveries").unwrap()
});
pub static CONTEXT_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "ptchan_context_requests_total",
        "Consumer context requests",
        &["result"]
    )
    .unwrap()
});
pub static SQLITE_ERRORS: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!("ptchan_sqlite_errors_total", "SQLite operation failures")
        .unwrap()
});
pub static REDACTION_DROPS: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "ptchan_redaction_drops_total",
        "Payloads dropped by redaction checks"
    )
    .unwrap()
});

pub fn init() {
    LazyLock::force(&SOCKET_RECONNECTS);
    LazyLock::force(&SOCKET_JOIN_FAILURES);
    LazyLock::force(&SOCKET_JOINED);
    LazyLock::force(&SOCKET_EVENTS);
    LazyLock::force(&SESSION_REFRESH);
    LazyLock::force(&WEBHOOK_DELIVERIES);
    LazyLock::force(&WEBHOOK_PENDING);
    LazyLock::force(&CONTEXT_REQUESTS);
    LazyLock::force(&SQLITE_ERRORS);
    LazyLock::force(&REDACTION_DROPS);
}

pub fn render() -> String {
    init();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .unwrap();
    String::from_utf8(buffer).unwrap()
}

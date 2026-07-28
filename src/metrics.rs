use std::sync::LazyLock;

use anyhow::{Context, Result};
use prometheus::{
    Encoder, Gauge, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    TextEncoder,
};

pub(crate) static SOCKET_CONNECTION_ATTEMPTS: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "ptchan_socket_connection_attempts_total",
        "Socket connection attempts"
    )
    .unwrap()
});
pub(crate) static SOCKET_ACTIVE_CONNECTIONS: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "ptchan_socket_active_connections",
        "Currently active upstream socket connections owned by this process"
    )
    .unwrap()
});
pub(crate) static SOCKET_CONNECTION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    prometheus::register_histogram!(
        "ptchan_socket_connection_seconds",
        "Upstream socket connection lifetime",
        vec![0.1, 0.5, 1.0, 3.0, 10.0, 30.0, 60.0, 300.0, 1800.0, 3600.0]
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
pub(crate) static SOCKET_LAST_EVENT_TIMESTAMP_SECONDS: LazyLock<Gauge> = LazyLock::new(|| {
    prometheus::register_gauge!(
        "ptchan_socket_last_event_timestamp_seconds",
        "Unix timestamp of the last accepted upstream socket event"
    )
    .unwrap()
});
pub(crate) static SOCKET_LAST_JOIN_TIMESTAMP_SECONDS: LazyLock<Gauge> = LazyLock::new(|| {
    prometheus::register_gauge!(
        "ptchan_socket_last_join_timestamp_seconds",
        "Unix timestamp of the last successful upstream room join"
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
pub(crate) static SESSION_EXPIRES_AT_SECONDS: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "ptchan_session_expires_at_seconds",
        "Unix timestamp when the current upstream management session expires"
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
pub(crate) static GATEWAY_RATE_LIMITED_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "ptchan_gateway_rate_limited_requests_total",
        "Integration API requests rate limited by the gateway",
        &["integration", "board", "capability", "scope"]
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
pub(crate) static PROCESS_CPU_TICKS: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "ptchan_process_cpu_ticks_total",
        "Total process user plus system CPU time in Linux scheduler ticks"
    )
    .unwrap()
});
pub(crate) static PROCESS_THREADS: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "ptchan_process_threads",
        "Process thread count read from /proc/self/status"
    )
    .unwrap()
});
pub(crate) static PROCESS_OPEN_FDS: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "ptchan_process_open_fds",
        "Process open file descriptor count read from /proc/self/fd"
    )
    .unwrap()
});

pub(crate) fn init() {
    LazyLock::force(&SOCKET_CONNECTION_ATTEMPTS);
    LazyLock::force(&SOCKET_ACTIVE_CONNECTIONS);
    LazyLock::force(&SOCKET_CONNECTION_SECONDS);
    LazyLock::force(&SOCKET_JOIN_FAILURES);
    LazyLock::force(&SOCKET_JOINED);
    LazyLock::force(&UPSTREAM_AUTH_HEALTHY);
    LazyLock::force(&UPSTREAM_REQUIRED);
    LazyLock::force(&SOCKET_EVENTS);
    LazyLock::force(&SOCKET_LAST_EVENT_TIMESTAMP_SECONDS);
    LazyLock::force(&SOCKET_LAST_JOIN_TIMESTAMP_SECONDS);
    LazyLock::force(&SESSION_REFRESH);
    LazyLock::force(&SESSION_EXPIRES_AT_SECONDS);
    LazyLock::force(&WEBHOOK_DELIVERIES);
    LazyLock::force(&WEBHOOK_PENDING);
    LazyLock::force(&WEBHOOK_PENDING_BY_WEBHOOK);
    LazyLock::force(&WEBHOOK_DELIVERY_SECONDS);
    LazyLock::force(&READING_REQUESTS);
    LazyLock::force(&READING_REQUEST_SECONDS);
    LazyLock::force(&POSTING_REQUESTS);
    LazyLock::force(&POSTING_REQUEST_SECONDS);
    LazyLock::force(&GATEWAY_RATE_LIMITED_REQUESTS);
    LazyLock::force(&SQLITE_ERRORS);
    LazyLock::force(&REDACTION_DROPS);
    LazyLock::force(&PROCESS_CPU_TICKS);
    LazyLock::force(&PROCESS_THREADS);
    LazyLock::force(&PROCESS_OPEN_FDS);
}

pub(crate) fn render() -> Result<String> {
    init();
    refresh_process_metrics();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .context("encode prometheus metrics")?;
    String::from_utf8(buffer).context("prometheus metrics were not utf8")
}

pub(crate) fn observe_now(gauge: &Gauge) {
    gauge.set(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |duration| duration.as_secs_f64()),
    );
}

pub(crate) fn refresh_process_metrics() {
    refresh_process_metrics_impl();
}

#[cfg(target_os = "linux")]
fn refresh_process_metrics_impl() {
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
        if let Some(cpu_ticks) = process_cpu_ticks_from_stat(&stat) {
            PROCESS_CPU_TICKS.set(i64::try_from(cpu_ticks).unwrap_or(i64::MAX));
        }
    }
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        if let Some(threads) = process_threads_from_status(&status) {
            PROCESS_THREADS.set(threads);
        }
    }
    if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
        PROCESS_OPEN_FDS.set(i64::try_from(entries.count()).unwrap_or(i64::MAX));
    }
}

#[cfg(not(target_os = "linux"))]
fn refresh_process_metrics_impl() {}

#[cfg(target_os = "linux")]
fn process_cpu_ticks_from_stat(stat: &str) -> Option<u64> {
    let after_comm = stat.rsplit_once(") ")?.1;
    let fields = after_comm.split_whitespace().collect::<Vec<_>>();
    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    Some(user_ticks.saturating_add(system_ticks))
}

#[cfg(target_os = "linux")]
fn process_threads_from_status(status: &str) -> Option<i64> {
    status.lines().find_map(|line| {
        let value = line.strip_prefix("Threads:")?.trim();
        value.parse::<i64>().ok()
    })
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn parses_process_cpu_ticks_from_proc_stat() {
        let stat = "12345 (ptchan-gateway) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 0 0 20 0 3 0 100";

        assert_eq!(process_cpu_ticks_from_stat(stat), Some(23));
    }

    #[test]
    fn parses_process_threads_from_proc_status() {
        let status = "Name:\tptchan-gateway\nThreads:\t11\n";

        assert_eq!(process_threads_from_status(status), Some(11));
    }
}

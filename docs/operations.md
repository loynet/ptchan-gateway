# Safety, Delivery, And Operations

This guide records the operational guarantees behind the public integration
contract. Wire formats, signing, and compatibility rules are in
[the contract guide](contract/README.md).

## Safety model

The gateway is a privacy boundary. Integration-facing JSON is gateway-owned and
contains only the documented moderation-safe contract fields. It does not
forward raw ptchan, jschan, or Socket.IO payloads.

Thread reads and webhook events may contain public board/thread/post
coordinates, URLs, timestamps, display labels, text, country, donor marker,
attachment count, quote relationships, public tripcode, optional origin, and an
optional integration-scoped poster fingerprint.

They never contain raw IPs, upstream cloaks, moderation hashes, sessions,
permission state, cookies, signatures, secrets, post passwords, tripcode
secrets, raw payloads, attachment file names, hashes, or poster fingerprints
from upstream identity data. The optional `poster_fingerprint` is derived per
webhook integration and must not be correlated across integrations.

The management session cookie is only used to refresh and join the moderation
event stream. Reading and posting clients never send it. Replies go only to
ptchan's public form endpoint, never `/modpost`, with no cookie; integrations
cannot supply arbitrary upstream form fields, uploads, new threads, moderation
actions, or account features.

## Delivery and recovery

Webhook delivery is durable and at-least-once. After accepting and sanitizing an
upstream event, the gateway commits the event and one delivery row per configured
webhook to SQLite before attempting delivery. Retries are bounded and concurrent;
one slow endpoint does not block unrelated integrations.

Events remain until every queued delivery succeeds. Retention cleanup removes
only fully delivered old events, never pending deliveries. A delivery can be
repeated when the gateway cannot know whether the previous HTTP attempt reached
the integration.

Ordering is best-effort. Consumers must deduplicate with `event_id` or
`x-ptchan-event-id` and accept duplicates, delayed events, and reordering.

Durability begins only after the gateway receives an upstream event and commits
it to SQLite. The Socket.IO room is a live stream, not a replayable event log:
events created while the gateway, management session, or socket is unavailable
are not backfilled after reconnect. The API does not enumerate threads or offer
a complete recovery feed. Treat webhooks as notifications and use signed thread
reads to reconcile threads already known to the integration.

## Metrics and access

`GET /metrics` is Prometheus text format and should remain internal. It includes
configured integration names and requested board names as bounded labels; do not
publish the runtime port directly. Allow the Prometheus collector and trusted
operators instead.

Metric families cover:

- upstream session/socket health: `ptchan_upstream_required`,
  `ptchan_upstream_auth_healthy`, `ptchan_socket_joined`,
  `ptchan_socket_connection_attempts_total`,
  `ptchan_socket_active_connections`, `ptchan_socket_connection_seconds`,
  `ptchan_socket_join_failures_total`,
  `ptchan_socket_last_join_timestamp_seconds`, `ptchan_session_refresh_total`,
  and `ptchan_session_expires_at_seconds`;
- intake and privacy filtering: `ptchan_socket_events_total`,
  `ptchan_socket_last_event_timestamp_seconds`, and `ptchan_redaction_drops_total`;
- webhook health: `ptchan_webhook_pending`,
  `ptchan_webhook_pending_by_webhook`, `ptchan_webhook_deliveries_total`,
  `ptchan_webhook_delivery_seconds`, and `ptchan_webhook_oldest_pending_seconds`;
- integration API usage: `ptchan_reading_requests_total`,
  `ptchan_reading_request_seconds`, `ptchan_posting_requests_total`,
  `ptchan_posting_request_seconds`, and
  `ptchan_gateway_rate_limited_requests_total`;
- storage: `ptchan_sqlite_errors_total`;
- process health: standard Prometheus process metrics plus Linux fallback gauges
  `ptchan_process_cpu_ticks_total`, `ptchan_process_threads`, and
  `ptchan_process_open_fds`.

Metrics must never expose cookies, signatures, raw upstream payloads,
fingerprints, moderation identity fields, raw errors, or per-poster labels.

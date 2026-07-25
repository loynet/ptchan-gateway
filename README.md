# ptchan-gateway

`ptchan-gateway` gives trusted integrations a small, signed interface around
ptchan.

It can:

- listen for ptchan `newPost` events and deliver sanitized signed webhooks;
- fetch sanitized thread state through a signed reading endpoint;
- create narrowly scoped replies through ptchan's public posting form.

The gateway stores delivery state in SQLite and exposes health, readiness, and
Prometheus metrics.

## Safety Model

The gateway intentionally exposes less than ptchan knows.

Webhook payloads and thread reads include public post data such as board,
thread/post IDs, URLs, timestamps, subject/message text, public author labels,
donor flag, country, attachment count, and quote relationships.

They do not include raw IPs, upstream cloaks, moderation hashes, session data,
permission state, raw upstream JSON, file names, file hashes, cookies,
signatures, or secrets.

Posting is least-privilege. Replies are sent to ptchan's public form endpoint
without a management cookie, so normal board protections such as rate limits,
captcha, block-bypass, locks, and reply limits still apply.

## Integrations

Configuration is centered on `[[integration]]` entries. Each integration has
one shared signing secret and any combination of capabilities:

- `reading`: lets it fetch sanitized thread state.
- `webhook`: sends it signed post events.
- `posting`: lets it reply to existing threads.

Integration names may use ASCII letters, digits, `_`, and `-`. They are used in
environment variable names, metrics, and `origin` fields.

`allowed_boards = []` means all boards. Otherwise the integration is limited to
the listed boards for every enabled capability.

Each integration also has signed API rate limits for reading and posting. By
default the gateway allows 120 reads per 60 seconds with a burst of 30, and 30
posts per 60 seconds with a burst of 5. Override them with
`[integration.rate_limit.reading]` and `[integration.rate_limit.posting]`:

```toml
[integration.rate_limit.reading]
requests = 120
window = "60s"
burst = 30

[integration.rate_limit.posting]
requests = 30
window = "60s"
burst = 5
```

The runtime also has global reading and posting rate limits across all
integrations. Defaults are intentionally higher: 1,000 reads per 60 seconds
with a burst of 200, and 100 posts per 60 seconds with a burst of 20.

```toml
[runtime.rate_limit.reading]
requests = 1000
window = "60s"
burst = 200

[runtime.rate_limit.posting]
requests = 100
window = "60s"
burst = 20
```

Capabilities are enabled by including their section and disabled by omitting it.
For example, omit `[integration.posting]` to reject signed posting requests for
that integration. Omit `[integration.webhook]` to skip webhook delivery; if no
webhooks are configured, the gateway does not start the management session or
Socket.IO client.

Example:

```toml
[[integration]]
name = "example"
allowed_boards = ["test"]

[integration.rate_limit.reading]
requests = 120
window = "60s"
burst = 30

[integration.rate_limit.posting]
requests = 30
window = "60s"
burst = 5

[integration.reading]

[integration.webhook]
url = "http://127.0.0.1:8081/internal/ptchan/events"
include_poster_fingerprint = false

[integration.posting]
display_name = "gw"
use_tripcode = true
use_post_password = true
timeout = "15s"
```

Secrets live in the environment, not TOML:

```text
PTCHAN_INTEGRATION_EXAMPLE_SECRET=change-me
PTCHAN_INTEGRATION_EXAMPLE_TRIPCODE=change-me
PTCHAN_INTEGRATION_EXAMPLE_POST_PASSWORD=change-me
```

`PTCHAN_SESSION_COOKIE` is required only when at least one webhook capability is
configured, because webhook delivery depends on the management event stream.
Reading and posting do not use that cookie.

`PTCHAN_FINGERPRINT_SECRET` is required only when a webhook sets
`include_poster_fingerprint = true`.

## Run Locally

```bash
cp .env.example .env.dev
cp config/example.toml config/dev.toml
make tools
make run
```

Useful checks:

```bash
make          # full verification
make check    # same as make
make build    # release build
make db-reset # reset the local dev SQLite database
make doctor   # show local tool versions
```

The development client reads `.env.dev` automatically:

```bash
cargo run --example gateway_client -- health
cargo run --example gateway_client -- read test 397 --limit 50
printf 'hello from gateway\n' | cargo run --example gateway_client -- post test 397 --stdin
cargo run --example gateway_client -- listen --read-after-receive
```

See [examples/README.md](examples/README.md) for the full client and webhook
test tools.

## Runtime Endpoints

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `GET /integration/v1/threads/:board/:thread_id?limit=50`
- `POST /integration/v1/threads/:board/:thread_id/replies`

Integration API requests use:

```http
x-ptchan-integration: example
x-ptchan-timestamp: 2026-07-19T12:00:00Z
x-ptchan-signature: hmac-sha256=...
```

The signed message for thread reads is:

```text
<timestamp>.<method>.<path-and-query>
```

The signed message for posting replies is:

```text
<timestamp>.<method>.<path-and-query>.<json body>
```

## Thread Reads

```http
GET /integration/v1/threads/test/397?limit=50
```

The response is a sanitized thread with posts in chronological order. `limit`
defaults to `50` and is capped at `200`.

Gateway-side read rate limits return `429` with the same error envelope used by
posting errors:

```json
{
  "error": {
    "code": "rate_limited",
    "message": "gateway rate limit exceeded",
    "retryable": true
  }
}
```

When a post was created through the gateway, its `origin` identifies the
integration:

```json
{
  "origin": { "kind": "integration", "name": "example" }
}
```

This lets integrations recognize their own posts without receiving tripcode
secrets or upstream identity fields. `origin` is best-effort in webhook and
thread-read payloads; if it is missing, treat the post as "not known to have
been produced by this gateway." It can be missing if ptchan accepted a reply but
the gateway could not decode the post id, later socket matching did not have
enough message data, the pending match expired, storage failed, or identical
pending replies made the match ambiguous.

## Webhooks

V1 emits:

- `thread.created`
- `post.created`

Webhook delivery is durable and at-least-once. The gateway writes events and
per-integration delivery rows to SQLite before sending webhooks, retries failed
deliveries with backoff, and retains pending events until all deliveries have
succeeded.

Ordering is best-effort. Integrations must use `x-ptchan-event-id` or
`event_id` for idempotency and tolerate duplicate, delayed, or out-of-order
events. Integrations that need current state should fetch the thread after a
webhook instead of deriving correctness from webhook order.

Webhook signatures are HMAC-SHA256 over:

```text
<timestamp>.<json body>
```

## Posting Replies

```http
POST /integration/v1/threads/test/397/replies
content-type: application/json
```

```json
{ "message": ">>405\nreply text", "sage": false }
```

Successful replies return post coordinates:

```json
{
  "board": "test",
  "thread_id": 397,
  "post_id": 406,
  "url": "https://ptchan.org/test/thread/397.html#406",
  "origin": { "kind": "integration", "name": "example" }
}
```

Errors return a stable code and retry hint:

```json
{
  "error": {
    "code": "rate_limited",
    "message": "Please wait before making another post",
    "retryable": true,
    "upstream_status": 429
  }
}
```

Gateway-side rate limits use the same `rate_limited` code with the message
`gateway rate limit exceeded` and no `upstream_status`. Upstream ptchan rate
limits include `upstream_status`.

Known reply error codes include `invalid_json`, `invalid_board`,
`invalid_thread_id`, `missing_message`, `message_too_long`, `invalid_message`,
`board_not_allowed`, `captcha_required`, `block_bypass_required`,
`rate_limited`, `thread_not_found`, `thread_locked`, `thread_reply_limit`,
`board_locked`, `rejected`, `origin_tracking_unavailable`, and
`reply_state_unknown`.

`reply_state_unknown` means ptchan may already have accepted the reply, so check
the thread before retrying.

File uploads and new thread creation are not part of the current write surface.

## Metrics

`GET /metrics` exposes Prometheus text metrics.

- Upstream socket/session health: `ptchan_upstream_required`,
  `ptchan_upstream_auth_healthy`, `ptchan_socket_joined`,
  `ptchan_socket_connection_attempts_total`,
  `ptchan_socket_join_failures_total`, `ptchan_session_refresh_total`.
- Event intake and privacy filtering: `ptchan_socket_events_total`,
  `ptchan_redaction_drops_total`.
- Webhook backlog and delivery behavior: `ptchan_webhook_pending`,
  `ptchan_webhook_pending_by_webhook`, `ptchan_webhook_deliveries_total`,
  `ptchan_webhook_delivery_seconds`.
- Integration API usage and latency: `ptchan_reading_requests_total`,
  `ptchan_reading_request_seconds`, `ptchan_posting_requests_total`, and
  `ptchan_posting_request_seconds`, labeled by integration, board, and result.
  Gateway-enforced rate limits also increment
  `ptchan_gateway_rate_limited_requests_total`, labeled by integration, board,
  capability, and scope (`integration` or `global`).
- Storage health: `ptchan_sqlite_errors_total`.
- Gateway process health on Linux/container deployments:
  `process_cpu_seconds_total`, `process_resident_memory_bytes`,
  `process_virtual_memory_bytes`, `process_open_fds`, `process_max_fds`,
  `process_threads`, and `process_start_time_seconds`.

Metrics must not expose cookies, signatures, raw upstream payloads, poster
fingerprints, moderation identity fields, or per-poster labels.

## Docker

```bash
make docker-deploy GATEWAY_ENV=prod DOCKER_NETWORK=integration-net
make docker-logs GATEWAY_ENV=prod
```

Docker images are tagged with the current commit by default, for example
`ptchan-gateway:abc1234`. Override `IMAGE` when pushing to a registry or using a
specific tag:

```bash
make docker-deploy GATEWAY_ENV=prod IMAGE=registry.example/ptchan-gateway:abc1234
```

`GATEWAY_ENV` selects the environment-specific inputs and resource names:

```text
GATEWAY_ENV=dev   -> .env.dev,  config/dev.toml,  ptchan-gateway-dev,  ptchan-gateway-dev-data
GATEWAY_ENV=prod  -> .env.prod, config/prod.toml, ptchan-gateway-prod, ptchan-gateway-prod-data
```

Inside the container, the selected config is mounted read-only at
`/etc/ptchan-gateway/config.toml` and SQLite is stored at
`/data/ptchan-gateway.db` on the named Docker volume. `docker-deploy` replaces
the container but keeps the volume.

Dev and prod can run on the same host at the same time. They see the same
SQLite path inside their containers, but that path is backed by different named
volumes:

```text
ptchan-gateway-dev   -> /data/ptchan-gateway.db on ptchan-gateway-dev-data
ptchan-gateway-prod  -> /data/ptchan-gateway.db on ptchan-gateway-prod-data
```

Logs go to stdout/stderr and are captured by Docker:

```bash
docker logs -f ptchan-gateway-prod
```

`DOCKER_NETWORK` should name an existing Docker network when integrations are
addressed by container name. The Makefile does not publish host ports by
default; on a shared Docker network, integrations can reach the gateway by
container name, such as `http://ptchan-gateway-prod:8080`.

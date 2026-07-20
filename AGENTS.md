# AGENTS.md

## What This Is

`ptchan-gateway` is a long-running Rust service. It connects to ptchan over
Socket.IO, joins `globalmanage-recent-hashed`, sanitizes `newPost` events,
queues them in SQLite, and delivers signed webhooks.

Privacy is a hard boundary. Never expose raw IPs, upstream cloaks, moderation
hashes, session data, permission state, webhook secrets, or poster fingerprints
unless the product scope explicitly changes.

Consumer-facing JSON must use types owned by this gateway. Reuse
`src/consumer.rs` for webhook payloads and future context APIs. Do not forward
raw ptchan/jschan response bodies to consumers.

Consumer context endpoints must require signed requests, enforce configured
board access, and return only `src/consumer.rs` types.

Webhook delivery must also enforce `allowed_boards`. A webhook with an empty
`allowed_boards` list receives all boards; otherwise it receives only matching
boards.

Webhook delivery is durable and at-least-once, but ordering is best-effort.
Retries can cause later events to reach a consumer before earlier events.
Consumers must use `x-ptchan-event-id` / `event_id` for idempotency and tolerate
duplicates, delayed delivery, and out-of-order delivery. Do not document or
implement consumer behavior that depends on strict webhook ordering unless the
storage and delivery model is explicitly changed.

## How To Work

Use the Makefile:

```bash
make        # full local verification
make run    # run with .env.dev and config/dev.toml
make build  # release build
make tools  # install cargo-machete and cargo-deny
```

`make` runs formatting checks, strict Clippy including `pedantic`, tests,
config validation, `cargo machete`, `cargo deny`, and a release build. Keep it
green before handing work back.

If dependency tools are missing, install them with `make tools`. Do not weaken
checks to make the target pass.

## Code Style

Keep the code direct:

- Prefer the standard library and existing dependencies before adding crates.
- Add a dependency only when it removes real risk or meaningful complexity.
- Avoid helper functions or abstractions unless behavior is shared, complex, or
  easier to test in isolation.
- Keep upstream parsing separate from the consumer contract.
- Use clear error context at I/O, network, config, and persistence boundaries.
- Keep reconnect, shutdown, retry, and readiness behavior explicit.
- Add small deterministic tests around privacy, signing, persistence, and
  operational state.

TOML config is for non-secret structure. Secrets belong in environment
variables:

- `PTCHAN_SESSION_COOKIE`
- `PTCHAN_WEBHOOK_<WEBHOOK_NAME>_SECRET`
- `PTCHAN_FINGERPRINT_SECRET`

Never log secrets, full Cookie headers, HMAC signatures, raw upstream payloads,
or sensitive moderation identity fields.

## Consumer Contract

V1 emits only `thread.created` and `post.created`.

Event IDs are:

```text
ptchan:<kind>:<board>:<post_id>
```

Payloads include a moderation-safe subset: board, thread/post IDs, URL,
timestamps, public author labels, donor flag, subject/message text, country,
attachment count, and typed `references` / `referenced_by` post refs.

Payloads must not include raw IPs, upstream cloaks, permissions, sessions, raw
upstream JSON, file names, file hashes, or secrets.

`references` means posts this post points at. `referenced_by` means posts that
point at this post. Use complete post coordinates:

```json
{ "board": "test", "thread_id": 397, "post_id": 399 }
```

`poster_fingerprint` is optional per webhook. It is scoped by webhook name and
must be derived from `PTCHAN_FINGERPRINT_SECRET`; consumers must not receive the
upstream cloak.

## Runtime Details

The runtime HTTP server exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `GET /consumer/v1/threads/:board/:thread_id?limit=50`

The thread context endpoint returns recent sanitized `consumer::Thread` posts in
chronological order. `limit` defaults to `50` and is capped at `200`.

Context requests use the configured consumer secret:

```http
x-ptchan-consumer: example
x-ptchan-timestamp: 2026-07-19T12:00:00Z
x-ptchan-signature: hmac-sha256=...
```

The context signature message is:

```text
<timestamp>.<method>.<path-and-query>
```

Webhook deliveries use:

```http
x-ptchan-event-id: ptchan:post.created:i:303239
x-ptchan-timestamp: 2026-07-19T12:00:00Z
x-ptchan-signature: hmac-sha256=...
```

The webhook signature message is:

```text
<timestamp>.<json body>
```

The gateway refreshes the rolling ptchan session by calling
`/globalmanage/recent.json` and applying any returned `Set-Cookie` value to
future Socket.IO reconnects. When `Expires` or `Max-Age` is available, schedule
refresh before expiry. If expiry is unknown, use
`ptchan.session_refresh_fallback_interval`.

SQLite keeps events until every queued webhook delivery succeeds. Once fully
delivered, the retention task removes old events after
`storage.event_retention`. Cleanup runs on startup and then hourly. Pending
deliveries must not be purged by retention.

Debug logs may include socket connection attempts, event shape, queued event
counts, session refresh status, webhook delivery status, and readiness details.
They must not include cookies, signatures, raw payloads, or moderation identity
fields.

## Known Exception

`rust_socketio 0.6.0` currently pulls unmaintained `backoff` and `instant`
transitively. The exception is recorded in `deny.toml`; revisit it before
production hardening by testing a maintained Socket.IO client or isolating the
protocol dependency.

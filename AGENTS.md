# AGENTS.md

## What This Is

`ptchan-gateway` is a long-running Rust service that gives configured
integrations a small signed interface around ptchan.

It can:

- join ptchan's `globalmanage-recent-hashed` Socket.IO room and receive
  `newPost` events;
- convert upstream posts into the gateway-owned contract in `src/contract.rs`;
- queue sanitized webhook deliveries in SQLite and retry them;
- serve signed sanitized thread reads from public thread JSON;
- post signed integration replies through ptchan's public posting form.

## Hard Boundaries

Privacy is a hard boundary. Never expose raw IPs, upstream cloaks, moderation
hashes, session data, permission state, webhook secrets, HMAC signatures,
tripcode secrets, post passwords, or poster fingerprints unless product scope
explicitly changes.

Integration-facing JSON must use gateway-owned types from `src/contract.rs`.
Do not forward raw ptchan/jschan response bodies or raw Socket.IO payloads to
integrations.

Keep upstream parsing separate from the integration contract. `src/upstream.rs`,
`src/event.rs`, and `src/reading.rs` may understand ptchan/jschan shapes;
external responses should remain contract-owned and moderation-safe.

## Least Privilege

Posting must never use the management session cookie and must never use
`/modpost`. `src/posting.rs` posts to `/forms/board/:board/post` as a public
form post, with a valid public thread `Referer`, and no cookie.

The management session cookie is only for the moderation event stream:

- session refresh calls `/globalmanage/recent.json`;
- the Socket.IO client uses the refreshed cookie to join
  `globalmanage-recent-hashed`;
- reading and posting clients do not send the cookie.

If no webhook capability is configured, the gateway must not require
`PTCHAN_SESSION_COOKIE`, must not start session refresh, and must not join the
management socket.

## Integration Model

Configuration is centered on `[[integration]]`. One integration may have any
combination of:

- `reading`: signed thread reads;
- `webhook`: signed event delivery;
- `posting`: signed replies to existing threads.

`allowed_boards` applies to every enabled capability. Empty means all boards.
Webhook delivery, reads, and posts must all enforce it.

Integration names are part of env var names, metric labels, and `origin`
values. Keep them bounded and boring: ASCII letters, digits, `_`, and `-`, with
no collisions after env-var normalization.

Each integration secret comes from:

```text
PTCHAN_INTEGRATION_<INTEGRATION_NAME>_SECRET
```

Posting-only secrets come from:

```text
PTCHAN_INTEGRATION_<INTEGRATION_NAME>_TRIPCODE
PTCHAN_INTEGRATION_<INTEGRATION_NAME>_POST_PASSWORD
```

Only require those posting secrets when the corresponding config flags are
enabled. Keep TOML for non-secret structure.

## Runtime API

The runtime HTTP server exposes:

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

Thread read signatures cover:

```text
<timestamp>.<method>.<path-and-query>
```

Posting signatures cover:

```text
<timestamp>.<method>.<path-and-query>.<json body>
```

Webhook deliveries use:

```http
x-ptchan-event-id: ptchan:post.created:i:303239
x-ptchan-timestamp: 2026-07-19T12:00:00Z
x-ptchan-signature: hmac-sha256=...
```

Webhook signatures cover:

```text
<timestamp>.<json body>
```

## Contract

V1 emits only `thread.created` and `post.created`.

Event IDs are:

```text
ptchan:<kind>:<board>:<post_id>
```

Payloads include a moderation-safe subset: board, thread/post IDs, URL,
timestamps, public author labels, donor flag, subject/message text, country,
attachment count, optional integration-scoped `poster_fingerprint`, optional
`origin`, and typed `references` / `referenced_by` post refs.

`references` means posts this post points at. `referenced_by` means posts that
point at this post. Use complete post coordinates:

```json
{ "board": "test", "thread_id": 397, "post_id": 399 }
```

`poster_fingerprint` is optional per webhook. It is scoped by webhook name and
derived from `PTCHAN_FINGERPRINT_SECRET`; integrations must not receive the
upstream cloak.

`origin` identifies posts created through the gateway:

```json
{ "kind": "integration", "name": "example" }
```

It must not reveal tripcode secrets, post passwords, cookies, or upstream
identity fields.

## Delivery Semantics

Webhook delivery is durable and at-least-once, but ordering is best-effort.
Retries can cause later events to reach an integration before earlier events.

Integrations must use `x-ptchan-event-id` / `event_id` for idempotency and
tolerate duplicates, delayed delivery, and out-of-order delivery. Do not
document or implement integration behavior that depends on strict webhook
ordering unless the storage and delivery model changes.

SQLite keeps events until every queued webhook delivery succeeds. Once fully
delivered, retention removes old events after `storage.event_retention`.
Cleanup runs on startup and then hourly. Pending deliveries must not be purged
by retention.

Produced-post origin tracking uses `produced_posts` and
`pending_produced_posts`. Keep it best-effort but avoid reporting a successful
reply before the gateway has recorded enough state to recognize its own post.
Document missing `origin` as "not known to be produced by this gateway", not as
proof that some other actor created the post. Best-effort matching can fail when
ptchan accepts a reply but the gateway cannot decode the post id, the later
socket event has no comparable message text, the pending row expired, storage
failed, or identical pending replies make the match ambiguous.

## Posting Rules

The current write surface is replies only. Do not add uploads, new threads,
moderation actions, cookies, or account features unless the product scope
explicitly changes.

Posting requests accept only gateway-defined JSON. Keep
`#[serde(deny_unknown_fields)]` on request types, validate board/thread/message,
and build the upstream form from trusted config plus validated request fields.
Do not allow integrations to provide arbitrary upstream form fields.

Known upstream rejections should return stable error codes and retry hints.
`reply_state_unknown` must be non-retryable and tell producers to inspect the
thread before retrying, because ptchan may have accepted the post.

## Metrics And Logs

Metrics should answer operational questions without leaking sensitive data:

- upstream auth/socket health;
- event intake and redaction drops;
- webhook backlog, delivery outcomes, and delivery latency;
- integration read/post outcomes and latency by configured integration, board,
  and result;
- storage errors;
- process CPU, memory, file descriptors, and threads on Linux/container builds.

Do not add per-poster labels, raw path labels, raw error labels, signatures,
cookies, fingerprints, IPs, cloaks, or raw payload fragments to metrics.
Unauthenticated integration API requests should use bounded labels such as
`integration="unknown"`.

Debug logs may include socket connection attempts, event shape, queued event
counts, session refresh status, webhook delivery status, posting result codes,
and readiness details. They must not include cookies, signatures, raw payloads,
tripcode secrets, post passwords, or moderation identity fields.

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

## Engineering Philosophy

This project should feel simple, boring, and explicit. Prefer ordinary Rust,
clear data flow, and small modules with obvious responsibilities over clever
generic machinery. Use idiomatic Rust naming and API shapes: predictable
methods, standard conversion traits where they help, common derived traits where
they are useful, and consistent word order for related types.

Follow the patterns already present in the codebase unless there is a clear
reason to improve them. If an existing pattern feels wrong, too coupled, or too
heavy for what it does, raise that directly and either fix it within the task or
leave a clear note for a follow-up.

The right abstraction here earns its keep. Add one when it removes real
duplication, makes a privacy/security boundary harder to misuse, or lets risky
behavior be tested directly. Do not add helper functions, traits, builders, or
configuration layers just to make code look more abstract.

Prefer clear ownership and concrete types. Avoid lifetime gymnastics,
unnecessary generics, trait objects, and shared mutable state unless the code's
data flow actually needs them. A cheap clone in non-hot code is often better
than making ownership hard to read.

Use types to protect important states and boundaries, especially privacy,
authorization, signatures, posting outcomes, and upstream/gateway contract
separation. Do not create wrapper types that only rename a single value without
reducing risk or complexity.

Be dependency-conscious. Use the standard library and existing dependencies
first. Add a crate when it avoids meaningful protocol/security risk or replaces
substantial fragile code. Do not reinvent hard things, but do not depend on the
universe for small conveniences.

Make boundaries visible in code:

- upstream ptchan/jschan shapes stay behind parsing/adaptation modules;
- integration-facing JSON uses `src/contract.rs`;
- signing, board authorization, and secret loading stay explicit;
- posting forms are built from trusted config plus validated request fields;
- least privilege is preferred over convenience.

Prefer readable error handling with context at I/O, network, config,
persistence, signing, and upstream boundaries. Producers and operators should
get clean errors; logs should help debug without leaking sensitive data.

Avoid `unwrap` and `expect` in runtime paths unless the invariant is internal,
obvious, and already guaranteed by earlier validation. Tests may use them when
they keep fixtures readable.

Keep async code explicit. Spawned tasks must have shutdown behavior, errors must
be observed, and blocking work should not quietly sit on the async executor.

Treat Clippy as a code-review partner. Fix warnings by making the code clearer
unless there is a documented false positive.

Use comments sparingly. Add a short, informal note when the intent or safety
reason is not obvious from the code. Do not narrate simple statements, and do
not bury straightforward code under long explanatory blocks.

Tests should be small and deterministic. Add them around privacy redaction,
signing, persistence, posting safety, metrics, config validation, and any edge
case where a future simplification could accidentally weaken a boundary.

When you notice a better structure, call it out. If it is directly connected to
the task and lowers risk or complexity, make the change. If it is broader, leave
a clear note rather than quietly expanding scope.

## Known Exception

`rust_socketio 0.6.0` currently pulls unmaintained `backoff` and `instant`
transitively. The exception is recorded in `deny.toml`; revisit it before
production hardening by testing a maintained Socket.IO client or isolating the
protocol dependency.

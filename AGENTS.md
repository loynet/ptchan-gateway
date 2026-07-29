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

Every posting integration requires its secure tripcode secret. Its
`posting.public_tripcode` TOML value is the corresponding public `!!` tripcode
emitted by ptchan and must be unique. Every posting integration also requires a
post password for ptchan's public post-management features. Keep TOML for
non-secret structure.

Configuration has two deliberate representations:

- private file types describe untrusted TOML and optional capability sections;
- runtime types contain validated settings, resolved secrets, and only enabled
  capabilities.

Keep that transition one-way. Do not deserialize directly into runtime types,
store empty secret sentinels, mutate parsed configuration into readiness, or
make callers repeatedly interpret optional TOML sections. Validate structure
before resolving secrets, then construct runtime configuration once.

## Runtime API

The runtime HTTP server exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `GET /integration/v1/openapi.json`
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

Signed integration attempts consume quota immediately after authentication and
capability/board authorization, before query, path, JSON, or message
validation. Keep reading and posting consistent. Oversized bodies are rejected
before complete-body signature verification and therefore cannot consume
authenticated quota.

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

V1 emits only `thread.created` and `post.created`. Every webhook carries
`"schema_version": "1"`.

All integration-facing DTOs and stable error codes belong in `src/contract.rs`.
Every integration API failure must use the contract-owned JSON error envelope;
do not fall back to empty or plain-text Axum rejections.

OpenAPI, standalone JSON Schemas, and canonical examples live under
`docs/contract/` and are generated from the Rust wire types. After an
intentional contract change, run:

```bash
cargo run -- --write-contract
```

Do not edit generated JSON by hand. `make` checks that committed artifacts are
current. Preserve V1 fields and meanings; additions must be optional, and
consumers are expected to ignore unknown response fields.

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
Delivery attempts are bounded but concurrent so one slow webhook does not
block unrelated integrations.

Integrations must use `x-ptchan-event-id` / `event_id` for idempotency and
tolerate duplicates, delayed delivery, and out-of-order delivery. Do not
document or implement integration behavior that depends on strict webhook
ordering unless the storage and delivery model changes.

SQLite keeps events until every queued webhook delivery succeeds. Once fully
delivered, retention removes old events after `storage.event_retention`.
Cleanup runs on startup and then hourly. Pending deliveries must not be purged
by retention.

Produced-post origin tracking is deterministic and stateless. Posting
integrations always submit a secure `##` tripcode, and webhook/thread posts are
attributed by exact match against the configured public `!!` tripcode. Do not
store post coordinates or message digests for origin tracking.

Preserve that identity on every integration-facing read and webhook payload:
`post.tripcode` is ptchan's public `!!` value and matching posts also receive
the integration `origin`. The private tripcode secret is used only to construct
the upstream posting form and must never enter the integration contract.

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
- webhook backlog, oldest pending age, delivery outcomes, and delivery latency;
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
generated-contract validation, config validation, `cargo machete`,
`cargo deny`, and a release build. Keep it green before handing work back.

If dependency tools are missing, install them with `make tools`. Do not weaken
checks to make the target pass.

## Engineering Philosophy

This project should feel simple, boring, and explicit. Prefer ordinary Rust,
clear data flow, and small modules with obvious responsibilities over clever
generic machinery. Use idiomatic Rust naming and API shapes: predictable
methods, standard conversion traits where they help, common derived traits where
they are useful, and consistent word order for related types.

Use the extension points provided by the libraries already in the service.
Axum extractors and responses, Reqwest request/response APIs, Serde models,
Tokio task and blocking boundaries, and protocol-focused crates should own
their standard concerns. Add custom machinery only for gateway domain policy or
when the library abstraction cannot preserve a documented requirement.

Keep control flow visible. Avoid pass-through methods, functions that merely
rename another call, duplicate derived state, and layers that require jumping
between files without adding a boundary. Put concrete domain behavior on the
type that owns the data when that makes the call site self-explanatory.

The right abstraction here earns its keep. Add one when it removes real
duplication, makes a privacy/security boundary harder to misuse, or lets risky
behavior be tested directly. Do not add helper functions, traits, builders, or
configuration layers just to make code look more abstract. Judge an abstraction
by whether it reduces total reasoning across definitions, callers, and tests,
not by whether it shortens one file.

Prefer clear ownership and concrete types. Avoid lifetime gymnastics,
unnecessary generics, trait objects, and shared mutable state unless the code's
data flow actually needs them. A cheap clone in non-hot code is often better
than making ownership hard to read.

Use types to protect important states and boundaries, especially privacy,
authorization, signatures, posting outcomes, and upstream/gateway contract
separation. Parsed input and valid runtime state should be different types when
that prevents partial initialization or repeated checks. Do not create wrapper
types that only rename a single value without reducing risk or complexity.

Split modules around responsibilities that can be understood and tested alone.
Do not split files or crates merely to hide line count, and do not introduce a
workspace until components have genuinely separate dependency, release, or
reuse boundaries. Use domain knowledge to delete state and reconciliation
machinery when a stable upstream fact can make the result deterministic.

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

## Socket Client Choice

The gateway currently uses `tokio-tungstenite` plus the small protocol decoder
in `src/socket/protocol.rs`. The available `rust_socketio` async API is
callback-oriented and documented as beta, and it does not improve the explicit
management-cookie, room-join readiness, and cooperative-shutdown behavior this
service needs. Revisit the choice when a maintained client can preserve those
requirements with less code.

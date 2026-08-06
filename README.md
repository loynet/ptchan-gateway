# ptchan-gateway

`ptchan-gateway` gives trusted integrations a small, signed, moderation-safe
interface around ptchan. It delivers sanitized webhooks, serves sanitized thread
reads, and submits narrowly scoped replies through ptchan's public posting form.

The gateway stores webhook delivery state in SQLite and exposes health,
readiness, and Prometheus metrics.

## Boundaries

Integration payloads contain public post data: board and post coordinates, URLs,
timestamps, public author labels, text, country, attachment count, quote
relationships, and optional integration-scoped identity fields.

They never contain raw IPs, cloaks, moderation hashes, session or permission
state, raw upstream JSON, attachment metadata, cookies, signatures, or secrets.

Posting is least-privilege: replies go to ptchan's public form endpoint without
a management cookie. Normal board controls, including rate limits, CAPTCHA,
locks, and reply limits, still apply.

Read the full [safety, delivery, and operations model](docs/operations.md)
before deploying or building an integration.

## Configure an integration

An `[[integration]]` has a shared signing secret and any combination of:

- `reading` — signed sanitized thread reads;
- `webhook` — durable signed event delivery;
- `posting` — replies to existing threads only.

`allowed_boards = []` allows every board; otherwise it applies to every enabled
capability. Integration names may contain ASCII letters, digits, `_`, and `-`.

```toml
[[integration]]
name = "example"
allowed_boards = ["test"]

[integration.reading]

[integration.webhook]
url = "http://127.0.0.1:8081/internal/ptchan/events"
include_poster_fingerprint = false

[integration.posting]
display_name = "gw"
public_tripcode = "!!X8NXmAS44="
```

Keep secrets in the environment:

```text
PTCHAN_INTEGRATION_EXAMPLE_SECRET=change-me
PTCHAN_INTEGRATION_EXAMPLE_TRIPCODE=change-me
PTCHAN_INTEGRATION_EXAMPLE_POST_PASSWORD=change-me
```

Every integration needs its shared secret. Posting also needs its secure
tripcode secret and post password. `PTCHAN_SESSION_COOKIE` is required only
when at least one webhook is configured; reading and posting never use it.
`PTCHAN_FINGERPRINT_SECRET` is required only for webhooks that enable
`include_poster_fingerprint`.

Per-integration and global reading/posting rate limits are configured in TOML;
see [config/example.toml](config/example.toml) for the defaults and complete
shape. Startup validates enabled capabilities and required secrets before
serving traffic.

## Run and verify

```bash
cp .env.example .env.dev
cp config/example.toml config/dev.toml
make tools
make run
```

```text
make check         formatting, lint, test, contract, config, dependency, and Go SDK checks
make verify        complete local verification, including the release build
make release-check compile the locked release binary
make build         release build and copy the binary
make db-reset      reset the selected local SQLite database
make doctor        show local tool versions
```

GitHub Actions runs `check` as the pull-request merge gate and `release-check`
after a merge reaches `main`, using cached Cargo and Go build state.

## Integrate

The supported Go SDK is the `clients/go` submodule:

```go
import "github.com/loynet/ptchan-gateway/clients/go"
```

It owns signing, webhook verification, bounded response reads, gateway error
decoding, and documented response invariants. It does not own retries,
idempotency, routing, or application policy. See its
[README](clients/go/README.md), including runnable webhook and thread/reply
examples. Releases use tags such as `clients/go/v0.1.0`.

Run the examples from `clients/go`:

```text
cd clients/go
go run ./examples/webhook-receiver
go run ./examples/thread-reply
```

## API and contract

Runtime endpoints:

- `GET /healthz`, `GET /readyz`, and `GET /metrics`
- `GET /integration/v1/openapi.json`
- `GET /integration/v1/threads/:board/:thread_id?limit=50`
- `POST /integration/v1/threads/:board/:thread_id/replies`

The read and reply endpoints require signed requests. Health, readiness,
metrics, and OpenAPI do not; expose them only to the appropriate operational
network. In particular, metrics include configured integration and requested
board labels, so keep them internal.

The generated [OpenAPI](docs/contract/openapi.json),
[schemas](docs/contract/schemas), and [canonical examples](docs/contract/examples)
are the public v1 contract. Read [the contract guide](docs/contract/README.md)
for signing, compatibility, wire details, and request-quota behavior.

Posts made through the gateway retain their public `tripcode` and receive an
`origin` only when that tripcode exactly matches a configured integration.
Private tripcode secrets never enter integration payloads.

## Delivery semantics and recovery

V1 emits `thread.created` and `post.created`. Webhooks are durable,
at-least-once, and best-effort ordered. Consumers must deduplicate using
`event_id` / `x-ptchan-event-id`, tolerate duplicates and reordering, and use
signed thread reads to reconcile known threads.

Durability starts after the gateway receives an upstream event and commits it to
SQLite. Pending deliveries survive an unavailable integration endpoint as long
as SQLite data is retained; an uncertain attempt can be delivered again.

The upstream Socket.IO room is live, not replayable. Events created while the
gateway, its management session, or its socket is unavailable are not backfilled
after reconnect because the gateway never received or stored them. The current
API has no thread enumeration or complete recovery feed.

The [operations guide](docs/operations.md) documents the storage boundary,
retention behavior, least-privilege posting, and operational consequences in
full.

Replies use a stable gateway error envelope. `reply_state_unknown` means ptchan
may have accepted the post; inspect the thread before retrying. Uploads and new
thread creation are intentionally out of scope.

## Operations

`GET /metrics` reports upstream session/socket health, intake/redaction,
webhook backlog and delivery outcomes, integration read/post outcomes, storage
errors, and process health. It must never expose secrets, raw upstream payloads,
fingerprints, moderation identity fields, or per-poster labels.

See [metric families and access requirements](docs/operations.md#metrics-and-access)
for the complete inventory.

For Docker deployment:

```bash
make docker-deploy GATEWAY_ENV=prod DOCKER_NETWORK=integration-net
make docker-logs GATEWAY_ENV=prod
```

`GATEWAY_ENV` selects `.env.<env>`, `config/<env>.toml`, and separate container
and SQLite-volume names. The selected config is mounted read-only, SQLite lives
on the named volume, and `docker-deploy` replaces the container without removing
that volume. Set `IMAGE` to override the default commit-tagged image. The
Makefile does not publish host ports; use an existing `DOCKER_NETWORK` when
integrations should reach the gateway by container name.

# ptchan-gateway

`ptchan-gateway` is a Rust service that listens to ptchan moderation events and
forwards a sanitized subset to consumer apps.

It connects to ptchan as a Socket.IO client, joins the
`globalmanage-recent-hashed` room, stores delivery state in SQLite, and sends
signed webhooks to configured consumers.

## What It Emits

V1 emits:

- `thread.created`
- `post.created`

Payloads include post IDs, board/thread IDs, URLs, timestamps, public post text,
public author labels, donor status, attachment counts, and typed
`references` / `referenced_by` post refs.

Payloads do not include raw IPs, upstream cloaks, session data, permissions,
raw upstream JSON, file names, file hashes, or secrets.

## Setup

```bash
cp .env.example .env.dev
cp config.example.toml config/dev.toml
make tools
```

Put secrets in `.env.dev` or the container environment:

- `PTCHAN_SESSION_COOKIE`
- `PTCHAN_WEBHOOK_<NAME>_SECRET`
- `PTCHAN_FINGERPRINT_SECRET`, only when a webhook enables poster fingerprints

Edit `config/dev.toml` for non-secret settings such as webhook URLs,
`allowed_boards`, logging, and SQLite location.

## Run Locally

```bash
make run
```

Run the example webhook consumer in another terminal:

```bash
PTCHAN_CONSUMER_SECRET=change-me cargo run --example webhook_consumer
```

Useful checks:

```bash
make          # full verification
make check    # same as make
make build    # release build
make db-reset # reset the local dev SQLite database
```

## Runtime Endpoints

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `GET /consumer/v1/threads/:board/:thread_id?limit=50`

The consumer thread endpoint requires the same signed-request headers used by
webhook consumers and respects each consumer's `allowed_boards`.

## Docker

```bash
make docker-deploy GATEWAY_ENV=prod DOCKER_NETWORK=consumer-net
make docker-logs GATEWAY_ENV=prod
```

`DOCKER_NETWORK` is optional and should name an existing Docker network when
consumers are addressed by container name.

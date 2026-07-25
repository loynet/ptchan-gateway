# Example Tools

This folder contains development tools for exercising the gateway like a real
integration.

## Gateway Client

`gateway_client` is a small JSON-first CLI for local experiments against a
running gateway.

The client reads `.env.dev` automatically. You can also override values in the
shell:

```bash
export PTCHAN_GATEWAY_URL=http://127.0.0.1:8080
export PTCHAN_INTEGRATION_NAME=example
export PTCHAN_INTEGRATION_SECRET=change-me
```

Check the gateway:

```bash
cargo run --example gateway_client -- health
```

Read sanitized thread state:

```bash
cargo run --example gateway_client -- read test 397 --limit 50
```

Post a reply through the public posting form:

```bash
cargo run --example gateway_client -- post test 397 --message ">>399\nhello from the gateway"
```

`post` prints the gateway JSON response. If the gateway returns a non-2xx
status, the JSON is still printed and the command exits non-zero, which makes
shell loops with `|| break` behave as expected.

For multi-line replies:

```bash
cargo run --example gateway_client -- post test 397 --stdin < reply.txt
```

Receive signed webhooks and print each accepted event JSON to stdout:

```bash
cargo run --example gateway_client -- listen --addr 127.0.0.1:8081
```

To also fetch and print the current sanitized thread after every webhook:

```bash
cargo run --example gateway_client -- listen --read-after-receive --limit 80
```

The listener verifies webhook signatures and excludes cookies, authorization,
and signatures from optional header summaries.

## Webhook Integration

`webhook_integration` is a minimal integration server that receives gateway
webhooks and logs a compact summary.

Run it with the same integration secret configured for the gateway:

```bash
PTCHAN_INTEGRATION_SECRET=change-me cargo run --example webhook_integration
```

The integration listens on `127.0.0.1:8081` by default and accepts events at:

```text
POST /internal/ptchan/events
```

It verifies:

- `x-ptchan-event-id`
- `x-ptchan-timestamp`
- `x-ptchan-signature`

The signature is HMAC-SHA256 over:

```text
<timestamp>.<json body>
```

Use `INTEGRATION_ADDR` to bind a different address.

For more visibility while testing locally:

```bash
RUST_LOG=webhook_integration=debug,tower_http=warn PTCHAN_INTEGRATION_SECRET=change-me cargo run --example webhook_integration
```

The integration logs safe header names, body size, parsed event IDs, board/post
IDs, attachment counts, message size, donor status, and whether a poster
fingerprint was present. It excludes cookies, authorization, and the webhook
signature from header summaries.

To inspect the received JSON body during local development only:

```bash
INTEGRATION_LOG_BODY=1 RUST_LOG=webhook_integration=debug PTCHAN_INTEGRATION_SECRET=change-me cargo run --example webhook_integration
```

This should stay off outside local testing. The body has already been sanitized
by the gateway, but it can still contain public post text and optional
integration-scoped fingerprints.

To also fetch sanitized thread state from the gateway after each accepted event,
provide the gateway URL:

```bash
PTCHAN_GATEWAY_URL=http://127.0.0.1:8080 PTCHAN_INTEGRATION_NAME=example PTCHAN_INTEGRATION_SECRET=change-me cargo run --example webhook_integration
```

Set `PTCHAN_READING_LIMIT` to change the requested thread post limit. It
defaults to `50`.

For the default `config/dev.toml`, set the gateway secret to the same value:

```bash
PTCHAN_INTEGRATION_EXAMPLE_SECRET=change-me
```

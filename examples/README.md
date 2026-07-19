# Example Consumer

This folder contains a minimal consumer for gateway webhooks.

Run it with the same webhook secret configured for the gateway:

```bash
PTCHAN_CONSUMER_SECRET=change-me cargo run --example webhook_consumer
```

The consumer listens on `127.0.0.1:8081` by default and accepts events at:

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

Use `CONSUMER_ADDR` to bind a different address.

For more visibility while testing locally:

```bash
RUST_LOG=webhook_consumer=debug,tower_http=warn PTCHAN_CONSUMER_SECRET=change-me cargo run --example webhook_consumer
```

The consumer logs safe header names, body size, parsed event IDs, board/post
IDs, attachment counts, message size, donor status, and whether a poster
fingerprint was present. It excludes cookies, authorization, and the webhook
signature from header summaries.

To inspect the received JSON body during local development only:

```bash
CONSUMER_LOG_BODY=1 RUST_LOG=webhook_consumer=debug PTCHAN_CONSUMER_SECRET=change-me cargo run --example webhook_consumer
```

This should stay off outside local testing. The body has already been sanitized
by the gateway, but it can still contain public post text and optional
consumer-scoped fingerprints.

To also fetch sanitized thread context from the gateway after each accepted
event:

```bash
PTCHAN_GATEWAY_URL=http://127.0.0.1:8080 PTCHAN_CONSUMER_NAME=example PTCHAN_CONSUMER_SECRET=change-me cargo run --example webhook_consumer
```

Set `PTCHAN_CONTEXT_LIMIT` to change the requested thread post limit. It
defaults to `50`.

For the default `config/dev.toml`, set the gateway secret to the same value:

```bash
PTCHAN_WEBHOOK_EXAMPLE_SECRET=change-me
```

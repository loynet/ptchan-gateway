# Integration Contract

The files in this directory are the machine-readable integration contract.
They are generated from the Rust wire types and endpoint declarations:

- `openapi.json` describes the signed reading and posting API.
- `schemas/` contains standalone JSON Schema Draft 2020-12 documents.
- `examples/` contains canonical payload examples.

The running gateway also serves the OpenAPI document at:

```text
GET /integration/v1/openapi.json
```

The supported Go implementation is
[`clients/go`](../../clients/go/README.md). It is hand-written around this
contract, and its tests read the canonical examples in this directory directly.

Regenerate committed artifacts after an intentional contract change:

```bash
cargo run -- --write-contract
```

`make` runs `--check-contract`, then verifies the Go client against these
artifacts.

## Compatibility

The HTTP API is versioned by its `/integration/v1` prefix. Webhooks carry
`"schema_version": "1"`.

Within version 1:

- existing fields and meanings are not removed or changed;
- new optional fields and new event kinds may be added;
- consumers should ignore unknown response fields;
- request bodies reject unknown fields;
- optional post fields are omitted when unavailable rather than serialized as
  `null`.

## Request Signing

Signed reads and replies send:

```http
x-ptchan-integration: example
x-ptchan-timestamp: 2026-07-19T12:00:00Z
x-ptchan-signature: hmac-sha256=<lowercase hex>
```

The timestamp must be RFC 3339 and within five minutes of gateway time.

After successful authentication and capability/board authorization, an API
attempt consumes rate-limit quota even if later query, path, JSON, or message
validation fails. Bodies rejected as oversized cannot be authenticated and do
not consume integration quota.

Read signatures cover the exact path and query:

```text
<timestamp>.<method>.<path-and-query>
```

Reply signatures additionally cover the exact JSON bytes sent:

```text
<timestamp>.<method>.<path-and-query>.<json body>
```

## Webhook Delivery

Webhook requests send:

```http
content-type: application/json
x-ptchan-event-id: ptchan:post.created:test:399
x-ptchan-timestamp: 2026-07-19T12:00:00Z
x-ptchan-signature: hmac-sha256=<lowercase hex>
```

Webhook signatures cover:

```text
<timestamp>.<exact json body>
```

Delivery is durable and at-least-once, with best-effort ordering. Consumers
must deduplicate using `event_id` or `x-ptchan-event-id` and tolerate duplicate,
delayed, and out-of-order events.

## Identity

`post.tripcode` is the public secure tripcode emitted by ptchan. It is never
the configured tripcode secret.

When the public tripcode exactly matches a configured posting integration,
`post.origin` identifies that integration. A missing `origin` means only that
the post did not carry a configured public tripcode.

`poster_fingerprint`, when enabled for a webhook, is scoped to that integration.
It is not an upstream cloak and must not be correlated across integrations.

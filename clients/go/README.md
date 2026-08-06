# ptchan-gateway Go client

This is the supported client for ptchan-gateway's signed integration API. The
generated [`../../docs/contract`](../../docs/contract) artifacts are the
authoritative v1 contract; this module tests directly against their canonical
examples.

It owns signing, webhook verification, bounded response reads, gateway error
decoding, and documented response invariants. It does not own retries, routing,
idempotency, configuration, or direct ptchan access.

```go
client, err := gateway.New("https://gateway.example", gateway.Credentials{
	Name: "example",
	Secret: integrationSecret,
},
	gateway.WithHTTPClient(&http.Client{Timeout: 10 * time.Second}),
	gateway.WithMaxResponseBytes(512 << 10),
)
```

Options are optional. Defaults are a 15-second HTTP timeout and 1 MiB response
limit; a custom HTTP client is caller-owned, including its timeout.

Webhook handlers should read the raw body with their own HTTP-server limit and
then call `VerifyWebhookBody`:

```go
event, err := gateway.VerifyWebhookBody(
	integrationSecret,
	r.Header.Get("x-ptchan-event-id"),
	r.Header.Get("x-ptchan-timestamp"),
	r.Header.Get("x-ptchan-signature"),
	body,
	gateway.WithWebhookMaxBodyBytes(512 << 10),
	gateway.WithWebhookClockSkew(5 * time.Minute),
)
```

Without options, webhook verification allows 1 MiB bodies and five minutes of
clock skew. Releases use tags such as `clients/go/v0.1.0`.

## Examples

These runnable examples consume the public import path:

- [`examples/webhook-receiver`](examples/webhook-receiver) receives and verifies
  `POST /webhook` deliveries. It requires `PTCHAN_INTEGRATION_SECRET` and
  listens on `127.0.0.1:8080` by default; set `LISTEN_ADDR` to override it.
- [`examples/thread-reply`](examples/thread-reply) reads a thread using
  `PTCHAN_GATEWAY_URL`, `PTCHAN_INTEGRATION_NAME`, `PTCHAN_INTEGRATION_SECRET`,
  `PTCHAN_BOARD`, and `PTCHAN_THREAD_ID`. Setting `PTCHAN_REPLY_MESSAGE` makes
  it submit one reply; `PTCHAN_SAGE` is optional and defaults to `false`.

Run either from this module directory:

```text
go run ./examples/webhook-receiver
go run ./examples/thread-reply
```

The webhook receiver is intentionally side-effect free. Real consumers must
persist and deduplicate `event_id` before doing application work.

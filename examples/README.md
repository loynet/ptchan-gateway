# Gateway Client

`gateway_client` is the single development and production smoke-test tool. Set
the target integration and gateway:

```bash
export PTCHAN_GATEWAY_URL=https://gateway.example.com
export PTCHAN_INTEGRATION_NAME=example
export PTCHAN_INTEGRATION_SECRET=change-me
```

Then use the same executable for health checks, signed reads, signed replies,
or receiving signed webhooks:

```bash
cargo run --example gateway_client -- health
cargo run --example gateway_client -- read test 397 50
cargo run --example gateway_client -- post test 397 "hello from the gateway"
printf 'multi-line reply\n' | cargo run --example gateway_client -- post test 397 --stdin
cargo run --example gateway_client -- listen 127.0.0.1:8081
```

The listener accepts `POST /internal/ptchan/events`, verifies the HMAC
signature, requires webhook schema version `1`, checks that the event ID header
matches the body, and logs the accepted post coordinates.

The complete machine-readable API and webhook contract is under
[`docs/contract`](../docs/contract/README.md).

When `PTCHAN_INTEGRATION_SECRET` is absent, the client also checks
`PTCHAN_INTEGRATION_<INTEGRATION_NAME>_SECRET`.

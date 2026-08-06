# Go Client Notes

This module implements the public, signed ptchan-gateway protocol. The root
`AGENTS.md` remains authoritative for privacy, compatibility, and product
boundaries; these notes only add Go-specific conventions.

## Scope

Keep this package small and standard-library-first. It owns request signing,
bounded response reading, gateway error-envelope decoding, webhook
verification, and documented response invariants. It does not own retries,
webhook routing, idempotency, configuration loading, application policy, or
direct ptchan access.

## API And Security

- Prefer a small explicit exported API with Go doc comments on exported names.
- Use functional options only for real transport, limit, or deterministic-test
  seams. Secure defaults must work without options.
- Never retain or include raw response bodies, webhook bodies, credentials,
  signatures, or secrets in errors.
- Preserve unknown additive JSON fields. Reject only malformed data and the
  documented v1 invariants the client promises to validate.
- Do not retry reply posts: an upstream timeout can leave a reply state unknown.

## Contract And Checks

- `docs/contract/` is the source of truth. Client tests read its canonical
  examples directly; do not create copied fixture sets.
- Run `gofmt -w .`, `go test ./...`, `go vet ./...`, and `go mod tidy` before
  handing work back.

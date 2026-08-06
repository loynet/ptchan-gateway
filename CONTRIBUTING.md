# Contributing

Small, focused pull requests are easiest to review and safest to deploy.

## Before opening a pull request

- Start from `main` and use a descriptive branch.
- Keep the gateway's privacy, contract, and least-privilege boundaries intact;
  [AGENTS.md](AGENTS.md) is the project guide.
- Run `make check`. Run `make verify` when changing Rust dependencies, release
  behavior, or build tooling.
- Regenerate contract artifacts with `cargo run -- --write-contract` when an
  intentional public-contract change requires it.
- Explain user-visible behavior, configuration changes, and how you tested the
  change. Do not include secrets, cookies, signatures, or raw upstream data.

## Review and merge

`main` is intended to stay deployable. Do not push directly to it: open a pull
request, let CI pass, and wait for maintainer approval. The maintainer applies
the repository ruleset described in the GitHub settings; this document does
not itself enforce GitHub permissions.

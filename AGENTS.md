# Paseo Relay — Agent Guide

Paseo Relay is an Elixir/OTP WebSocket relay for Paseo. Cowboy/Ranch serves
the listener; a per-serverId Owner and Syn registry keep each session on
one BEAM node. Frames are end-to-end encrypted.

This repository owns the application, tests, generic Dockerfile, image publishing
and documented runtime contract. Consumers integrate through the GHCR image
pinned by digest and the environment settings in README.md. Keep provider
configuration, inventories, proxies and operator procedures outside this repo.

## Development

```sh
asdf install
mix deps.get
mix test
scripts/ci.sh
```

## Conventions

- Platform-agnostic core: no provider dependencies in application or scripts.
- Use real Cowboy listeners, WebSockets and :peer BEAM nodes, without mocks.
- Every behavior change needs a red test first and evidence in TDD.md.
- Fail closed: monitor Owner, Writer and Capacity and close explicitly on loss.
- Capacity limits and timeouts belong to the README runtime contract; document
  changes there in the same commit.
- Never dump full process state on live nodes or combine production actions.

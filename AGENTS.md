# Paseo Relay — Agent Guide

Paseo Relay is a distributed, protocol-compatible WebSocket relay for
[Paseo](https://github.com/getpaseo/paseo). Daemons and clients meet here by
`serverId`; frames are end-to-end encrypted by the Paseo protocol, so the
relay never sees content. It is written in Elixir/OTP: Bandit serves the
public listener, a per-`serverId` owner process pins each session to one BEAM
node via [Syn](https://hexdocs.pm/syn/readme.html), and a deployment adapter
reroutes WebSocket upgrades to the owning node so frames never cross nodes.

**This is critical production infrastructure.** People run their entire
working day through it, and a blip of even a few seconds is user-visible.
Read the bar and the diagnostics discipline in
[OPERATIONS.md](OPERATIONS.md) before touching anything production-shaped —
in particular: never dump full process state (`:sys.get_state/1`) on live
nodes, and never stack production actions.

## Docs

| Doc | What's in it |
| --- | --- |
| [README.md](README.md) | Protocol compatibility, dev setup, configuration reference, black-box load testing |
| [OPERATIONS.md](OPERATIONS.md) | The production bar, diagnostics discipline, capacity model, failure behavior, metrics/alerting |
| [TDD.md](TDD.md) | Red/green evidence log for every behavior — the test methodology record |
| [deployment/fly/README.md](deployment/fly/README.md) | The Fly.io adapter: bootstrap, manual deployment policy, and a generic health-check/incident cookbook |

For a Fly health check, read `OPERATIONS.md` first and then follow the cookbook
in `deployment/fly/README.md`. Health checks are read-only: do not deploy,
restart, resize, cordon, or stop a Machine unless the user explicitly asks for
an intervention after the failure is confirmed.

## Development

```sh
asdf install
mix deps.get
mix test
mix format --check-formatted
MIX_ENV=prod mix release        # production release build
```

## Conventions

- **Platform-agnostic core.** Nothing under `lib/` or `scripts/` may depend
  on a deployment provider. Provider specifics live in explicit adapters
  under `deployment/`; the core speaks only the generic settings documented
  in README.md.
- **Tests use real dependencies.** Real Bandit listeners, real WebSockets,
  real `:peer` BEAM nodes for distributed behavior — no mocks of the things
  under test. Every behavior change gets a red test first; record the
  red/green evidence in TDD.md as the existing entries do.
- **Fail closed.** Sockets monitor the processes they depend on (owner,
  Registry) and close with an explicit code rather than lingering in a
  half-alive state. Follow that pattern for anything new.
- **No silent capacity changes.** Listener ceilings, connection limits, and
  timeouts are part of the operational contract in OPERATIONS.md — change
  the doc in the same commit.

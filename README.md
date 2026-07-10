# Paseo Relay

A distributed, protocol-compatible relay for [Paseo](https://github.com/getpaseo/paseo).

Paseo Relay keeps its public WebSocket protocol independent from its deployment platform. Nodes use OTP only for discovery and route ownership; frames stay in memory on one node or travel directly over TCP between nodes.

This project is under active development and its internal protocols may change without notice.

## Development

The Elixir and Erlang versions are managed with [asdf](https://asdf-vm.com/):

```sh
asdf install
mix deps.get
mix test
```

See [LICENSE](LICENSE).

## Operations

The release has no deployment-provider dependency. Its settings are intentionally
generic:

| Setting | Default | Meaning |
| --- | --- | --- |
| `PASEO_RELAY_HOST` | `127.0.0.1` | Advertised relay host for future routing wiring. |
| `PASEO_RELAY_PORT` | `4000` | Public HTTP/WebSocket listener. |
| `PASEO_RELAY_INTERNAL_PORT` | `4001` | Reserved internal relay transport port. |
| `PASEO_RELAY_DRAIN` | `false` | Start not-ready while existing sessions drain. |
| `RELEASE_NODE` / `RELEASE_COOKIE` | unset | Standard distributed-release identity. |

`GET /health` is a liveness probe. `GET /ready` returns `200` only while the
node accepts new work, and returns `503 {"status":"draining"}` once
`PaseoRelay.Drain.begin/0` is called. `GET /metrics` is Prometheus text and
currently exposes readiness/draining gauges; connection and frame counters are
reserved for the routing lane, which owns those events.

Build a production release with `MIX_ENV=prod asdf exec mix release`, or build
the generic container with `docker build -t paseo-relay .`. The explicit
provider adapter in [`deployment/fly`](deployment/fly) translates its platform
node input into `RELEASE_NODE`; nothing under `lib/` or `scripts/` depends on it.

## Black-box load testing

The executable client uses actual WebSockets and the deployed v2 query contract:
`serverId`, `role`, optional `connectionId`, and `v=2`. It opens matching
server-data and client roles, so it can measure bidirectional traffic without
importing relay code. It prints one JSON object containing connection success and
failure counts, frame throughput, p50/p95/p99 latency, duration, client RSS/CPU,
and optional relay RSS/CPU (`--relay-pid`).

Safe local smoke test:

```sh
node scripts/relay-load.mjs --scenario idle --connections 10 --duration 10
node scripts/relay-load.mjs --scenario sustained --connections 10 --rate 10 --duration 10
```

Two-node localhost test (after starting relays on ports 4000 and 4002):

```sh
node scripts/relay-load.mjs --endpoints ws://127.0.0.1:4000/ws,ws://127.0.0.1:4002/ws --scenario burst --burst 100
```

Capacity tests need an appropriate file-descriptor limit and kernel socket
budget. Example high-load commands, deliberately not defaults:

```sh
ulimit -n 120000
node scripts/relay-load.mjs --scenario idle --connections 50000 --duration 300 --relay-pid "$RELAY_PID"
node scripts/relay-load.mjs --scenario sustained --connections 1000 --rate 5 --duration 300 --relay-pid "$RELAY_PID"
node scripts/relay-load.mjs --scenario reconnect --connections 1000 --reconnects 20 --duration 10
```

The sustained example sends in both directions: 1,000 pairs × 5 ticks/s × 2
frames = 10,000 frames/s. The current bootstrap does not yet implement v2 relay
routing, so load commands are expected to report connection/frame failures until
the protocol/routing branch is wired in.

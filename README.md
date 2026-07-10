# Paseo Relay

A distributed, protocol-compatible relay for [Paseo](https://github.com/getpaseo/paseo).

Paseo Relay keeps its public WebSocket protocol independent from its deployment platform. Nodes use OTP only for discovery and route ownership. A deployment adapter reroutes WebSocket upgrades to the owning node, so frames stay inside one BEAM node.

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
| `PASEO_RELAY_HOST` | `127.0.0.1` | Public listener IP. |
| `PASEO_RELAY_PORT` | `4000` | Public HTTP/WebSocket listener. |
| `PASEO_RELAY_DRAIN` | `false` | Start not-ready while existing sessions drain. |
| `PASEO_RELAY_OWNERSHIP_TARGET` | `local` | Opaque target advertised to other relay nodes. |
| `PASEO_RELAY_REROUTE_HEADER` | `x-reroute-target` | Response header used by the deployment adapter. |
| `PASEO_RELAY_CLUSTER_QUERY` | unset | Optional DNS query used to discover BEAM peers. |
| `PASEO_RELAY_MIN_CLUSTER_SIZE` | `1` | Minimum nodes required before accepting unowned sessions. |
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
failure counts, requested pairs and WebSockets, setup and steady durations, frame
throughput, p50/p95/p99 latency, duration, client RSS/CPU,
and optional relay RSS/CPU (`--relay-pid`).

Safe local smoke test:

```sh
node scripts/relay-load.mjs --scenario idle --pairs 10 --duration 10
node scripts/relay-load.mjs --scenario sustained --pairs 10 --rate 10 --duration 10
```

Two-node localhost test (after starting relays on ports 4000 and 4002):

```sh
node scripts/relay-load.mjs --endpoints ws://127.0.0.1:4000/ws,ws://127.0.0.1:4002/ws --scenario burst --burst 100
```

Capacity tests need an appropriate file-descriptor limit and kernel socket
budget. Example high-load commands, deliberately not defaults:

```sh
ulimit -n 120000
node scripts/relay-load.mjs --scenario idle --pairs 25000 --batch-size 250 --ramp-ms 100 --duration 300 --relay-pid "$RELAY_PID"
node scripts/relay-load.mjs --scenario sustained --pairs 1000 --batch-size 250 --ramp-ms 100 --rate 5 --duration 300 --relay-pid "$RELAY_PID"
node scripts/relay-load.mjs --scenario reconnect --pairs 1000 --batch-size 250 --ramp-ms 100 --reconnects 20 --duration 10
```

The sustained example sends in both directions: 1,000 pairs × 5 ticks/s × 2
frames = 10,000 frames/s. The 50,000-socket target is 25,000 pairs plus one
control socket. `--batch-size` bounds concurrent opens; `--ramp-ms` spaces each
batch. `--duration` measures steady traffic only, while JSON reports setup and
steady durations separately.

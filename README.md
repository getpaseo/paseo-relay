# Paseo Relay

A distributed, protocol-compatible relay for [Paseo](https://github.com/getpaseo/paseo).

Paseo Relay keeps its public WebSocket protocol independent from its deployment platform. Nodes use OTP only for discovery and route ownership. A deployment adapter reroutes WebSocket upgrades to the owning node, so frames stay inside one BEAM node.

This project is under active development and its internal protocols may change without notice.

## Protocol compatibility

`serverId` and v2 `connectionId` are accepted only up to 256 bytes. This is a
deliberate compatibility difference from the Durable Object relay, which does
not impose this relay-side route-key limit. Oversized identifiers receive `400`
before they can create ownership or session state.

## Development

The Elixir and Erlang versions are managed with [asdf](https://asdf-vm.com/):

```sh
asdf install
mix deps.get
mix test
scripts/ci.sh
```

`scripts/ci.sh` runs dependency checks, formatting, tests, the production release
and generic container health, readiness, metrics and restart checks.

See [LICENSE](LICENSE).

## Runtime contract

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
| `PASEO_RELAY_CLUSTER_NODES` | unset | Optional comma-separated `name@host` peers, connected at startup and retried every 5 seconds alongside DNS discovery. |
| `PASEO_RELAY_MIN_CLUSTER_SIZE` | `1` | Minimum nodes required before accepting unowned sessions. |
| `PASEO_RELAY_ACCEPTORS` | `100` | Listener acceptor processes. |
| `PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR` | `200` | Capacity factor multiplied by the acceptor count to set the node-local active-WebSocket ceiling; the default is 20,000. |
| `PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS` | `15000` | Maximum idle time for pre-upgrade HTTP parsing and unread request bodies. Upgraded WebSockets remain exempt. |
| `PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS` | `5000` | Maximum wait for a state-changing Capacity decision before the exact ledger epoch is invalidated. Choose a value appropriate to the runtime. |
| `PASEO_RELAY_INGRESS_BUDGET_BYTES` | `536870912` | Node-wide weighted ceiling for complete WebSocket messages admitted to relay delivery. Must admit one maximum message at the configured weight. |
| `PASEO_RELAY_INGRESS_WEIGHT` | `4` | Conservative memory weight charged per wire payload byte. |
| `PASEO_RELAY_DELIVERY_TIMEOUT_MS` | `30000` | Maximum Writer reservation/write-barrier wait before a slow destination is shed. Also the shared budget for starting delivery of a closing peer's last messages. |
| `PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS` | `35000` | TCP send timeout. Must be greater than the Writer deadline so application shedding is recorded before the transport's final fallback. |
| `PASEO_RELAY_CONTROL_QUEUE_BYTES` | `1048576` | Per-destination bound for queued control notifications. |
| `PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS` | `15000` | Maximum time a v2 client frame waits for its daemon-data socket. |
| `PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES` | `65536` | Per-socket TCP receive buffer. |
| `PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS` | `33554432` | Per-WebSocket BEAM heap fuse, including shared binaries. Values below this protocol-safe floor are rejected. |
| `PASEO_RELAY_MEMORY_WATERMARK_BYTES` | `0` | Optional BEAM total-memory watermark that pauses admission and sheds WebSockets with `1013` until measured memory reaches the recovery threshold; disabled generically because the safe threshold depends on the deployment memory limit. Can change live; see [Live settings](#live-settings). |
| `PASEO_RELAY_HEAL_ENABLED` | `true` | Move sessions back to their home node after a failover. See [Session healing](#session-healing). Can change live. |
| `PASEO_RELAY_HEAL_DELAY_MS` | `360000` | Time a home node must stay ready before this node moves sessions back to it, from 0 to 86400000. Set it above the time a reconnecting daemon needs to reach the home node again, for example the DNS TTL of the relay name. Can change live. |
| `PASEO_RELAY_HEAL_RATE_PER_SEC` | `25` | Maximum number of sessions one node moves per second, from 1 to 500. Can change live. |
| `RELEASE_NODE` / `RELEASE_COOKIE` | unset | Standard distributed-release identity. |

`GET /health` is a liveness probe. `GET /ready` returns `200` only while the
node accepts new work, and returns `503 {"status":"unready"}` while draining
or below the configured cluster floor, when the node-local Capacity ledger does
not answer its one-second status observation, during a memory-pressure episode,
or while the application WebSocket ceiling is full. Temporary ingress-byte occupancy does not make the application unready. `GET
/metrics` is Prometheus text and exposes readiness, draining, active WebSockets,
active sessions, reroutes, connection rejections, delivery pressure and latency,
frame sizes, slow-consumer closes, ingress reservations, the applied memory
watermark, misplaced sessions, healed sessions, home promotions, and BEAM memory
for the local node. If Capacity is unavailable, its five gauge families,
including `paseo_relay_memory_watermark_bytes`, are omitted rather than
reported as zero.

State-changing Capacity decisions use a separate configured fail-closed
boundary. Each caller captures the current ledger PID; a timeout kills that
exact Capacity epoch, and `:rest_for_one` drains its listener and sockets before
a clean replacement admits traffic. Read-only readiness and metrics status uses
one second and never kills the ledger.

Payloads never pass through a node-wide relay mailbox. Each `serverId` Owner
stores topology metadata only, and every destination has a Writer allowing one
payload write at a time. During delivery Cowboy's native `{active, false}`
flow control suspends source reads while the WebSocket process remains available
for full-duplex writes; the source is rearmed only after the destination's
synchronous HTTP/1 send barrier. One absolute delivery deadline includes Owner
lookup, destination attachment, Writer reservation, and the send barrier;
accepted control notifications are either written within their deadline or
close the destination with retryable `1013`. Kernel TCP pressure therefore
reaches the producer without deadlocking simultaneous opposite-direction
traffic. Cowboy
may finish parsing frames already buffered before suspension, so every completed
message receives an explicit token from one node-local capacity ledger and is
queued in source order. The same ledger owns connection slots, weighted retained
bytes, delivery state, pressure order, and their gauges; a socket monitor, rather
than its termination callback, releases all tokens after abnormal death. Losing
the ledger stops the production listener and its existing connections before a
fresh ledger can reopen admission. Budget exhaustion closes that source with
retryable `1013`.

Client-originated JSON frames with type `hello` or `e2ee_hello` are the narrow
exception to opaque forwarding. After message-capacity admission, a stateless
validation requires the `key` to be canonical padded Base64 for a canonical
32-byte X25519 public key and rejects unsupported values. It checks every
matching text or binary frame on both routing versions; other frames remain
opaque. A rejection is never forwarded and closes the client with `1008 Invalid
handshake key`.

The compatible masked data-frame ceiling remains exactly 32 MiB, which permits
`32 MiB - 14 bytes` of payload. Cowboy applies that payload limit to individual
frames and reassembled fragmented messages and closes an oversized message with
`1009`. The only supported inbound v2 control message is the legacy JSON ping;
control input has a separate 64 KiB Cowboy ceiling and is charged to the same
ledger through parsing. Outbound control notifications use the same Writer
boundary with their own bounded byte queue. Cowboy assembles a complete payload
before the application can request its byte-only Capacity token, and incomplete
fragment assembly remains inside the connection process. The 32 MiB ceiling,
per-WebSocket heap fuse, and configured node memory watermark limit and shed
this staging risk; they do not provide a strict pre-parser memory reservation.
Generic operators should set a nonzero watermark from their runtime limit;
blocked deliveries are shed first.

## Session healing

A session can stay on a slower node after a failover, a restart, a memory-pressure
shed, or a client that claims the session before its daemon. Session healing
moves the session back to its home node. Daemons and clients need no change.

The home of a session is the node where its daemon control connection arrives
directly. A daemon control connection is a v1 `role=server` connection or a v2
`role=server` connection with an empty `connectionId`. Client connections and v2
daemon data connections never set or change a home. The relay records the
arrival before it decides to reroute, so a daemon that gets a `409` at its home
still sets that home.

An upgrade is direct when both conditions are true:

1. The request has no `x-paseo-rerouted` header.
2. If `PASEO_RELAY_HOST` is a specific address, the TCP peer address is the same
   address. With `0.0.0.0` or `::`, only the header decides.

A deployment adapter must follow this rule. A same-host proxy for public traffic
connects from the `PASEO_RELAY_HOST` address. A proxy that forwards a rerouted
upgrade to another node connects from a different address or sets
`x-paseo-rerouted`.

A node records a first home for a `serverId` only when its cluster is complete:
every `PASEO_RELAY_CLUSTER_NODES` peer is connected, or the
`PASEO_RELAY_MIN_CLUSTER_SIZE` floor is met when no static peers are set. This
stops a daemon that failed over during an outage from being pinned to the
fallback node.

Each node keeps a copy of every home and sends each change to all connected
nodes. A node that connects receives a full copy. The newest write wins. A node
keeps at most 200,000 homes and removes the oldest home when the map is full. A
direct arrival at the home node writes the home again. A home that was not
written for 7 days is removed, unless its session is owned on that home node
now: a daemon that stays connected keeps its home, and the node writes it again.

Each node asks its peers once per second whether they are ready, with the same
rule as `GET /ready` and a one-second timeout. A peer that drains, has memory
pressure, is full, does not answer, or disconnects stops being ready at the next
poll, or at once on disconnect. Its ready period then starts again.

Once per second, if healing is enabled and the node is not draining, the node
selects the sessions it owns whose home has been ready for
`PASEO_RELAY_HEAL_DELAY_MS`. Sessions with no client connections go first. It
moves at most `PASEO_RELAY_HEAL_RATE_PER_SEC` of them: the session Owner stops at
once, without the 30-second idle grace, and its WebSockets close with
`1012 Session owner moved`. The daemon reconnects and claims its home. A node
does not move the same session again for 120 seconds.

A daemon can return directly to the node that moved it, for example because its
network now prefers that node. If it returns within 120 seconds while its home is
ready, the node waits 30 minutes before it moves that session again. If the
daemon returns a second time, the node becomes its home and increments
`paseo_relay_home_promotions_total`.

Known limits:

- A client that reconnects before the daemon can claim the session on another
  node again. The node then moves the session again after 120 seconds.
- If a home node leaves the cluster permanently, its sessions stay counted in
  `paseo_relay_misplaced_sessions` until their homes expire after 7 days.

The session metrics are:

| Metric | Type | Meaning |
| --- | --- | --- |
| `paseo_relay_misplaced_sessions` | gauge | Sessions this node owns whose home is another node. |
| `paseo_relay_sessions_healed_total` | counter | Session Owners this node stopped to move sessions to their home. |
| `paseo_relay_home_promotions_total` | counter | Homes moved to this node after a daemon returned twice. |

## Live settings

You can change four settings on a running node without a restart:
`memory_watermark_bytes`, `heal_enabled`, `heal_delay_ms` and
`heal_rate_per_sec`. They use the same validation as their environment
settings. Existing WebSockets stay open. A watermark below the current memory
starts the normal memory-pressure shedding.

```sh
bin/paseo_relay rpc 'IO.inspect(PaseoRelay.RuntimeConfig.put(%{memory_watermark_bytes: 5_368_709_120}))'
bin/paseo_relay rpc 'IO.inspect(PaseoRelay.RuntimeConfig.get())'
```

`put/1` returns `{:ok, settings}` or `{:error, message}`. An invalid value
rejects the whole change. If Capacity or Placement does not answer, `put/1`
returns `{:error, "Capacity is unavailable; nothing changed"}` or
`{:error, "Placement is unavailable; nothing changed"}` and the node keeps the
previous values. Capacity fails closed and restarts, as for any Capacity
timeout. `put/1` waits at most 2 seconds for Placement. The node keeps the
values when Capacity or Placement restarts inside the same VM. A new VM reads
the environment again, so also change the environment setting.
`paseo_relay_memory_watermark_bytes` reports the watermark that Capacity
applies now; `0` means disabled.

Build a production release with `MIX_ENV=prod asdf exec mix release`, or build
the generic container with `docker build -t paseo-relay .`.

CI publishes `ghcr.io/getpaseo/paseo-relay` after successful verification on
`main`, tags, and same-repository pull requests. Main also updates
`latest`; pull requests publish only `sha-<commit>` candidate tags. Pin consumers
to the `sha256` digest reported in the publish job summary:

```sh
docker pull ghcr.io/getpaseo/paseo-relay@sha256:<64-hex-digest>
docker run --rm -p 4000:4000 ghcr.io/getpaseo/paseo-relay@sha256:<64-hex-digest>
```

The image and this environment contract are the supported integration boundary.
The container starts the release on port 4000; set `RELEASE_NODE`,
`RELEASE_DISTRIBUTION=name` and `RELEASE_COOKIE` for distributed operation.
Standard OTP settings (`ERL_FLAGS`, `ERL_AFLAGS`, `ERL_EPMD_ADDRESS`) control
VM scheduling and distribution listeners.

Cluster ownership uses [Syn](https://hexdocs.pm/syn/readme.html), an eventually
consistent distributed process registry. A network partition can temporarily
admit one owner for the same `serverId` on each side. When the registry
converges, Syn keeps one owner and the losing owner's WebSockets close with
`1012`, causing clients to reconnect through normal routing. Existing
WebSockets have no transparent migration or cross-partition forwarding. Restore
or fence a prolonged partition before treating reconnects as one session again.

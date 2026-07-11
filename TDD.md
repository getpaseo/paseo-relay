# TDD evidence

## v1 pairing

- Red: `mix test test/relay_protocol_test.exs` failed with `WebSockex.RequestError{code: 404}` because the bootstrap router had no `/ws` upgrade route.
- Green: the same real Bandit/WebSocket test passed after adding query validation, the WebSock upgrade, and the single-node session registry.

## v2 control and buffering

- Red: the first control assertion failed because it compared serialized JSON rather than the control message it represents.
- Green: the test now decodes the real received control frame and verifies the `sync` and `connected` messages, then verifies ordered buffered text and binary delivery after the daemon data socket connects.

## duplicate daemon data

- Red: `mix test test/relay_protocol_test.exs:102` timed out waiting for the replacement daemon data socket after the displaced socket's termination deleted the new route.
- Green: the registry now deletes a v2 data route only when its current owner disconnects; the focused real-WebSocket test passes.

## distributed ownership and reroute

- Red: the real peer-node test exposed that tying ownership to the first request could move an otherwise active session when that request process exited.
- Green: a per-`serverId` owner now reserves upgrades, monitors every attached WebSocket, expires abandoned reservations, and remains authoritative until the whole session is idle. Real `:peer` tests cover concurrent claims, remote lookup, owner loss, and takeover.
- Red: the pre-upgrade router test reached WebSocket negotiation on a non-owner node.
- Green: the non-owner now returns the configured opaque reroute response before upgrade; the owner still completes a real Bandit WebSocket handshake.

## public identifier bounds

- Red: a 257-byte `serverId` completed a `101 Switching Protocols` response and claimed distributed ownership.
- Green: identifiers longer than 256 bytes now receive `400` before ownership, while empty client connection IDs retain the compatible generated-ID behavior.

## sharded load generation

- Red: a real black-box run requesting `--no-control` still opened five WebSockets for two pairs because every load process unconditionally opened its own daemon control socket.
- Green: sharded runs can omit that single shared socket and use an explicit connection-ID prefix. A real Bandit/WebSocket test verifies four data sockets, bidirectional frames, and clean shutdown without importing relay internals.
- Red: the same real-server test had no keepalive accounting when a keepalive interval was requested.
- Green: every open test socket can now send a small, separately-counted keepalive frame during long ramps; timers are cleared on close and finalization.

## Relay parity hardening (`6dbe13c`)

### Reject invalid upgrades before ownership

- Red: `mix test test/paseo_relay/router_integration_test.exs:18` sent a plain
  `GET /ws` request and received `500 Internal Server Error` from
  `WebSockAdapter.UpgradeError` after `Ownership.route/2` had run.
- Green: the same real TCP request receives `426 Expected WebSocket upgrade`
  and `Ownership.owner_pid/1` returns `:undefined`.

### Legacy JSON control keepalive

- Red: `mix test test/relay_protocol_test.exs:77` sent `{"type":"ping"}` on a
  real v2 control WebSocket and timed out waiting for a response.
- Green: the same socket receives a JSON object with `type: "pong"` and an
  integer timestamp.

### Stuck control recovery

- Red: `mix test test/relay_protocol_test.exs:94` connected a client without a
  matching server-data socket; after 11 seconds, control had received no sync
  nudge.
- Green: control receives the current `sync` list at 10 seconds and, when data
  is still absent at 15 seconds, closes with `1011 Control unresponsive`.

### Registry crash fail-closed behavior

- Red: after a verified client-to-data frame, killing the registered Registry
  left the real client WebSocket open past one second.
- Green: each socket monitors the Registry process that attached it; the same
  process-level crash closes client and data WebSockets with
  `1012 Registry unavailable`.

### No relay idle disconnect

- Red: the real idle-WebSocket regression ran against the adapter's inherited
  60-second timeout and closed before its 61-second assertion completed.
- Green: the route passes `timeout: nil`; the same real socket remains open
  past 61 seconds.
- Verification correction: ExUnit's default per-test timeout is also 60
  seconds, so the regression test is explicitly tagged `timeout: 75_000`.
  The assertion remains a real idle socket held open for 61 seconds.

## Complete malformed-handshake validation

- Red: `mix test test/paseo_relay/router_integration_test.exs:28` sent
  `Upgrade: websocket` without `Connection: Upgrade`. The adapter raised
  `WebSockAdapter.UpgradeError` after routing, returned `500`, and
  `Ownership.owner_pid/1` was a live PID.
- Green: the router now runs
  `WebSockAdapter.UpgradeValidation.validate_upgrade/1` before
  `Ownership.route/2`; the same real TCP request returns `426` and ownership
  remains `:undefined`.

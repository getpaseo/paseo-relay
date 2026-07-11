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

## live partition healing

- Red: the first real two-node partition fixture used OTP's default fully
  connected topology. `:global` correctly prevented the overlapping partition
  by disconnecting the remaining links, so the test lost its control path
  before it could exercise Syn's conflict resolution.
- Green: the peers now use the same explicit-connect topology as Syn's own
  network-partition suite. Two real Bandit listeners accept the same
  `serverId` while disconnected; after reconnect, all observers converge on
  one owner, the losing WebSocket receives `1012 Session owner moved`, and a
  new WebSocket upgrade on the losing listener receives a `409` reroute to the
  winner. The focused test passed three consecutive seeded runs.

## owner call pressure tolerance

- Red: a real owner process paused for 1.1 seconds caused `Owner.reserve/1` to
  return `:closed` even though the process was still healthy, because its local
  `GenServer.call` used a one-second timeout.
- Green: owner coordination now uses the standard five-second local call
  bound. The same paused owner resumes and returns a valid reservation. Registry
  attachment remains at five seconds; the 15,000-WebSocket run already exercises
  that shared mailbox, so its timeout was not widened to mask overload.

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

- Red: the real idle-WebSocket regression ran with `timeout: nil` and received
  a remote `1002` close after 60 seconds. Bandit treats `nil` as no timeout
  override, so ThousandIsland retained its server-level 60-second read timer.
- Green: the route passes `timeout: :infinity`, which ThousandIsland handles as
  a persistent override and uses to cancel the read timer. The same real socket
  remains open past 61 seconds.
- Verification correction: ExUnit's default per-test timeout is also 60
  seconds, so the regression test is explicitly tagged `timeout: 75_000`.
  The assertion remains a real idle socket held open for 61 seconds.

### Honest load-test cleanup accounting

- Red: a live load run received clean `1000` closes after roughly 5.3 seconds,
  beyond the harness's fixed 5-second cleanup window, and reported them as
  connection failures. The public-contract tests also failed while
  `--cleanup-grace` and `cleanup_timeouts` were absent.
- Green: teardown now has a configurable 15-second default grace and reports
  sockets that outlive it separately as `cleanup_timeouts`. A cleanup timeout
  still fails the run, while the existing close listener continues to count
  abnormal closes such as `1006` as connection failures. The real-server load
  test verifies successful teardown through the public JSON result. A live
  201-WebSocket run then completed with zero failures or cleanup timeouts even
  though teardown took about 10.3 seconds, beyond the previous fixed window.

### Failed setup owns pending sockets through teardown

- Red: a real relay endpoint delayed the server-data upgrade while a second
  real HTTP endpoint rejected its matching client upgrade. The CLI printed the
  setup failure but did not exit within the test's three-second bound because
  the sibling opened after cleanup had snapshotted only already-open sockets.
- Green: every created socket now has a completion lifecycle before its upgrade
  settles. Finalization marks pending siblings, closes them if they subsequently
  open, and waits for their completion. The same real-network test exits in
  about 600 milliseconds with a failed status and non-`101` error, no cleanup
  timeout, and zero active relay WebSockets. After bounded cleanup, the CLI
  explicitly exits so a transport stuck below the WebSocket API cannot retain
  the load process indefinitely.

## Complete malformed-handshake validation

- Red: `mix test test/paseo_relay/router_integration_test.exs:28` sent
  `Upgrade: websocket` without `Connection: Upgrade`. The adapter raised
  `WebSockAdapter.UpgradeError` after routing, returned `500`, and
  `Ownership.owner_pid/1` was a live PID.
- Green: the router now runs
  `WebSockAdapter.UpgradeValidation.validate_upgrade/1` before
  `Ownership.route/2`; the same real TCP request returns `426` and ownership
  remains `:undefined`.

## Ownership surge and bounded admission

### Replace synchronous global ownership

- Red: the production-shaped three-node `:global` path returned `503 owner` for
  a 500-session reconnect surge. Locally, 10,000 distinct ownership claims took
  roughly 14.6 seconds. Removing the redundant transaction and `global.sync/0`
  still took roughly 13 seconds because `global.register_name/2` is itself a
  synchronous cluster-wide registration.
- Green: ownership now uses Syn's strict distributed registry and advertises the
  opaque reroute target as registration metadata. A real three-node BEAM test
  covers concurrent conflicts, convergence, owner loss, remote takeover, and
  distinct-server surges. The same local machine completed 10,000 claims in
  roughly 200 milliseconds and 50,000 in roughly 1.17 seconds.
- Safety: Syn may briefly admit competing owners during a race or partition.
  Every real relay socket monitors its owner and closes with `1012 Session owner
  moved` if Syn discards that owner. The real Bandit/WebSockex regression was red
  before owner monitoring and green afterward.

### Exercise ownership through real WebSockets

- Red: the black-box load client rejected `--scenario ownership`; it could only
  create many connections under one shared `serverId`, so it did not exercise
  the distributed ownership bottleneck.
- Green: the scenario opens one real v2 daemon-control WebSocket for every
  distinct `serverId`, in bounded batches, through the public `/ws` contract. A
  committed real-server test opens 1,000 distinct sessions. A manual local run
  opened 15,000 real WebSockets in 1.50 seconds with zero connection, send, or
  cleanup failures. Relay RSS was about 602–642 MB across repeated runs.
- Load-generator boundary: a single macOS source/destination tuple has 16,384
  ephemeral ports (`49152..65535`), so a 50,000-socket attempt failed in the
  client around 16.3k connections. The 50,000-owner three-node BEAM test and the
  15,000 real-socket test measure the two layers without misreporting client port
  exhaustion as relay failure.

### Shed overload using the listener's built-in ceiling

- Red: a real relay configured for one acceptor and two connections still
  upgraded all three requested WebSockets because the application ignored its
  listener admission settings.
- Green: the application maps generic listener settings to Thousand Island's
  existing per-acceptor `DynamicSupervisor.max_children` limit and bounded retry
  policy. The same real-network test upgrades exactly two sockets, rejects the
  excess connection, fails the load run, and increments
  `paseo_relay_connection_rejections_total` once.
- No application queue was added. A queued WebSocket handshake would retain the
  file descriptor and memory while hiding overload from the client. Bounded
  listener retries followed by connection shedding preserve an explicit retry
  boundary.

### Fail closed across owner and metrics races

- Red: after obtaining a real owner reservation, killing that owner before
  `WebSock.init/1` made `Owner.attach/3` exit with `:noproc`, crashing the socket
  process before it could install its owner monitor.
- Green: owner calls now translate owner death and call timeout into `:closed`.
  The same reserved-owner regression returns the existing
  `1012 Session expired` close path. Lookup-to-reserve and
  reservation-to-attach use the same bounded call boundary.
- Red: killing `PaseoRelay.Metrics` with `:kill` left its fixed telemetry handler
  registered. The replacement failed with `already_exists`, exhausted the
  application supervisor, and left the relay stopped.
- Green: metrics initialization reclaims the handler before attaching it and
  reuses the existing counter store. Fault injection now produces a new metrics
  PID while the original relay supervisor stays alive, `/metrics` returns 200,
  and counter values survive the restart.

### Prove distributed convergence after the ownership surge

- The original surge test asserted only that each landing node returned a local
  owner. Because Syn replication is asynchronous, that measured local owner
  creation plus RPC throughput but not a usable converged registry.
- The strengthened test snapshots per-origin registry counts on every node,
  creates distinct owners round-robin across three real BEAM nodes, and waits
  until every observer sees the exact expected count from every origin. It then
  resolves sampled IDs from a non-owner node and verifies their opaque reroute
  targets.
- With `PASEO_OWNERSHIP_SURGE_COUNT=50000`, registration, full three-node count
  convergence, and cross-node route sampling complete in roughly 1.5 seconds on
  the local test cluster.

### Keep production capacity inside the measured memory envelope

- A real worst-case run held 15,000 distinct owner WebSockets with zero failures
  at about 642 MB relay RSS. The earlier 50,000-owner result exercised BEAM
  ownership without network sockets and therefore did not validate the 2 GB Fly
  Machine memory boundary.
- Fly now starts spare capacity at 10,000 connections and refuses new connections
  at 15,000. The generic listener is a final safety net at 20,000; operators with
  larger Machines can raise it explicitly. No production default exceeds the
  real-socket capacity run.

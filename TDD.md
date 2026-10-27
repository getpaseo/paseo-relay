# TDD evidence

## Native Cowboy delivery and admission reshape

### Final standards architectural audit

## v1 pairing

- Red: `mix test test/relay_protocol_test.exs` failed with `WebSockex.RequestError{code: 404}` because the bootstrap router had no `/ws` upgrade route.
- Green: the same real WebSocket test passed after adding query validation, the upgrade, and the single-node session registry.

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
- Green: the non-owner now returns the configured opaque reroute response before upgrade; the owner still completes a real WebSocket handshake.

## live partition healing

- Red: the first real two-node partition fixture used OTP's default fully
  connected topology. `:global` correctly prevented the overlapping partition
  by disconnecting the remaining links, so the test lost its control path
  before it could exercise Syn's conflict resolution.
- Green: the peers now use the same explicit-connect topology as Syn's own
  network-partition suite. Two real Cowboy listeners accept the same
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
- Green: sharded runs can omit that single shared socket and use an explicit connection-ID prefix. A real WebSocket test verifies four data sockets, bidirectional frames, and clean shutdown without importing relay internals.
- Red: the same real-server test had no keepalive accounting when a keepalive interval was requested.
- Green: every open test socket can now send a small, separately-counted keepalive frame during long ramps; timers are cleared on close and finalization.

## Relay parity hardening (`6dbe13c`)

### Reject invalid upgrades before ownership

- Red: `mix test test/paseo_relay/router_integration_test.exs:18` sent a plain
  `GET /ws` request and reached ownership before WebSocket upgrade validation.
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

- Red: the real idle-WebSocket regression received a remote close after 60
  seconds because the listener retained its default idle timer.
- Green: the Cowboy WebSocket and protocol listener use `idle_timeout: :infinity`.
  The same real socket
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
  `Upgrade: websocket` without `Connection: Upgrade`, reached routing, and
  created an owner before the handshake failed.
- Green: the Cowboy handler checks `is_upgrade_request/1` before
  `Ownership.route/2`; the same real TCP request returns `426` and ownership
  remains `:undefined`.

## Ownership surge and bounded admission

### Replace synchronous global ownership

### Exercise ownership through real WebSockets

- Red: the black-box load client rejected `--scenario ownership`; it could only
  create many connections under one shared `serverId`, so it did not exercise
  the distributed ownership bottleneck.
- Green: the scenario opens one real v2 daemon-control WebSocket for every
  distinct `serverId`, in bounded batches, through the public `/ws` contract. A
  committed real-server test opens 1,000 distinct sessions. A manual local run
  opened 15,000 real WebSockets in 1.50 seconds with zero connection, send, or
  cleanup failures. This was the pre-Cowboy implementation; relay RSS was about
  602–642 MB across repeated runs.
- Load-generator boundary: a single macOS source/destination tuple has 16,384
  ephemeral ports (`49152..65535`), so a 50,000-socket attempt failed in the
  client around 16.3k connections. The 50,000-owner three-node BEAM test and the
  15,000 real-socket test measure the two layers without misreporting client port
  exhaustion as relay failure.

### Enforce the active-WebSocket ceiling explicitly

- Red: Ranch removes upgraded connections from its connection accounting when
  Cowboy takes over the WebSocket. A Ranch `max_connections` setting therefore
  limits concurrent HTTP handling, not the relay's long-lived WebSockets.
- Green: a node-local active-WebSocket budget admits exactly the configured
  number of real upgrades. The next valid local upgrade receives `503 Relay
  connection capacity` and increments the rejection counter. Closing an
  admitted WebSocket releases its monitored slot, after which the rejected
  session completes a real `101` upgrade. The test finishes at the exact zero
  baseline, covering normal and monitored cleanup rather than process internals.
- Fail-closed evidence: every admitted WebSocket monitors the budget process.
  A real socket receives `1013 Relay capacity unavailable` when that process is
  killed; after supervision replaces the budget, a new real upgrade succeeds
  and its slot returns to zero on close. A budget restart therefore cannot erase
  admission counts while old sockets remain unaccounted.

### Fail closed across owner and metrics races

- Red: after obtaining a real owner reservation, killing that owner before
  WebSocket initialization made `Owner.attach/3` exit with `:noproc`, crashing the socket
  process before it could install its owner monitor.
- Green: owner calls now translate owner death and call timeout into `:closed`.
  The same reserved-owner regression returns the existing
  `1012 Session expired` close path. Lookup-to-reserve and
  reservation-to-attach use the same bounded call boundary.
- Red: killing `PaseoRelay.Metrics` with `:kill` left the old listener-specific
  telemetry handler registered. The replacement failed with `already_exists`,
  exhausted the application supervisor, and left the relay stopped.
- Green: the native listener has an explicit active-WebSocket admission boundary
  and increments its rejection counter directly, so Metrics owns no external
  telemetry handler. Fault injection now produces a new Metrics PID while the
  original relay supervisor stays alive, `/metrics` returns 200, and counter
  values survive the restart.

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

## Deterministic full-suite ownership teardown

- Red: an independent `asdf exec mix test --seed 268085` completed 57/58. In
  `a remote node can claim after the current owner dies`, the monitor delivered
  `{:DOWN, ref, :process, owner, :noproc}` for the expected ref and owner PID,
  while the test required the incidental reason `:normal`. A local pre-change
  rerun of the exact command passed 58/58 in 99.1 seconds, confirming that the
  reason race was intermittent rather than a deterministic behavior failure.
- Green: both owner-death tests accept any reason only for the pinned monitor
  ref and owner PID, then wait through the public ownership API until the
  server resolves `:unowned` before reclaiming it. Production owner behavior is
  unchanged.
- A subsequent full-suite seed-1 red returned the documented retryable
  `{:unavailable, :owner}` during two simultaneous cross-node claims. The test
  helper crashed while decoding that result. It now retries only this explicit
  retryable response through `Ownership.route/2`; all winner PID, liveness,
  convergence, and opaque-target assertions remain intact.
- The same seed-1 run also supplied a real maximum-frame red: the socket
  process reached the configured `16,777,216`-word shared-binary heap fuse while
  unmasking the maximum legal `32 MiB - 14 bytes` client payload. The destination
  received nothing, proving the default fuse could kill protocol-valid traffic.
  The configurable default and minimum are now `33,554,432` words; values below
  that proven-safe floor are rejected. The 32 MiB wire-frame ceiling itself is
  unchanged. The focused real-network/config gate passed 9 tests with 14
  excluded in 0.7 seconds.
- Final full-suite matrix (`asdf exec mix test --seed SEED`), all 59 tests each:
  seed 1, 95.2s; 17, 96.3s; 101, 95.6s; 313, 93.2s; 997, 96.5s; 2027,
  93.8s; 4099, 95.4s; 8191, 95.5s; 16381, 93.5s; and audited seed 268085,
  101.1s. Result: 590/590 passed across 10 full-suite seeds.
- Final gates: `asdf exec mix format --check-formatted` exited 0;
  `asdf exec mix compile --warnings-as-errors` compiled two changed files and
  exited 0; `git diff --check` exited 0; and
  `MIX_ENV=prod asdf exec mix release --overwrite` assembled
  `paseo_relay-0.1.0` successfully. The release was not started or deployed.

## Final admission, deadline, and pressure boundaries

## Validate client X25519 handshake inputs

- Red: focused real-WebSocket regressions demonstrated the missing validation
  behavior for `e2ee_hello` and legacy `hello` frames on v1 and v2.
- Green: one stateless `HandshakeValidation.check/2` boundary now classifies
  every matching client text or binary frame after Capacity admission.
  Canonical Base64, exact key length, and a supported public-key encoding are
  required before forwarding. A rejection finishes its admitted message token,
  increments one fixed-dimension counter, closes the client with `1008`, and is
  not delivered. No handshake phase is retained.
- Coverage: unit tests exercise a generated valid X25519 key across both
  opcodes and both handshake type names, plus malformed, incorrectly sized, and
  unsupported encodings. Real v1/v2 sockets prove unchanged forwarding, retry
  handling, binary legacy input, pipelined frames, rejection behavior, and
  accepted/rejected metric increments.
- Final targeted gate: `asdf exec mix test
  test/paseo_relay/handshake_validation_test.exs test/relay_protocol_test.exs`
  passed 20/20 in 81.4 seconds. `asdf exec mix format`, `asdf exec mix compile
  --warnings-as-errors --force`, and `git diff --check` exited zero.

## Deliver a closing peer's last messages before detach

- Red: eight real-WebSocket regressions in `test/relay_backpressure_test.exs`
  cover v1 and v2, a daemon or client source, and an empty or coded Close. Each
  establishes a route, suspends the real Owner, and writes three messages plus a
  Close in one TCP write. The source echoes the Close before Owner can route
  anything. All eight failed: v2 destinations received the counterpart `1012` or
  `1001` instead of the messages, and v1 destinations received nothing. Two
  runs delivered the in-flight first message and lost the queued rest.
- Cause: Cowboy keeps parsing data it has already read after `{active, false}`
  and sends a Close straight to `terminate/3` without calling the handler.
  `Socket.terminate/3` then killed the in-flight delivery task and discarded
  `pending`, before casting `Owner.detach/2`.
- Green: on `:remote` and `{:remote, code, payload}` termination, the socket waits
  for its in-flight delivery and then delivers `pending` in order before releasing
  its slot and detaching. Queued messages start only while one shared deadline,
  `delivery_timeout_ms` from the start of termination, has time left. The in-flight
  delivery keeps its earlier deadline. The drain stops at the first failed
  delivery and on memory pressure, `relay_close`, or Owner, Writer, or budget
  `DOWN`. Capacity mutations keep their configured timeout because an expiry kills
  the ledger. Delivery tasks no longer capture the whole socket state, so they do
  not copy the pending queue.
- Review red: two independent reviews found that a drain-side timer at the shared
  deadline could kill a queued delivery before its `Owner.destinations/4` timeout
  retired a stalled Owner. "a queued delivery shares the close deadline and still
  retires a stalled Owner" holds the first delivery at a real write barrier, stalls
  Owner, and failed: Owner stayed alive. Green: the drain no longer times out a
  delivery itself; like an open socket, it waits for the delivery to end at its own
  deadline. That test reads the queued lookup's deadline from Owner's mailbox and
  asserts it is the drain's, then suspends the delivery task past that deadline so
  a drain-side timer would deterministically win the race, and asserts the task
  survives and retires Owner.
- Guards: "a close drain stops at the first failed delivery" traces the real Owner
  and counts route lookups after `:erlang.trace_delivered/1`, asserting exactly one;
  "a close drain behind a stalled Owner ends at the in-flight deadline and retires
  it" asserts Owner death and a bounded source lifetime; "a queued delivery to a
  stalled Writer still ends with the drain" blocks a queued reservation on a
  suspended Writer, observes that reserve call in the Writer's mailbox, and asserts
  the source still ends with Owner alive and transient gauges released, since the
  drain has no timer of its own; "memory pressure interrupts a close drain" first proves the drain is
  waiting on a route lookup, then asserts prompt source death without delivery.
  Against the original socket, the first-failure guard passes (the old code also
  made one lookup before dropping everything); the stalled-Owner, shared-deadline,
  and pressure tests fail, because the old code neither retires the Owner nor keeps
  a draining source alive.
- Mutation checks against the green code: continuing after a failed delivery makes
  the first-failure guard see 3 lookups instead of 1; giving each queued delivery a
  fresh deadline puts the queued lookup's deadline 860 ms after the send against a
  700 ms bound; restoring a drain-side timer fails "the drain cancelled a delivery
  before it could retire Owner" on every run.
- Gate: the thirteen focused tests passed on three random seeds. The unread-fanout
  test "a real unread fanout peer reaches its send deadline without delaying
  healthy order" sits near its 45 s timeout on a heavily loaded host: in an
  interleaved A/B it passed 3/3 with this change and 2/3 without it.



## Static peer discovery

- Red: `mix test test/paseo_relay/static_cluster_test.exs` started an isolated
  real `:peer` over its standard-I/O control channel (not distribution), loaded
  `PASEO_RELAY_CLUSTER_NODES`, and failed with `KeyError :cluster_nodes` before
  implementation. Green: the configured peer connects without DNS, reconnects
  after `Node.disconnect/1`, and ignores its own node name. Malformed names are
  rejected at the configuration boundary; unset peers leave discovery disabled.

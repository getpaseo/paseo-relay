# Operating Paseo Relay

## The bar

The relay is critical infrastructure. People run their entire working day
through it — agents, terminals, mobile sessions — and a blip of even a few
seconds is user-visible. A stuck node is a broken workday for everyone whose
sessions it owns. Treat every production action — deploys, restarts,
diagnostics, config changes — as something users will feel, and never stack
two of them (a diagnostic during a reconnect surge, a deploy during a
migration) without deciding that the combination is safe. When in doubt,
don't touch production.

## Diagnostics discipline

Every recurring observability question must be answerable from `/metrics`,
logs, or a new low-cardinality metric — never by interrogating live
processes.

- **Never call `:sys.get_state/1` (or any full-state dump) on a production
  Owner or singleton process.** Copying a large state term blocks that process.
  The retired node-wide Registry was previously wedged this way during a
  reconnect surge. Payloads now bypass global coordination, but full-state
  diagnostics remain unsafe and unnecessary.
- Targeted RPC reads during an incident are acceptable: `:syn.lookup/2` for a
  single key, one counter, one small map. Full-table or full-state scans are
  not, and the busiest node during a surge is the worst possible target.
- If a question keeps coming up (sessions by role, ownership counts, orphan
  sessions), add a gauge to `PaseoRelay.Metrics` and read it from `/metrics`
  like everything else.

## Capacity model

The relay does not assume a particular production topology. An operator may
run one Machine, several regional Machines, or provision stopped spares. The
Fly template disables automatic Machine starts so capacity changes remain an
explicit operator action. Fly never creates Machines automatically.

The template sets a 10,000-connection soft limit and a 15,000-connection hard
limit per Machine. The soft limit is a Fly placement signal, not a relay
failure threshold. Crossing it alone says nothing about application health;
with automatic starts disabled, it does not start a spare. At the hard limit,
Fly stops assigning new connections to that Machine. Existing connections
remain open. Any increase in `paseo_relay_connection_rejections_total` is a
separate application-level signal and should be treated as real user impact.

Stopped Machines still participate in deployment management: a Machine lease
or state transition can block `fly deploy` even though it is not serving
traffic. A stopped Machine that must remain unavailable to Fly Replay should
also be cordoned; stopping and disabling autostart are not routing fences.

Relay sessions are intentionally not rebalanced. A `serverId` is owned by one
BEAM node so its frames never cross nodes. Additional Machines increase total
fleet capacity for new sessions; they cannot split one exceptionally large
session across Machines.

The relay has a second, provider-independent safety ceiling. The application
multiplies `PASEO_RELAY_ACCEPTORS` by
`PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR`, yielding 20,000 active WebSockets per
node by default. Every valid local upgrade reserves one node-local slot before
Cowboy takes over the WebSocket. At the ceiling, the upgrade receives `503`,
`paseo_relay_connection_rejections_total` increments, and existing WebSockets
remain open. Slots are released explicitly at normal termination and by process
monitoring after abnormal death.

Ranch's own connection accounting is not this safety boundary: Cowboy removes
an upgraded connection from Ranch accounting at WebSocket takeover. Ranch
limits only concurrent pre-upgrade HTTP handling and applies TCP backlog
pressure there. Pre-upgrade HTTP parsing and unread request bodies have a
15-second idle deadline; upgraded WebSockets keep their protocol-level infinite
idle lifetime. The relay adds no application queue for over-capacity upgrades;
an explicit retryable rejection is safer than retaining another socket and
hiding overload as a timeout.

The native-Cowboy capacity run held 15,000 distinct real WebSockets with zero
failures and measured 1,300,512,768 bytes peak relay RSS on the test host. That
is the measured real-socket envelope; the generic 20,000-slot ceiling is a
final admission fuse, not evidence that 20,000 sockets fit every 2 GB runtime.
Provider limits and machine sizing must stay inside a locally measured envelope
with room for the configured ingress budget and VM overhead.

## Failure behavior

- **Relay process or Machine exits:** WebSockets on that owner disconnect. OTP
  removes the dead owner, clients reconnect, and a surviving node can claim the
  session. There is no transparent connection migration.
- **One region is unavailable:** Fly routes reconnects to a healthy region. The
  extra round trip is temporary; the replacement owner is then stable.
- **A node crosses the soft limit:** this is capacity information, not an
  incident. Existing sessions remain on their owners. If the deployment has
  automatic starts enabled, Fly may start an existing stopped Machine; the
  repository's Fly template keeps automatic starts disabled.
- **A node reaches the hard limit:** Fly stops sending it new connections.
  Existing connections remain open. A client pinned to that owner may fail to
  reconnect until capacity is available on the owner or the owner disappears.
- **A rolling deployment replaces an owner:** its WebSockets reconnect just as
  they would after a Machine exit. Ownership convergence can make one logical
  session reconnect more than once during a rollout; there is no single-reconnect
  guarantee. Drain is process-local admission state, initialized at boot with
  `PASEO_RELAY_DRAIN` or changed inside the application through
  `PaseoRelay.Drain`; there is no drain HTTP endpoint. Fly's deployment
  lifecycle does not activate this state, so it does not protect ownership
  during `fly deploy`.
- **A node wedges but stays clustered — the worst failure mode.** If a node
  stops answering HTTP (health check critical, `/metrics` unresponsive) while
  its BEAM stays connected to the cluster, Syn keeps its ownership
  registrations alive: reconnecting daemons are rerouted into the wedged node
  and cannot re-home elsewhere. Every session it owned is held hostage until
  the node dies. Remedy: restart the Machine promptly — the restart drops the
  node from the cluster, purges its registrations, and stranded sessions
  re-claim on healthy nodes within seconds. Do not wait for a wedged node to
  recover on its own, and do not diagnose it with anything heavier than its
  logs. Planned follow-up: an in-VM watchdog that self-terminates the node
  when its own readiness or Owner call latency degrades, so this recovery
  does not require an operator.
- **Fly Proxy loses the path to a locally healthy Machine:** forced-instance
  requests time out even though loopback HTTP, Owner routing, CPU, and memory
  remain healthy. New clients cannot reach sessions owned by that Machine.
  Start and verify the stopped spare in the same region before disturbing the
  affected Machine, then restart the affected Machine once. If the failure
  returns after a restart, replace the Machine on a fresh Fly host instead of
  repeatedly restarting it. This is a platform ingress failure, not relay
  pressure, and changing relay size will not repair it.
- **A destination stops reading:** its Writer permits only one payload write.
  Source reads remain suspended while its WebSocket process can still service
  full-duplex writes. The Writer records slow-consumer shedding at its delivery
  deadline, before the longer TCP send deadline, while healthy fanout
  destinations continue. If an active source disappears after its frame reaches
  the destination write barrier, that Writer fails closed and rejects its queue;
  it never grants a successor behind the still-outstanding send. A peer that
  resumes reading in time can receive the queued `1013`; a peer whose TCP
  receive path remains completely blocked can only observe transport closure
  because no WebSocket close frame can traverse that blocked path. Increasing
  `paseo_relay_backpressured_sources` is expected during brief congestion;
  sustained growth plus delivery timeouts or slow-consumer closes is actionable.
- **A session Owner stops servicing its mailbox:** the absolute delivery
  deadline closes the source and forcibly retires the timed-out Owner. Syn
  removes its registration, so reconnect can claim a fresh Owner instead of
  adding work to an indefinitely stalled session authority.
- **A peer closes right after its last messages:** Cowboy parses a Close that
  arrives in the same read as earlier messages without returning to the socket,
  so those messages can still be in flight or queued in source order. On a
  normal remote Close, empty or coded, the source keeps its Owner attachment,
  connection slot, and message tokens and delivers them in order to the
  destinations that can still receive them before it detaches. Any v2 counterpart
  Close that the detach causes (`1012` to clients, `1001` to the daemon data
  socket after the last client) therefore follows them; v1 has no counterpart
  Close.

  Queued messages start only while one shared `PASEO_RELAY_DELIVERY_TIMEOUT_MS`
  deadline has time left. It starts when termination begins, after Cowboy has
  answered the Close. Each delivery then ends at its own deadline exactly as on an
  open socket, so a stalled Owner is still retired and a slow destination still
  shed. A `Capacity.start_delivery` call begun before the deadline keeps its own
  configured timeout and can extend the source's lifetime by at most one mutation
  window, whether it succeeds late or times out and restarts the ledger. The drain
  stops at the first failed delivery and on memory pressure, a relay-initiated
  close, or Owner, Writer, or connection-budget death; remaining messages are
  dropped as before. Those notifications wait while a Capacity call is in
  progress. Abnormal exits do not drain, and listener shutdown interrupts a drain
  in progress. While draining, the source's own outbound traffic is not serviced
  because Cowboy has already answered the Close.
- **A control destination stops reading:** all control notifications use that
  destination's Writer. Once its bounded control queue fills, the control socket
  receives retryable `1013`; an accepted queued notification that reaches its
  absolute deadline also closes the destination instead of disappearing.
  Notifications cannot accumulate in an unbounded WebSocket mailbox.
- **Ingress reaches the node budget:** every complete message Cowboy delivers
  receives a token before protocol dispatch. Frames already buffered when source
  reads are suspended are separately tokenized and retained in source order. One
  monitored capacity ledger owns connection slots, weighted reservations, active
  deliveries, pressure order, and all related gauges. Socket death atomically
  drops every token, including after a forced heap-fuse exit. If the ledger dies,
  the runtime supervisor stops the listener and every old connection before
  restarting the ledger and reopening admission; this intentionally creates a
  node-local reconnect wave rather than overlapping accounting epochs. If the
  weighted total would cross the configured ceiling, that source closes with
  retryable `1013`; `paseo_relay_ingress_reserved_bytes` never exceeds the
  ceiling.

  Known remote owners are rerouted without consuming local capacity. For local
  or unowned sessions, connection admission is acquired before reserving or
  creating an Owner, so a capacity or pressure rejection cannot leave transient
  session state behind. Pre-upgrade HTTP parsing and unread request bodies have
  a finite idle lifetime; upgraded WebSockets explicitly retain an infinite
  protocol idle timeout and rely on their application lifecycle instead.

  Public state-changing decisions capture the exact current Capacity PID and
  wait up to the configured Capacity mutation timeout for that epoch's reply.
  A reply is the decision. If the captured PID dies first, the caller returns
  the existing unavailable result. If the call times out, the caller kills that
  exact PID and returns
  unavailable; it never kills a process subsequently registered under the same
  name. The runtime supervisor's `:rest_for_one` ordering then stops the
  listener and every socket from the failed epoch before replacement Capacity
  can admit traffic. This deliberately turns an authority stall at that bound
  into a node-local reconnect wave so that no externally failed mutation
  survives in a later accounting epoch. The generic default and Fly template
  currently select 5,000 ms, provisionally; the combined staging epoch gate must
  certify that value for the intended topology and Machine size before rollout.

  The Fly-only manual gate distributes exactly 23,001 real WebSockets across
  three exact Machines: 7,667 per Machine, below both deployed application
  ceilings and Fly placement limits. It is a short POSIX procedure around three
  ordinary `relay-load.mjs` sustained runs. The target shard establishes all
  sockets but holds its publisher while the unaffected shards run normally.
  Immediately after `:sys.suspend/1` acknowledges, the procedure signals the
  target publisher to start. Every pair then sends a frame containing
  1,024 padding bytes plus timestamp, direction, and sequence metadata in both
  directions once per second, while each control socket sends a valid ping,
  including throughout the target Machine's Capacity stall. The procedure reads
  the deployed ceiling and mutation timeout
  from each release, resets each Machine's cgroup-v2 `memory.peak`, and records
  the exact target Capacity PID. The fault reports the VM monotonic timestamp
  immediately after `:sys.suspend/1` acknowledges. The deliberately started
  public message traffic then causes the configured timeout to invalidate that
  exact epoch, making the acknowledgement the lower-bound clock origin. The
  first replacement observation must fall between that timeout and the timeout
  plus the validated replacement-observation tolerance, provisionally 1,000 ms;
  the final result records both monotonic timestamps and their difference.

  EXIT, SIGINT, and SIGTERM cleanup always asks the target release to kill the
  captured PID only if it is still the registered Capacity, then stops load
  processes, waits the Owner grace interval, and requires every exact-Machine
  proxy to be ready with zero Capacity gauges. Every release must report all
  three staged server IDs unowned, and each final `memory.peak` must remain under
  the operator-supplied ceiling. Unaffected shard JSON permits zero connection
  failure, send failure, ordering failure, cleanup timeout, or frame loss. All
  7,667 old target sockets must report an abnormal epoch disconnect. A second
  full 7,667-socket sustained run then reuses the same target `serverId` through
  the replacement listener and requires zero connection, send, ordering,
  cleanup, or frame-loss failures. The exact credentials, three-Machine
  topology, timeout, replacement tolerance, application connection ceiling,
  memory ceiling, and free local ports are operator inputs. The gate has not
  been run.

  The gate requires a persistent operator artifact directory before destructive
  work. It retains each unchanged load-producer JSON, bounded redacted stderr,
  child exits, initial/final diagnostics, per-observer ownership, timing,
  readiness, raw final metrics, and identified `memory.peak` reset/read evidence.
  One short `summary.json` is emitted on success or failure and contains file
  references, key numeric checks, and stable
  `{check,node,shard,expected,actual,reason}` failures. Evidence is never reduced
  into a second certification schema or deleted by cleanup.

  Every sustained result must cover its requested duration. Unaffected and
  replacement traffic must send at least
  `(duration_seconds * rate - 2) * 7,667` frames; the affected minimum uses the
  configured timeout window because that epoch intentionally drains. This
  conservative two-tick allowance prevents a one-tick result from passing a
  90-second or 10-second run while tolerating interval startup variance.

  Initial connection admission monitors Cowboy's persistent connection
  process. Attachment is accepted only from that same process and reuses its
  monitor; the five-second attachment lease still removes a live holder that
  never upgrades. Message tokens belong to an already-monitored socket.
  Release, finish, and cancel are idempotent one-way cleanup optimizations;
  holder `DOWN` is sufficient for correctness. Synchronous pressure checks and
  watermark changes use the same exact-PID timeout invalidation, so an
  unavailable result cannot be followed by a late mutation in a surviving
  epoch.

  One tagged Capacity status observation supplies authority availability, the
  configured listener namespace's admission state, and all four transient
  gauges through a read-only one-second call. It fits inside the production
  readiness probe's two-second deadline and never kills Capacity.
  `/ready` returns `503` when Capacity is unavailable, memory pressure is active,
  or the application WebSocket ceiling is full. The Fly soft limit and temporary
  occupancy of the ingress byte budget are not readiness conditions. `/metrics`
  performs the same single observation per render; when Capacity is unavailable
  it reports ready zero, retains independent counters, sessions, histograms, and
  BEAM metrics, and omits the four unknown Capacity gauges. Actual Capacity
  process death remains the only epoch trigger: `:rest_for_one` stops
  the listener and old connections before a replacement ledger reopens
  admission.

  The masked client data-frame wire ceiling remains exactly 32 MiB, so Cowboy
  bounds each complete or reassembled fragmented message to `32 MiB - 14 bytes`
  and sends `1009` for one byte more. Inbound v2 control input is limited to
  64 KiB, charged through JSON parsing, and oversized input receives `1009`.
  Cowboy has already assembled a complete payload before `websocket_handle/2`
  can request its byte-only Capacity token. The 32 MiB ceiling, per-WebSocket
  heap fuse, and node memory watermark limit and shed that staging risk, but do
  not create a strict pre-parser memory reservation. During a Capacity stall,
  one completed payload per active socket can remain staged for the configured
  mutation timeout before the epoch is drained. That reconnect and memory
  exposure must be exercised at the documented 23,001-socket staging gate
  before rollout; it has not been certified by the local suite. Incomplete
  fragments also remain inside Cowboy until completion. A pressure episode
  pauses new connection and message admission, sheds the oldest blocked
  delivery first and then the newest active sockets as a conservative proxy for
  recent fragment retention, and sizes subsequent bounded batches from measured
  BEAM memory relief. Admission
  resumes only below a one-maximum-message hysteresis threshold. The generic
  watermark is disabled because the safe threshold depends on the runtime
  limit; a strict generic deployment must set a nonzero threshold. The 2 GB Fly
  template enables it at 1.5 GB.
- **A client sends an invalid X25519 handshake key:** after ingress admission,
  the relay checks every JSON `hello` and `e2ee_hello` frame on both routing
  versions, regardless of text or binary opcode. Malformed Base64, invalid
  encodings, and unsupported public keys close only that client with `1008
  Invalid handshake key`; the frame is never forwarded. The validation stores
  no handshake phase, does not inspect daemon-originated frames, and never logs
  key material or `serverId`.
- **Two nodes concurrently claim a previously unowned `serverId`:** Syn favors
  availability, so both WebSockets can initially open against different local
  owners. Conflict resolution keeps one owner and closes sockets on the loser
  with `1012 Session owner moved`. Clients must reconnect and route to the
  winner. This is most visible during simultaneous daemon/client startup or a
  reconnect wave after ownership expires. A test that waits only for a control
  message and ignores the `1012` close will report a misleading timeout.
- **An upstream proxy sits in front of the relay (e.g. during a migration):**
  the proxy has its own connection lifecycle. Its deploys can sever a large
  and unpredictable fraction of relay connections at once, and its code
  propagation windows can strand clients on the old path. Treat any upstream
  deploy as a fleet reconnect storm: schedule it deliberately, never stack it
  with other production actions, and verify session convergence afterward.
- **The BEAM cluster partitions:** ownership uses Syn's available, strongly
  eventually consistent registry rather than a quorum or shared store. Each side
  can admit an owner for the same `serverId` while disconnected. When the
  cluster heals, Syn resolves the conflict to one owner; sockets monitoring a
  losing owner close with `1012` and reconnect. Existing WebSockets cannot
  forward or migrate across the partition. Restore connectivity or fence one
  side before treating reconnects as converged; do not advertise this topology
  as lossless failover.

## Incident response guardrails

Protect connected users before restoring the preferred topology.

- Confirm a failure with two independent signals or repeated probes. An OOM,
  Machine exit, failed health check, or unreachable forced-instance endpoint is
  already a concrete signal; CPU steal alone is not.
- Never restart more than one Machine at a time. Never restart the whole
  cluster. Existing sockets on the restarted Machine will disconnect.
- Before restarting an unhealthy Machine, start a stopped spare in its region
  when one is available and wait for `/ready` to pass. Do not stop any Machine
  that has acquired sessions during an incident merely to restore the preferred
  topology.
- Restart a Machine when it is wedged or unreachable, not merely busy. Afterward,
  verify the Machine's forced-instance readiness, cluster readiness, Owner
  responsiveness, and reconnect/session convergence before taking another
  action.
- Do not repeatedly restart the same Machine. A recurring Fly ingress failure
  calls for replacement on a fresh host; recurring relay pressure calls for an
  application fix. Repeated restarts only create repeated user-visible blips.
- During unattended monitoring, do not deploy, resize, destroy Machines, change
  configuration, or run load tests. Record every intervention, its evidence,
  and the post-action verification.

## Metrics

Fly scrapes `/metrics` every 15 seconds when the deployment adapter's metrics
configuration is enabled. Custom series are local to a Machine and receive Fly
labels such as app, region, host, and instance. Do not add `serverId` or
`connectionId` as labels; their cardinality is unbounded.

`paseo_relay_handshake_accepted_total` and
`paseo_relay_handshake_rejected_total` expose the protocol validation with only
the fixed `routing_version` (`v1`/`v2`) and `type`
(`hello`/`e2ee_hello`) labels. They count matching client frames accepted or
rejected by the validation; they never contain route identifiers or key
material.

The endpoint fetches Capacity availability, admission state, and all transient
capacity gauges in one bounded ledger call. If that authority is stalled, the
scrape returns a zero ready gauge and omits the four unknown Capacity gauge
families rather than fabricating zeros or serially blocking once per gauge.

Start with dashboards and alerts for:

- `paseo_relay_ready == 0` or `paseo_relay_draining == 1`;
- active WebSockets approaching the deployment's configured soft limit;
- allocated file descriptors above 70% of the Machine limit;
- sustained high memory, CPU, scheduler pressure, or network throughput;
- Machine exits, OOM kills, and unhealthy checks;
- unexpected spikes in reroutes or WebSocket reconnects;
- any increase in `paseo_relay_connection_rejections_total`.

Fly's managed Grafana provides dashboards, but alert delivery needs a separate
Grafana/Alertmanager setup. The next metrics needed for incident diagnosis are
low-cardinality counters for rejected upgrades, close reasons, ownership
takeovers, and cluster peer count.

## VM mesh adapter

[deployment/mesh/README.md](deployment/mesh/README.md) defines the Ubuntu,
WireGuard and Caddy deployment. **The production size is 8 GB per node.** Set
the BEAM memory watermark only after subtracting Caddy's measured peak RSS at
the planned ingress tunnel count, OS/WireGuard overhead and safety headroom
from actual VM RAM. The watermark watches BEAM allocation, not combined node memory;
Caddy retains memory even for tunnels whose relay owner is on another node.
The sample 6 GiB watermark is unvalidated and must be lowered if that combined
budget requires it.

The small-VM rehearsal measured Caddy at 997 MiB RSS with 7,801 WebSockets
(about 130 KiB per proxied socket) and BEAM at 526 MiB RSS. A 2 GB node became
unresponsive under memory pressure at 8,401 admitted sockets; BEAM allocation
was still below its 1.5 GiB watermark in the last complete sample. The 12k
attempt failed before sustained publishing; no OOM kill was established.
That configuration is unsuitable, and these measurements do not certify the
8 GB size. The Vultr staging gate must demonstrate bounded combined
Caddy/BEAM/host memory and zero failures at the target load before production.
The 20,000-WebSocket application ceiling remains unchanged and is not a VM
capacity claim.

A non-owner returns one `409` and Caddy retries the unchanged upgrade directly
on the advertised owner over WireGuard. An unreachable target fails before
`101`; the client's reconnect re-resolves ownership. There is no second-owner
retry. Frames travel as raw proxy bytes, never over BEAM distribution. Both
the ingress proxy and owner VM remain dependencies of that socket.

`deploy.sh` serializes fleet operations, begins drain and verifies `/ready`=503
before replacing any relay, waits 60 seconds for health-monitor withdrawal,
and aborts with drain cancelled if sockets remain unless the operator explicitly
selects a disconnect rollout. Drain blocks new ownership, not attachments to
existing owners. A replacement closes its owned sockets; no sessions migrate.
The script leaves Caddy running and checks every node twice before the next
replacement. DNS health-monitor withdrawal must fit inside the grace period;
DNS caches mean it does not guarantee zero fresh arrivals.

Never reload Caddy during routine operations: its `stream_close_delay 24h`
delays reload closes, it does not eliminate them. Certificate renewal is not a
reload. Schedule any necessary proxy replacement as its own reconnect event.
Scrape per-node metrics and provision alerts separately; this adapter does not
install a monitoring backend. Do not combine DNS cutover, proxy maintenance,
WireGuard changes, load testing and relay replacement in one production action.

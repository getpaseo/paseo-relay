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

- **Never call `:sys.get_state/1` (or any full-state dump) on the production
  Registry or any singleton process.** Copying a large state term blocks the
  process for the duration of the copy. This has wedged a production node:
  full-state dumps issued while a reconnect surge was in flight saturated the
  Registry mailbox, WebSocket attaches timed out and failed closed, the VM
  degraded until its health check went critical, and the Machine had to be
  restarted. The Registry serializes every frame on its node; seconds of
  blocking are an outage.
- Targeted RPC reads during an incident are acceptable: `:syn.lookup/2` for a
  single key, one counter, one small map. Full-table or full-state scans are
  not, and the busiest node during a surge is the worst possible target.
- If a question keeps coming up (sessions by role, ownership counts, orphan
  sessions), add a gauge to `PaseoRelay.Metrics` and read it from `/metrics`
  like everything else.

## Capacity model

The production topology keeps one Machine running in each region and one
stopped spare in each region. Fly Proxy starts an existing spare when the
running Machines selected for a request are above the 10,000-connection soft
limit. A Machine stops accepting new connections at the 15,000-connection hard
limit. Fly never creates Machines automatically, and started spares remain
running until an operator stops them. Stopped spares still participate in
deployment management: a Machine lease or state transition can block
`fly deploy` even though that Machine is not serving traffic.

Relay sessions are intentionally not rebalanced. A `serverId` is owned by one
BEAM node so its frames never cross nodes. Additional Machines increase total
fleet capacity for new sessions; they cannot split one exceptionally large
session across Machines.

The listener has a second, provider-independent safety ceiling. Thousand Island
runs 100 acceptors with 200 connections each by default, for 20,000 live
connections per node. When one acceptor is full it performs five bounded retries
one second apart, then closes the new connection and emits
`paseo_relay_connection_rejections_total`. The values are configurable through
the generic `PASEO_RELAY_ACCEPTORS`,
`PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR`,
`PASEO_RELAY_CONNECTION_RETRY_COUNT`, and
`PASEO_RELAY_CONNECTION_RETRY_WAIT_MS` settings.

There is deliberately no application queue for WebSocket upgrades. Holding an
upgraded socket while ownership work waits would consume the same scarce file
descriptor and memory and turn overload into an invisible client timeout. The
listener provides a bounded wait; after that, shedding the connection lets the
existing client reconnect policy provide backpressure.

## Failure behavior

- **Relay process or Machine exits:** WebSockets on that owner disconnect. OTP
  removes the dead owner, clients reconnect, and a surviving node can claim the
  session. There is no transparent connection migration.
- **One region is unavailable:** Fly routes reconnects to a healthy region. The
  extra round trip is temporary; the replacement owner is then stable.
- **A node crosses the soft limit:** Fly starts a stopped Machine when one is
  available. Existing sessions remain on their owners; new session ownership
  spreads through normal edge placement.
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
  when its own readiness or Registry call latency degrades, so this recovery
  does not require an operator.
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

## Metrics

Fly scrapes `/metrics` every 15 seconds when the deployment adapter's metrics
configuration is enabled. Custom series are local to a Machine and receive Fly
labels such as app, region, host, and instance. Do not add `serverId` or
`connectionId` as labels; their cardinality is unbounded.

Start with dashboards and alerts for:

- `paseo_relay_ready == 0` or `paseo_relay_draining == 1`;
- active WebSockets approaching 10,000 on a Machine;
- allocated file descriptors above 70% of the Machine limit;
- sustained high memory, CPU, scheduler pressure, or network throughput;
- Machine exits, OOM kills, and unhealthy checks;
- unexpected spikes in reroutes or WebSocket reconnects;
- any increase in `paseo_relay_connection_rejections_total`.

Fly's managed Grafana provides dashboards, but alert delivery needs a separate
Grafana/Alertmanager setup. The next metrics needed for incident diagnosis are
low-cardinality counters for rejected upgrades, close reasons, ownership
takeovers, and cluster peer count.

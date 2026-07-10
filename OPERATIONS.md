# Operating Paseo Relay

## Capacity model

The production topology keeps one Machine running in each region and one
stopped spare in each region. Fly Proxy starts an existing spare when the
running Machines selected for a request are above the 10,000-connection soft
limit. A Machine stops accepting new connections at the 50,000-connection hard
limit. Fly never creates Machines automatically, and started spares remain
running until an operator stops them.

Relay sessions are intentionally not rebalanced. A `serverId` is owned by one
BEAM node so its frames never cross nodes. Additional Machines increase total
fleet capacity for new sessions; they cannot split one exceptionally large
session across Machines.

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
  they would after a Machine exit. The drain endpoint protects new ownership,
  but graceful connection handoff is not yet part of the deployment lifecycle.
- **The BEAM cluster partitions:** ownership correctness depends on distributed
  Erlang's view of the cluster. Partition and recovery behavior must be tested
  before treating multi-region ownership as lossless.

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
- unexpected spikes in reroutes or WebSocket reconnects.

Fly's managed Grafana provides dashboards, but alert delivery needs a separate
Grafana/Alertmanager setup. The next metrics needed for incident diagnosis are
low-cardinality counters for rejected upgrades, close reasons, ownership
takeovers, and cluster peer count.

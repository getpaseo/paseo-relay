# Fly deployment

This adapter maps Fly runtime values into Paseo Relay's generic clustering and
reroute configuration. Fly-specific environment variables do not enter the
core application.

The first deployment bootstraps one node with a cluster floor of one. After
cloning it into two more regions, deploy again with the committed floor of two.

```sh
fly apps create YOUR_APP
fly secrets set -a YOUR_APP RELEASE_COOKIE="$(openssl rand -base64 48)"

fly deploy -a YOUR_APP -c deployment/fly/fly.toml \
  --ha=false \
  --primary-region ams \
  --env PASEO_RELAY_MIN_CLUSTER_SIZE=1

fly machines list -a YOUR_APP
fly machine clone FIRST_MACHINE_ID -a YOUR_APP --region iad
fly machine clone FIRST_MACHINE_ID -a YOUR_APP --region sin

fly deploy -a YOUR_APP -c deployment/fly/fly.toml
```

Each Machine advertises `instance=<machine-id>` as an opaque ownership target.
When a WebSocket request reaches a non-owner, the generic reroute adapter emits
`fly-replay: instance=<machine-id>` before WebSocket negotiation. Fly Proxy then
replays the unchanged upgrade request to the owner.

The entrypoint raises the per-process file descriptor limit to 100,000 by
default. Override `PASEO_RELAY_NOFILE` when a deployment needs a different
ceiling. Fly Proxy starts an existing stopped Machine when every running
Machine in the selected region is above the 10,000-connection soft limit.
It refuses new connections to a Machine at the 15,000-connection hard limit.
Fly does not create Machines, so provision stopped spares explicitly.
Stopped spares are still deployment targets; a Machine lease or in-progress
state transition can block `fly deploy`. Rolling deploys can reconnect one
logical relay session more than once while ownership converges. The generic
drain mechanism is process-local admission state, initialized at boot with
`PASEO_RELAY_DRAIN` or changed through the internal `PaseoRelay.Drain` API.
There is no drain HTTP endpoint, and Fly's deployment lifecycle does not
activate this state or protect session ownership during a deploy.

Fly scrapes `/metrics` into its managed Prometheus service. The custom relay
series appear in Fly's managed Grafana alongside its built-in Machine, proxy,
memory, CPU, network, and file-descriptor metrics.

Useful checks:

```sh
fly config validate -a YOUR_APP -c deployment/fly/fly.toml
fly status -a YOUR_APP
curl https://YOUR_APP.fly.dev/health
curl https://YOUR_APP.fly.dev/ready
curl https://YOUR_APP.fly.dev/metrics
```

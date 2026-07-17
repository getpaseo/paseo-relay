# Fly deployment and operations

This adapter maps Fly runtime values into Paseo Relay's generic clustering and
reroute configuration. Fly-specific environment variables do not enter the
core application. This guide deliberately makes no assumptions about a
particular operator's hostname, regions, Machine IDs, or spare topology.

Read [OPERATIONS.md](../../OPERATIONS.md) before operating a live relay.

## Manual deployment policy

This repository intentionally contains no automatic deployment workflow.
Deployments are manual maintenance operations because replacing a relay owner
disconnects every WebSocket it owns. Do not run `fly deploy` as part of a
health check or incident diagnosis.

If deployment automation is added later, it must require an explicit manual
approval; a push to the repository must never deploy automatically.

The template also sets `auto_start_machines = false`. This is separate from
deployment automation: it prevents Fly Proxy from starting stopped capacity.
Operators must start additional Machines deliberately.

## Bootstrap

Copy `fly.toml` and replace its app and primary-region placeholders. Choose the
cluster floor and regions for your deployment; the relay does not require the
three-region example below.

```sh
export APP=your-relay-app
export PRIMARY_REGION=your-primary-region

fly apps create "$APP"
fly secrets set -a "$APP" RELEASE_COOKIE="$(openssl rand -base64 48)"

fly deploy -a "$APP" -c deployment/fly/fly.toml \
  --ha=false \
  --primary-region "$PRIMARY_REGION" \
  --env PASEO_RELAY_MIN_CLUSTER_SIZE=1
```

To add capacity or regions, clone a known-good Machine and choose the desired
region. Set `PASEO_RELAY_MIN_CLUSTER_SIZE` to the minimum number of clustered
nodes required before a node reports ready.

```sh
fly machines list -a "$APP"
fly machine clone SOURCE_MACHINE_ID -a "$APP" --region TARGET_REGION
```

Each Machine advertises `instance=<machine-id>` as an opaque ownership target.
When a WebSocket request reaches a non-owner, the generic reroute adapter emits
`fly-replay: instance=<machine-id>` before WebSocket negotiation. Fly Proxy then
replays the unchanged upgrade request to the owner.

The entrypoint raises the per-process file descriptor limit to 100,000 by
default. Override `PASEO_RELAY_NOFILE` when a deployment needs a different
ceiling. The sample VM size and connection limits in `fly.toml` are starting
points, not universal capacity claims; validate them against the deployment's
traffic and memory profile.

## Read-only health cookbook

Start every check by declaring the deployment rather than relying on remembered
values:

```sh
export APP=your-relay-app
export RELAY_URL=https://your-relay-hostname.example
```

### 1. Check the public path and inventory

```sh
curl -fsS "$RELAY_URL/health"
curl -fsS "$RELAY_URL/ready"
fly machines list -a "$APP"
fly machines list -a "$APP" --json \
  | jq -r '.[] | [.id, .region, .state] | @tsv'
```

`/health` proves the HTTP process is alive. `/ready` additionally proves the
node selected by Fly is not draining and the configured cluster floor is met.
Neither endpoint proves every Machine is healthy, so continue with forced
per-Machine checks.

### 2. Check every started Machine directly

Take each started Machine ID from the inventory. Use HTTP/1.1 so Fly applies the
forced-instance header consistently to the WebSocket service path.

```sh
export MACHINE_ID=machine-id-from-inventory

curl --http1.1 -fsS \
  -H "Fly-Force-Instance-Id: $MACHINE_ID" \
  "$RELAY_URL/ready"

curl --http1.1 -fsS \
  -H "Fly-Force-Instance-Id: $MACHINE_ID" \
  "$RELAY_URL/metrics" \
  | grep -E '^paseo_relay_(ready|draining|active_websockets|active_sessions|reroute_responses_total|connection_rejections_total|frames_forwarded_total|bytes_forwarded_total) '
```

Repeat for every started Machine. Record the values by region and Machine ID;
never paste IDs from another deployment into a runbook.

Interpret the important series as follows:

| Signal | Meaning |
| --- | --- |
| `paseo_relay_ready 1` | The node admits relay work |
| `paseo_relay_draining 1` | The node intentionally refuses new ownership |
| `active_websockets` / `active_sessions` | Current application load, not failure by itself |
| configured Fly soft limit crossed | Placement/capacity signal only |
| configured Fly hard limit reached | Fly will not assign new connections to that Machine |
| `connection_rejections_total` increases | The relay rejected connections; users are affected |

### 3. Check Machine events and logs

```sh
fly machine status "$MACHINE_ID" -a "$APP"
fly logs -a "$APP" --machine "$MACHINE_ID" --no-tail
```

An OOM kill, exit, failed Fly health check, or increasing rejection counter is
concrete evidence. A single slow probe, a transient Registry queue, CPU steal,
or load above the soft limit is not an incident by itself.

### 4. Distinguish Fly ingress from application health

If a forced-instance request fails, check loopback HTTP from inside that same
Machine. This command is read-only:

```sh
fly ssh console -a "$APP" --machine "$MACHINE_ID" -C \
  'sh -lc '\''exec 3<>/dev/tcp/127.0.0.1/4000; printf "GET /ready HTTP/1.0\r\nHost: localhost\r\n\r\n" >&3; cat <&3'\'''
```

- Forced-instance failure plus successful loopback means the process is alive
  and the Fly ingress path is suspect.
- Forced-instance and loopback failure together point at the application or VM.
- Do not deploy to test either hypothesis.

### 5. Use only targeted BEAM diagnostics

Only after ordinary metrics are insufficient, inspect the Registry's queue
length and memory. Never call `:sys.get_state/1` or enumerate Registry state on
a live relay.

```sh
fly ssh console -a "$APP" --machine "$MACHINE_ID" -C \
  'sh -lc '\''RELEASE_NODE="paseo_relay@$FLY_PRIVATE_IP" RELEASE_DISTRIBUTION=name ERL_AFLAGS="-proto_dist inet6_tcp" ELIXIR_ERL_OPTIONS=+fnu /app/bin/paseo_relay rpc "IO.inspect(Process.info(Process.whereis(PaseoRelay.Registry), [:message_queue_len, :memory]))"'\'''
```

Take two small samples several seconds apart. A queue that drains with readiness
intact and no rejection growth is transient. A sustained or growing queue,
timed-out RPC, readiness loss, or increasing rejections is actionable.

## Verdicts

Use a short verdict and evidence, not a wall of telemetry:

- **HEALTHY:** public and forced readiness pass; no new exits, OOMs, or
  rejections; targeted queues are stable.
- **WATCH:** one weak or transient signal without user impact. Re-sample; do not
  alert or intervene merely because a Machine is busy.
- **INCIDENT:** repeated readiness failure, OOM/exit, sustained Registry
  pressure, unreachable owner, or increasing rejection counter.

Confirm an incident with repeated probes or two independent signals, except for
an explicit OOM or Machine exit, which is already concrete evidence.

## Manual capacity and recovery

The template's soft limit does not start capacity because autostart is disabled.
If additional capacity is needed, start a pre-provisioned Machine explicitly and
wait for its forced `/ready` check to pass.

```sh
fly machine uncordon SPARE_MACHINE_ID -a "$APP"
fly machine start SPARE_MACHINE_ID -a "$APP"
```

Before intentionally stopping a Machine, first make sure it does not own traffic.
Cordon it so Fly Replay cannot start or route to it, then stop it. These commands
are interventions, not health checks:

```sh
fly machine cordon SPARE_MACHINE_ID -a "$APP"
fly machine stop SPARE_MACHINE_ID -a "$APP"
```

Never restart or stop more than one Machine at a time. Existing WebSockets do
not migrate; a Machine restart, stop, resize, or deployment disconnects them.
Follow every intervention with the complete read-only health cookbook before
taking another action.

Fly scrapes `/metrics` into its managed Prometheus service. The custom relay
series appear in managed Grafana alongside Fly's Machine, proxy, memory, CPU,
network, and file-descriptor metrics.

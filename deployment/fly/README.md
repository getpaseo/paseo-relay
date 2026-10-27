# Fly deployment and operations

This adapter maps Fly runtime values into Paseo Relay's generic clustering and
reroute configuration. Fly-specific environment variables do not enter the
core application. The generic template supports other deployments;
`production.toml` records the production app configuration.

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

## Production rollout

From the repository root, after local QA and green CI:

```sh
bash scripts/fly-e2e-all.sh
fly deploy -c deployment/fly/production.toml --ha=false
bash scripts/fly-e2e-all.sh
```

Production uses 4 GB performance Machines, with automatic starts and stops
disabled. Rolling deployment replaces one running Machine at a time and waits
for its checks. Its sessions disconnect and reconnect; connections do not
migrate. `--ha=false` prevents additional spare creation.

Use flyctl v0.4.114 or later with verified stopped-Machine preservation. That
version updates existing stopped Machines with `skip_launch`, leaving them
stopped. Capture `fly machines list -a paseo-relay-next --json` before and after;
verify the same running/stopped IDs, sizes, and new image on every Machine.

Every managed Machine must have `fly_platform_version=v2` and
`fly_process_group=app` metadata. API-created Machines without the platform
marker are skipped by `fly deploy`. Register them through the Machines metadata
API before their first managed rollout; this does not restart them.

The global script checks both message directions on each running Machine.
Also run the replay probe below with different owner and landing Machines to
verify cross-region routing. Keep the previous image digest for rollback using
the same deployment command with `--image <previous-image>`.

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

Run that command from the repository root. The config selects
`deployment/fly/Dockerfile`, whose entrypoint installs the Fly clustering and
reroute adapter; `scripts/ci.sh` derives and validates the same target from the
config instead of maintaining a second path.

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

After a deliberate deployment, the provider-specific black-box check can force
the owner and initial landing to different Machines while exercising the public
WebSocket protocol:

```sh
MIX_ENV=test mix run -e \
  'Code.require_file("deployment/fly/replay-e2e.exs"); PaseoRelay.FlyReplayE2E.run(System.argv())' -- \
  --endpoint "$RELAY_URL" \
  --owner OWNER_MACHINE_ID \
  --landing LANDING_MACHINE_ID
```

The entrypoint raises the per-process file descriptor limit to 100,000 by
default. Override `PASEO_RELAY_NOFILE` when a deployment needs a different
ceiling. The sample VM size and connection limits in `fly.toml` are starting
points, not universal capacity claims; validate them against the deployment's
traffic and memory profile. The generic template leaves the BEAM memory
watermark disabled because its safe value depends on the live Machine size;
set it explicitly only after validating the deployment's memory profile.

Before rollout, run this destructive gate only against a disposable
three-Machine staging app of the intended size:

```sh
ulimit -n 100000
export FLY_API_TOKEN=...
export PASEO_FLY_ARTIFACT_DIR="$PWD/staging-evidence/$(date +%Y%m%d-%H%M%S)"
export PASEO_FLY_CONFIRM_STAGING_ONLY=yes
export PASEO_FLY_APP=paseo-relay-staging
export PASEO_FLY_MACHINES=MACHINE_A,MACHINE_B,MACHINE_C
export PASEO_FLY_TARGET_MACHINE=MACHINE_A
export PASEO_FLY_EXPECTED_TIMEOUT_MS=5000
export PASEO_FLY_EXPECTED_CONNECTION_CEILING=20000
export PASEO_FLY_REPLACEMENT_TOLERANCE_MS=1000
export PASEO_FLY_MAX_PEAK_BYTES=1800000000
export PASEO_FLY_PORT_BASE=41000
sh deployment/fly/staging-gate.sh
```

The app, three distinct running Machine IDs, target, deployed timeout and
application ceiling, memory peak ceiling, token, and three free local ports are
explicit operator decisions. The script verifies the deployed values through
bounded exact-Machine release RPC before creating sockets. It starts three
ordinary `relay-load.mjs` sustained processes through exact-Machine `fly proxy`
connections: 3,833 pairs plus one control socket per Machine, or 7,667 each and
23,001 total. The target process opens all sockets but holds its sustained
publisher; the two unaffected processes publish normally. Every publishing
pair sends a frame containing 1,024 padding bytes in
addition to timestamp, direction, and sequence metadata in both directions once
per second, and every control socket sends a valid protocol ping on the same
cadence.

Before socket setup, the script resets each exact Machine's cgroup-v2
`memory.peak`. It captures the target Capacity PID, calls `:sys.suspend/1`, and
records the VM monotonic timestamp immediately after that call acknowledges.
The script then sends `SIGUSR1` to the target load process, which immediately
starts data in both directions on every pair plus control ping traffic. Because
the handler is installed before socket setup and target publishing begins only
after acknowledgement, that acknowledgement is the lower-bound clock origin
for the real Capacity mutation timeout. The first replacement observation must
be no earlier than the deployed timeout and no later than that timeout plus
`PASEO_FLY_REPLACEMENT_TOLERANCE_MS`, which defaults to 1,000 ms and must be
between 1 and 5,000 ms. The retained `timing.json` records the
acknowledged-suspension and replacement-observation VM monotonic timestamps,
their difference, and the post-acknowledgement publisher signal. All
7,667 old target sockets must report abnormal epoch disconnects; the two
unaffected shard JSON results allow zero connection failures, send failures,
ordering failures, cleanup timeouts, or frame loss. After replacement readiness,
a second full 7,667-socket sustained run reuses the same target `serverId` and
must complete bidirectional data and control ping/pong with all those failure
counts at zero.

The POSIX EXIT/SIGINT/SIGTERM trap always calls exact-PID recovery: it kills the
captured PID only when that PID is still the registered Capacity. It then stops
remaining load processes, waits the Owner grace interval, and requires every
Machine to return ready with zero active WebSockets, reserved ingress bytes,
inflight delivery bytes, and backpressured sources. Every release must report
all three staged server IDs unowned, and every final `memory.peak` must be at or
below the operator-supplied limit. Any failed recovery or check makes the
command nonzero.

The artifact directory is mandatory and is created before inventory, sockets,
or fault work. Do not point it at a temporary directory. The command prints the
same short index written as `summary.json`:

```sh
sh deployment/fly/staging-gate.sh | jq .
```

Both success and failure retain the four unchanged `relay-load.mjs` JSON files,
bounded credential-redacted child stderr, child exit statuses, initial and final
release diagnostics, per-observer ownership, readiness, raw final metrics,
suspension/replacement timing, and each cgroup `memory.peak` reset/read record.
`summary.json` is deliberately only an index plus key expected/actual checks and
stable failures; it does not project those producer files into a second schema.
Retain the complete directory with the rollout record.

For an unaffected or replacement shard, the conservative traffic floor is
`(duration_seconds * rate - 2) * 7,667` frames and steady duration must reach the
requested duration. The two-tick allowance covers interval startup and bounded
scheduler variance but prevents a one-tick run from satisfying either the
90-second original shard or 10-second replacement shard. The affected shard
uses the same all-socket frame width and derives its minimum tick count from the
configured Capacity timeout because its epoch is deliberately interrupted.

The exact environment contract can be checked without inventory, sockets, or a
fault using `sh deployment/fly/staging-gate.sh --validate-config`; it still
requires and writes the operator artifact directory. The timing
predicate can be checked independently with
`sh deployment/fly/staging-gate.sh --check-replacement-window ELAPSED_MS`; both
commands execute the same validation used by the destructive run.

CI separately builds and boots the exact Fly image, invokes the small diagnostic
snapshot through `/app/bin/paseo_relay rpc`, and invokes the packaged
`replay-e2e.exs` through the same release boundary. Replay is only a two-frame
public Fly image smoke; it is not ownership convergence or fleet certification.
Actual cgroup reset permission and the 23,001-socket gate remain staging-only
evidence. This gate has not been run and its timeout/memory choices are not
production-certified.

After the branch is pushed and the hosted `verify` check exists, a repository
administrator must configure that check as required in the applicable GitHub
branch rule or ruleset. This repository diff does not and cannot certify that
external protection setting.

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
node selected by Fly is not draining, the configured cluster floor is met, the
node-local Capacity ledger answered inside its one-second status window, memory
pressure is inactive, and the application WebSocket ceiling has room. Fly's
soft connection limit is only a placement signal.
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
  | grep -E '^paseo_relay_(ready|draining|active_websockets|active_sessions|reroute_responses_total|connection_rejections_total|backpressured_sources|ingress_reserved_bytes|inflight_delivery_bytes|delivery_timeouts_total|slow_consumer_disconnects_total|beam_total_memory_bytes|beam_binary_memory_bytes) '
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
| `connection_rejections_total` increases | The relay rejected active-WebSocket admission; users are affected |

### 3. Check Machine events and logs

```sh
fly machine status "$MACHINE_ID" -a "$APP"
fly logs -a "$APP" --machine "$MACHINE_ID" --no-tail
```

An OOM kill, exit, failed Fly health check, or increasing rejection counter is
concrete evidence. A single slow probe, transient backpressure, CPU steal,
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

### 5. Use the bounded-pressure metrics

Do not inspect Owner process state on a live relay. Take two `/metrics` samples
several seconds apart. Ingress reservations and in-flight delivery bytes must
remain under their configured ceilings. Backpressured sources that drain with
readiness intact and no timeout growth are transient. Sustained pressure,
increasing timeouts/slow-consumer closes, readiness loss, or increasing
connection rejections is actionable.

## Verdicts

Use a short verdict and evidence, not a wall of telemetry:

- **HEALTHY:** public and forced readiness pass; no new exits, OOMs, or
  rejections; targeted queues are stable.
- **WATCH:** one weak or transient signal without user impact. Re-sample; do not
  alert or intervene merely because a Machine is busy.
- **INCIDENT:** repeated readiness failure, OOM/exit, sustained relay pressure,
  an unreachable owner, or an increasing rejection counter.

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

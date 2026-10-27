# VM mesh adapter

A node is an Ubuntu VM, host WireGuard, and two host-network containers: Caddy
and the **generic** relay image. No provider API or Fly service participates in
this path. `inventory.toml` is the fleet boundary; provider, region and size are
operator metadata, not commands. One node per region is the intended topology.

Read [OPERATIONS.md](../../OPERATIONS.md) first. This adapter is proven locally;
fresh-host bootstrap, DNS-01 issuance, regional latency, load capacity and cloud
firewall behavior still require staging certification. Nothing in CI deploys.

## What a connection does

1. DNS selects a healthy regional Caddy. It forwards the original WebSocket
   upgrade (path, query and WebSocket headers) to its local relay's WireGuard
   address, port 4000.
2. If Syn reports an owner elsewhere, that relay returns `409` with its owner's
   `x-reroute-target: 10.77.0.N:4000`. Caddy intercepts the **upstream response**
   header and sends the unchanged upgrade to that relay over WireGuard. A header
   supplied by a client does not select an upstream.
3. The owner returns `101`. Encrypted frames flow as bytes through that Caddy
   and the owner relay, never through BEAM distribution. The proxy does not
   buffer the WebSocket stream or impose a read, write or stream idle timeout.

There is exactly one reroute hop, with no alternate owner or proxy retries. An
unreachable target fails the upgrade before `101`; a second `409` is returned
to the client. The client's normal reconnect re-resolves ownership. Syn's
partition/conflict behavior is unchanged: reconnects are not lossless migration.
The tunnel through Caddy on node B still depends on B even when A owns it.

The shared [Caddyfile template](Caddyfile.tmpl) implements this using Caddy's
[response interception](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#intercepting-responses).
Both proxy hops set `stream_close_delay 24h`: a config reload delays closing old
tunnels for 24 hours rather than closing immediately. **Never reload or restart
Caddy as routine relay operations.** A reload still schedules later closes;
this delay does not make it harmless. Certificate renewal does not reload the
config. A necessary proxy change is a separate scheduled reconnect event.

## Inventory and secrets

Use Python 3.11+, Bash, OpenSSH, Docker with Compose v2, and (for local probes)
the repo's Elixir/OTP and Node versions on the operator machine. The checked-in
inventory has documentation IPs and placeholder keys; replace them in an
operator copy. Do not bootstrap it as supplied.

Each `[[nodes]]` row has:

| Field | Meaning |
| --- | --- |
| `name` | Unique lowercase node name, also the rendered directory and key filename |
| `provider`, `region`, `size` | Human inventory; a VM can be on any provider |
| `public_ip` | Stable IPv4 address, WireGuard peer endpoint and root SSH destination |
| `wg_ip` | Unique host in `10.77.0.0/24`; routed as `/32`, no default route |
| `wg_pubkey` | Unique public key matching this node's private key |
| `memory_watermark_bytes` | Explicit BEAM watermark, selected from VM memory and staging evidence |

`[mesh]` sets the public `hostnames` list, `minimum_cluster_size`, fixed
`distribution_port` (9100 in the sample), and `ssh_cidr`. SSH is allowed only
from that administration network. The public firewall otherwise admits TCP
80/443 and UDP 51820 only. Use the same restrictions in any provider firewall.
WireGuard peers get access over `wg0`; distribution is encrypted by WireGuard,
not by Erlang TLS. EPMD binds to `ERL_EPMD_ADDRESS` (OTP also permits loopback),
and distribution binds with `inet_dist_use_interface` and one fixed port.
The relay HTTP listener also binds only to its WireGuard address.

The sample selects a 6 GiB watermark for an 8 GiB VM. That leaves room for the
OS, Caddy, WireGuard and transient allocations; it is a **staging starting
point**, not certification for 12k sockets. Application capacity stays at 20,000
WebSockets (100 acceptors × 200); no Fly placement ceiling applies here. The
existing 30s delivery and 35s transport send deadlines stay unchanged. Both
containers have a 100,000 file-descriptor limit; each Caddy tunnel uses a client
and an upstream socket.

Create private operator directories (the default paths below are gitignored):

```sh
umask 077
mkdir -p deployment/mesh/secrets
openssl rand -hex 48 > deployment/mesh/secrets/cookie
# Install wireguard-tools on the operator machine for wg genkey/pubkey.
for node in fra ewr nrt; do
  wg genkey > "deployment/mesh/secrets/$node.key"
  wg pubkey < "deployment/mesh/secrets/$node.key"
done
```

Put each printed **public** key in that node's inventory row. Keep private keys
and the shared cookie out of Git. Add `secrets/cloudflare_token`, containing
only a Cloudflare API token with Zone:Read and DNS:Edit restricted to the chosen
zone, as required by the [Caddy Cloudflare module](https://github.com/caddy-dns/cloudflare).
Begin with `hostnames = ["relay-next.paseo.sh"]`. Every node obtains certificates
for every listed name via DNS-01; the name need not point at the fleet yet.
The day before cutover use `hostnames = ["relay-next.paseo.sh", "relay.paseo.sh"]`
and install the rendered Caddyfiles deliberately on each node. Adding names is
scheduled proxy maintenance, separate from relay rollouts. Caddy's admin API is
disabled, so apply this pre-cutover config with one scheduled Caddy recreation
per node, verify both names and readiness, then proceed. No DNS proxy is needed.

Build and publish the two images to a registry your hosts can pull from:

```sh
docker build -t YOUR_REGISTRY/relay:RELEASE .
docker build -f deployment/mesh/Dockerfile.caddy -t YOUR_REGISTRY/caddy:RELEASE .
docker push YOUR_REGISTRY/relay:RELEASE
docker push YOUR_REGISTRY/caddy:RELEASE
```

Record the returned immutable registry digests. Stock Caddy lacks DNS-01
Cloudflare support; `Dockerfile.caddy` pins Caddy and the DNS module. A private
registry also needs Docker credentials on each host before pulling images.
For that path, install Docker Engine/Compose and run `docker login` as root on
each host before bootstrap; bootstrap's package installation is idempotent.
Public images need no registry login.

```sh
export RELAY_IMAGE=YOUR_REGISTRY/relay@sha256:THE_64_HEX_DIGEST
export CADDY_IMAGE=YOUR_REGISTRY/caddy@sha256:THE_64_HEX_DIGEST
deployment/mesh/render.sh --inventory /path/to/fleet.toml \
  --secrets deployment/mesh/secrets --output deployment/mesh/generated
```

Rendering validates the inventory and writes one directory per node:
`wg0.conf`, `relay.env`, `Caddyfile`, `caddy.env`, `compose.yml`, `.env`,
`firewall.sh`, `node.conf` and `rpc.sh`. Directories are mode 0700, files 0600. These
outputs contain secrets. They are transferred over SSH, never uploaded as CI
artifacts. Inventory changes are deliberate maintenance: rendering alone does
not update hosts or reload proxies.

## Fresh Ubuntu host to running fleet

Start with dedicated Ubuntu 24.04 VMs, root key-based SSH, stable public IPv4,
and no existing relay, `wg0` or firewall customization. Reserve the WireGuard
subnet and ensure neither 4000, 4369 nor 9100 is occupied. Keep provider console
access available while setting the firewall. Add the administration machine's
actual source CIDR (or its VPN egress) to the inventory before rendering.

Verify each host SSH fingerprint out of band and enroll it in `known_hosts`.
The scripts enforce strict host-key checking and never accept unknown keys.
Then bootstrap **one host at a time**:

```sh
deployment/mesh/bootstrap.sh /path/to/fleet.toml deployment/mesh/generated fra
deployment/mesh/bootstrap.sh /path/to/fleet.toml deployment/mesh/generated ewr
deployment/mesh/bootstrap.sh /path/to/fleet.toml deployment/mesh/generated nrt
```

Bootstrap installs Docker Engine/Compose from Docker's Ubuntu repository,
WireGuard and UFW, checks that the private key matches the inventory, installs
`/etc/wireguard/wg0.conf`, enables Docker and `wg-quick@wg0`, pulls the pinned
images, and starts Compose in `/opt/paseo-relay`. It checks local `/health`,
not `/ready`: the first node cannot meet a multi-node floor until its peers
exist. It refuses an already configured host; it is not an upgrade script.
Partial bootstrap failures require inspection over SSH/console before retrying.

After all hosts exist, on **every** host require:

```sh
ssh root@NODE_PUBLIC_IP 'wg show wg0 latest-handshakes'
ssh root@NODE_PUBLIC_IP 'curl --max-time 2 -fsS http://NODE_WG_IP:4000/ready'
ssh root@NODE_PUBLIC_IP 'curl --max-time 2 -fsS http://NODE_WG_IP:4000/metrics'
ssh root@NODE_PUBLIC_IP 'cd /opt/paseo-relay && bash rpc.sh '\''IO.inspect(Node.list())'\'''
ssh root@NODE_PUBLIC_IP 'ss -lntup'
curl --resolve HOSTNAME:443:NODE_PUBLIC_IP https://HOSTNAME/ready
```

Check every configured peer is connected and has a recent WireGuard handshake.
Confirm the relay/distribution sockets listen on WireGuard only (EPMD also
permits loopback), and public interfaces expose only the documented ports with
restricted SSH. Verify an external attempt to connect to 4000/4369/9100 fails.
Require valid TLS and `200` readiness on each forced node; `/health` alone is
insufficient. Review logs without dumping process state. Configure scraping of
`http://WG_IP:4000/metrics` over an administration WireGuard peer or SSH tunnel,
with alerts from OPERATIONS.md; Caddy also proxies `/metrics` publicly as the
current Fly endpoint does. No metrics stack is installed by bootstrap.

## Serial rollout and rollback

There is no push-triggered deploy. Keep the previous digest and a rollout
record; run green CI and staging gates before scheduling production work.
A future GitHub Actions caller must be manually approved, use a pinned
`known_hosts` and SSH key secret, and serialize fleet operations.

```sh
deployment/mesh/deploy.sh /path/to/fleet.toml "$NEW_RELAY_IMAGE_DIGEST"
# A scheduled rollout explicitly allowed to reconnect remaining active sockets:
deployment/mesh/deploy.sh /path/to/fleet.toml "$NEW_RELAY_IMAGE_DIGEST" --allow-disconnect
```

The script acquires a fleet lock on the first inventory host, checks every
node ready with the full inventory visible in Syn, and processes inventory order
serially. For each node it pulls the
new image before draining, saves `.env.previous`, calls `PaseoRelay.Drain.begin/0`
through release RPC, requires draining=1 and `/ready`=503, and waits 60 seconds
for health monitors and natural closes. Drain refuses **new ownership**;
existing owned sessions and reroutes still work. It does not evict owners or
migrate sockets. The normal command aborts and cancels drain if any sockets
remain. `--allow-disconnect` explicitly permits replacing those sockets after
the grace period; owners disconnect and clients reconnect normally.

Use `bash rpc.sh 'EXPRESSION'` from `/opt/paseo-relay` for release RPC.
The helper binds its temporary hidden node to WireGuard with an ephemeral
port, avoiding a collision with the running relay's fixed distribution port.

Only the relay service is recreated. Caddy, certificates and proxy tunnels to
other owners stay up. The script requires the replacement ready within two
minutes, then checks **every node** for readiness and full Syn membership twice, 15
seconds apart, before touching
the next node. Any failure stops the rollout; inspect metrics, logs and
reconnect/session convergence before resuming. These health checks do not
certify data flow or saturation. Run the cross-node probe and sustained load
checks on staging, and watch rejection/timeouts and reconnects in production.

An interrupted SSH run can leave a drain active or a lock directory behind.
Determine the actual running image and readiness before recovery. Cancel a
leftover drain from `/opt/paseo-relay` with `bash rpc.sh 'PaseoRelay.Drain.cancel()'`. Remove `/opt/paseo-relay/rollout.lock` on the first
host **only after confirming no rollout remains active**. A normal failure
removes the lock automatically. Keep failures as incidents, not retry loops.

Rollback uses the **same** serial command with the previously recorded image
digest. If an unhealthy node prevents fleet preflight, repair just that node:
read its `.env.previous`, begin drain, observe the same grace/active-socket
policy, and use `docker compose --env-file .env.previous up -d --no-deps relay`.
After readiness passes, restore the corresponding `RELAY_IMAGE` in `.env` and
check the whole fleet before another action. Do not blindly rerun a failed
fleet rollout or overwrite the original rollback record. A proxy/config or
WireGuard change needs its own reviewed maintenance operation; `deploy.sh`
updates only the relay image.

## Local CI proof

```sh
deployment/mesh/e2e/run.sh
scripts/ci.sh
```

The e2e starts three generic relays and three real Caddy containers on a plain
Docker network, with static peer names and a cluster floor of three. It renders
the **same proxy template** as production, substituting HTTP port 80 and omitting
TLS. It waits for every node ready, forces daemon control to A and client/data
to B by connecting to their distinct addresses, sends text client→daemon and
binary daemon→client, then requires B's reroute counter to increase by at least
two. It repeats C→A so every node participates in data flow. Unique probe IDs
avoid stale ownership; sockets close in reverse order and Compose cleans up on
exit. Defaults bind localhost ports 4410–4412; override `MESH_E2E_PORT_A/B/C`
when running concurrent independent checkouts.

This proves Caddy's one-hop upgrade replay, static clustering and unchanged
WebSocket data path. It does not prove WireGuard, real TLS, VM sizing, the
Cloudflare control plane or cloud SSH/firewall provisioning. CI also builds the
Cloudflare-enabled Caddy image and preserves all existing Fly checks.

## Staging gates and production cutover (later phases)

Do not execute this section during local Phase 1.

1. Bring up three staging VMs under `relay-next.paseo.sh`. Verify per-node TLS,
   full-mesh reachability, bounded distribution listeners and all peer/readiness
   checks above. Force owner/landing to different node addresses using the
   probe against their public IPs using `--hostname` to preserve the shared TLS
   name. Tunnel the landing relay's metrics port privately, for example:

   ```sh
   ssh -N -L 14400:NODE_B_WG_IP:4000 root@NODE_B_PUBLIC_IP
   # In a second terminal (valid public TLS certificates required):
   MIX_ENV=test mix run --no-start deployment/mesh/e2e/probe.exs \
     --owner wss://NODE_A_PUBLIC_IP --landing wss://NODE_B_PUBLIC_IP \
     --hostname relay-next.paseo.sh --metrics http://127.0.0.1:14400
   ```

   Require reroute metrics and both frame directions.
   Also run Paseo's `live-relay.real.e2e.test.ts` against staging.
2. Run sustained `scripts/relay-load.mjs` at 12k sockets on the chosen VM size,
   with zero rejections, bounded RSS and no frame/order/send/cleanup failures.
   Record duration, both proxy-hop traffic, VM/Caddy/BEAM memory and CPU, and
   bandwidth. Certify the existing Capacity epoch timeout/recovery behavior on
   this topology before production; Fly's destructive gate is not mesh evidence.
   Test one serial rollout and one rollback with existing sockets, and measure
   reconnect convergence. Do not claim staging certification from local CI.
3. Provision the remaining regions (intended fleet: fra, ewr, sjc, sgp, nrt),
   update the full mesh deliberately, and repeat readiness/data checks. Allow
   capacity and bandwidth headroom, including both endpoints of inter-node
   proxy traffic. Do not add nodes just to purchase bandwidth.
4. **Day before cutover:** only after the fleet has passed Phase 2 under
   `relay-next.paseo.sh`, add `relay.paseo.sh` as a second hostname to every
   node's inventory/Caddy configuration and the Cloudflare Load Balancer.
   DNS-01 permits issuing its certificate before DNS points at the fleet.
   Apply this deliberate proxy maintenance serially, separate from relay
   updates. Verify each node before proceeding:

   ```sh
   curl --resolve relay.paseo.sh:443:NODE_PUBLIC_IP https://relay.paseo.sh/ready
   curl --resolve relay-next.paseo.sh:443:NODE_PUBLIC_IP https://relay-next.paseo.sh/ready
   ```

   Use DNS-only Load Balancer routing with region/country geo steering and
   HTTPS `/ready` monitors. Verify current entitlement, pricing and health-monitor
   withdrawal latency; it must fit the rollout grace. Keep both names active.
   Lower `relay.paseo.sh` TTL from today's 300 seconds to 60 seconds. Its current
   CNAME target is `paseo-relay-next.fly.dev`; leave that target in place today.
5. **Cutover, quiet hour:** swap the `relay.paseo.sh` CNAME from
   `paseo-relay-next.fly.dev` to the Cloudflare Load Balancer's DNS-only record.
   Confirm authoritative and regional resolver answers, per-node readiness and
   new fleet connections. Existing sessions remain on Fly until disconnected.
   Saved pairings retain the same hostname; no client release is needed. The
   fleets have separate ownership registries: a newly reconnecting client on
   the mesh cannot reach a daemon still on Fly until that daemon reconnects.
   The forced reconnect below completes the transition.
6. **Force reconnect off Fly:** after TTL plus margin (10 minutes), stop **all**
   Fly Machines, without destroying them. Execute stops one at a time, verify
   fleet health after each, and continue until every Fly Machine is stopped.
   This is the scheduled reconnect event: every remaining Fly session drops;
   daemons and apps reconnect, re-resolve DNS and land on the mesh. Daemon backoff
   is `min(30 s, 1 s × attempt)`, so allow for that convergence. Watch fleet
   active-WebSocket counts climb toward the current approximately 38k total,
   alongside ownership convergence, reroutes, frames, rejections, timeouts,
   resource pressure, and Fly egress reaching zero. Follow the Fly operations
   cookbook for the actual interventions; do not stack unrelated maintenance.
7. **Rollback:** repoint the `relay.paseo.sh` CNAME to
   `paseo-relay-next.fly.dev`, start the preserved Fly Machines serially and
   require readiness, then stop the fleet's **relay containers** to cut its
   existing sessions. Clients reconnect and re-resolve back to Fly. Allow DNS
   propagation (TTL plus margin) before cutting mesh sessions so cached fleet
   answers do not strand them. Verify traffic and reconnect convergence on Fly.
   Rehearse this exact reverse mechanism once on `relay-next.paseo.sh` during
   Phase 2. Keep Caddy and fleet configuration available for diagnosis/recovery.
8. **Teardown:** after two quiet weeks, with separately authorized deletion,
   delete the stopped Fly app and remove `deployment/fly/` and its replay
   entrypoint/checks. Until then retain the Fly fleet, configuration and secrets
   for rollback. Remove `relay-next.paseo.sh` or retain it as the staging name.

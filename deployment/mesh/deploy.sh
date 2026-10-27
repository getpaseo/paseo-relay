#!/usr/bin/env bash
set -euo pipefail
if [[ $# -lt 2 || $# -gt 3 || (${3:-} != '' && ${3:-} != --allow-disconnect) ]]; then
  echo "usage: $0 INVENTORY RELAY_IMAGE_DIGEST [--allow-disconnect]" >&2; exit 2
fi
root=$(cd "$(dirname "$0")" && pwd)
image=$2
[[ $image =~ ^[a-zA-Z0-9._:/-]+@sha256:[a-f0-9]{64}$ ]] || { echo 'use an immutable relay image digest' >&2; exit 2; }
inventory_rows=$("$root/render.sh" --inventory "$1" --list)
mapfile -t nodes <<<"$inventory_rows"
[[ ${#nodes[@]} -gt 1 ]] || { echo 'rolling deploy needs at least two nodes' >&2; exit 2; }
ssh_options=(-o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10)
read -r _ coordinator _ <<<"${nodes[0]}"
# One fleet-wide lock, shared by invocations from different operator hosts/CI jobs.
ssh "${ssh_options[@]}" "root@$coordinator" 'mkdir /opt/paseo-relay/rollout.lock'
interrupted=false
trap 'interrupted=true; exit 130' INT TERM HUP
unlock() {
  if "$interrupted"; then
    echo 'interrupted rollout: fleet lock retained; inspect remote state before clearing it' >&2
    return
  fi
  ssh "${ssh_options[@]}" "root@$coordinator" 'rmdir /opt/paseo-relay/rollout.lock' || true
}
trap unlock EXIT
fleet_ready() {
  for row in "${nodes[@]}"; do
    read -r name address wg_ip <<<"$row"
    ssh "${ssh_options[@]}" "root@$address" \
      "curl --max-time 2 -fsS http://$wg_ip:4000/ready >/dev/null"
    ssh "${ssh_options[@]}" "root@$address" \
      "timeout 10s bash /opt/paseo-relay/rpc.sh 'unless PaseoRelay.Ownership.ready?(${#nodes[@]}), do: raise(\"cluster incomplete\")'"
  done
}
fleet_ready
for row in "${nodes[@]}"; do
  read -r name address wg_ip <<<"$row"
  echo "Rolling $name ($address)"
  ssh "${ssh_options[@]}" "root@$address" bash -se -- "$image" "$wg_ip" "${3:-require-empty}" <<'REMOTE'
set -euo pipefail
cd /opt/paseo-relay
image=$1
wg_ip=$2
allow_disconnect=$3
# Pull while the old relay is still ready. Preserve rollback configuration.
docker pull "$image"
cp .env .env.previous
sed "s|^RELAY_IMAGE=.*|RELAY_IMAGE=$image|" .env > .env.next
chmod 600 .env.next .env.previous
cancel_drain() { bash rpc.sh 'PaseoRelay.Drain.cancel()' || true; }
trap cancel_drain EXIT
bash rpc.sh 'PaseoRelay.Drain.begin()'
metrics=$(curl --max-time 2 -fsS "http://$wg_ip:4000/metrics")
grep -qx 'paseo_relay_draining 1' <<<"$metrics"
[[ $(curl --max-time 2 -s -o /dev/null -w '%{http_code}' "http://$wg_ip:4000/ready") == 503 ]]
# Readiness removes new DNS placements; existing sessions do not migrate.
# Give health monitors and natural socket closes 60 seconds before replacement.
for _ in $(seq 1 60); do
  sleep 1
  metrics=$(curl --max-time 2 -fsS "http://$wg_ip:4000/metrics")
done
if ! grep -qx 'paseo_relay_active_websockets 0' <<<"$metrics"; then
  [[ $allow_disconnect == --allow-disconnect ]] || {
    echo 'active sockets remain; cancelling drain. Schedule an explicitly approved disconnect rollout.' >&2
    exit 1
  }
fi
# Caddy is deliberately untouched: its tunnels to OTHER owners must survive.
docker compose --env-file .env.next up -d --no-deps relay
mv .env.next .env
for _ in $(seq 1 120); do
  if curl --max-time 2 -fsS "http://$wg_ip:4000/ready" >/dev/null 2>&1; then exit 0; fi
  sleep 1
done
echo 'replacement unready; rollout halted. Previous digest is in .env.previous.' >&2
exit 1
REMOTE
  # Two fleet-wide observations before the next intervention.
  fleet_ready
  sleep 15
  fleet_ready
done

#!/usr/bin/env bash
# Exercise production's private bind and RPC port behavior without host networking.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
export COMPOSE_PROJECT_NAME="relay-mesh-bind-$$"
export MESH_RELAY_IMAGE="${MESH_RELAY_IMAGE:?}"
work=$(mktemp -d)
cleanup() { (cd "$work" && docker compose down --volumes >/dev/null); rm -rf "$work"; }
trap cleanup EXIT
cp "$root/rpc.sh" "$work/rpc.sh"
printf 'WG_IP=127.0.0.2\n' > "$work/node.conf"
cat > "$work/compose.yml" <<'COMPOSE'
services:
  relay:
    image: ${MESH_RELAY_IMAGE}
    network_mode: none
    environment:
      ERL_FLAGS: +S 2
      ELIXIR_ERL_OPTIONS: +fnu
      RELEASE_NODE: paseo_relay@127.0.0.2
      RELEASE_DISTRIBUTION: name
      RELEASE_COOKIE: local-binding-probe-only
      ERL_EPMD_ADDRESS: 127.0.0.2
      ERL_AFLAGS: "-kernel inet_dist_use_interface '{127,0,0,2}' inet_dist_listen_min 9100 inet_dist_listen_max 9100"
      PASEO_RELAY_HOST: 127.0.0.2
COMPOSE
cd "$work"
docker compose up -d
ready=false
for _ in $(seq 1 40); do
  if bash rpc.sh 'unless Application.get_env(:kernel, :inet_dist_use_interface) == {127,0,0,2}, do: raise("wrong interface"); unless PaseoRelay.Listener.port(PaseoRelay.Listener) == 4000, do: raise("listener not started")' >/dev/null 2>&1; then ready=true; break; fi
  sleep 0.25
done
"$ready" || { docker compose logs --tail 30 >&2; exit 1; }
# deploy.sh streams its remote shell over SSH stdin. RPC must leave the next
# shell command there, rather than forwarding it into the container.
bash -se <<'REMOTE'
bash rpc.sh 'PaseoRelay.Drain.cancel()'
touch rpc-returned
REMOTE
[[ -f rpc-returned ]] || { echo 'RPC consumed the remote deployment script' >&2; exit 1; }
bash rpc.sh 'PaseoRelay.Drain.begin(); unless PaseoRelay.Drain.draining?(), do: raise("drain failed"); PaseoRelay.Drain.cancel()'
docker compose exec -T relay cat /proc/net/tcp > sockets
python3 - <<'PY'
from pathlib import Path
listeners = {(line.split()[1].split(":")[0], int(line.split()[1].split(":")[1], 16))
             for line in Path("sockets").read_text().splitlines()[1:] if line.split()[3] == "0A"}
assert listeners == {("0200007F", 4000), ("0200007F", 9100), ("0200007F", 4369), ("0100007F", 4369)}, listeners
print("private relay, EPMD and fixed distribution listeners verified; release RPC and drain passed")
PY

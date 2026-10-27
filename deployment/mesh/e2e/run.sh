#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
export MESH_RELAY_IMAGE="${MESH_RELAY_IMAGE:-paseo-relay-mesh-e2e:local}"
if [[ ${1:-} != --skip-build ]]; then
  docker build -t "$MESH_RELAY_IMAGE" .
fi
export MESH_E2E_CONFIG_DIR
MESH_E2E_CONFIG_DIR=$(mktemp -d)
compose=(docker compose -p "relay-mesh-e2e-$$" -f deployment/mesh/e2e/compose.yml)
cleanup() {
  status=$?
  if [[ $status != 0 ]]; then "${compose[@]}" logs --tail 80 >&2; fi
  "${compose[@]}" down --volumes --remove-orphans >/dev/null
  rm -rf "$MESH_E2E_CONFIG_DIR"
}
trap cleanup EXIT
for node in a b c; do
  sed -e 's/@SITES@/:80/' -e 's/@TLS@//' -e "s/@LOCAL@/relay-$node:4000/" \
    deployment/mesh/Caddyfile.tmpl > "$MESH_E2E_CONFIG_DIR/$node.Caddyfile"
done
"${compose[@]}" up -d
for port in "${MESH_E2E_PORT_A:-4410}" "${MESH_E2E_PORT_B:-4411}" "${MESH_E2E_PORT_C:-4412}"; do
  ready=false
  for _ in $(seq 1 120); do
    if curl --max-time 2 -fsS "http://127.0.0.1:$port/ready" >/dev/null 2>&1; then ready=true; break; fi
    sleep 0.5
  done
  "$ready" || { echo "mesh node on $port not ready" >&2; exit 1; }
done
# The node addresses force placement; no provider placement header is involved.
MIX_ENV=test mix run --no-start deployment/mesh/e2e/probe.exs --owner "ws://127.0.0.1:${MESH_E2E_PORT_A:-4410}" \
  --landing "ws://127.0.0.1:${MESH_E2E_PORT_B:-4411}" --metrics "http://127.0.0.1:${MESH_E2E_PORT_B:-4411}"
MIX_ENV=test mix run --no-start deployment/mesh/e2e/probe.exs --owner "ws://127.0.0.1:${MESH_E2E_PORT_C:-4412}" \
  --landing "ws://127.0.0.1:${MESH_E2E_PORT_A:-4410}" --metrics "http://127.0.0.1:${MESH_E2E_PORT_A:-4410}"

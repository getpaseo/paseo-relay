#!/usr/bin/env bash
# A release RPC starts a temporary hidden BEAM node. It cannot reuse the running
# relay's fixed distribution port; keep the private bind, use an ephemeral port.
set -euo pipefail
cd "$(dirname "$0")"
source node.conf
exec docker compose exec -T \
  -e "ERL_AFLAGS=-kernel inet_dist_use_interface '{${WG_IP//./,}}'" \
  relay /app/bin/paseo_relay rpc "$@"

#!/usr/bin/env bash
# Deliberately one host per invocation. Bootstrap is not a rollout command.
set -euo pipefail
if [[ $# != 3 ]]; then echo "usage: $0 INVENTORY RENDERED_DIRECTORY NODE_NAME" >&2; exit 2; fi
root=$(cd "$(dirname "$0")" && pwd)
inventory=$1
rendered=$2
node=$3
address=$("$root/render.sh" --inventory "$inventory" --list | awk -v name="$node" '$1 == name {print $2}')
[[ -n $address && -d "$rendered/$node" ]] || { echo 'unknown or unrendered node' >&2; exit 1; }
# SSH host keys must already be enrolled and verified by the operator.
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes "root@$address" 'test ! -e /etc/wireguard/wg0.conf && test ! -e /opt/paseo-relay/compose.yml && install -d -m 700 /opt/paseo-relay'
scp -q -o BatchMode=yes -o StrictHostKeyChecking=yes "$rendered/$node/"{compose.yml,Caddyfile,relay.env,caddy.env,.env,wg0.conf,firewall.sh,node.conf,rpc.sh} "root@$address:/opt/paseo-relay/"
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes "root@$address" 'bash -se' <<'REMOTE'
set -euo pipefail
cd /opt/paseo-relay
source node.conf
# Prevent accidental bootstrap over an existing relay or WireGuard installation.
[[ ! -e /etc/wireguard/wg0.conf ]] || { echo 'wg0 already configured; use the maintenance runbook' >&2; exit 1; }
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y ca-certificates curl wireguard ufw
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
source /etc/os-release
printf 'deb [arch=%s signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu %s stable\n' "$(dpkg --print-architecture)" "$VERSION_CODENAME" > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
actual_key=$(sed -n 's/^PrivateKey = //p' wg0.conf | wg pubkey)
[[ $actual_key == "$WG_PUBKEY" ]] || { echo 'WireGuard private key does not match inventory' >&2; exit 1; }
bash firewall.sh
install -m 600 wg0.conf /etc/wireguard/wg0.conf
systemctl enable --now docker wg-quick@wg0
# Caddy stays up during relay updates. Containers use host networking: no Docker NAT bypass of UFW.
docker compose pull
docker compose up -d
curl --retry 20 --retry-connrefused --retry-all-errors --retry-delay 1 --max-time 2 -fsS "http://$WG_IP:4000/health"
REMOTE
echo "Bootstrapped $node. Finish every host, then require every /ready before serving traffic."

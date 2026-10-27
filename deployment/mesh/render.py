"""Inventory boundary and deterministic per-host configuration. Python 3.11+."""
import argparse
import base64
import ipaddress
import os
from pathlib import Path
import re
import shutil
import tomllib

ROOT = Path(__file__).resolve().parent


def require(condition, message):
    if not condition:
        raise ValueError(message)


def key(value):
    require(len(base64.b64decode(value, validate=True)) == 32, "WireGuard keys must be 32-byte base64")
    return value


def inventory(path):
    data = tomllib.loads(Path(path).read_text())
    mesh, nodes = data["mesh"], data["nodes"]
    hostnames = mesh["hostnames"]
    require(isinstance(hostnames, list) and hostnames and len(set(hostnames)) == len(hostnames), "hostnames must be a nonempty unique list")
    require(all(re.fullmatch(r"[a-z0-9][a-z0-9.-]*\.[a-z]{2,}", name) for name in hostnames), "invalid hostname")
    require(1 <= mesh["minimum_cluster_size"] <= len(nodes), "invalid cluster floor")
    require(1024 <= mesh["distribution_port"] <= 65535 and mesh["distribution_port"] not in (4000, 4369, 51820), "invalid distribution port")
    admin = ipaddress.IPv4Network(mesh["ssh_cidr"])
    require(admin.prefixlen > 0, "SSH must be restricted to an administration network")
    for field in ("name", "public_ip", "wg_ip", "wg_pubkey"):
        require(len({n[field] for n in nodes}) == len(nodes), f"duplicate {field}")
    for node in nodes:
        require(re.fullmatch(r"[a-z][a-z0-9-]{0,31}", node["name"]), "invalid node name")
        public, private = ipaddress.IPv4Address(node["public_ip"]), ipaddress.IPv4Address(node["wg_ip"])
        require(public != private and private in ipaddress.IPv4Network("10.77.0.0/24"), "invalid node addresses")
        require(private.packed[-1] not in (0, 255), "invalid WireGuard host")
        key(node["wg_pubkey"])
        require(268435456 <= node["memory_watermark_bytes"] <= 68719476736, "invalid memory watermark")
        for field in ("provider", "region", "size"):
            require(isinstance(node[field], str) and node[field], f"missing {field}")
    return mesh, nodes


def secret(path):
    value = path.read_text().strip()
    require(re.fullmatch(r"[A-Za-z0-9_+/=.-]+", value), f"invalid secret format in {path.name}")
    return value


def write(path, text):
    path.write_text(text)
    path.chmod(0o600)


def render(mesh, nodes, secrets, output):
    images = {}
    for name in ("RELAY_IMAGE", "CADDY_IMAGE"):
        value = os.environ.get(name, "")
        require(re.fullmatch(r"[A-Za-z0-9._:/-]+@sha256:[a-f0-9]{64}", value), f"{name} must be an immutable registry digest")
        images[name] = value
    cookie = secret(secrets / "cookie")
    require(len(cookie) >= 32, "cookie must contain at least 32 characters")
    token = secret(secrets / "cloudflare_token")
    for node in nodes:
        directory = output / node["name"]
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory.chmod(0o700)
        private_key = key(secret(secrets / f'{node["name"]}.key'))
        wg = f'[Interface]\nAddress = {node["wg_ip"]}/32\nListenPort = 51820\nPrivateKey = {private_key}\n'
        for peer in nodes:
            if peer != node:
                wg += f'\n[Peer]\nPublicKey = {peer["wg_pubkey"]}\nAllowedIPs = {peer["wg_ip"]}/32\nEndpoint = {peer["public_ip"]}:51820\nPersistentKeepalive = 25\n'
        write(directory / "wg0.conf", wg)
        ip = node["wg_ip"]
        dist = mesh["distribution_port"]
        flags = f"-kernel inet_dist_use_interface '{{{ip.replace('.', ',')}}}' inet_dist_listen_min {dist} inet_dist_listen_max {dist}"
        env = {
            "RELEASE_NODE": f"paseo_relay@{ip}", "RELEASE_DISTRIBUTION": "name",
            "RELEASE_COOKIE": cookie, "ERL_EPMD_ADDRESS": ip, "ERL_AFLAGS": f'"{flags}"',
            "ELIXIR_ERL_OPTIONS": "+fnu", "PASEO_RELAY_HOST": ip, "PASEO_RELAY_PORT": "4000",
            "PASEO_RELAY_OWNERSHIP_TARGET": f"{ip}:4000", "PASEO_RELAY_REROUTE_HEADER": "x-reroute-target",
            "PASEO_RELAY_CLUSTER_NODES": ",".join(f'paseo_relay@{p["wg_ip"]}' for p in nodes if p != node),
            "PASEO_RELAY_MIN_CLUSTER_SIZE": mesh["minimum_cluster_size"],
            "PASEO_RELAY_MEMORY_WATERMARK_BYTES": node["memory_watermark_bytes"],
        }
        write(directory / "relay.env", "".join(f"{k}={v}\n" for k, v in env.items()))
        write(directory / "caddy.env", f"CF_API_TOKEN={token}\n")
        write(directory / ".env", "".join(f"{k}={v}\n" for k, v in images.items()))
        caddy = (ROOT / "Caddyfile.tmpl").read_text().replace("@SITES@", ", ".join(mesh["hostnames"])).replace("@LOCAL@", f"{ip}:4000").replace("@TLS@", "\ttls {\n\t\tdns cloudflare {env.CF_API_TOKEN}\n\t}")
        write(directory / "Caddyfile", caddy)
        write(directory / "rpc.sh", (ROOT / "rpc.sh").read_text())
        shutil.copyfile(ROOT / "compose.yml", directory / "compose.yml")
        (directory / "compose.yml").chmod(0o600)
        # Public inbound surface is web and WireGuard; SSH has a restricted source.
        firewall = f'''#!/usr/bin/env bash
set -euo pipefail
ufw default deny incoming
ufw default allow outgoing
ufw allow from {mesh["ssh_cidr"]} to any port 22 proto tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw allow 51820/udp
ufw allow in on wg0
ufw --force enable
'''
        write(directory / "firewall.sh", firewall)
        write(directory / "node.conf", f'WG_IP={ip}\nWG_PUBKEY={node["wg_pubkey"]}\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", default=str(ROOT / "inventory.toml"))
    parser.add_argument("--list", action="store_true", help="validated name/public-IP/WireGuard-IP TSV, no secrets needed")
    parser.add_argument("--secrets", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    mesh, nodes = inventory(args.inventory)
    if args.list:
        for node in nodes:
            print(node["name"], node["public_ip"], node["wg_ip"], sep="\t")
    else:
        require(args.secrets and args.output, "--secrets and --output are required")
        os.umask(0o077)
        render(mesh, nodes, args.secrets, args.output)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError) as error:
        raise SystemExit(str(error)) from None

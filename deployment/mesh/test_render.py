"""Run with python3 deployment/mesh/test_render.py. No external services or secrets."""
import base64
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent


class RenderTest(unittest.TestCase):
    def test_inventory_renders_full_mesh_and_private_release_boundary(self):
        with tempfile.TemporaryDirectory() as temporary:
            temporary = Path(temporary)
            secrets = temporary / "secrets"
            secrets.mkdir()
            (secrets / "cookie").write_text("c" * 48)
            (secrets / "cloudflare_token").write_text("test-token")
            for node in ("fra", "ewr", "nrt"):
                (secrets / f"{node}.key").write_text(base64.b64encode(bytes(range(32))).decode())
            environment = {**os.environ, "RELAY_IMAGE": "registry.example/relay@sha256:" + "a" * 64,
                           "CADDY_IMAGE": "registry.example/caddy@sha256:" + "b" * 64}
            inventory = temporary / "inventory.toml"
            inventory.write_text((ROOT / "inventory.toml").read_text().replace(
                'hostnames = ["relay-next.example.com"]',
                'hostnames = ["relay-next.example.com", "relay.example.com"]'))
            output = temporary / "generated"
            result = subprocess.run([str(ROOT / "render.sh"), "--inventory", str(inventory), "--secrets", str(secrets),
                                     "--output", str(output)], env=environment, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("test-token", result.stdout + result.stderr)
            for index, name in enumerate(("fra", "ewr", "nrt"), start=1):
                node = output / name
                wg = (node / "wg0.conf").read_text()
                self.assertEqual(wg.count("[Peer]"), 2)
                self.assertNotIn("AllowedIPs = 0.0.0.0/0", wg)
                env = (node / "relay.env").read_text()
                ip = f"10.77.0.{index}"
                self.assertIn(f"ERL_EPMD_ADDRESS={ip}\n", env)
                self.assertIn("inet_dist_use_interface '{" + ip.replace(".", ",") + "}'", env)
                self.assertIn("inet_dist_listen_min 9100 inet_dist_listen_max 9100", env)
                self.assertIn(f"PASEO_RELAY_HOST={ip}\n", env)
                self.assertIn(f"PASEO_RELAY_OWNERSHIP_TARGET={ip}:4000\n", env)
                self.assertIn("PASEO_RELAY_REROUTE_HEADER=x-reroute-target\n", env)
                peers = next(line for line in env.splitlines() if line.startswith("PASEO_RELAY_CLUSTER_NODES="))
                self.assertEqual(peers.count("paseo_relay@"), 2)
                self.assertNotIn("@" + ip, peers)
                self.assertNotIn("FLY_", env)
                self.assertEqual(node.stat().st_mode & 0o777, 0o700)
                self.assertEqual((node / "relay.env").stat().st_mode & 0o777, 0o600)
                caddy = (node / "Caddyfile").read_text()
                self.assertIn("dns cloudflare {env.CF_API_TOKEN}", caddy)
                self.assertIn("relay-next.example.com, relay.example.com {", caddy)
                firewall = (node / "firewall.sh").read_text()
                self.assertNotIn("ufw allow 22", firewall)
                self.assertIn("ufw allow from 198.51.100.10/32 to any port 22 proto tcp", firewall)
            environment["RELAY_IMAGE"] = "relay:latest"
            bad = subprocess.run([str(ROOT / "render.sh"), "--inventory", str(inventory), "--secrets", str(secrets), "--output", str(output)],
                                 env=environment, capture_output=True, text=True)
            self.assertNotEqual(bad.returncode, 0)
            self.assertIn("immutable registry digest", bad.stderr)

    def test_duplicate_node_address_is_rejected_before_rendering(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "inventory.toml"
            path.write_text((ROOT / "inventory.toml").read_text().replace('wg_ip = "10.77.0.2"', 'wg_ip = "10.77.0.1"'))
            result = subprocess.run([str(ROOT / "render.sh"), "--inventory", str(path), "--list"], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("duplicate wg_ip", result.stderr)


if __name__ == "__main__":
    unittest.main()

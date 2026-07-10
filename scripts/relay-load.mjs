#!/usr/bin/env node
/**
 * Black-box WebSocket load client for Paseo Relay v2.
 * It intentionally knows only the public endpoint query contract: serverId,
 * role, connectionId, and v=2. It does not import relay implementation code.
 */
import process from "node:process";
import { spawnSync } from "node:child_process";

const usage = `Usage: node scripts/relay-load.mjs [options]

  --endpoints <ws-url,...>  Relay /ws endpoint(s); two values exercise two nodes.
  --server-id <id>          v2 serverId (default: load-server)
  --connections <n>         Paired client/server data sockets (default: 10)
  --scenario <name>         idle, sustained, burst, or reconnect (default: idle)
  --duration <seconds>      Measurement duration (default: 10)
  --rate <messages/s>       Bidirectional sustained rate (default: 10)
  --burst <n>               Bidirectional messages sent once after connection
  --reconnects <n>          Reconnection waves for reconnect scenario (default: 3)
  --payload-bytes <n>       Frame size target (default: 128)
  --relay-pid <pid>         Sample this relay process's RSS and CPU with ps
  --json                    Print one machine-readable JSON result (default)

The client opens v2 server-data and client sockets sharing each connectionId.
Use real relay nodes; it never mocks or embeds a relay implementation.
`;

function args(argv) {
  const value = (name, fallback) => {
    const index = argv.indexOf(name);
    return index === -1 ? fallback : argv[index + 1];
  };
  if (argv.includes("--help") || argv.includes("-h")) return { help: true };
  const number = (name, fallback) => {
    const parsed = Number(value(name, fallback));
    if (!Number.isFinite(parsed) || parsed < 0) throw new Error(`${name} must be a non-negative number`);
    return parsed;
  };
  const endpoints = String(value("--endpoints", "ws://127.0.0.1:4000/ws"))
    .split(",").map((endpoint) => endpoint.trim()).filter(Boolean);
  if (!endpoints.length) throw new Error("--endpoints needs at least one WebSocket URL");
  const scenario = value("--scenario", "idle");
  if (!new Set(["idle", "sustained", "burst", "reconnect"]).has(scenario)) {
    throw new Error("--scenario must be idle, sustained, burst, or reconnect");
  }
  return {
    endpoints, serverId: value("--server-id", "load-server"), scenario,
    connections: number("--connections", 10), durationMs: number("--duration", 10) * 1000,
    rate: number("--rate", 10), burst: number("--burst", 0), reconnects: number("--reconnects", 3),
    payloadBytes: Math.max(32, number("--payload-bytes", 128)), relayPid: value("--relay-pid", null),
  };
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const percentile = (values, p) => {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)];
};
const now = () => performance.now();

function endpoint(url, query) {
  const parsed = new URL(url);
  for (const [key, value] of Object.entries(query)) parsed.searchParams.set(key, value);
  return parsed.toString();
}

function open(url, stats) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    let opened = false;
    let failed = false;
    const fail = () => {
      if (!failed) stats.connection_failures += 1;
      failed = true;
    };
    const timeout = setTimeout(() => reject(new Error("WebSocket open timed out")), 10_000);
    socket.addEventListener("open", () => { opened = true; clearTimeout(timeout); stats.connection_successes += 1; resolve(socket); }, { once: true });
    socket.addEventListener("error", () => { fail(); if (!opened) { clearTimeout(timeout); reject(new Error("WebSocket connection failed")); } }, { once: true });
    socket.addEventListener("close", (event) => { if (event.code !== 1000) fail(); });
  });
}

async function connectPair(options, index, stats, latencies) {
  const connectionId = `load-${process.pid}-${index}`;
  const serverEndpoint = options.endpoints[0];
  const clientIndex = Number(String(index).split("-").at(-1));
  const clientEndpoint = options.endpoints[(clientIndex + 1) % options.endpoints.length];
  const shared = { serverId: options.serverId, connectionId, v: "2" };
  const [server, client] = await Promise.all([
    open(endpoint(serverEndpoint, { ...shared, role: "server" }), stats),
    open(endpoint(clientEndpoint, { ...shared, role: "client" }), stats),
  ]);
  for (const socket of [server, client]) {
    socket.addEventListener("message", (event) => {
      stats.frames_received += 1;
      stats.bytes_received += Buffer.byteLength(String(event.data));
      const timestamp = Number(String(event.data).split(":", 1)[0]);
      if (Number.isFinite(timestamp)) latencies.push(Date.now() - timestamp);
    });
  }
  return { server, client };
}

function send(socket, payload, stats) {
  if (socket.readyState !== WebSocket.OPEN) { stats.send_failures += 1; return; }
  socket.send(payload);
  stats.frames_sent += 1;
  stats.bytes_sent += Buffer.byteLength(payload);
}

function sampleRelay(pid) {
  if (!pid) return null;
  const result = spawnSync("ps", ["-o", "rss=,%cpu=", "-p", pid]);
  if (result.status !== 0) return null;
  const [rssKb, cpu] = result.stdout.toString().trim().split(/\s+/).map(Number);
  return { rss_bytes: rssKb * 1024, cpu_percent: cpu };
}

async function main() {
  let options;
  try { options = args(process.argv.slice(2)); } catch (error) { console.error(error.message); process.exitCode = 2; return; }
  if (options.help) { process.stdout.write(usage); return; }
  const stats = { connection_successes: 0, connection_failures: 0, send_failures: 0, frames_sent: 0, frames_received: 0, bytes_sent: 0, bytes_received: 0 };
  const latencies = [];
  const started = now();
  let pairs = [];
  let control;
  try {
    control = await open(endpoint(options.endpoints[0], { serverId: options.serverId, role: "server", v: "2" }), stats);
    const waves = options.scenario === "reconnect" ? options.reconnects + 1 : 1;
    for (let wave = 0; wave < waves; wave += 1) {
      pairs = await Promise.all(Array.from({ length: options.connections }, (_, index) => connectPair(options, `${wave}-${index}`, stats, latencies)));
      if (wave < waves - 1) { pairs.flatMap(Object.values).forEach((socket) => socket.close(1000, "load reconnect")); await sleep(100); }
    }
    const payload = (direction) => `${Date.now()}:${direction}:${"x".repeat(options.payloadBytes)}`;
    const publish = () => pairs.forEach(({ server, client }) => { send(client, payload("client"), stats); send(server, payload("server"), stats); });
    if (options.scenario === "burst" || options.burst) for (let i = 0; i < Math.max(1, options.burst); i += 1) publish();
    if (options.scenario === "sustained") {
      const period = Math.max(1, Math.floor(1000 / Math.max(1, options.rate)));
      const timer = setInterval(publish, period);
      await sleep(options.durationMs);
      clearInterval(timer);
    } else {
      await sleep(options.durationMs);
    }
  } catch (error) {
    stats.connection_failures += 1;
    stats.error = error.message;
  } finally {
    control?.close(1000, "load complete");
    pairs.flatMap(Object.values).forEach((socket) => socket.close(1000, "load complete"));
    await sleep(50);
  }
  const durationMs = now() - started;
  const resource = process.resourceUsage();
  const relay = sampleRelay(options.relayPid);
  process.stdout.write(`${JSON.stringify({
    protocol: { version: 2, roles: ["server-data", "client"] }, scenario: options.scenario,
    endpoints: options.endpoints, requested_connections: options.connections, duration_ms: Math.round(durationMs),
    ...stats, throughput_frames_per_second: Number((stats.frames_received / (durationMs / 1000)).toFixed(2)),
    latency_ms: { p50: percentile(latencies, 0.5), p95: percentile(latencies, 0.95), p99: percentile(latencies, 0.99) },
    client: { rss_bytes: process.memoryUsage().rss, cpu_microseconds: resource.userCPUTime + resource.systemCPUTime }, relay,
  })}\n`);
  if (stats.connection_failures || stats.send_failures) process.exitCode = 1;
}

main();

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
  --pairs <n>               Paired client/server data sockets (default: 10)
  --connections <n>         Backward-compatible alias for --pairs
  --connection-prefix <id>  Unique connection ID prefix for sharded runs
  --no-control              Do not open the shared v2 daemon control socket
  --batch-size <n>          Maximum pairs opened concurrently (default: 100)
  --ramp-ms <milliseconds>  Delay between opening batches (default: 0)
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
  const integer = (name, fallback, minimum = 0) => {
    const parsed = number(name, fallback);
    if (!Number.isInteger(parsed) || parsed < minimum) {
      throw new Error(`${name} must be an integer greater than or equal to ${minimum}`);
    }
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
    connectionPrefix: value("--connection-prefix", String(process.pid)),
    control: !argv.includes("--no-control"),
    pairs: argv.includes("--pairs") ? integer("--pairs", 10) : integer("--connections", 10),
    batchSize: integer("--batch-size", 100, 1), rampMs: number("--ramp-ms", 0),
    durationMs: number("--duration", 10) * 1000, rate: number("--rate", 10),
    burst: number("--burst", 0), reconnects: integer("--reconnects", 3),
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
  let socket;
  const opening = new Promise((resolve, reject) => {
    socket = new WebSocket(url);
    const state = { opened: false, finalizing: false, closed: false };
    socket.loadState = state;
    let failed = false;
    const fail = () => {
      if (!failed) stats.connection_failures += 1;
      failed = true;
    };
    const timeout = setTimeout(() => {
      fail();
      socket.close();
      reject(new Error("WebSocket open timed out"));
    }, 10_000);
    state.waitForClose = new Promise((resolveClose) => { state.resolveClose = resolveClose; });
    socket.addEventListener("open", () => { state.opened = true; clearTimeout(timeout); stats.connection_successes += 1; resolve(socket); }, { once: true });
    socket.addEventListener("error", (event) => {
      fail();
      if (!state.opened) {
        clearTimeout(timeout);
        reject(new Error(`WebSocket connection failed${event.message ? `: ${event.message}` : ""}`));
      }
    }, { once: true });
    socket.addEventListener("close", (event) => {
      state.closed = true;
      state.resolveClose();
      if (event.code !== 1000 && !(state.finalizing && [1001, 1005, 1012].includes(event.code))) fail();
    });
  });
  opening.socket = socket;
  return opening;
}

async function connectPair(options, index, stats, latencies) {
  const connectionId = `load-${options.connectionPrefix}-${index}`;
  const serverEndpoint = options.endpoints[0];
  const clientIndex = Number(String(index).split("-").at(-1));
  const clientEndpoint = options.endpoints[(clientIndex + 1) % options.endpoints.length];
  const shared = { serverId: options.serverId, connectionId, v: "2" };
  const openings = [
    open(endpoint(serverEndpoint, { ...shared, role: "server" }), stats),
    open(endpoint(clientEndpoint, { ...shared, role: "client" }), stats),
  ];
  let server;
  let client;
  try {
    [server, client] = await Promise.all(openings);
  } catch (error) {
    await finish(openings.map((opening) => opening.socket), stats);
    throw error;
  }
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

async function connectPairs(options, wave, stats, latencies) {
  const pairs = [];
  for (let first = 0; first < options.pairs; first += options.batchSize) {
    const size = Math.min(options.batchSize, options.pairs - first);
    const results = await Promise.allSettled(Array.from({ length: size }, (_, offset) =>
      connectPair(options, `${wave}-${first + offset}`, stats, latencies)));
    pairs.push(...results.filter(({ status }) => status === "fulfilled").map(({ value }) => value));
    const failure = results.find(({ status }) => status === "rejected");
    if (failure) {
      await finish(pairs.flatMap(Object.values), stats);
      throw failure.reason;
    }
    if (first + size < options.pairs && options.rampMs) await sleep(options.rampMs);
  }
  return pairs;
}

async function finish(sockets, stats) {
  const openSockets = sockets.filter((socket) => socket.loadState?.opened && !socket.loadState.closed);
  openSockets.forEach((socket) => { socket.loadState.finalizing = true; });
  openSockets.forEach((socket) => socket.close(1000, "load complete"));
  const completed = Promise.all(openSockets.map((socket) => socket.loadState.waitForClose));
  let timeout;
  const expired = new Promise((resolve) => { timeout = setTimeout(() => resolve(true), 5_000); });
  const result = await Promise.race([completed.then(() => false), expired]);
  clearTimeout(timeout);
  if (result) {
    stats.connection_failures += openSockets.filter((socket) => !socket.loadState.closed).length;
  }
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
  let setupDurationMs = 0;
  let steadyDurationMs = 0;
  let pairs = [];
  let control;
  try {
    if (options.control) {
      control = await open(endpoint(options.endpoints[0], { serverId: options.serverId, role: "server", v: "2" }), stats);
    }
    const waves = options.scenario === "reconnect" ? options.reconnects + 1 : 1;
    for (let wave = 0; wave < waves; wave += 1) {
      pairs = await connectPairs(options, wave, stats, latencies);
      if (wave < waves - 1) { pairs.flatMap(Object.values).forEach((socket) => socket.close(1000, "load reconnect")); await sleep(100); }
    }
    setupDurationMs = now() - started;
    const steadyStarted = now();
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
    steadyDurationMs = now() - steadyStarted;
  } catch (error) {
    stats.connection_failures += 1;
    stats.error = error.message;
  } finally {
    await finish([...pairs.flatMap(Object.values), ...(control ? [control] : [])], stats);
  }
  const durationMs = now() - started;
  const resource = process.resourceUsage();
  const relay = sampleRelay(options.relayPid);
  process.stdout.write(`${JSON.stringify({
    protocol: { version: 2, roles: ["server-data", "client"] }, scenario: options.scenario,
    endpoints: options.endpoints, requested_pairs: options.pairs,
    requested_websockets: options.pairs * 2 + (options.control ? 1 : 0),
    setup_duration_ms: Math.round(setupDurationMs), steady_duration_ms: Math.round(steadyDurationMs), duration_ms: Math.round(durationMs),
    ...stats, throughput_frames_per_second: Number((stats.frames_received / (durationMs / 1000)).toFixed(2)),
    latency_ms: { p50: percentile(latencies, 0.5), p95: percentile(latencies, 0.95), p99: percentile(latencies, 0.99) },
    client: { rss_bytes: process.memoryUsage().rss, cpu_microseconds: resource.userCPUTime + resource.systemCPUTime }, relay,
  })}\n`);
  if (stats.connection_failures || stats.send_failures) process.exitCode = 1;
}

main();

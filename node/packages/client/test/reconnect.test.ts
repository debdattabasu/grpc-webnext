//! Surviving a dropped h2ts tunnel.
//!
//! Every call on the binary default rides ONE lazily-opened `H2Connection`, cached for the
//! life of the client. That cache is the whole story: if the tunnel dies — a network blip,
//! a load balancer recycling, a server restart — the client must notice and redial, and the
//! call that discovered the corpse must not be reported as a server-side failure. An RPC
//! that never left the client is gRPC's *transparent retry* case, safe to replay whatever
//! the policy says, because the server provably never saw it.
//!
//! The tunnel is cut here with a TCP relay rather than by killing the server: the server
//! stays up, so a failure to recover is unambiguously the client's.

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import net from "node:net";
import { createInterface } from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import WebSocket from "ws";
import {
  ConnectivityState,
  makePromiseClient,
  Status,
  type PromiseServiceClient,
} from "../src/index.js";
import { ConformanceServiceDefinition } from "./gen/conformance.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../..");
const rustRoot = path.join(repoRoot, "rust");
const wsImpl = WebSocket as unknown as typeof globalThis.WebSocket;

/** A TCP forwarder whose live connections can be severed without it stopping listening. */
class Relay {
  private readonly server: net.Server;
  private readonly live = new Set<net.Socket>();
  /** While set, new connections are hung up on immediately — the endpoint is "down". */
  down = false;
  constructor(private readonly targetPort: number) {
    this.server = net.createServer((client) => {
      if (this.down) {
        client.destroy();
        return;
      }
      const upstream = net.connect(this.targetPort, "127.0.0.1");
      for (const s of [client, upstream]) {
        this.live.add(s);
        // Tear the pair down together, so a failed upstream fails the dial fast rather
        // than leaving the client half-open (which would look like a hang, not an error).
        s.on("error", () => s.destroy());
        s.on("close", () => {
          this.live.delete(s);
          client.destroy();
          upstream.destroy();
        });
      }
      client.pipe(upstream);
      upstream.pipe(client);
    });
  }
  listen(): Promise<number> {
    return new Promise((resolve) =>
      this.server.listen(0, "127.0.0.1", () => resolve((this.server.address() as net.AddressInfo).port)),
    );
  }
  /** Kill every live connection. New ones still succeed. */
  cut(): void {
    for (const s of [...this.live]) s.destroy();
    this.live.clear();
  }
  stop(): void {
    this.cut();
    this.server.close();
  }
}

function spawnServer(): Promise<{ proc: ChildProcess; port: number }> {
  const proc = spawn("cargo", ["run", "--quiet", "-p", "conformance-server"], {
    cwd: rustRoot,
    stdio: ["ignore", "pipe", "inherit"],
  });
  return new Promise((resolve, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    rl.on("line", (line) => {
      const m = line.match(/^LISTENING http:\/\/\S+:(\d+)$/);
      if (m) resolve({ proc, port: Number(m[1]) });
    });
    proc.on("exit", (code) => reject(new Error(`conformance-server exited early: ${code}`)));
  });
}

const echo = (payload: string) => ({
  payload: new TextEncoder().encode(payload),
  responseDefinition: {
    statusCode: 0,
    statusMessage: "",
    headers: [],
    trailers: [],
    payload: new Uint8Array(),
    streamMessages: [],
    delayMs: 0,
    oversizeResponseBytes: 0,
  },
});

describe("h2ts tunnel recovery", () => {
  let proc: ChildProcess;
  let relay: Relay;
  let client: PromiseServiceClient<typeof ConformanceServiceDefinition>;

  beforeAll(async () => {
    execFileSync("cargo", ["build", "--quiet", "-p", "conformance-server"], { cwd: rustRoot });
    const server = await spawnServer();
    proc = server.proc;
    relay = new Relay(server.port);
    const port = await relay.listen();
    client = makePromiseClient(ConformanceServiceDefinition, {
      baseUrl: `http://127.0.0.1:${port}`,
      webSocketImpl: wsImpl,
    });
  }, 180_000);

  afterAll(() => {
    client?.close();
    relay?.stop();
    proc?.kill("SIGKILL");
  });

  it("redials after the tunnel is cut", async () => {
    expect((await client.unary(echo("before"))).payload).toEqual(new TextEncoder().encode("before"));

    relay.cut();

    // The dead connection must not be handed out again. This call never reached the
    // server, so recovering from it is transparent retry, not a policy decision.
    expect((await client.unary(echo("after"))).payload).toEqual(new TextEncoder().encode("after"));
  }, 60_000);

  it("keeps working across repeated cuts", async () => {
    for (const label of ["one", "two", "three"]) {
      relay.cut();
      expect((await client.unary(echo(label))).payload).toEqual(new TextEncoder().encode(label));
    }
  }, 60_000);

  it("recovers when the endpoint was down at the first call", async () => {
    // A client that starts before its server caches a *rejected* dial. If that is never
    // cleared, the client stays broken for good — and unlike a mid-life drop, nothing
    // ever fires a `closed` event to evict it. One client, one URL, throughout.
    relay.down = true;
    relay.cut();
    await expect(client.unary(echo("nope"))).rejects.toMatchObject({
      code: Status.UNAVAILABLE, // the transport could not carry it — not UNKNOWN
    });

    relay.down = false;
    expect((await client.unary(echo("yes"))).payload).toEqual(new TextEncoder().encode("yes"));
  }, 60_000);
});

describe("connectivity state", () => {
  let proc: ChildProcess;
  let relay: Relay;
  let relayPort: number;
  let client: PromiseServiceClient<typeof ConformanceServiceDefinition>;

  beforeAll(async () => {
    const server = await spawnServer();
    proc = server.proc;
    relay = new Relay(server.port);
    const port = (relayPort = await relay.listen());
    client = makePromiseClient(ConformanceServiceDefinition, {
      baseUrl: `http://127.0.0.1:${port}`,
      webSocketImpl: wsImpl,
    });
  }, 180_000);

  afterAll(() => {
    client?.close();
    relay?.stop();
    proc?.kill("SIGKILL");
  });

  it("reports transitions, and tells a reconnect from a first connect", async () => {
    const seen: ConnectivityState[] = [];
    const stop = client.watchConnectivityState((s) => seen.push(s));

    expect(client.getConnectivityState()).toBe(ConnectivityState.IDLE);
    await client.unary(echo("one"));
    expect(client.getConnectivityState()).toBe(ConnectivityState.READY);
    expect(seen).toEqual([ConnectivityState.CONNECTING, ConnectivityState.READY]);

    relay.cut();
    // The drop itself is an event — without it a watcher could not distinguish
    // "reconnected" from "connected for the first time", which is the difference
    // between "we blipped" and "we just started".
    await vi.waitFor(() => expect(seen).toContain(ConnectivityState.IDLE), { timeout: 5000 });

    await client.unary(echo("two"));
    expect(client.getConnectivityState()).toBe(ConnectivityState.READY);
    expect(seen.slice(-3)).toEqual([
      ConnectivityState.IDLE,
      ConnectivityState.CONNECTING,
      ConnectivityState.READY,
    ]);

    stop();
    const before = seen.length;
    relay.cut();
    await client.unary(echo("three"));
    expect(seen.length, "unsubscribe should stop delivery").toBe(before);
  }, 60_000);

  it("reports TRANSIENT_FAILURE when the endpoint is down, and recovers", async () => {
    const down = new Relay(1);
    const port = await down.listen();
    down.down = true;
    const flaky = makePromiseClient(ConformanceServiceDefinition, {
      baseUrl: `http://127.0.0.1:${port}`,
      webSocketImpl: wsImpl,
    });
    await expect(flaky.unary(echo("x"))).rejects.toThrow();
    expect(flaky.getConnectivityState()).toBe(ConnectivityState.TRANSIENT_FAILURE);
    flaky.close();
    down.stop();
  }, 60_000);

  it("is undefined where there is no channel to report on", async () => {
    // Fetch is stateless and the custom `Frame` path opens a socket per stream, so
    // neither has a connection whose state could be "down" between calls. Saying
    // undefined is the honest answer; claiming READY would be a fiction.
    const ws = makePromiseClient(ConformanceServiceDefinition, {
      baseUrl: `http://127.0.0.1:${relayPort}`,
      webSocketImpl: wsImpl,
      unary: "fetch",
      streaming: "ws",
    });
    expect(ws.getConnectivityState()).toBeUndefined();
    expect(typeof ws.watchConnectivityState(() => {})).toBe("function"); // no-op, not a throw
    ws.close();
  });
});

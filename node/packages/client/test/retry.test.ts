//! Retry: the policy arithmetic, then the same rules over the real wire.
//!
//! Retries are **off unless configured**, which is both gRPC's default and this repo's
//! stated position on retry storms (`doc/BACKLOG.md`, the removed proxy-side retry). The
//! interesting rules are the ones that say *don't* retry: a committed stream, a
//! non-retryable status, a server pushing back, an exhausted throttle.

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { makePromiseClient, Metadata, Status, type PromiseServiceClient } from "../src/index.js";
import {
  backoffMs,
  nextDelay,
  readPushback,
  resolvePolicy,
  Throttle,
  PUSHBACK_HEADER,
} from "../src/retry.js";
import type { StatusResult } from "../src/transport.js";
import { ConformanceServiceDefinition } from "./gen/conformance.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../..");
const rustRoot = path.join(repoRoot, "rust");
const wsImpl = WebSocket as unknown as typeof globalThis.WebSocket;

const status = (code: Status, metadata = new Metadata()): StatusResult => ({
  code,
  details: "",
  metadata,
});

describe("retry policy", () => {
  it("grows the backoff geometrically, capped, with full jitter", () => {
    const p = resolvePolicy({ initialBackoffMs: 100, backoffMultiplier: 2, maxBackoffMs: 350 });
    // random() is the jitter; pin it at 1 to read the ceiling of each attempt's range.
    expect(backoffMs(p, 1, () => 1)).toBe(100);
    expect(backoffMs(p, 2, () => 1)).toBe(200);
    expect(backoffMs(p, 3, () => 1)).toBe(350); // capped, not 400
    // Jitter is the point: without it every client that failed together retries together.
    expect(backoffMs(p, 3, () => 0)).toBe(0);
    expect(backoffMs(p, 3, () => 0.5)).toBe(175);
  });

  it("retries only listed statuses, and only within the attempt budget", () => {
    const p = resolvePolicy({ maxAttempts: 3 });
    expect(nextDelay(p, null, status(Status.UNAVAILABLE), 1)).not.toBeNull();
    expect(nextDelay(p, null, status(Status.UNAVAILABLE), 2)).not.toBeNull();
    expect(nextDelay(p, null, status(Status.UNAVAILABLE), 3)).toBeNull(); // budget spent
    expect(nextDelay(p, null, status(Status.INVALID_ARGUMENT), 1)).toBeNull(); // not listed
    expect(nextDelay(p, null, status(Status.OK), 1)).toBeNull();
  });

  it("maxAttempts: 1 means no retries at all", () => {
    const p = resolvePolicy({ maxAttempts: 1 });
    expect(nextDelay(p, null, status(Status.UNAVAILABLE), 1)).toBeNull();
  });

  it("obeys server pushback over its own backoff", () => {
    // The policy's own backoff here is jittered somewhere in [0, 5000); pushback replaces
    // it with an exact value, which is what makes this assertion sharp.
    const p = resolvePolicy({ initialBackoffMs: 5000, maxBackoffMs: 5000 });
    const md = new Metadata();
    md.set(PUSHBACK_HEADER, "42");
    expect(nextDelay(p, null, status(Status.UNAVAILABLE, md), 1)).toBe(42);

    // Pushback also *creates* retryability: the server asked to be retried.
    expect(nextDelay(p, null, status(Status.INVALID_ARGUMENT, md), 1)).toBe(42);
  });

  it("stops on negative or unreadable pushback", () => {
    const p = resolvePolicy({});
    for (const raw of ["-1", "later", ""]) {
      const md = new Metadata();
      md.set(PUSHBACK_HEADER, raw);
      expect(nextDelay(p, null, status(Status.UNAVAILABLE, md), 1), raw).toBeNull();
    }
    expect(readPushback(undefined)).toEqual({ kind: "none" });
  });

  it("throttles once the bucket falls to half", () => {
    const p = resolvePolicy({ maxAttempts: 100 });
    const throttle = new Throttle({ maxTokens: 4, tokenRatio: 0.5 });
    expect(nextDelay(p, throttle, status(Status.UNAVAILABLE), 1)).not.toBeNull();

    throttle.onFailure(); // 3
    expect(throttle.allowed).toBe(true);
    throttle.onFailure(); // 2 — not > maxTokens/2
    expect(throttle.allowed).toBe(false);
    expect(nextDelay(p, throttle, status(Status.UNAVAILABLE), 1)).toBeNull();

    throttle.onSuccess(); // 2.5, back above half
    expect(nextDelay(p, throttle, status(Status.UNAVAILABLE), 1)).not.toBeNull();
  });

  it("never lets the bucket run away in either direction", () => {
    const throttle = new Throttle({ maxTokens: 2, tokenRatio: 1 });
    for (let i = 0; i < 10; i++) throttle.onFailure();
    expect(throttle.allowed).toBe(false);
    for (let i = 0; i < 10; i++) throttle.onSuccess();
    throttle.onFailure(); // from a full bucket of 2 -> 1, i.e. it was never above 2
    expect(throttle.allowed).toBe(false);
  });
});

// --- over the wire ------------------------------------------------------------------------

const definition = () => ({
  statusCode: 0,
  statusMessage: "",
  headers: [],
  trailers: [],
  payload: new Uint8Array(),
  streamMessages: [],
  delayMs: 0,
  oversizeResponseBytes: 0,
});

function spawnServer(): Promise<{ proc: ChildProcess; baseUrl: string }> {
  const proc = spawn("cargo", ["run", "--quiet", "-p", "conformance-server"], {
    cwd: rustRoot,
    stdio: ["ignore", "pipe", "inherit"],
  });
  return new Promise((resolve, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    rl.on("line", (line) => {
      const m = line.match(/^LISTENING (\S+)$/);
      if (m) resolve({ proc, baseUrl: m[1] });
    });
    proc.on("exit", (code) => reject(new Error(`conformance-server exited early: ${code}`)));
  });
}

describe("retry end to end", () => {
  let proc: ChildProcess;
  let baseUrl: string;

  beforeAll(async () => {
    execFileSync("cargo", ["build", "--quiet", "-p", "conformance-server"], { cwd: rustRoot });
    const server = await spawnServer();
    proc = server.proc;
    baseUrl = server.baseUrl;
  }, 180_000);

  afterAll(() => proc?.kill("SIGKILL"));

  /**
   * A client whose Fetch transport fails its first `failures` attempts at the network
   * level — the shape of a blip — and then talks to the real server. Injected through the
   * `fetch` option the library already exposes, so the protocol under test stays real.
   */
  function flakyClient(failures: number, extra: object = {}) {
    const calls = { n: 0 };
    const client = makePromiseClient(ConformanceServiceDefinition, {
      baseUrl,
      unary: "fetch",
      fetch: ((input: RequestInfo | URL, init?: RequestInit) => {
        calls.n++;
        return calls.n <= failures
          ? Promise.reject(new TypeError("fetch failed"))
          : fetch(input as RequestInfo, init);
      }) as typeof fetch,
      ...extra,
    });
    return { client, calls };
  }

  it("replays a blip and succeeds, without the caller seeing it", async () => {
    const { client, calls } = flakyClient(2, { retry: { initialBackoffMs: 1 } });
    const reply = await client.unary({
      payload: new TextEncoder().encode("hi"),
      responseDefinition: definition(),
    });
    expect(new TextDecoder().decode(reply.payload)).toBe("hi");
    expect(calls.n).toBe(3); // two failures, then the real call
    client.close();
  }, 60_000);

  it("gives up after maxAttempts and reports the last status", async () => {
    const { client, calls } = flakyClient(99, {
      retry: { maxAttempts: 3, initialBackoffMs: 1 },
    });
    await expect(
      client.unary({ payload: new Uint8Array(), responseDefinition: definition() }),
    ).rejects.toMatchObject({ code: Status.UNAVAILABLE });
    expect(calls.n).toBe(3);
    client.close();
  }, 60_000);

  it("does not retry at all without a policy", async () => {
    const { client, calls } = flakyClient(99);
    await expect(
      client.unary({ payload: new Uint8Array(), responseDefinition: definition() }),
    ).rejects.toMatchObject({ code: Status.UNAVAILABLE });
    expect(calls.n).toBe(1);
    client.close();
  }, 60_000);

  it("does not retry a status the server actually returned", async () => {
    const { client, calls } = flakyClient(0, { retry: { initialBackoffMs: 1 } });
    await expect(
      client.unary({
        payload: new Uint8Array(),
        responseDefinition: { ...definition(), statusCode: Status.INVALID_ARGUMENT },
      }),
    ).rejects.toMatchObject({ code: Status.INVALID_ARGUMENT });
    expect(calls.n).toBe(1);
    client.close();
  }, 60_000);

  it("honors a server that refuses retries via pushback", async () => {
    // UNAVAILABLE would normally be retried; `grpc-retry-pushback-ms: -1` overrides that.
    const { client, calls } = flakyClient(0, { retry: { initialBackoffMs: 1 } });
    await expect(
      client.unary({
        payload: new Uint8Array(),
        responseDefinition: {
          ...definition(),
          statusCode: Status.UNAVAILABLE,
          trailers: [{ key: PUSHBACK_HEADER, asciiValue: "-1", binValue: undefined }],
        },
      }),
    ).rejects.toMatchObject({ code: Status.UNAVAILABLE });
    expect(calls.n).toBe(1);
    client.close();
  }, 60_000);

  // --- streaming: the commit rule ---------------------------------------------------------

  /** Counts WebSocket constructions, i.e. stream attempts on the `ws` transport. */
  function countingClient(extra: object = {}) {
    const opened = { n: 0 };
    const Counting = class extends (WebSocket as never as { new (...a: unknown[]): object }) {
      constructor(...args: unknown[]) {
        opened.n++;
        super(...args);
      }
    } as unknown as typeof globalThis.WebSocket;
    const client: PromiseServiceClient<typeof ConformanceServiceDefinition> = makePromiseClient(
      ConformanceServiceDefinition,
      { baseUrl, streaming: "ws", unary: "fetch", webSocketImpl: Counting, ...extra },
    );
    return { client, opened };
  }

  it("retries a stream that failed before its first message", async () => {
    const { client, opened } = countingClient({ retry: { maxAttempts: 3, initialBackoffMs: 1 } });
    const stream = client.serverStream({
      responseDefinition: { ...definition(), statusCode: Status.UNAVAILABLE },
    });
    await expect(
      (async () => {
        for await (const _ of stream) void _;
      })(),
    ).rejects.toMatchObject({ code: Status.UNAVAILABLE });
    expect(opened.n).toBe(3);
    client.close();
  }, 60_000);

  it("does not retry once a message has been delivered", async () => {
    // gRPC's commit rule: the caller has now seen something a replay would contradict.
    const { client, opened } = countingClient({ retry: { maxAttempts: 3, initialBackoffMs: 1 } });
    const stream = client.serverStream({
      responseDefinition: {
        ...definition(),
        statusCode: Status.UNAVAILABLE,
        streamMessages: [{ payload: new TextEncoder().encode("a"), delayMs: 0 }],
      },
    });
    const seen: string[] = [];
    await expect(
      (async () => {
        for await (const m of stream) seen.push(new TextDecoder().decode(m.payload));
      })(),
    ).rejects.toMatchObject({ code: Status.UNAVAILABLE });
    expect(seen).toEqual(["a"]); // delivered once, not three times
    expect(opened.n).toBe(1);
    client.close();
  }, 60_000);

  it("emits metadata once, from the attempt that committed", async () => {
    const { client } = countingClient({ retry: { maxAttempts: 3, initialBackoffMs: 1 } });
    const headers: string[] = [];
    const stream = client.serverStream(
      {
        responseDefinition: {
          ...definition(),
          streamMessages: [{ payload: new TextEncoder().encode("a"), delayMs: 0 }],
          headers: [{ key: "x-attempt", asciiValue: "1", binValue: undefined }],
        },
      },
      { onHeader: (md) => headers.push(String(md.get("x-attempt")[0])) },
    );
    for await (const _ of stream) void _;
    expect(headers).toEqual(["1"]);
    client.close();
  }, 60_000);
});

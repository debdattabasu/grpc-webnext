//! Consumer flow control on the response stream.
//!
//! The two streaming transports differ in what they can honor, and that asymmetry is
//! deliberate — so it is pinned here rather than left to be rediscovered:
//!
//! - **h2ts (the binary default)** carries real HTTP/2, and h2ts response bodies are
//!   `highWaterMark: 0` and consumption-driven — a `WINDOW_UPDATE` goes out only as the
//!   application reads. So a client that stops reading stops replenishing the window and
//!   the *server* blocks. That is genuine end-to-end backpressure, and the first test
//!   proves it by showing the stream cannot finish while the consumer is away.
//! - **The custom `Frame` WebSocket path** cannot do this at all: the browser WebSocket
//!   API has no receive-side flow control, `onmessage` fires whether or not anyone is
//!   listening. There the messages queue client-side, which is what every browser
//!   WebSocket library does, and the application's lever is `readableLength`.
//!
//! Independent of transport, a **flowing** consumer (`.on("data")`) must retain nothing:
//! the stream used to feed its own internal buffer as well, so a callback consumer held
//! every message it had already handled for the life of the stream.

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { makePromiseClient, type PromiseServiceClient } from "../src/index.js";
import type { ClientReadableStream } from "../src/call.js";
import {
  ConformanceServiceDefinition,
  type ConformancePayload,
  type StreamMessage,
} from "./gen/conformance.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../..");
const rustRoot = path.join(repoRoot, "rust");
const wsImpl = WebSocket as unknown as typeof globalThis.WebSocket;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Small enough to be cheap, large enough that the set overruns an HTTP/2 window. */
const CHUNK_BYTES = 64 * 1024;
const CHUNKS = 40; // 2.5 MiB total vs h2ts's 1 MiB default per-stream receive window
const HIGH_WATER_MARK = 4;

/** `CHUNKS` messages, each tagged in its first byte so order is checkable. */
function chunks(): StreamMessage[] {
  return Array.from({ length: CHUNKS }, (_, i) => {
    const payload = new Uint8Array(CHUNK_BYTES);
    payload[0] = i;
    return { payload, delayMs: 0 };
  });
}

const streamRequest = () => ({
  responseDefinition: {
    statusCode: 0,
    statusMessage: "",
    headers: [],
    trailers: [],
    payload: new Uint8Array(),
    streamMessages: chunks(),
    delayMs: 0,
    oversizeResponseBytes: 0,
  },
});

type Conformance = PromiseServiceClient<typeof ConformanceServiceDefinition>;

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

describe("response-stream flow control", () => {
  let proc: ChildProcess;
  let baseUrl: string;
  let h2ts: Conformance;
  let ws: Conformance;

  beforeAll(async () => {
    execFileSync("cargo", ["build", "--quiet", "-p", "conformance-server"], { cwd: rustRoot });
    const server = await spawnServer();
    proc = server.proc;
    baseUrl = server.baseUrl;
    const common = { baseUrl, webSocketImpl: wsImpl, readableHighWaterMark: HIGH_WATER_MARK };
    h2ts = makePromiseClient(ConformanceServiceDefinition, common);
    ws = makePromiseClient(ConformanceServiceDefinition, {
      ...common,
      unary: "fetch",
      streaming: "ws",
    });
  }, 180_000);

  afterAll(() => {
    h2ts?.close();
    ws?.close();
    proc?.kill("SIGKILL");
  });

  it("h2ts: a consumer that stops reading stops the server", async () => {
    const stream = h2ts.serverStream(streamRequest()) as ClientReadableStream<ConformancePayload>;
    let finished = false;
    stream.on("status", () => (finished = true));

    // Take exactly one message, then walk away.
    const it = stream[Symbol.asyncIterator]();
    await it.next();
    await sleep(300);

    // Nothing accumulated past the mark: the transport stopped calling read().
    expect(stream.readableLength).toBeLessThanOrEqual(HIGH_WATER_MARK);
    // And the RPC has not terminated. This is the load-bearing assertion — the response
    // is 2.5 MiB against a 1 MiB stream window, so the server *cannot* have handed it all
    // to the transport. It is blocked on a window we are not replenishing.
    expect(finished, "the server ran to completion despite a consumer that stopped reading").toBe(
      false,
    );

    // Resuming delivers the rest, in order and complete.
    const seen = [0];
    for (;;) {
      const next = await it.next();
      if (next.done) break;
      seen.push(next.value.payload[0]);
    }
    expect(seen).toEqual(Array.from({ length: CHUNKS }, (_, i) => i));
    expect(finished).toBe(true);
  }, 60_000);

  it("ws: the same consumer buffers instead, and says so", async () => {
    const stream = ws.serverStream(streamRequest()) as ClientReadableStream<ConformancePayload>;
    const it = stream[Symbol.asyncIterator]();
    await it.next();
    await sleep(300);

    // The browser WebSocket API has no way to say "stop" — so, unlike h2ts above, the
    // backlog grows past the mark. `readableLength` is how an application notices.
    expect(stream.readableLength).toBeGreaterThan(HIGH_WATER_MARK);

    // Buffered, not dropped: every message still arrives, in order.
    const seen = [0];
    for (;;) {
      const next = await it.next();
      if (next.done) break;
      seen.push(next.value.payload[0]);
    }
    expect(seen).toEqual(Array.from({ length: CHUNKS }, (_, i) => i));
  }, 60_000);

  it("a flowing consumer retains nothing", async () => {
    const stream = h2ts.serverStream(streamRequest()) as ClientReadableStream<ConformancePayload>;
    const seen: number[] = [];
    const peak: number[] = [];
    await new Promise<void>((resolve, reject) => {
      stream.on("data", (m) => {
        seen.push(m.payload[0]);
        peak.push(stream.readableLength);
      });
      stream.on("end", () => resolve());
      stream.on("error", reject);
    });

    expect(seen).toEqual(Array.from({ length: CHUNKS }, (_, i) => i));
    // A `data` listener takes the message, so nothing is ever queued behind it. This used
    // to grow to `CHUNKS`: the stream fed its own iterator buffer as well, and a callback
    // consumer never drains that.
    expect(Math.max(...peak)).toBe(0);
    expect(stream.readableLength).toBe(0);
  }, 60_000);

  it("pause() holds delivery and resume() releases it in order", async () => {
    const stream = h2ts.serverStream(streamRequest()) as ClientReadableStream<ConformancePayload>;
    const seen: number[] = [];
    const done = new Promise<void>((resolve, reject) => {
      stream.on("data", (m) => {
        seen.push(m.payload[0]);
        if (seen.length === 1) stream.pause();
      });
      stream.on("end", () => resolve());
      stream.on("error", reject);
    });

    await sleep(300);
    expect(seen).toEqual([0]); // paused right after the first message
    expect(stream.readableLength).toBeGreaterThan(0); // held, not dropped

    stream.resume();
    await done;
    expect(seen).toEqual(Array.from({ length: CHUNKS }, (_, i) => i));
  }, 60_000);
});

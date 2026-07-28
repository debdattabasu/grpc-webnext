//! `ClientReadableStream` queue mechanics, in isolation from any transport.
//!
//! `deliver()` is the transport-facing seam: it returns a promise exactly when the
//! consumer has fallen behind, and a transport that awaits it stops reading (see
//! flow-control.test.ts for that end-to-end). These pin the contract itself — including
//! the release-on-finish case, which no live transport can reach because the h2ts loop
//! cannot deliver a terminal status while it is parked.

import { describe, expect, it } from "vitest";
import { ClientReadableStream } from "../src/call.js";
import { ServiceError, Status } from "../src/status.js";
import { Metadata } from "../src/metadata.js";

const noop = () => {};
const mk = (highWaterMark = 2) => new ClientReadableStream<number>(noop, highWaterMark);

describe("ClientReadableStream", () => {
  it("queues for an iterating consumer and reports the backlog", () => {
    const s = mk();
    expect(s.deliver(1)).toBeUndefined(); // under the mark: no push-back
    expect(s.readableLength).toBe(1);
    expect(s.deliver(2)).toBeInstanceOf(Promise); // at the mark: park the transport
    expect(s.readableLength).toBe(2);
  });

  it("releases a parked delivery once the consumer catches up", async () => {
    const s = mk();
    s.deliver(1);
    const parked = s.deliver(2) as Promise<void>;

    let released = false;
    void parked.then(() => (released = true));
    await Promise.resolve();
    expect(released, "released before the consumer read anything").toBe(false);

    const it = s[Symbol.asyncIterator]();
    await it.next();
    await parked;
    expect(released).toBe(true);
  });

  it("hands straight to a flowing consumer, retaining nothing", () => {
    const s = mk();
    const seen: number[] = [];
    s.on("data", (m) => seen.push(m));
    expect(s.deliver(1)).toBeUndefined();
    expect(s.deliver(2)).toBeUndefined(); // would be at the mark if it were queuing
    expect(seen).toEqual([1, 2]);
    expect(s.readableLength).toBe(0);
  });

  it("pause() queues even for a flowing consumer; resume() flushes in order", () => {
    const s = mk(10);
    const seen: number[] = [];
    s.on("data", (m) => seen.push(m));
    s.deliver(1);
    s.pause();
    s.deliver(2);
    s.deliver(3);
    expect(seen).toEqual([1]);
    expect(s.readableLength).toBe(2);

    s.resume();
    expect(seen).toEqual([1, 2, 3]);
    expect(s.readableLength).toBe(0);
  });

  it("releases parked deliveries when the stream finishes", async () => {
    const s = mk();
    s.deliver(1);
    const parked = s.deliver(2) as Promise<void>;
    // Nothing will drain the queue now, so a transport awaiting this would hang forever.
    s.emit("end");
    await expect(parked).resolves.toBeUndefined();
  });

  it("clamps a zero high-water mark, which would otherwise park every delivery", () => {
    const s = new ClientReadableStream<number>(noop, 0);
    expect(s.deliver(1)).toBeInstanceOf(Promise); // parked at a mark of 1, not 0
    const it = s[Symbol.asyncIterator]();
    void it.next();
    expect(s.readableLength).toBe(0); // ...and releasable, which a mark of 0 never is
  });

  it("delivers queued messages before surfacing the terminal error", async () => {
    const s = mk(10);
    s.deliver(1);
    s.emit("error", new ServiceError(Status.INTERNAL, "boom", new Metadata()));

    const it = s[Symbol.asyncIterator]();
    await expect(it.next()).resolves.toEqual({ value: 1, done: false });
    await expect(it.next()).rejects.toThrow("boom");
  });
});

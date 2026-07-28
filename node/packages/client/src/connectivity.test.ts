//! `ConnectivityWatch` semantics, independent of any transport.
//!
//! The e2e tests (test/reconnect.test.ts) prove the states are reported at the right
//! moments; these pin the contract the watch itself promises. Mirrors the Rust
//! client's `state.rs` tests, deliberately — the two clients expose the same states
//! and should agree on what a watcher sees.

import { describe, expect, it, vi } from "vitest";
import { ConnectivityState, ConnectivityWatch } from "./connectivity.js";

describe("ConnectivityWatch", () => {
  it("starts idle", () => {
    expect(new ConnectivityWatch().current).toBe(ConnectivityState.IDLE);
  });

  it("delivers transitions but not repeats", () => {
    const watch = new ConnectivityWatch();
    const seen: ConnectivityState[] = [];
    watch.watch((s) => seen.push(s));

    watch.set(ConnectivityState.CONNECTING);
    watch.set(ConnectivityState.CONNECTING); // same value: not an event
    watch.set(ConnectivityState.READY);
    watch.set(ConnectivityState.IDLE);

    expect(seen).toEqual([
      ConnectivityState.CONNECTING,
      ConnectivityState.READY,
      ConnectivityState.IDLE,
    ]);
    expect(watch.current).toBe(ConnectivityState.IDLE);
  });

  it("setting the state it already has is not an event", () => {
    // The load-bearing half of "repeats are collapsed": a caller polling a state
    // machine must not be able to spam every watcher.
    const watch = new ConnectivityWatch();
    watch.set(ConnectivityState.READY);
    const seen: ConnectivityState[] = [];
    watch.watch((s) => seen.push(s));
    watch.set(ConnectivityState.READY);
    watch.set(ConnectivityState.READY);
    expect(seen).toEqual([]);
  });

  it("a watcher added later sees only what follows", () => {
    const watch = new ConnectivityWatch();
    watch.set(ConnectivityState.READY);
    const seen: ConnectivityState[] = [];
    watch.watch((s) => seen.push(s));
    watch.set(ConnectivityState.IDLE);
    expect(seen).toEqual([ConnectivityState.IDLE]);
  });

  it("unsubscribing stops delivery", () => {
    const watch = new ConnectivityWatch();
    const seen: ConnectivityState[] = [];
    const stop = watch.watch((s) => seen.push(s));
    watch.set(ConnectivityState.CONNECTING);
    stop();
    watch.set(ConnectivityState.READY);
    expect(seen).toEqual([ConnectivityState.CONNECTING]);
  });

  it("one listener throwing does not rob the others", () => {
    // A watcher is usually UI code; a render error in one subscriber must not
    // silently strand every other subscriber on a stale state.
    const logged = vi.spyOn(console, "error").mockImplementation(() => {});
    const watch = new ConnectivityWatch();
    const seen: ConnectivityState[] = [];
    watch.watch(() => {
      throw new Error("render failed");
    });
    watch.watch((s) => seen.push(s));

    expect(() => watch.set(ConnectivityState.READY)).not.toThrow();
    expect(seen).toEqual([ConnectivityState.READY]);
    expect(logged, "the fault must not vanish silently").toHaveBeenCalled();
    logged.mockRestore();
  });
});

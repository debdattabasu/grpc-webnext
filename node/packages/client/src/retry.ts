//! Client-side retry, following gRPC's retry design (service-config `retryPolicy`).
//!
//! Two things here are deliberately *not* the same mechanism:
//!
//! - **Transparent retry** — replaying an RPC that provably never reached the server —
//!   is not configured and not counted here. It lives in the transport, because only the
//!   transport knows whether the bytes went anywhere. See `H2tsTransport.unary`.
//! - **Policy retry** — replaying an RPC the server *may* have seen — is what this file
//!   does, and it is **off unless configured**. That is gRPC's default too, and it is the
//!   right one: a retry that the caller did not ask for turns one failing server into a
//!   thundering herd. The same reasoning removed retry from the proxy (`doc/BACKLOG.md`).

import type { Metadata } from "./metadata.js";
import { Status } from "./status.js";
import type { StatusResult } from "./transport.js";

/** Per-client retry policy; mirrors the service-config `retryPolicy` fields. */
export interface RetryPolicy {
  /** Total attempts including the first. Default 5, floor 1 (1 = no retries). */
  maxAttempts?: number;
  /** Delay before the first retry, before jitter. Default 100 ms. */
  initialBackoffMs?: number;
  /** Ceiling for the backoff, before jitter. Default 2000 ms. */
  maxBackoffMs?: number;
  /** Growth factor per attempt. Default 2. */
  backoffMultiplier?: number;
  /** Statuses worth retrying. Default `[UNAVAILABLE]`. */
  retryableStatusCodes?: Status[];
}

/**
 * gRPC's retry throttle: a token bucket shared by every call on the client, so a server
 * that is failing everything cannot be retried into the ground. Opt-in, as in gRPC.
 */
export interface RetryThrottling {
  /** Bucket size; also the starting balance. */
  maxTokens: number;
  /** Tokens returned per successful call (typically a small fraction, e.g. 0.1). */
  tokenRatio: number;
}

interface ResolvedPolicy {
  maxAttempts: number;
  initialBackoffMs: number;
  maxBackoffMs: number;
  backoffMultiplier: number;
  retryableStatusCodes: Set<Status>;
}

export function resolvePolicy(policy: RetryPolicy): ResolvedPolicy {
  return {
    maxAttempts: Math.max(1, Math.floor(policy.maxAttempts ?? 5)),
    initialBackoffMs: Math.max(0, policy.initialBackoffMs ?? 100),
    maxBackoffMs: Math.max(0, policy.maxBackoffMs ?? 2000),
    backoffMultiplier: Math.max(1, policy.backoffMultiplier ?? 2),
    retryableStatusCodes: new Set(policy.retryableStatusCodes ?? [Status.UNAVAILABLE]),
  };
}

/**
 * Token bucket. A retry costs a token and is only allowed above half-full, so a run of
 * failures disables retrying long before it can amplify the load.
 */
export class Throttle {
  private tokens: number;
  constructor(private readonly config: RetryThrottling) {
    this.tokens = config.maxTokens;
  }
  get allowed(): boolean {
    return this.tokens > this.config.maxTokens / 2;
  }
  onFailure(): void {
    this.tokens = Math.max(0, this.tokens - 1);
  }
  onSuccess(): void {
    this.tokens = Math.min(this.config.maxTokens, this.tokens + this.config.tokenRatio);
  }
}

/** `grpc-retry-pushback-ms`: the server dictating the delay, or refusing retries outright. */
export const PUSHBACK_HEADER = "grpc-retry-pushback-ms";

export type Pushback = { kind: "none" } | { kind: "stop" } | { kind: "delay"; ms: number };

export function readPushback(metadata: Metadata | undefined): Pushback {
  const raw = metadata?.get(PUSHBACK_HEADER)?.[0];
  if (raw === undefined) return { kind: "none" };
  const ms = Number.parseInt(typeof raw === "string" ? raw : new TextDecoder().decode(raw), 10);
  // Negative means "do not retry". Unparseable gets the same treatment: the server meant
  // to say something about retries and we could not read it, so the safe reading is stop.
  if (!Number.isFinite(ms) || ms < 0) return { kind: "stop" };
  return { kind: "delay", ms };
}

/**
 * Delay before retry `attempt` (1 = the first retry), with gRPC's full jitter:
 * `random(0, min(initial * multiplier^(attempt-1), max))`. Jitter is the point — a
 * fixed backoff re-synchronizes every client that failed together.
 */
export function backoffMs(policy: ResolvedPolicy, attempt: number, random = Math.random): number {
  const target = policy.initialBackoffMs * policy.backoffMultiplier ** (attempt - 1);
  return random() * Math.min(target, policy.maxBackoffMs);
}

/** Whether `status` is worth another attempt under this policy. */
export function retryable(policy: ResolvedPolicy, status: StatusResult, attempt: number): boolean {
  return attempt < policy.maxAttempts && policy.retryableStatusCodes.has(status.code);
}

/** Sleep, resolving early (and reporting it) if the call is aborted meanwhile. */
export function sleep(ms: number, signal: AbortSignal): Promise<boolean> {
  if (signal.aborted) return Promise.resolve(false);
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve(true);
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      resolve(false);
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

/**
 * The retry decision for one completed attempt, shared by the unary and streaming paths
 * so they cannot drift. Returns the delay to wait, or `null` to give up and report
 * `status` to the caller.
 */
export function nextDelay(
  policy: ResolvedPolicy,
  throttle: Throttle | null,
  status: StatusResult,
  attempt: number,
): number | null {
  const pushback = readPushback(status.metadata);
  if (pushback.kind === "stop") return null;
  // A server that names a delay has decided this is retryable; the policy still caps how
  // many times, and the throttle can still veto.
  if (pushback.kind === "delay") {
    if (attempt >= policy.maxAttempts) return null;
  } else if (!retryable(policy, status, attempt)) {
    return null;
  }
  if (throttle && !throttle.allowed) return null;
  return pushback.kind === "delay" ? pushback.ms : backoffMs(policy, attempt);
}

import {
  ClientDuplexStream,
  ClientReadableStream,
  ClientUnaryCall,
  ClientWritableStream,
  RequestCallback,
  statusToError,
} from "./call.js";
import type { ConnectivityListener, ConnectivityState } from "./connectivity.js";
import { CallContext, createCallContext, statusForAbort } from "./context.js";
import { FetchTransport } from "./fetch-transport.js";
import { Metadata } from "./metadata.js";
import { ServiceError, Status } from "./status.js";
import { nextDelay, resolvePolicy, sleep, Throttle } from "./retry.js";
import type { RetryPolicy, RetryThrottling } from "./retry.js";
import type {
  StatusResult,
  StreamCall,
  Transport,
  TransportCallOptions,
  UnaryResponse,
} from "./transport.js";
import { WebSocketTransport } from "./ws-transport.js";
import { H2tsTransport } from "./h2ts-transport.js";

/** Per-call options, mirroring the subset of grpc-js `CallOptions` we support. */
export interface CallOptions {
  /** Absolute deadline: a Date or ms-since-epoch. */
  deadline?: Date | number;
  /** Abort signal (maps to CANCELLED). */
  signal?: AbortSignal;
}

/** Wire codec for application messages. */
export type Codec = "proto" | "json";

/** Where unary calls go: real gRPC over h2ts (proto default), or the buffered-trailer Fetch path. */
export type UnaryTransport = "h2ts" | "fetch";
/**
 * Where streaming calls go: real gRPC over h2ts, H2-multiplexed over one WebSocket
 * (proto default), or the custom `Frame` protocol, one WebSocket per stream.
 */
export type StreamTransport = "h2ts" | "ws";

export interface ClientOptions {
  /** Base URL, e.g. "http://localhost:8080" or "https://api.example.com". */
  baseUrl: string;
  maxMessageBytes?: number;
  /** Override the WebSocket constructor (node needs the `ws` package). */
  webSocketImpl?: typeof WebSocket;
  fetch?: typeof fetch;
  /** Message codec: binary protobuf (default) or JSON. */
  codec?: Codec;
  /**
   * Where unary calls go. Proto default `"h2ts"` (real gRPC over one HTTP/2 tunnel);
   * `"fetch"` uses the buffered-trailer Fetch path. Forced to `"fetch"` for `codec: "json"`.
   */
  unary?: UnaryTransport;
  /**
   * Where streaming calls go. Proto default `"h2ts"` (H2-multiplexed over one WebSocket);
   * `"ws"` uses the custom Frame protocol, one WebSocket per stream. Forced to `"ws"`
   * for `codec: "json"`.
   */
  streaming?: StreamTransport;
  /**
   * How many response messages queue for a `for await` consumer before the transport
   * is asked to stop reading (default 32). Honored for real on the h2ts path, where
   * not reading blocks the server; advisory on the `ws` path, which cannot stop a
   * browser WebSocket from delivering. See `ClientReadableStream.deliver`.
   */
  readableHighWaterMark?: number;
  /**
   * Retry policy, **off unless set** — as in gRPC, where retries come from service config.
   * `{}` takes every default (5 attempts, 100 ms → 2 s, ×2, on UNAVAILABLE). Applies to
   * unary and to server-streaming *before its first message*; see the README for why
   * client-streaming and bidi are excluded.
   *
   * Reconnecting a dropped tunnel is **not** this: an RPC that never reached the server is
   * replayed transparently whatever this says.
   */
  retry?: RetryPolicy;
  /**
   * gRPC's retry throttle — a token bucket shared by every call, so a server that is
   * failing everything cannot be retried into the ground. Opt-in; recommended whenever
   * `retry` is set.
   */
  retryThrottling?: RetryThrottling;
}

/**
 * Resolve the per-client transport selection from the config, applying the JSON lock and
 * rejecting unsupported combinations. Exposed for testing.
 *
 * - `codec: "json"` is locked to `{ unary: "fetch", streaming: "ws" }` (JSON stays
 *   plaintext and never rides h2ts); combining it with an h2ts knob throws.
 * - proto defaults to `{ unary: "h2ts", streaming: "h2ts" }`; an explicit `streaming: "ws"`
 *   defaults unary to `"fetch"` (its only valid pairing).
 * - `{ unary: "h2ts", streaming: "ws" }` (both explicit) throws: an h2ts unary already opens
 *   the tunnel, so streaming should ride it too (or send unary over Fetch).
 */
export function resolveTransportSelection(options: {
  codec?: Codec;
  unary?: UnaryTransport;
  streaming?: StreamTransport;
}): { unary: UnaryTransport; streaming: StreamTransport } {
  if ((options.codec ?? "proto") === "json") {
    if (options.unary === "h2ts" || options.streaming === "h2ts") {
      throw new Error(
        "grpc-webnext: codec 'json' cannot use h2ts — JSON stays plaintext (unary over Fetch, one WebSocket per stream)",
      );
    }
    return { unary: "fetch", streaming: "ws" };
  }
  const streaming = options.streaming ?? "h2ts";
  // `ws` streaming pairs only with `fetch` unary, so default unary to match.
  const unary = options.unary ?? (streaming === "ws" ? "fetch" : "h2ts");
  if (unary === "h2ts" && streaming === "ws") {
    throw new Error(
      "grpc-webnext: { unary: 'h2ts', streaming: 'ws' } is unsupported — an h2ts unary opens the tunnel, so use streaming: 'h2ts', or unary: 'fetch' to keep streams on 'ws'",
    );
  }
  return { unary, streaming };
}

type Serialize<T> = (value: T) => Uint8Array;
type Deserialize<T> = (bytes: Uint8Array) => T;

/**
 * Base client, mirroring @grpc/grpc-js `Client`. Generated service stubs extend
 * this and call the `make*Request` methods. Unary uses Fetch; streaming uses
 * WebSocket — matching the grpc-webnext protocol split.
 */
export class Client {
  private readonly unaryTransport: Transport;
  private readonly streamTransport: Transport;
  private readonly readableHighWaterMark?: number;
  private readonly policy: ReturnType<typeof resolvePolicy> | null;
  private readonly throttle: Throttle | null;

  constructor(options: ClientOptions) {
    const selection = resolveTransportSelection(options);
    this.readableHighWaterMark = options.readableHighWaterMark;
    this.policy = options.retry ? resolvePolicy(options.retry) : null;
    this.throttle = options.retryThrottling ? new Throttle(options.retryThrottling) : null;
    // When either surface uses h2ts, share ONE H2Connection across both (H2 multiplexes).
    const h2ts =
      selection.unary === "h2ts" || selection.streaming === "h2ts"
        ? new H2tsTransport({ baseUrl: options.baseUrl, webSocketImpl: options.webSocketImpl })
        : undefined;

    this.unaryTransport =
      selection.unary === "h2ts"
        ? h2ts!
        : new FetchTransport({
            baseUrl: options.baseUrl,
            maxMessageBytes: options.maxMessageBytes,
            fetch: options.fetch,
            codec: options.codec,
          });

    this.streamTransport =
      selection.streaming === "h2ts"
        ? h2ts!
        : new WebSocketTransport({
            baseUrl: options.baseUrl,
            webSocketImpl: options.webSocketImpl,
            codec: options.codec,
          });
  }

  close(): void {
    this.unaryTransport.close();
    this.streamTransport.close();
  }

  /**
   * The channel's connectivity state, or `undefined` when the configured transport
   * keeps no persistent connection.
   *
   * That `undefined` is the honest answer, not a gap: only h2ts has a channel to
   * report on. Fetch is stateless, and the custom `Frame` path opens one WebSocket
   * per stream — a call there brings its own connection, so there is nothing that
   * could be "down" between calls. An app can read `undefined` as "no banner to
   * show".
   */
  getConnectivityState(): ConnectivityState | undefined {
    return this.channel?.current;
  }

  /**
   * Watch connectivity transitions; returns an unsubscribe. Repeats are collapsed,
   * so every call is a real change. A no-op (and never fires) on a transport with
   * no persistent connection — see {@link getConnectivityState}.
   *
   * The Rust WASM client exposes the same states as `Client::state_changes`.
   */
  watchConnectivityState(listener: ConnectivityListener): () => void {
    return this.channel?.watch(listener) ?? (() => {});
  }

  /** The transport that owns a persistent connection, if any. */
  private get channel() {
    return this.streamTransport.connectivity ?? this.unaryTransport.connectivity;
  }

  makeUnaryRequest<Req, Res>(
    path: string,
    serialize: Serialize<Req>,
    deserialize: Deserialize<Res>,
    argument: Req,
    metadata: Metadata,
    options: CallOptions,
    callback: RequestCallback<Res>,
  ): ClientUnaryCall {
    const ctx = createCallContext(options?.deadline, options?.signal);
    const call = new ClientUnaryCall(() => ctx.abort());
    const request = serialize(argument);

    void (async () => {
      // A unary call is the easy retry case: nothing has been delivered to the caller, so
      // any attempt can simply be thrown away. The deadline needs no special handling —
      // it aborts `ctx.signal`, which ends both the attempt and the backoff.
      for (let attempt = 1; ; attempt++) {
        let res: UnaryResponse | undefined;
        let status: StatusResult;
        try {
          res = await this.unaryTransport.unary(path, request, transportOptions(metadata, ctx));
          status = res.status;
        } catch (e) {
          const err = errorForFailure(ctx.signal, e);
          status = { code: err.code, details: err.details, metadata: new Metadata() };
        }

        const delay = this.policy ? nextDelay(this.policy, this.throttle, status, attempt) : null;
        if (delay !== null) {
          this.throttle?.onFailure();
          if (await sleep(delay, ctx.signal)) continue;
        } else if (status.code === Status.OK) {
          this.throttle?.onSuccess();
        } else if (this.policy?.retryableStatusCodes.has(status.code)) {
          this.throttle?.onFailure();
        }

        ctx.dispose();
        if (res) {
          call.emit("metadata", res.headers);
          call.emit("status", res.status);
        }
        const err = statusToError(status);
        if (err) callback(err);
        else callback(null, deserialize(res!.message));
        return;
      }
    })();

    return call;
  }

  makeServerStreamRequest<Req, Res>(
    path: string,
    serialize: Serialize<Req>,
    deserialize: Deserialize<Res>,
    argument: Req,
    metadata: Metadata,
    options: CallOptions,
  ): ClientReadableStream<Res> {
    const ctx = createCallContext(options?.deadline, options?.signal);
    const request = serialize(argument);
    let stream!: ClientReadableStream<Res>;
    let call: StreamCall;
    let attempt = 1;
    // gRPC's commit rule: an RPC may be replayed until the caller has been shown
    // something that a replay would contradict — here, the first response message.
    // After that the attempt is committed and its status is the call's status.
    let committed = false;
    // Held rather than emitted, because a retried attempt would emit `metadata` twice.
    // Headers alone do not commit the RPC; the first message does.
    let headers: Metadata | undefined;

    const commit = () => {
      if (!committed) {
        committed = true;
        if (headers) stream.emit("metadata", headers);
      }
    };

    const start = () => {
      call = this.streamTransport.startStream(path, transportOptions(metadata, ctx), {
        onHeaders: (md) => (headers = md),
        onMessage: (bytes) => {
          commit();
          return stream.deliver(deserialize(bytes));
        },
        onStatus: (status) => {
          if (!committed) {
            const delay = this.policy ? nextDelay(this.policy, this.throttle, status, attempt) : null;
            if (delay !== null) {
              this.throttle?.onFailure();
              attempt++;
              void sleep(delay, ctx.signal).then((ok) => (ok ? start() : finish(status)));
              return;
            }
            if (status.code === Status.OK) this.throttle?.onSuccess();
            else if (this.policy?.retryableStatusCodes.has(status.code)) this.throttle?.onFailure();
          }
          finish(status);
        },
      });
      call.send(request);
      call.halfClose();
    };

    const finish = (status: StatusResult) => {
      commit();
      ctx.dispose();
      stream.emit("status", status);
      const err = statusToError(status);
      if (err) stream.emit("error", err);
      else stream.emit("end");
    };

    start();
    stream = new ClientReadableStream<Res>(() => call.cancel(), this.readableHighWaterMark);
    return stream;
  }

  makeClientStreamRequest<Req, Res>(
    path: string,
    serialize: Serialize<Req>,
    deserialize: Deserialize<Res>,
    metadata: Metadata,
    options: CallOptions,
    callback: RequestCallback<Res>,
  ): ClientWritableStream<Req> {
    const ctx = createCallContext(options?.deadline, options?.signal);
    let last: Res | undefined;
    const call = this.streamTransport.startStream(path, transportOptions(metadata, ctx), {
      onMessage: (bytes) => {
        last = deserialize(bytes);
      },
      onStatus: (status) => {
        ctx.dispose();
        const err = statusToError(status);
        if (err) callback(err);
        else callback(null, last);
      },
    });
    return new ClientWritableStream<Req>(call, serialize);
  }

  makeBidiStreamRequest<Req, Res>(
    path: string,
    serialize: Serialize<Req>,
    deserialize: Deserialize<Res>,
    metadata: Metadata,
    options: CallOptions,
  ): ClientDuplexStream<Req, Res> {
    // Not retried, deliberately: replaying a request stream means holding every message
    // the caller has written for as long as the RPC might still be replayed, and an
    // unbounded client-side buffer is exactly what this client declined to grow on the
    // response side. Client-streaming (below) is excluded for the same reason.
    const ctx = createCallContext(options?.deadline, options?.signal);
    let stream!: ClientDuplexStream<Req, Res>;
    const call = this.streamTransport.startStream(
      path,
      transportOptions(metadata, ctx),
      streamHandlers(() => stream, deserialize, ctx),
    );
    stream = new ClientDuplexStream<Req, Res>(call, serialize, this.readableHighWaterMark);
    return stream;
  }
}

function streamHandlers<Res>(
  getStream: () => ClientReadableStream<Res>,
  deserialize: Deserialize<Res>,
  ctx: CallContext,
) {
  return {
    onHeaders: (metadata: Metadata) => getStream().emit("metadata", metadata),
    // `deliver` (not `emit("data")`) so a slow consumer can push back: it returns a
    // promise while the consumer is behind, which the h2ts transport awaits.
    onMessage: (bytes: Uint8Array) => getStream().deliver(deserialize(bytes)),
    onStatus: (status: StatusResult) => {
      ctx.dispose();
      const stream = getStream();
      stream.emit("status", status);
      const err = statusToError(status);
      if (err) stream.emit("error", err);
      else stream.emit("end");
    },
  };
}

function transportOptions(metadata: Metadata, ctx: CallContext): TransportCallOptions {
  return {
    metadata: metadata ?? new Metadata(),
    timeoutMillis: ctx.timeoutMillis,
    signal: ctx.signal,
  };
}

/** Map a transport failure to a ServiceError. An aborted signal means the call
 * was cancelled or timed out (fetch rejects with the abort reason, not always an
 * AbortError), so classify by the signal, not the error shape.
 *
 * Anything else is the transport failing to carry the call — a refused dial, a dropped
 * tunnel, a network error. gRPC's status for that is UNAVAILABLE, which is also what
 * makes it retryable under the default policy; it used to report UNKNOWN, which is the
 * status for "the server did something we could not interpret". */
function errorForFailure(signal: AbortSignal, e: unknown): ServiceError {
  if (signal.aborted) {
    const status = statusForAbort(signal);
    return new ServiceError(status.code, status.details);
  }
  return new ServiceError(Status.UNAVAILABLE, e instanceof Error ? e.message : String(e));
}

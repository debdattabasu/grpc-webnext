import type { ConnectivityWatch } from "./connectivity.js";
import type { Metadata } from "./metadata.js";
import type { Status } from "./status.js";

/**
 * Options passed to a transport for a single call. Deadlines are already
 * resolved to a relative `timeoutMillis` by the client layer.
 */
export interface TransportCallOptions {
  metadata: Metadata;
  /** Relative deadline in ms (0/undefined = none). */
  timeoutMillis?: number;
  /** Aborts the call (maps to gRPC CANCELLED). */
  signal?: AbortSignal;
}

/** Result of a unary call. */
export interface UnaryResponse {
  message: Uint8Array;
  headers: Metadata;
  status: StatusResult;
}

export interface StatusResult {
  code: Status;
  details: string;
  metadata: Metadata;
}

/** Event callbacks for a streaming call. All are optional. */
export interface StreamHandlers {
  onHeaders?(metadata: Metadata): void;
  /**
   * One response message. Returning a promise means the consumer has fallen
   * behind: a transport that can stop receiving SHOULD await it before reading
   * again. On the h2ts path that is real backpressure — not reading stops HTTP/2
   * window replenishment, so the server blocks. The custom `Frame` WebSocket path
   * cannot: the browser WebSocket API has no receive-side flow control, so it
   * ignores the promise and the messages queue client-side.
   */
  onMessage?(message: Uint8Array): void | Promise<void>;
  onStatus?(status: StatusResult): void;
}

/** A live streaming call at the byte level. */
export interface StreamCall {
  /** Send one request message. */
  send(message: Uint8Array): void;
  /** Signal the client is done sending. */
  halfClose(): void;
  /** Abort the call (CANCELLED). */
  cancel(): void;
}

/**
 * A grpc-webnext transport. Unary goes over Fetch; streaming over WebSocket.
 * Both deal in raw message bytes so the transport is serializer-agnostic.
 */
export interface Transport {
  unary(path: string, request: Uint8Array, options: TransportCallOptions): Promise<UnaryResponse>;
  startStream(path: string, options: TransportCallOptions, handlers: StreamHandlers): StreamCall;
  close(): void;
  /**
   * Connectivity, for a transport that keeps a **persistent connection** — only
   * h2ts. Absent on the others, and that absence is the honest answer rather than
   * an omission: Fetch is stateless, and the custom `Frame` path opens one
   * WebSocket per stream, so on those paths there is no channel whose state could
   * be reported. See `Client.getConnectivityState`.
   */
  readonly connectivity?: ConnectivityWatch;
}

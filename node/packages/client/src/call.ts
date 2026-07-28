import { Emitter } from "./emitter.js";
import type { Metadata } from "./metadata.js";
import { ServiceError, Status } from "./status.js";
import type { StatusResult, StreamCall } from "./transport.js";

/** grpc-js-style callback for unary / client-streaming responses. */
export type RequestCallback<Res> = (
  error: ServiceError | null,
  value?: Res,
) => void;

interface CallEvents {
  metadata: (metadata: Metadata) => void;
  status: (status: StatusResult) => void;
}

/** Handle for a unary call; mirrors grpc-js `ClientUnaryCall`. */
export class ClientUnaryCall extends Emitter<CallEvents> {
  constructor(private readonly canceller: () => void) {
    super();
  }
  cancel(): void {
    this.canceller();
  }
}

interface ReadableEvents<Res> extends CallEvents {
  data: (message: Res) => void;
  end: () => void;
  error: (error: ServiceError) => void;
}

/**
 * How many messages queue for an async-iterable consumer before the transport is
 * asked to stop reading. Only a transport that *can* stop obeys — see `deliver`.
 */
export const DEFAULT_READABLE_HIGH_WATER_MARK = 32;

/**
 * Server -> client message stream; mirrors grpc-js `ClientReadableStream`.
 *
 * Two consumption modes, as in Node: attaching a `data` listener puts the stream
 * in **flowing** mode and messages are handed straight over, never queued;
 * with no `data` listener they queue for the async iterator (`for await`).
 * Using both is not supported — the listener wins and the iterator starves.
 */
export class ClientReadableStream<Res> extends Emitter<ReadableEvents<Res>> {
  private readonly queue: Res[] = [];
  private ended = false;
  private errored: ServiceError | null = null;
  private paused = false;
  private waiter: {
    resolve: (r: IteratorResult<Res>) => void;
    reject: (e: unknown) => void;
  } | null = null;
  /** Resolvers for deliveries parked because the consumer is behind. */
  private readonly ready: (() => void)[] = [];

  private readonly highWaterMark: number;

  constructor(
    private readonly canceller: () => void,
    highWaterMark: number = DEFAULT_READABLE_HIGH_WATER_MARK,
  ) {
    super();
    // At least one: a mark of 0 would park every delivery with nothing able to
    // release it, since the queue can never be shorter than empty.
    this.highWaterMark = Math.max(1, Math.floor(highWaterMark));
    this.on("end", () => this.finish());
    this.on("error", (e) => this.fail(e));
  }

  cancel(): void {
    this.canceller();
  }

  /** Messages received but not yet consumed. Zero in flowing mode. */
  get readableLength(): number {
    return this.queue.length;
  }

  /** Stop delivering messages. Queued and inbound messages accumulate until `resume()`. */
  pause(): void {
    this.paused = true;
  }

  /** Resume delivery, flushing anything queued while paused. */
  resume(): void {
    if (!this.paused) return;
    this.paused = false;
    // Hand the backlog to a flowing consumer; an iterating one drains it itself.
    while (this.queue.length && !this.paused && this.emit("data", this.queue[0])) {
      this.queue.shift();
    }
    if (this.queue.length && this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w.resolve({ value: this.queue.shift() as Res, done: false });
    }
    this.release();
  }

  /**
   * Deliver one message from the transport.
   *
   * Returns a promise when the consumer has fallen behind (queue at the
   * high-water mark, or paused), resolving once it catches up. A transport that
   * awaits it gets **real end-to-end backpressure**: the h2ts path stops reading
   * the response body, so HTTP/2 windows stop being replenished and the server
   * itself blocks. The custom `Frame` WebSocket path cannot honor it — the
   * browser WebSocket API has no receive-side flow control, `onmessage` fires
   * regardless — so there it queues and this signal is advisory (`readableLength`
   * is how an application sees the backlog).
   */
  deliver(message: Res): void | Promise<void> {
    if (this.ended || this.errored) return;
    if (!this.paused) {
      // Flowing: a `data` listener takes the message and nothing is retained.
      if (this.emit("data", message)) return;
      if (this.waiter) {
        const w = this.waiter;
        this.waiter = null;
        w.resolve({ value: message, done: false });
        return;
      }
    }
    this.queue.push(message);
    if (!this.paused && this.queue.length < this.highWaterMark) return;
    return new Promise<void>((resolve) => this.ready.push(resolve));
  }

  /** Let a parked transport read again, once the consumer is back under the mark. */
  private release(): void {
    if (this.paused || this.queue.length >= this.highWaterMark) return;
    for (const resolve of this.ready.splice(0)) resolve();
  }

  private finish(): void {
    this.ended = true;
    // Nothing more will arrive, so a parked transport must not stay parked.
    for (const resolve of this.ready.splice(0)) resolve();
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      if (this.errored) w.reject(this.errored);
      else w.resolve({ value: undefined, done: true });
    }
  }
  private fail(e: ServiceError): void {
    this.errored = e;
    this.finish();
  }

  [Symbol.asyncIterator](): AsyncIterator<Res> {
    return {
      next: (): Promise<IteratorResult<Res>> => {
        // Deliver any queued messages before surfacing an error/end.
        if (this.queue.length) {
          const value = this.queue.shift() as Res;
          this.release();
          return Promise.resolve({ value, done: false });
        }
        if (this.errored) return Promise.reject(this.errored);
        if (this.ended) return Promise.resolve({ value: undefined, done: true });
        return new Promise((resolve, reject) => (this.waiter = { resolve, reject }));
      },
    };
  }
}

/** Client -> server message stream; mirrors grpc-js `ClientWritableStream`. */
export class ClientWritableStream<Req> extends Emitter<CallEvents> {
  constructor(
    private readonly call: StreamCall,
    private readonly serialize: (req: Req) => Uint8Array,
  ) {
    super();
  }
  write(message: Req): void {
    this.call.send(this.serialize(message));
  }
  end(): void {
    this.call.halfClose();
  }
  cancel(): void {
    this.call.cancel();
  }
}

/** Bidi stream; mirrors grpc-js `ClientDuplexStream`. */
export class ClientDuplexStream<Req, Res> extends ClientReadableStream<Res> {
  constructor(
    private readonly duplex: StreamCall,
    private readonly serializeReq: (req: Req) => Uint8Array,
    highWaterMark?: number,
  ) {
    super(() => duplex.cancel(), highWaterMark);
  }
  write(message: Req): void {
    this.duplex.send(this.serializeReq(message));
  }
  end(): void {
    this.duplex.halfClose();
  }
}

/** Convert a non-OK status into a ServiceError, or null if OK. */
export function statusToError(status: StatusResult): ServiceError | null {
  if (status.code === Status.OK) return null;
  return new ServiceError(status.code, status.details, status.metadata);
}

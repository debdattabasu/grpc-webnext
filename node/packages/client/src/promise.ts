import { Client, ClientOptions } from "./client.js";
import type { ConnectivityListener, ConnectivityState } from "./connectivity.js";
import { Metadata } from "./metadata.js";
import { methodCodec, type MethodInfo, type Serializer, type ServiceDefinition } from "./service.js";
import type { StatusResult } from "./transport.js";

/**
 * Options for a promise-based call. Cancellation is via `signal` (the call's
 * `context`), deadline via `deadline`; response metadata is surfaced through
 * the `onHeader` / `onTrailer` callbacks.
 */
export interface PromiseCallOptions {
  deadline?: Date | number;
  signal?: AbortSignal;
  metadata?: Metadata;
  onHeader?(metadata: Metadata): void;
  onTrailer?(metadata: Metadata): void;
}

type ReqOf<M> = M extends { requestType: Serializer<infer Req> } ? Req : never;
type ResOf<M> = M extends { responseType: Serializer<infer Res> } ? Res : never;

type UnaryFn<Req, Res> = (request: Req, options?: PromiseCallOptions) => Promise<Res>;
type ServerStreamFn<Req, Res> = (
  request: Req,
  options?: PromiseCallOptions,
) => AsyncIterable<Res>;
type ClientStreamFn<Req, Res> = (
  source: AsyncIterable<Req>,
  options?: PromiseCallOptions,
) => Promise<Res>;
type BidiFn<Req, Res> = (
  source: AsyncIterable<Req>,
  options?: PromiseCallOptions,
) => AsyncIterable<Res>;

type PromiseMethodFn<M extends MethodInfo> = M["requestStream"] extends false
  ? M["responseStream"] extends false
    ? UnaryFn<ReqOf<M>, ResOf<M>>
    : ServerStreamFn<ReqOf<M>, ResOf<M>>
  : M["responseStream"] extends false
    ? ClientStreamFn<ReqOf<M>, ResOf<M>>
    : BidiFn<ReqOf<M>, ResOf<M>>;

/** The promise-based client surface for a service definition. */
export type PromiseServiceClient<Def extends ServiceDefinition> = {
  [K in keyof Def["methods"]]: PromiseMethodFn<Def["methods"][K]>;
} & {
  close(): void;
  /** See {@link Client.getConnectivityState}. */
  getConnectivityState(): ConnectivityState | undefined;
  /** See {@link Client.watchConnectivityState}. */
  watchConnectivityState(listener: ConnectivityListener): () => void;
};

/**
 * Build a promise-based client from a ts-proto service definition. Unary and
 * client-streaming return `Promise<Res>`; server-streaming and bidi return
 * `AsyncIterable<Res>` (consume with `for await`). Companion to `makeClient`,
 * which is the callback/EventEmitter flavor.
 */
export function makePromiseClient<Def extends ServiceDefinition>(
  definition: Def,
  options: ClientOptions,
): PromiseServiceClient<Def> {
  const client = new Client(options);
  const result: Record<string, unknown> = {
    close: () => client.close(),
    getConnectivityState: () => client.getConnectivityState(),
    watchConnectivityState: (l: ConnectivityListener) => client.watchConnectivityState(l),
  };

  for (const key of Object.keys(definition.methods)) {
    const m = definition.methods[key];
    const path = `/${definition.fullName}/${m.name}`;
    const { serialize, deserialize } = methodCodec(m, options.codec);
    result[key] = makeMethod(client, m, path, serialize, deserialize);
  }
  return result as PromiseServiceClient<Def>;
}

function makeMethod(
  client: Client,
  m: MethodInfo,
  path: string,
  serialize: (req: any) => Uint8Array,
  deserialize: (bytes: Uint8Array) => any,
): (...args: any[]) => unknown {
  const base = (o?: PromiseCallOptions) => ({
    metadata: o?.metadata ?? new Metadata(),
    callOptions: { deadline: o?.deadline, signal: o?.signal },
  });
  const wireMeta = (call: { on: (e: any, fn: any) => unknown }, o?: PromiseCallOptions) => {
    if (o?.onHeader) call.on("metadata", o.onHeader);
    if (o?.onTrailer) call.on("status", (s: StatusResult) => o.onTrailer!(s.metadata));
  };

  if (!m.requestStream && !m.responseStream) {
    return (request: any, o?: PromiseCallOptions) =>
      new Promise((resolve, reject) => {
        const { metadata, callOptions } = base(o);
        const call = client.makeUnaryRequest(
          path,
          serialize,
          deserialize,
          request,
          metadata,
          callOptions,
          (err, val) => (err ? reject(err) : resolve(val)),
        );
        wireMeta(call, o);
      });
  }

  if (!m.requestStream && m.responseStream) {
    return (request: any, o?: PromiseCallOptions) => {
      const { metadata, callOptions } = base(o);
      const stream = client.makeServerStreamRequest(
        path,
        serialize,
        deserialize,
        request,
        metadata,
        callOptions,
      );
      wireMeta(stream, o);
      return stream;
    };
  }

  if (m.requestStream && !m.responseStream) {
    return (source: AsyncIterable<any>, o?: PromiseCallOptions) =>
      new Promise((resolve, reject) => {
        const { metadata, callOptions } = base(o);
        const writable = client.makeClientStreamRequest(
          path,
          serialize,
          deserialize,
          metadata,
          callOptions,
          (err, val) => (err ? reject(err) : resolve(val)),
        );
        wireMeta(writable, o);
        void pump(source, (req) => writable.write(req), () => writable.end(), () => {
          writable.cancel();
          reject(new Error("request stream failed"));
        });
      });
  }

  return (source: AsyncIterable<any>, o?: PromiseCallOptions) => {
    const { metadata, callOptions } = base(o);
    const duplex = client.makeBidiStreamRequest(
      path,
      serialize,
      deserialize,
      metadata,
      callOptions,
    );
    wireMeta(duplex, o);
    void pump(source, (req) => duplex.write(req), () => duplex.end(), () => duplex.cancel());
    return duplex;
  };
}

/** Drain a request source into a writer, ending on completion, cancelling on error. */
async function pump<Req>(
  source: AsyncIterable<Req>,
  write: (req: Req) => void,
  end: () => void,
  onError: () => void,
): Promise<void> {
  try {
    for await (const req of source) write(req);
    end();
  } catch {
    onError();
  }
}

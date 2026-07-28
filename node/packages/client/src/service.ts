import {
  ClientDuplexStream,
  ClientReadableStream,
  ClientUnaryCall,
  ClientWritableStream,
  RequestCallback,
} from "./call.js";
import { CallOptions, Client, ClientOptions } from "./client.js";
import type { ConnectivityListener, ConnectivityState } from "./connectivity.js";
import { Metadata } from "./metadata.js";

/** Minimal message serializer shape (satisfied by ts-proto message types). */
export interface Serializer<T> {
  encode(message: T): { finish(): Uint8Array };
  decode(input: Uint8Array): T;
  toJSON?(message: T): unknown;
  fromJSON?(object: unknown): T;
}

/** Build byte-level (de)serializers for a method from the chosen codec. */
export function methodCodec(
  m: MethodInfo,
  codec: "proto" | "json" | undefined,
): { serialize: (req: any) => Uint8Array; deserialize: (bytes: Uint8Array) => any } {
  if (codec === "json") {
    const enc = new TextEncoder();
    const dec = new TextDecoder();
    return {
      serialize: (req) => enc.encode(JSON.stringify(m.requestType.toJSON!(req))),
      deserialize: (bytes) => m.responseType.fromJSON!(JSON.parse(dec.decode(bytes))),
    };
  }
  return {
    serialize: (req) => m.requestType.encode(req).finish(),
    deserialize: (bytes) => m.responseType.decode(bytes),
  };
}

/** One method of a service definition (ts-proto `generic-definitions` shape). */
export interface MethodInfo<Req = any, Res = any> {
  name: string;
  requestType: Serializer<Req>;
  requestStream: boolean;
  responseType: Serializer<Res>;
  responseStream: boolean;
  options?: unknown;
}

/** A ts-proto `generic-definitions` service definition. */
export interface ServiceDefinition {
  name: string;
  fullName: string;
  methods: Record<string, MethodInfo>;
}

type ReqOf<M> = M extends { requestType: Serializer<infer Req> } ? Req : never;
type ResOf<M> = M extends { responseType: Serializer<infer Res> } ? Res : never;

interface UnaryFn<Req, Res> {
  (request: Req, callback: RequestCallback<Res>): ClientUnaryCall;
  (request: Req, metadata: Metadata, callback: RequestCallback<Res>): ClientUnaryCall;
  (
    request: Req,
    metadata: Metadata,
    options: CallOptions,
    callback: RequestCallback<Res>,
  ): ClientUnaryCall;
}
type ServerStreamFn<Req, Res> = (
  request: Req,
  metadata?: Metadata,
  options?: CallOptions,
) => ClientReadableStream<Res>;
interface ClientStreamFn<Req, Res> {
  (callback: RequestCallback<Res>): ClientWritableStream<Req>;
  (metadata: Metadata, callback: RequestCallback<Res>): ClientWritableStream<Req>;
  (
    metadata: Metadata,
    options: CallOptions,
    callback: RequestCallback<Res>,
  ): ClientWritableStream<Req>;
}
type BidiFn<Req, Res> = (
  metadata?: Metadata,
  options?: CallOptions,
) => ClientDuplexStream<Req, Res>;

type MethodFn<M extends MethodInfo> = M["requestStream"] extends false
  ? M["responseStream"] extends false
    ? UnaryFn<ReqOf<M>, ResOf<M>>
    : ServerStreamFn<ReqOf<M>, ResOf<M>>
  : M["responseStream"] extends false
    ? ClientStreamFn<ReqOf<M>, ResOf<M>>
    : BidiFn<ReqOf<M>, ResOf<M>>;

/** The generated client surface for a service definition. */
export type ServiceClient<Def extends ServiceDefinition> = {
  [K in keyof Def["methods"]]: MethodFn<Def["methods"][K]>;
} & {
  close(): void;
  /** See {@link Client.getConnectivityState}. */
  getConnectivityState(): ConnectivityState | undefined;
  /** See {@link Client.watchConnectivityState}. */
  watchConnectivityState(listener: ConnectivityListener): () => void;
};

/**
 * Build a typed, grpc-js-shaped client from a ts-proto service definition,
 * analogous to grpc-js `makeGenericClientConstructor`. Unary methods take a
 * callback; streaming methods return call objects.
 */
export function makeClient<Def extends ServiceDefinition>(
  definition: Def,
  options: ClientOptions,
): ServiceClient<Def> {
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

  return result as ServiceClient<Def>;
}

function makeMethod(
  client: Client,
  m: MethodInfo,
  path: string,
  serialize: (req: any) => Uint8Array,
  deserialize: (bytes: Uint8Array) => any,
): (...args: any[]) => unknown {
  if (!m.requestStream && !m.responseStream) {
    return (request: any, ...rest: any[]) => {
      const { metadata, options, callback } = normalize(rest);
      return client.makeUnaryRequest(path, serialize, deserialize, request, metadata, options, callback!);
    };
  }
  if (!m.requestStream && m.responseStream) {
    return (request: any, ...rest: any[]) => {
      const { metadata, options } = normalize(rest);
      return client.makeServerStreamRequest(path, serialize, deserialize, request, metadata, options);
    };
  }
  if (m.requestStream && !m.responseStream) {
    return (...rest: any[]) => {
      const { metadata, options, callback } = normalize(rest);
      return client.makeClientStreamRequest(path, serialize, deserialize, metadata, options, callback!);
    };
  }
  return (...rest: any[]) => {
    const { metadata, options } = normalize(rest);
    return client.makeBidiStreamRequest(path, serialize, deserialize, metadata, options);
  };
}

/** Normalize the optional (metadata, options, callback) trailing arguments. */
function normalize(args: any[]): {
  metadata: Metadata;
  options: CallOptions;
  callback?: RequestCallback<any>;
} {
  const rest = [...args];
  let callback: RequestCallback<any> | undefined;
  if (typeof rest[rest.length - 1] === "function") callback = rest.pop();
  const metadata = rest[0] instanceof Metadata ? (rest.shift() as Metadata) : new Metadata();
  const options = (rest[0] as CallOptions) ?? {};
  return { metadata, options, callback };
}

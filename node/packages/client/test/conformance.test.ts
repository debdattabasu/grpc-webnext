//! The grpc-webnext conformance runner (TS client driver).
//!
//! Loads the language-neutral cases in `conformance/cases/*.yaml` and drives each one
//! against **every** server implementation — the Rust and Go `conformance-server`s, which
//! each serve `ConformanceService` over grpc-webnext — under every applicable transport
//! profile: the h2ts binary path, the custom `Frame` binary path, and the JSON custom
//! path. That `{server impls} × {cases} × {transports} × {codecs}` matrix is the
//! cross-implementation anti-drift guard, exercised over the real wire.

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { load } from "js-yaml";
import WebSocket from "ws";
import { makeClient, Metadata, Status, type ClientOptions } from "../src/index.js";
import {
  // Imported as *values*, not just types: the REST driver decodes bare JSON
  // response bodies with `fromJSON`, since a REST call carries no protobuf framing
  // for the client to unwrap.
  ClientStreamResponse,
  ConformancePayload,
  ConformanceServiceDefinition,
  type Metadatum,
  type ResponseDefinition,
} from "./gen/conformance.js";
import type { StatusResult } from "../src/transport.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "../../../..");
const rustRoot = path.join(repoRoot, "rust");
const goRoot = path.join(repoRoot, "go");
const casesDir = path.join(repoRoot, "conformance", "cases");
const wsImpl = WebSocket as unknown as typeof globalThis.WebSocket;

// --- case types (the YAML shape; see conformance/schema/case.schema.json) ---------------
type Bytes = { text?: string; b64?: string; zeros?: number };
type Meta = { key: string; ascii?: string; b64?: string };
type RD = {
  status_code?: number;
  status_message?: string;
  headers?: Meta[];
  trailers?: Meta[];
  payload?: Bytes;
  stream_messages?: { payload?: Bytes; delay_ms?: number }[];
  delay_ms?: number;
  oversize_response_bytes?: number;
};
type Msg = { payload?: Bytes; response_definition?: RD };
type Matcher = {
  payload?: Bytes;
  request_info?: { request_headers_contain?: Meta[]; timeout_present?: boolean; json?: boolean };
};
type Rest = { verb: string; path: string; body?: string; bodies?: string[]; content_type?: string };
type Case = {
  name: string;
  rpc: "Unary" | "ServerStream" | "ClientStream" | "BidiStream";
  rest?: Rest;
  codecs?: ("proto" | "json")[];
  timeout_millis?: number;
  header_timeout_millis?: number;
  request_metadata?: Meta[];
  requires?: { max_message_bytes?: number; transcoder?: boolean };
  request?: Msg;
  requests?: Msg[];
  cancel_after_messages?: number;
  expect: {
    status?: { code?: number; message_contains?: string; not_ok?: boolean };
    http_status?: number;
    response?: Matcher;
    messages?: Matcher[];
    message_count?: number;
    raw_body?: string;
    raw_messages?: string[];
    received_count?: number;
    headers_contain?: Meta[];
    trailers_contain?: Meta[];
  };
};
type Suite = { suite: string; cases: Case[] };

// --- converters: case value -> proto / Metadata -----------------------------------------
const toBytes = (b?: Bytes): Uint8Array =>
  !b
    ? new Uint8Array()
    : b.zeros !== undefined
      ? new Uint8Array(b.zeros)
      : b.text !== undefined
        ? new TextEncoder().encode(b.text)
        : new Uint8Array(Buffer.from(b.b64 ?? "", "base64"));

const metaValue = (m: Meta): string | Uint8Array =>
  m.b64 !== undefined ? new Uint8Array(Buffer.from(m.b64, "base64")) : (m.ascii ?? "");

function toMetadata(items?: Meta[]): Metadata {
  const md = new Metadata();
  for (const m of items ?? []) md.set(m.key, metaValue(m));
  return md;
}

function toMetadatumList(items?: Meta[]): Metadatum[] {
  return (items ?? []).map((m) =>
    m.b64 !== undefined
      ? { key: m.key, asciiValue: undefined, binValue: new Uint8Array(Buffer.from(m.b64, "base64")) }
      : { key: m.key, asciiValue: m.ascii ?? "", binValue: undefined },
  );
}

function toRD(rd?: RD): ResponseDefinition {
  return {
    statusCode: rd?.status_code ?? 0,
    statusMessage: rd?.status_message ?? "",
    headers: toMetadatumList(rd?.headers),
    trailers: toMetadatumList(rd?.trailers),
    payload: toBytes(rd?.payload),
    streamMessages: (rd?.stream_messages ?? []).map((s) => ({
      payload: toBytes(s.payload),
      delayMs: s.delay_ms ?? 0,
    })),
    delayMs: rd?.delay_ms ?? 0,
    oversizeResponseBytes: rd?.oversize_response_bytes ?? 0,
  };
}

// --- transport profiles + server config profiles ----------------------------------------
type Profile = { name: string; config: Partial<ClientOptions> };
function profilesFor(c: Case): Profile[] {
  // A REST case names its own URL, which fixes both the codec (JSON, always) and
  // the transport (the spec's rule: a unary annotation URL is Fetch, a streaming
  // one is a WebSocket). Nothing left to permute — it runs once.
  if (c.rest) return [{ name: "rest", config: {} }];
  const codecs = c.codecs ?? ["proto", "json"];
  const out: Profile[] = [];
  if (codecs.includes("proto")) {
    out.push({ name: "proto/h2ts", config: {} });
    out.push({ name: "proto/ws", config: { unary: "fetch", streaming: "ws" } });
  }
  if (codecs.includes("json")) out.push({ name: "json", config: { codec: "json" } });
  return out;
}

const requiresKey = (r?: Case["requires"]): string => {
  const p: string[] = [];
  if (r?.max_message_bytes) p.push(`max:${r.max_message_bytes}`);
  if (r?.transcoder === false) p.push("notranscoder");
  return p.length ? p.join(",") : "default";
};
function requiresEnv(r?: Case["requires"]): Record<string, string> {
  const env: Record<string, string> = {};
  if (r?.max_message_bytes) env.CONFORMANCE_MAX_MESSAGE_BYTES = String(r.max_message_bytes);
  if (r?.transcoder === false) env.CONFORMANCE_TRANSCODER = "0";
  return env;
}

// --- driving a single call ---------------------------------------------------------------
interface Result {
  headers: Metadata;
  messages: ConformancePayload[];
  response?: ConformancePayload;
  status: StatusResult;
  /** REST only: the raw HTTP status, for surface rejections that never become an RPC. */
  httpStatus?: number;
  /** ClientStream only: how many request messages the server reports having received. */
  receivedCount?: number;
  /**
   * REST only: the response body / streamed messages as raw JSON text. A
   * `response_body` binding answers with a bare field value rather than the RPC's
   * response message, so there is nothing for a message matcher to decode.
   */
  rawBody?: string;
  rawMessages?: string[];
}
type Client = ReturnType<typeof makeClient<typeof ConformanceServiceDefinition>>;

function callOptions(c: Case) {
  // Deliberately ignores `header_timeout_millis`: that case exists to prove the
  // *server* enforces the deadline, so the client must not arm a timer of its own.
  return c.timeout_millis ? { deadline: Date.now() + c.timeout_millis } : {};
}

/** Request metadata, plus a raw `grpc-timeout` when the case asks the server to enforce it. */
function requestMetadata(c: Case): Metadata {
  const md = toMetadata(c.request_metadata);
  if (c.header_timeout_millis) md.set("grpc-timeout", `${c.header_timeout_millis}m`);
  return md;
}

function runUnary(client: Client, c: Case): Promise<Result> {
  const req = { payload: toBytes(c.request?.payload), responseDefinition: toRD(c.request?.response_definition) };
  return new Promise((resolve) => {
    let headers = new Metadata();
    let status: StatusResult | undefined;
    const call = client.unary(req, requestMetadata(c), callOptions(c), (err, value) => {
      // The error path (deadline/cancel/backend error) surfaces via the callback, not a
      // `status` event — derive the status from the ServiceError when it didn't fire.
      if (!status) {
        status = err
          ? { code: err.code, details: err.details ?? "", metadata: err.metadata ?? new Metadata() }
          : { code: Status.OK, details: "", metadata: new Metadata() };
      }
      resolve({ headers, messages: [], response: value as ConformancePayload | undefined, status });
    });
    call.on("metadata", (md) => (headers = md));
    call.on("status", (st) => (status = st));
  });
}

function runServerStream(client: Client, c: Case): Promise<Result> {
  const req = { responseDefinition: toRD(c.request?.response_definition) };
  return new Promise((resolve) => {
    let headers = new Metadata();
    const messages: ConformancePayload[] = [];
    let status: StatusResult;
    const stream = client.serverStream(req, requestMetadata(c), callOptions(c));
    stream.on("metadata", (md: Metadata) => (headers = md));
    stream.on("data", (m: ConformancePayload) => messages.push(m));
    stream.on("status", (st: StatusResult) => (status = st));
    const done = () => resolve({ headers, messages, status });
    stream.on("end", done);
    stream.on("error", done);
  });
}

function runClientStream(client: Client, c: Case): Promise<Result> {
  const reqs = (c.requests ?? []).map((r, i) => ({
    payload: toBytes(r.payload),
    responseDefinition: toRD(i === 0 ? r.response_definition : undefined),
  }));
  return new Promise((resolve) => {
    const stream = client.clientStream(toMetadata(c.request_metadata), callOptions(c), (err, value) => {
      const status: StatusResult = err
        ? { code: err.code, details: err.details ?? "", metadata: new Metadata() }
        : { code: Status.OK, details: "", metadata: new Metadata() };
      const cs = value as { payload?: ConformancePayload; receivedCount?: number } | undefined;
      resolve({
        headers: new Metadata(),
        messages: [],
        response: cs?.payload,
        receivedCount: cs?.receivedCount,
        status,
      });
    });
    for (const r of reqs) stream.write(r);
    stream.end();
  });
}

function runBidiStream(client: Client, c: Case): Promise<Result> {
  const reqs = (c.requests ?? []).map((r, i) => ({
    payload: toBytes(r.payload),
    responseDefinition: toRD(i === 0 ? r.response_definition : undefined),
  }));
  return new Promise((resolve) => {
    let headers = new Metadata();
    const messages: ConformancePayload[] = [];
    let status: StatusResult;
    const stream = client.bidiStream(toMetadata(c.request_metadata), callOptions(c));
    stream.on("metadata", (md: Metadata) => (headers = md));
    stream.on("data", (m: ConformancePayload) => {
      messages.push(m);
      if (c.cancel_after_messages && messages.length >= c.cancel_after_messages) stream.cancel();
    });
    stream.on("status", (st: StatusResult) => (status = st));
    const done = () => resolve({ headers, messages, status });
    stream.on("end", done);
    stream.on("error", done);
    for (const r of reqs) stream.write(r);
    // A cancel case keeps the stream open (so the server doesn't complete first) and
    // cancels from the data handler once it has seen `cancel_after_messages` messages.
    if (!c.cancel_after_messages) stream.end();
  });
}

function runCase(client: Client, c: Case): Promise<Result> {
  switch (c.rpc) {
    case "Unary":
      return runUnary(client, c);
    case "ServerStream":
      return runServerStream(client, c);
    case "ClientStream":
      return runClientStream(client, c);
    case "BidiStream":
      return runBidiStream(client, c);
  }
}

// --- driving a REST (google.api.http) call --------------------------------------------------
//
// These deliberately do NOT go through the client: the whole point of a REST alias is that a
// caller who knows nothing about grpc-webnext can use it with an ordinary HTTP request, so the
// driver issues one. `fetch` for a unary method's URL, a raw WebSocket for a streaming one —
// which is the spec's rule, not a choice this driver makes.

/**
 * A unary annotation URL over Fetch. The JSON surface carries the gRPC status in
 * `grpc-status`/`grpc-message` headers and the message as a bare JSON body, so the whole
 * response is one HTTP round trip with no framing to decode.
 */
async function runRestFetch(baseUrl: string, c: Case): Promise<Result> {
  const rest = c.rest!;
  const headers: Record<string, string> = {};
  if (rest.body !== undefined) headers["content-type"] = rest.content_type ?? "application/json";
  else if (rest.content_type) headers["content-type"] = rest.content_type;
  // A deadline is ordinary gRPC framing on the REST surface too — the same
  // `grpc-timeout` header, since a REST alias is a route, not a different protocol.
  if (c.timeout_millis) headers["grpc-timeout"] = `${c.timeout_millis}m`;
  for (const m of c.request_metadata ?? []) {
    // `-bin` metadata is base64 on the HTTP wire; the driver spells it the same way.
    headers[m.key] = m.b64 !== undefined ? m.b64 : (m.ascii ?? "");
  }

  const resp = await fetch(baseUrl + rest.path, { method: rest.verb, headers, body: rest.body });
  const text = await resp.text();

  // Initial and trailing metadata both land in headers on the JSON surface, so the same
  // Metadata satisfies `headers_contain` and `trailers_contain`.
  const md = new Metadata();
  resp.headers.forEach((value, key) => {
    if (!key.startsWith("grpc-") && key !== "content-type" && key !== "content-length") md.set(key, value);
  });

  const code = Number(resp.headers.get("grpc-status") ?? Status.UNKNOWN);
  const status: StatusResult = {
    code,
    details: decodeURIComponent(resp.headers.get("grpc-message") ?? ""),
    metadata: md,
  };
  // Decode as a ConformancePayload only when the body IS one. A `response_body`
  // binding answers with a bare field value, so the case asserts `raw_body` instead
  // and decoding would be meaningless (or throw).
  const decodable = resp.ok && code === Status.OK && text.startsWith("{");
  const response = decodable ? ConformancePayload.fromJSON(JSON.parse(text)) : undefined;
  return { headers: md, messages: [], response, status, httpStatus: resp.status, rawBody: text };
}

/** The request messages a REST case sends: one `body`, several `bodies`, or none at all. */
const restBodies = (rest: Rest): string[] => rest.bodies ?? (rest.body !== undefined ? [rest.body] : []);

/**
 * A streaming annotation URL over WebSocket: text-locked single-stream JSON. A body-less
 * (GET-style) binding still needs one frame to open the stream — `{}` — after which the server
 * injects the request built from the URL and half-closes on the client's behalf.
 */
function runRestWebSocket(baseUrl: string, c: Case): Promise<Result> {
  const rest = c.rest!;
  const url = "ws" + baseUrl.slice("http".length) + rest.path;
  // Blank by default, which is how a browser reaches an annotated route; a case can offer a
  // wrong-surface subprotocol instead to exercise rejection.
  const protocols = rest.content_type ? [rest.content_type] : [];
  const ws = new wsImpl(url, protocols);
  const bodies = restBodies(rest);

  return new Promise((resolve) => {
    let headers = new Metadata();
    const messages: ConformancePayload[] = [];
    // A client-streaming route answers once, so its reply is the `response`, not a message.
    let response: ConformancePayload | undefined;
    let receivedCount: number | undefined;
    const rawMessages: string[] = [];
    let status: StatusResult | undefined;

    ws.onopen = () => {
      const open: Record<string, unknown> = {};
      const md = Object.fromEntries((c.request_metadata ?? []).map((m) => [m.key, m.ascii ?? ""]));
      if (Object.keys(md).length) open.metadata = md;
      if (c.timeout_millis) open.timeoutMillis = c.timeout_millis;
      // The opening frame carries the first request message inline; the rest follow as
      // ordinary message frames. A body-less binding sends none at all — the server builds
      // the one request from the URL.
      if (bodies.length) open.message = JSON.parse(bodies[0]);
      ws.send(JSON.stringify(open));
      for (const body of bodies.slice(1)) ws.send(JSON.stringify({ message: JSON.parse(body) }));
      // The send side is done. On a body-less binding the server has already half-closed on
      // our behalf and this is a no-op; on a `body: "*"` binding it is what lets a
      // client-streaming or single-request RPC finish.
      ws.send(JSON.stringify({ halfClose: true }));
    };
    ws.onmessage = (ev) => {
      const frame = JSON.parse(String(ev.data)) as {
        metadata?: Record<string, string>;
        message?: unknown;
        status?: { code?: number; message?: string };
      };
      const meta = (m?: Record<string, string>) =>
        toMetadata(Object.entries(m ?? {}).map(([key, ascii]) => ({ key, ascii })));
      if (frame.status) {
        status = {
          code: frame.status.code ?? Status.OK,
          details: frame.status.message ?? "",
          metadata: meta(frame.metadata),
        };
      } else if (frame.message !== undefined) {
        // A client-streaming RPC replies with a ClientStreamResponse wrapping the payload;
        // every other cardinality replies with a bare ConformancePayload.
        rawMessages.push(JSON.stringify(frame.message));
        if (c.rpc === "ClientStream") {
          const cs = ClientStreamResponse.fromJSON(frame.message);
          response = cs.payload;
          receivedCount = cs.receivedCount;
        } else if (typeof frame.message === "object" && frame.message !== null) {
          // Same reasoning as the Fetch path: a `response_body` binding streams bare
          // field values, which are asserted with `raw_messages`.
          messages.push(ConformancePayload.fromJSON(frame.message));
        }
      } else {
        headers = meta(frame.metadata);
      }
    };
    ws.onclose = (ev) => {
      // A pre-RPC rejection arrives only as a close code — the `4000 + gRPC code` convention —
      // so it becomes the status when no terminal frame did.
      if (!status) {
        const code = ev.code >= 4000 && ev.code <= 4016 ? ev.code - 4000 : Status.UNAVAILABLE;
        status = { code, details: ev.reason ?? "", metadata: new Metadata() };
      }
      resolve({ headers, messages, response, receivedCount, rawMessages, status });
    };
    ws.onerror = () => {
      if (!status) status = { code: Status.UNAVAILABLE, details: "websocket error", metadata: new Metadata() };
    };
  });
}

/**
 * Only a *unary* method's annotation URL is a Fetch endpoint. Every streaming
 * cardinality — client-streaming included, since it is a stream that happens to answer
 * once — takes the WebSocket surface, exactly as on the main endpoints.
 */
const runRestCase = (baseUrl: string, c: Case): Promise<Result> =>
  c.rpc === "Unary" ? runRestFetch(baseUrl, c) : runRestWebSocket(baseUrl, c);

// --- assertions --------------------------------------------------------------------------
const bytesEq = (a: Uint8Array | undefined, b: Uint8Array) =>
  expect(Array.from(a ?? new Uint8Array())).toEqual(Array.from(b));

function assertMetaContains(md: Metadata, items: Meta[]) {
  for (const m of items) {
    const values = md.get(m.key);
    if (m.b64 !== undefined) {
      const want = Array.from(new Uint8Array(Buffer.from(m.b64, "base64")));
      const got = values.map((v) => (typeof v === "string" ? v : Array.from(v)));
      expect(got, `metadata ${m.key} (bin)`).toContainEqual(want);
    } else {
      expect(values, `metadata ${m.key}`).toContain(m.ascii ?? "");
    }
  }
}

function assertPayload(matcher: Matcher, cp: ConformancePayload | undefined) {
  if (matcher.payload) bytesEq(cp?.payload, toBytes(matcher.payload));
  const ri = matcher.request_info;
  if (ri?.request_headers_contain) {
    const echoed = cp?.requestInfo?.requestHeaders ?? [];
    for (const want of ri.request_headers_contain) {
      const hit = echoed.some(
        (m) =>
          m.key === want.key &&
          (want.b64 !== undefined
            ? m.binValue !== undefined &&
              Buffer.from(m.binValue).toString("base64") === want.b64
            : m.asciiValue === (want.ascii ?? "")),
      );
      expect(hit, `server echoed request header ${want.key}`).toBe(true);
    }
  }
}

function assertCase(c: Case, r: Result) {
  // A surface rejection never becomes an RPC, so it is asserted as a raw HTTP status
  // (REST/Fetch only — a WebSocket rejection arrives as a `4000 + code` close, which the
  // driver has already turned back into an ordinary gRPC status).
  if (c.expect.http_status !== undefined) {
    expect(r.httpStatus, "HTTP status").toBe(c.expect.http_status);
    return;
  }
  const want = c.expect.status!;
  if (want.not_ok) {
    // The exact code is transport-dependent (a mid-upload rejection may surface as a stream
    // failure rather than a clean RESOURCE_EXHAUSTED) — assert only that it was rejected.
    expect(r.status.code, `expected non-OK (details: "${r.status.details}")`).not.toBe(Status.OK);
  } else {
    expect(r.status.code, `status (details: "${r.status.details}")`).toBe(want.code);
  }
  if (want.message_contains) expect(r.status.details).toContain(want.message_contains);
  if (c.expect.response) assertPayload(c.expect.response, r.response);
  if (c.expect.messages) {
    expect(r.messages.length).toBeGreaterThanOrEqual(c.expect.messages.length);
    c.expect.messages.forEach((pm, i) => assertPayload(pm, r.messages[i]));
  }
  if (c.expect.message_count !== undefined) expect(r.messages.length).toBe(c.expect.message_count);
  // The one assertion that proves a client stream's messages after the first arrived at all:
  // the response payload comes from the FIRST request's ResponseDefinition regardless.
  if (c.expect.received_count !== undefined)
    expect(r.receivedCount, "received_count").toBe(c.expect.received_count);
  // Raw matchers, for bodies that are not the RPC's response message.
  if (c.expect.raw_body !== undefined) expect(r.rawBody, "raw_body").toBe(c.expect.raw_body);
  if (c.expect.raw_messages) expect(r.rawMessages ?? [], "raw_messages").toEqual(c.expect.raw_messages);
  if (c.expect.headers_contain) assertMetaContains(r.headers, c.expect.headers_contain);
  if (c.expect.trailers_contain) assertMetaContains(r.status.metadata, c.expect.trailers_contain);
}

// --- server implementations under test -----------------------------------------------------
/**
 * Each entry is one grpc-webnext server implementation. `prepare` builds it once (so a cold
 * compile never eats into a per-server startup budget) and `command` is spawned per config
 * profile. Both are launched directly rather than through a build tool's `run` wrapper, so
 * killing the process actually kills the server rather than orphaning a grandchild.
 */
interface ServerImpl {
  name: string;
  prepare(): void;
  command: () => { file: string; args: string[]; cwd: string };
}

const goBinary = path.join(goRoot, "target", "conformance-server");

const SERVERS: ServerImpl[] = [
  {
    name: "rust",
    prepare: () => execFileSync("cargo", ["build", "--quiet", "-p", "conformance-server"], { cwd: rustRoot }),
    command: () => ({
      file: "cargo",
      args: ["run", "--quiet", "-p", "conformance-server"],
      cwd: rustRoot,
    }),
  },
  {
    name: "go",
    prepare: () => execFileSync("go", ["build", "-o", goBinary, "./examples/conformance-server"], { cwd: goRoot }),
    command: () => ({ file: goBinary, args: [], cwd: goRoot }),
  },
];

// --- harness -----------------------------------------------------------------------------
const suites: Suite[] = fs
  .readdirSync(casesDir)
  .filter((f) => f.endsWith(".yaml"))
  .sort()
  .map((f) => load(fs.readFileSync(path.join(casesDir, f), "utf8")) as Suite);

const serverProfiles = new Map<string, Record<string, string>>();
for (const s of suites) for (const c of s.cases) serverProfiles.set(requiresKey(c.requires), requiresEnv(c.requires));

/** Keyed by `<impl>|<config profile>` — one running server per combination. */
const servers = new Map<string, { proc: ChildProcess; baseUrl: string }>();
const serverKey = (impl: string, requires: Case["requires"]) => `${impl}|${requiresKey(requires)}`;

function spawnServer(impl: ServerImpl, env: Record<string, string>): Promise<{ proc: ChildProcess; baseUrl: string }> {
  const { file, args, cwd } = impl.command();
  const proc = spawn(file, args, {
    cwd,
    stdio: ["ignore", "pipe", "inherit"],
    env: { ...process.env, ...env },
  });
  return new Promise((resolve, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    rl.on("line", (line) => {
      const m = line.match(/^LISTENING (\S+)$/);
      if (m) resolve({ proc, baseUrl: m[1] });
    });
    proc.on("error", (e) => reject(new Error(`${impl.name} conformance-server failed to start: ${e.message}`)));
    proc.on("exit", (code) => reject(new Error(`${impl.name} conformance-server exited early: ${code}`)));
  });
}

beforeAll(async () => {
  for (const impl of SERVERS) impl.prepare();
  await Promise.all(
    SERVERS.flatMap((impl) =>
      [...serverProfiles].map(async ([key, env]) =>
        servers.set(`${impl.name}|${key}`, await spawnServer(impl, env)),
      ),
    ),
  );
}, 300_000);

afterAll(() => {
  for (const { proc } of servers.values()) proc.kill("SIGKILL");
});

for (const impl of SERVERS) {
  for (const suite of suites) {
    describe(`conformance: ${suite.suite} [${impl.name} server]`, () => {
      for (const c of suite.cases) {
        for (const profile of profilesFor(c)) {
          it(`${c.name} [${profile.name}]`, async () => {
            const server = servers.get(serverKey(impl.name, c.requires))!;
            // REST cases bypass the client entirely — an annotated URL is meant to be
            // reachable by an ordinary HTTP caller, so the driver is one.
            if (c.rest) {
              assertCase(c, await runRestCase(server.baseUrl, c));
              return;
            }
            const client = makeClient(ConformanceServiceDefinition, {
              baseUrl: server.baseUrl,
              webSocketImpl: wsImpl,
              ...profile.config,
            });
            try {
              assertCase(c, await runCase(client, c));
            } finally {
              client.close();
            }
          });
        }
      }
    });
  }
}

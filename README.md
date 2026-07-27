<div align="center">

# grpc-webnext

**Full bidirectional gRPC in the browser — real HTTP/2, REST, and WebSockets, on the same port as native gRPC.**

[![CI](https://github.com/debdattabasu/grpc_webnext/actions/workflows/ci.yml/badge.svg)](https://github.com/debdattabasu/grpc_webnext/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/grpc-webnext.svg?label=crates.io%20server)](https://crates.io/crates/grpc-webnext)
[![npm](https://img.shields.io/npm/v/@grpc-webnext/client.svg?label=npm%20client)](https://www.npmjs.com/package/@grpc-webnext/client)
[![Spec](https://img.shields.io/badge/spec-normative-success.svg)](spec/PROTOCOL.md)
[![Conformance](https://img.shields.io/badge/conformance-passing-brightgreen.svg)](conformance/)
&nbsp;·&nbsp;
Rust · TypeScript · Go

</div>

---

grpc-webnext brings the **complete** gRPC experience to the browser — all four call types,
deadlines, metadata, trailers, and cancellation — with **no translation on the default path**.
The browser speaks *real HTTP/2* (trailers, flow control, native multiplexing) tunneled over a
WebSocket by [**h2ts**](https://github.com/debdattabasu/h2ts), straight into an **unmodified**
gRPC server. Want plaintext instead? Flip one switch for **JSON over Fetch + WebSocket**, or
annotate a method for **plain REST**. One endpoint serves browsers, REST clients, and native
gRPC clients — all on the same port.

```ts
import { makePromiseClient } from "@grpc-webnext/client";
import { GreeterDefinition } from "./gen/greeter.js";

const rpc = makePromiseClient(GreeterDefinition, { baseUrl: "https://api.example.com" });

const reply = await rpc.sayHello({ name: "world" });        // unary
for await (const tick of rpc.countdown({ from: 3 })) …      // server-stream
for await (const msg of rpc.chat(source, { signal })) …     // bidi, cancel via AbortSignal
```

## ✨ Features

- **Real gRPC, end to end.** The default binary path is *real HTTP/2 over h2ts* into an
  unmodified server — no envoy-style translation, no lossy shim. Trailers, flow control, and
  stream multiplexing are the genuine article.
- **Same port as native gRPC.** `content-type` disambiguates `application/grpc` from
  grpc-webnext; native gRPC clients pass through untouched. One listener, every audience.
- **Two codecs.** Efficient **binary protobuf**, or **plaintext JSON** you can read in the
  browser's Network tab. Pick per client.
- **Two streaming transports.** *h2ts* (one WebSocket, many multiplexed streams) or a
  *custom `Frame` protocol* (one WebSocket per stream) — configurable per client.
- **Raw REST, built in.** Annotate methods with [`google.api.http`](spec/PROTOCOL.md#rest-transcoding-googleapihttp)
  (the grpc-gateway / Envoy standard) to expose real HTTP verbs and REST URLs with JSON bodies.
- **WebSockets in JSON *or* binary.** Stream over a debuggable JSON WebSocket or a compact
  binary one — same semantics, your choice.
- **Full gRPC semantics.** Unary · server-stream · client-stream · bidi, plus `grpc-timeout`
  deadlines, metadata (ASCII + `-bin`), canonical status codes, and cancellation.
- **In-process *or* proxy.** Wrap a tonic `Routes` for a zero-hop native endpoint, or run the
  schema-agnostic **proxy** in front of *any* gRPC server, in any language.
- **Standard auth.** No bespoke hooks — authorization is a per-RPC gRPC interceptor
  (in-process) or your mesh's `ext_authz` (proxy), uniform across every transport.
- **Polyglot, drift-proof.** One wire format, implemented per language, held to a
  language-neutral [conformance suite](conformance/) run over the real wire.
- **Familiar client API.** Callback/EventEmitter *and* promise/async-iterable flavors,
  modeled on `@grpc/grpc-js` and Connect.

## How it works

```
   Browser / Node clients            (TypeScript ✅ · Rust-WASM ⬜)
        │
        │   proto (default)          json / proto-ws
        │   real HTTP/2 ⤵ h2ts       custom Frame protocol ⤵
        ▼                            (Fetch unary · WebSocket streams)
   ┌───────────────────────────────────────────────────────────┐
   │  grpc-webnext endpoint — one port                          │
   │    in-process server:  Rust ✅ · Go ⬜ · Node ⬜             │
   │    or standalone proxy (Rust) — front any gRPC upstream    │
   └───────────────────────────────────────────────────────────┘
        │   native application/grpc (same port, byte-for-byte passthrough)
        ▼
   Any gRPC server — tonic · grpc-go · grpc-java · …
```

Two worlds share one endpoint:

- **Binary (default) → real gRPC over h2ts.** The browser runs a real HTTP/2 stack in
  TypeScript, tunneled over a WebSocket by [h2ts](https://github.com/debdattabasu/h2ts); the
  server is unmodified tonic behind an h2ts gateway. No translation — trailers, multiplexing,
  and flow control are native.
- **JSON (and binary `streaming: "ws"`) → the custom `Frame` protocol.** Unary rides Fetch
  (the response body is `[len│message][len│trailer]`, since browsers can't read HTTP trailers);
  streaming is **one WebSocket per stream**, one protobuf `Frame` per message
  (`Subscribe` · `Message` · `HalfClose` · `Trailer` · `Reset` · `Header`). Plaintext JSON keeps
  it debuggable in the browser.

Transport is a per-client config `{ codec, unary, streaming }`:

| Client config | Unary | Streaming | On the wire |
|---|---|---|---|
| `{ codec: "proto" }` &nbsp;*(default)* | h2ts | h2ts | **Real HTTP/2** over one WebSocket — multiplexed, unmodified gRPC |
| `{ codec: "proto", unary: "fetch" }` | Fetch | h2ts | Fetch unary + multiplexed HTTP/2 streams |
| `{ codec: "proto", streaming: "ws" }` | Fetch | one WS / stream | Custom `Frame` protocol — **binary** |
| `{ codec: "json" }` | Fetch | one WS / stream | Custom `Frame` protocol — **plaintext JSON** |
| **REST** (server-annotated) | any HTTP verb | — | Plain JSON on `/v1/…` URLs — grpc-gateway-style |

The wire format is defined once, in [`proto/grpc_webnext.proto`](proto/grpc_webnext.proto) and
the normative [`spec/PROTOCOL.md`](spec/PROTOCOL.md), and every implementation is held to it by
the [conformance suite](conformance/). See [`spec/COMPATIBILITY.md`](spec/COMPATIBILITY.md) for
per-transport gRPC-semantics fidelity.

## Quickstart

**Run the demo** — starts the native Rust server and drives it from the TypeScript client:

```bash
cd node/packages/client && npm install && npm run demo
```

**Use the client** — `npm install @grpc-webnext/client`; two flavors share one transport (add
`codec: "json"` for plaintext):

```ts
import { makeClient, makePromiseClient } from "@grpc-webnext/client";
import { GreeterDefinition } from "./gen/greeter.js"; // ts-proto generic definitions

// Callback / EventEmitter — mirrors @grpc/grpc-js
const cb = makeClient(GreeterDefinition, { baseUrl: "https://api.example.com" });
cb.sayHello({ name: "world" }, (err, reply) => console.log(reply.message));

// Promise / async-iterable — mirrors Connect / nice-grpc
const rpc = makePromiseClient(GreeterDefinition, { baseUrl: "https://api.example.com" });
const reply = await rpc.sayHello({ name: "world" });          // unary → Promise
for await (const t of rpc.countdown({ from: 3 })) log(t);     // server-stream → AsyncIterable
const sum = await rpc.concat(source);                         // client-stream → Promise
for await (const m of rpc.chat(source, { signal })) log(m);   // bidi, cancel via AbortSignal
```

**Wrap a tonic server in-process** (serves grpc-webnext **and** native gRPC on one port):

```rust
use grpc_webnext::{bind_and_serve_in_process, ServerConfig};

let routes = tonic::service::Routes::new(GreeterServer::new(svc));
let (addr, handle) = bind_and_serve_in_process(routes, ServerConfig::default()).await?;
```

**Front an existing gRPC server with the proxy** (language-agnostic, no `.proto` required):

```bash
cd rust
UPSTREAM=http://localhost:50051 LISTEN=127.0.0.1:8080 cargo run -p grpc-webnext-proxy
```

## Status

| Component | Location | State |
|---|---|---|
| Wire protocol + normative spec | [`proto/`](proto/) · [`spec/PROTOCOL.md`](spec/PROTOCOL.md) | ✅ the contract |
| Rust server + proxy&nbsp;·&nbsp;[crates.io](https://crates.io/crates/grpc-webnext) | [`rust/crates/grpc-webnext`](rust/crates/grpc-webnext) | ✅ h2ts, custom `Frame`, `+json`, REST, deadlines, cancel, size limits, native same-port |
| TypeScript client (browser + Node)&nbsp;·&nbsp;[npm](https://www.npmjs.com/package/@grpc-webnext/client) | [`node/packages/client`](node/packages/client) | ✅ h2ts + Fetch + WebSocket, typed codegen, callback + promise APIs |
| Conformance suite | [`conformance/`](conformance/) | ✅ language-neutral cases + Rust server + TS driver, run over the real wire |
| Go in-process server | [`go/webnext`](go/webnext) | ⬜ skeleton (API + router; protocol stubbed) |
| Node in-process server | [`node/packages/server`](node/packages/server) | ⬜ skeleton |
| Rust client (WASM / frontend) | `rust/crates/grpc-webnext-client` | ⬜ planned |

> **Pre-1.0.** The Rust server/proxy and TypeScript client are feature-complete and covered by
> the conformance suite; the polyglot servers are the next milestone. See
> [`doc/BACKLOG.md`](doc/BACKLOG.md) — including a plan to terminate grpc-webnext *inside stock
> Envoy* via a Rust dynamic-module filter, no sidecar.

## Repository layout

Organized by language ecosystem — each toolchain owns its subtree — with the cross-language
contract at the root.

```
proto/                 wire envelope (Frame, Metadatum, …) — source of truth
spec/                  PROTOCOL.md (normative) + COMPATIBILITY.md
conformance/           language-neutral conformance suite (proto, cases, driver)
doc/                   design notes (STATUS, BACKLOG, UNIFICATION, H2TS_INTEGRATION)

rust/                  Cargo workspace
  crates/grpc-webnext/   server library + proxy binary (grpc-webnext-proxy)
  examples/              Greeter service, end-to-end demo, conformance server

node/                  npm workspace
  packages/client/       @grpc-webnext/client — Fetch + WebSocket + h2ts transports
  packages/server/       @grpc-webnext/server — in-process (skeleton)

go/                    Go module (github.com/grpc-webnext/grpc-webnext/go)
  webnext/               in-process server (skeleton)
```

## Development

```bash
cd rust && cargo test --workspace         # Rust: server + proxy + conformance
cd rust && cargo clippy --workspace --all-targets
cd node/packages/client && npm test       # TypeScript: codec, e2e, conformance (spawns Rust servers)
cd go && go build ./... && go vet ./...    # Go: skeleton builds clean
```

A Rust toolchain is needed for the full TypeScript suite — the e2e and conformance tests spawn
the Rust example servers. Servers print `LISTENING http://<addr>` when ready.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
this project by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.

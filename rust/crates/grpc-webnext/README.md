# grpc-webnext

**Full bidirectional gRPC for the browser — real HTTP/2 tunneled over WebSockets via [h2ts](https://github.com/debdattabasu/h2ts), plus REST and JSON — served in front of any gRPC service.**

[![crates.io](https://img.shields.io/crates/v/grpc-webnext.svg)](https://crates.io/crates/grpc-webnext)
[![docs.rs](https://img.shields.io/docsrs/grpc-webnext.svg)](https://docs.rs/grpc-webnext)
[![license](https://img.shields.io/crates/l/grpc-webnext.svg)](#license)

The Rust server for [**grpc-webnext**](https://github.com/debdattabasu/grpc-webnext) — it brings
the **complete** gRPC experience (all four call types, deadlines, metadata, trailers,
cancellation) to the browser, on the **same port** as native gRPC. Use it two ways:

- **In-process** — wrap a `tonic` `Routes` and serve grpc-webnext **and** native
  `application/grpc` from one listener, zero extra hops.
- **Standalone proxy** — the `grpc-webnext-proxy` binary fronts *any* gRPC upstream, in any
  language, no `.proto` required (schema fetched via server reflection for the JSON/REST paths).

The browser side is the TypeScript client [**`@grpc-webnext/client`**](https://www.npmjs.com/package/@grpc-webnext/client).

## How it works

Two worlds share one endpoint; `content-type` and the WebSocket subprotocol disambiguate:

- **Binary (default) → real gRPC over h2ts.** The browser runs a real HTTP/2 stack tunneled over
  a WebSocket by [h2ts](https://github.com/debdattabasu/h2ts); this crate runs unmodified `tonic`
  behind an h2ts gateway. **No translation** — trailers, multiplexing, and flow control are native.
- **JSON (and binary `streaming: "ws"`) → the custom `Frame` protocol.** Unary rides Fetch
  (trailer buffered into the body, since browsers can't read HTTP trailers); streaming is one
  WebSocket per stream. Plaintext JSON stays debuggable in the browser's Network tab.
- **REST → `google.api.http` transcoding.** Annotate methods to expose real HTTP verbs and JSON
  bodies on REST URLs — the grpc-gateway / Envoy standard.

Native `application/grpc` clients pass through byte-for-byte on the same port.

## Install

Library:

```toml
[dependencies]
grpc-webnext = "0.1"
```

Proxy binary:

```bash
cargo install grpc-webnext        # installs `grpc-webnext-proxy`
```

## In-process — serve grpc-webnext + native gRPC on one port

```rust
use grpc_webnext::{bind_and_serve_in_process, ServerConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let routes = tonic::service::Routes::new(GreeterServer::new(MyGreeter::default()));
    let (addr, handle) = bind_and_serve_in_process(routes, ServerConfig::default()).await?;
    println!("listening on {addr}");
    handle.await?;
    Ok(())
}
```

Browsers reach it with `@grpc-webnext/client`; native gRPC clients (grpc-go, tonic, grpcurl…)
connect to the *same* address unchanged.

## Proxy — front an existing gRPC server

```bash
UPSTREAM=http://localhost:50051 LISTEN=127.0.0.1:8080 grpc-webnext-proxy
```

The proxy is schema-agnostic. For the JSON/REST transcoding paths it fetches message descriptors
from the upstream via **server reflection** (`SCHEMA=reflection`) or a precompiled descriptor set
(`DESCRIPTOR_SET=path`). The binary passthrough (h2ts) path needs no schema at all.

## Authorization

No bespoke hooks — authorization is a per-RPC concern, uniform across every transport: a `tonic`
interceptor in-process, or your mesh's `ext_authz` in front of the proxy.

## Ecosystem

- **Repository & docs:** https://github.com/debdattabasu/grpc-webnext — normative
  [`spec/PROTOCOL.md`](https://github.com/debdattabasu/grpc-webnext/blob/main/spec/PROTOCOL.md)
  and the language-neutral [conformance suite](https://github.com/debdattabasu/grpc-webnext/tree/main/conformance).
- **Browser / Node client:** [`@grpc-webnext/client`](https://www.npmjs.com/package/@grpc-webnext/client) (npm).
- **h2ts** — real HTTP/2 over WebSocket, the default transport: https://github.com/debdattabasu/h2ts.

## License

Dual-licensed under either [Apache-2.0](https://github.com/debdattabasu/grpc-webnext/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/debdattabasu/grpc-webnext/blob/main/LICENSE-MIT), at your option.

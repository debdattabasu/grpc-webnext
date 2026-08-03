# grpc-webnext-client

A gRPC client for **Rust WASM frontends** (Leptos, Yew, Dioxus, …), speaking real gRPC to a
[grpc-webnext](https://github.com/debdattabasu/grpc-webnext) endpoint over an
[h2ts](https://github.com/debdattabasu/h2ts) WebSocket tunnel.

**No tonic, no hyper, no tokio.** The wire is real HTTP/2 — trailers, multiplexing, flow
control — so there is nothing to translate: message framing plus a status read off the
trailers is the whole client.

```rust,ignore
use grpc_webnext_client::{connect, CallOptions, TypedClient};

let client = connect("https://api.example.com")?;   // lazy: dials on first call

let reply: HelloReply = client
    .unary_typed("/helloworld.Greeter/SayHello", HelloRequest { name: "world".into() }, CallOptions::new())
    .await?
    .into_inner();
```

All four cardinalities are supported. `Streaming::message()` yields responses as they
arrive, and `Streaming::status()` gives the terminal status afterwards.

## Generated stubs (tonic)

The native API above deals in method paths and message bytes. If you would rather call
`greeter.say_hello(request)`, **tonic's own generated stubs run over the tunnel** — enable
the `tonic` feature and pass the client where a `Channel` would go:

```toml
[dependencies]
grpc-webnext-client = { version = "0.1", features = ["tonic"] }
# No server, no channel, no TLS — none of which build for wasm, and none of which this
# needs. `codegen` comes with the feature above, since generated code opens with
# `use tonic::codegen::*`.
tonic = { version = "0.14", default-features = false }
tonic-prost = "0.14"

[build-dependencies]
tonic-prost-build = "0.14"
```

```rust,ignore
// build.rs
tonic_prost_build::configure()
    .build_client(true)
    .build_server(false)
    // Essential. Without it the stub also gets a `connect()` constructor returning
    // `Self<tonic::transport::Channel>` — hyper, TCP and TLS, which do not exist in a
    // browser and do not compile for wasm32. Off, it leaves `GreeterClient::new(T)`,
    // which is the constructor that matters: the transport is passed in, and ours is
    // the tunnel.
    .build_transport(false)
    .compile_protos(&["proto/greeter.proto"], &["proto"])?;
```

```rust,ignore
use grpc_webnext_client::connect;
use pb::greeter_client::GreeterClient;

let mut greeter = GreeterClient::new(connect("https://api.example.com")?.into_tonic());

let reply = greeter.say_hello(HelloRequest { name: "world".into() }).await?;   // unary
let mut ticks = greeter.countdown(CountdownRequest { from: 3 }).await?.into_inner();
let joined = greeter.concat(futures::stream::iter(words)).await?;              // client streaming
let mut chat = greeter.chat(outbound).await?.into_inner();                     // bidi
```

That is the whole difference from a native tonic program — one line, because the wire is
real HTTP/2 either way and there is nothing to translate. tonic keeps doing codec, framing,
status, compression and interceptors; this crate keeps doing what tonic cannot do in a
browser: dial, tunnel, reconnect. **All four cardinalities work**, including the two
(client- and bidi-streaming) that grpc-web cannot express at all.

Three things differ from tonic over a `Channel`, all of them deliberate:

- **`Request::set_timeout` is enforced locally**, not only sent as `grpc-timeout`, and it
  covers the whole call including a stream that stalls after it opens. On a `Channel` the
  header is advisory and only an `Endpoint::timeout` layer enforces anything; in a tab, a
  peer that ignores the header would hang the call forever. Same promise as
  [`CallOptions::timeout`](#deadlines) on the native API.
- **The client stays single-threaded.** tonic's stubs require `T::ResponseBody: Send`, and
  the engine underneath is `!Send` on purpose — see below.
- **There is no `connect()` constructor**, which is what `build_transport(false)` removes.
  The transport is this client.

### The `Send` bound, honestly — and no, it does not mean threads

tonic's generated stubs bound `T::ResponseBody: Send + 'static`. The h2ts engine is
deliberately `!Send` (`Rc`, boxed non-`Send` streams), which is what a browser actually is,
so asserting `Send` with an `unsafe impl` would be a lie the compiler could no longer
check. The adapter uses [`send_wrapper`](https://docs.rs/send_wrapper) instead: it carries
the value across the bound and records the thread that created it, so a value that really
does move threads **panics at the boundary** rather than racing. Natively, keep the client
on a `LocalSet` — where a `!Send` client belongs anyway.

**`Send` is a compile-time marker, not a runtime.** Requiring it links nothing and spawns
nothing, and this feature brings no threading with it — which matters, because a wasm
bundle that quietly needed shared memory would not run where you want to ship it. In a
release build of the stub path:

- **zero atomic instructions** and no shared memory — threads on wasm require both;
- no `thread::spawn` / `tokio::spawn` / `JoinHandle` symbols anywhere;
- `tokio` resolves to `default,sync` only. `tokio::spawn` lives behind the `rt` feature,
  which is off, so the code path *cannot* spawn a task — that is a compile-time property,
  not an observation about this build.

`send_wrapper` itself is a `ThreadId` comparison, not a threading primitive, and it was
already in the tree before this feature existed (`futures-timer` pulls it in for its
wasm-bindgen shim). Plain `wasm32-unknown-unknown` — no `+atomics` — is the supported and
measured configuration.

If you *do* build with wasm threads, the rule is **one client per worker**: a `Client`
moved across workers panics at the `SendWrapper` boundary, deliberately, because the
alternative is a data race on `Rc`. Per-worker clients are the right shape regardless —
each worker gets its own tunnel.

### What it costs in the bundle

The feature is **off by default** because it is not free: tonic goes in the dependency tree,
and so in the wasm bundle. Measured, rather than guessed — same five RPCs, release profile
(`opt-level = "z"`, LTO, `panic = "abort"`), `wasm-bindgen` then `wasm-opt -Oz`:

| | `.wasm` | gzipped |
|---|---|---|
| empty crate (the wasm-bindgen floor) | 20.4 KB | 9.4 KB |
| native API, unary only | 145.0 KB | 62.9 KB |
| tonic stubs, unary only | 208.3 KB | **90.1 KB** |
| native API, all four cardinalities | 158.0 KB | 68.4 KB |
| tonic stubs, all four cardinalities | 221.0 KB | **94.6 KB** |

**≈26 KB gzipped**, and note it is *flat*: the same whether you call one unary method or
all four cardinalities. What you are paying for is tonic's core — codec, `Status`,
`MetadataMap`, the decode state machine — which does not tree-shake away by cardinality. As
a share of the gRPC layer (bundle minus the floor) it is +44%.

Worth it for the ergonomics and for interop with tonic's ecosystem; not worth it if bytes
are the binding constraint. Both APIs are here precisely so that stays your call — with the
feature off, nothing above changes and the client is tonic-free.

## Codec

The client deals in **message bytes**, so it is codec-agnostic. The default `prost` feature
adds typed helpers ([`TypedClient`]) over `prost::Message`, and a service is then a handful
of thin wrappers over `unary`, `server_streaming`, `client_streaming` and `bidi_streaming`.
For generated stubs instead of hand-written wrappers, see
[Generated stubs](#generated-stubs-tonic).

## Deadlines

`CallOptions::timeout` sends `grpc-timeout` **and** arms a local timer. Both matter: the
header lets a server or proxy enforce the deadline, and the timer means the call still ends
if nothing does.

On a **stream** the deadline bounds the whole call, not just opening it. One timer spans
the request and every message read after it, so a server that sends headers promptly and
then goes quiet still ends the call — that being the case a deadline is actually for. When
it fires the stream is dropped, which resets the HTTP/2 stream and releases the server's
handler rather than leaving it producing for a caller that has gone.

## Backpressure

Free, and real. `h2ts-client` replenishes the HTTP/2 receive window only as the response body
is polled, so a consumer that stops reading stops the *server* rather than filling the tab's
memory.

## Reconnect

`Client` is a gRPC **channel**, not a handle to a socket. The tunnel opens on the first call
and reopens if it drops — the same contract `tonic::transport::Channel` has, so an app never
owns socket lifecycle to make an RPC.

The call that finds the tunnel dead reports the failure; the next one reconnects. The
transport does not silently replay a request the server may already have seen — that is a
retry policy decision, and not the transport's to make.

Connectivity is observable, which a frontend needs more than a backend does (the answer is
usually a banner):

```rust,ignore
match client.state() {
    ConnectivityState::Ready => { /* ... */ }
    ConnectivityState::TransientFailure => show_offline_banner(),
    _ => {}
}

// gRPC's WaitForStateChange, as a stream. Repeats are collapsed.
while let Some(state) = client.state_changes().next().await {
    log(state);
}
```

No reconnect **backoff**: as in tonic, a redial happens when a call asks for one, so your
call rate bounds the dial rate. A client built with `Client::over_transport` cannot redial —
the transport is consumed — and says so instead of pretending to be live.

## Bringing your own transport

`connect` is the browser entry point and needs wasm. Anywhere else — a test, a CLI, a
conformance driver — build the byte transport yourself and hand it to
`Client::over_transport`. Everything the signature needs is re-exported here, so this does
not mean depending on `h2ts-client` directly:

```rust,ignore
use grpc_webnext_client::{Client, ConnectOptions, Transport, TransportError};

let transport = Transport::new(Box::pin(reader), Box::pin(writer));
let (client, driver) = Client::over_transport(transport, "api.example.com", ConnectOptions::default());
spawn_local(driver);   // the driver must be polled for anything to happen
```

Such a client is single-shot: the transport is consumed, so it cannot redial (see
[Reconnect](#reconnect)). For one that *can*, build a `Connector` — a function returning a
fresh `H2Connection` per dial — and pass it to `Client::with_connector`; `open_tunnel` is
`h2ts_client::connect` re-exported so that needs no second dependency either. The
[`tonic-stub-client`](https://github.com/debdattabasu/grpc-webnext/tree/main/rust/examples/tonic-stub-client)
example does exactly this over `tokio-tungstenite`, in about thirty lines.

## Testing

The end-to-end tests run on the **host**, not in a browser. `h2ts-client` is a sans-I/O
engine behind a pluggable byte transport, so swapping `web_sys::WebSocket` for
`tokio-tungstenite` exercises the identical code path against a real grpc-webnext server —
framing, HPACK, flow control, trailers, all of it. What a browser adds is a socket
implementation, not gRPC behavior.

The repo's [`conformance-driver`](https://github.com/debdattabasu/grpc-webnext/tree/main/rust/examples/conformance-driver)
is the same idea at a larger scale: it runs this client through the language-neutral
conformance suite against every server implementation, over a real socket.

## License

MIT OR Apache-2.0, at your option.

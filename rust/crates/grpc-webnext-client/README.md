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

## Why not tonic

tonic requires `T::ResponseBody: Send + 'static`. The h2ts engine is deliberately `!Send`
(`Rc`, boxed non-`Send` streams), which is correct for a browser — so going through tonic
would mean asserting `Send` on a target that has no threads. Everything here is honestly
single-threaded instead.

## Codec

The client deals in **message bytes**, so it is codec-agnostic. The default `prost` feature
adds typed helpers ([`TypedClient`]) over `prost::Message`. There is no generated service
stub layer yet: a service is a handful of thin wrappers over `unary`, `server_streaming`,
`client_streaming` and `bidi_streaming`.

## Deadlines

`CallOptions::timeout` sends `grpc-timeout` **and** arms a local timer. Both matter: the
header lets a server or proxy enforce the deadline, and the timer means the call still ends
if nothing does.

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

## Testing

The end-to-end tests run on the **host**, not in a browser. `h2ts-client` is a sans-I/O
engine behind a pluggable byte transport, so swapping `web_sys::WebSocket` for
`tokio-tungstenite` exercises the identical code path against a real grpc-webnext server —
framing, HPACK, flow control, trailers, all of it. What a browser adds is a socket
implementation, not gRPC behavior.

## License

MIT OR Apache-2.0, at your option.

# Examples

## Greeter end-to-end demo

A [`Greeter`](greeter.proto) service exercising every RPC cardinality:

| RPC | Cardinality | Transport used by the client |
|---|---|---|
| `SayHello` | unary | Fetch |
| `Countdown` | server streaming | WebSocket |
| `Chat` | bidi streaming | WebSocket |

- [`greeter-server/`](greeter-server/) — a Rust binary that serves `Greeter` over
  grpc-webnext **and** native gRPC on one port, using the native server library
  (the `grpc-webnext` crate).
- The TypeScript client demo lives at
  [`node/packages/client/examples/greeter.ts`](../../node/packages/client/examples/greeter.ts).
  It spawns the server (via `cargo run` in `../rust`), then drives all three RPCs
  with the generated client.

### Run it

```bash
cd node/packages/client
npm install
npm run demo
```

Expected output:

```
[unary]  SayHello -> "Hello, world!"
[server-stream]  Countdown(3):
   tick 3 … tick 0
[bidi]  Chat:
   client: "hi" … server: "echo: hi" …
Demo complete. ✅
```

### Run just the server

```bash
cargo run -p example-greeter-server
# prints: LISTENING http://127.0.0.1:PORT
```

You can then point any grpc-webnext client at it, or a native gRPC client
(`grpcurl`, tonic) at the same port — the content-type disambiguates.

## tonic's generated stubs, over the tunnel

[`tonic-stub-client/`](tonic-stub-client/) drives the same `Greeter` through **stock
`tonic-prost-build` client stubs** — `GreeterClient::new(client.into_tonic())` — instead of
the Rust client's native path-and-bytes API. Nothing in the codegen is
grpc-webnext-specific; the wire is real HTTP/2 either way, so all four cardinalities work,
including the two grpc-web cannot express at all.

```bash
cargo run -p example-tonic-stub-client                     # spawns the server itself
cargo run -p example-tonic-stub-client -- http://HOST:PORT # or point at a running one
```

```
[unary]         SayHello -> "Hello, world!"
[server-stream] Countdown(3) -> 3 2 1 0
[client-stream] Concat -> "real HTTP/2 in a browser"
[bidi]          Chat -> "echo: hi" "echo: again"
[deadline]      Sleep(5s) -> DeadlineExceeded: deadline exceeded
[metadata]      SayHello -> "Hello, metadata!"
```

The one setting a browser target needs is `build_transport(false)` in
[`build.rs`](tonic-stub-client/build.rs) — without it the stub carries a `connect()`
constructor over `tonic::transport::Channel`, which does not compile for wasm32. See the
[client crate README](../crates/grpc-webnext-client/README.md#generated-stubs-tonic).

Its [`tests/e2e.rs`](tonic-stub-client/tests/e2e.rs) is the real coverage: a generated
`GreeterServer` behind grpc-webnext, driven by the generated `GreeterClient` over a real
WebSocket, pinning every cardinality plus metadata (both legs), trailers-only errors,
deadlines on a stalled stream, backpressure, and reconnect.

# Backlog / deferred work

Tracked items intentionally not done yet, so they aren't forgotten. Nothing here
blocks the current milestone; each is a follow-up pass.

> **Swept 2026-07-27.** Several items had been overtaken by the h2ts pivot (which retired
> multiplexing and the WS pool) and by the `+json`/transcoding work, and two contradicted
> `[x]` entries elsewhere in this same file. Each is now closed with the evidence that
> closes it rather than left as a standing TODO. What remains genuinely open: the Envoy
> filters, the HttpRule tail, REST conformance cases + the TS REST helper, TS client
> retry/reconnect and consumer backpressure, and one vestigial proto field.
> *(Graceful shutdown and Go REST transcoding both landed later the same day; REST
> conformance cases, two of the HttpRule gaps, and the vestigial field followed on
> 2026-07-28. What is left is the Envoy filters, the HttpRule tail, and three client-side
> items — see the TypeScript section. The HttpRule tail, consumer backpressure, and
> retry/reconnect all closed later that day, and the TS REST helper was **declined** —
> so the Envoy filters are the only open items left in this file.)*
>
> A pattern worth naming: **four separate items — server-side backpressure, large-payload
> streaming, fragmentation, and now client-side backpressure — all closed with the same
> answer.** Each was a bespoke solution to a problem HTTP/2 already solves, and each
> dissolved when the binary default moved to real H2 over h2ts. Before adding transport
> machinery to the custom `Frame` path, check whether the default path already has it.

## Proxy — full gRPC semantics (README point 8)

The proxy round-trips unary (Fetch) and streaming (WebSocket) end-to-end. As of the
2026-07-27 sweep **nothing here is open** — the section is kept as the decision record for
how each item was resolved (or deliberately dropped):

- [x] ~~Client cancellation → upstream.~~ Verified: a `Reset` frame (or full WebSocket
  disconnect) aborts the response task, which drops the tonic `Streaming` and sends
  RST_STREAM upstream. Covered by `proxy/tests/cancel.rs` (Reset + disconnect) and
  `server/tests/cancel.rs` (in-process handler drop).
- [x] ~~**Backpressure / flow control.**~~ **Resolved by the single-stream + h2ts pivot
  (2026-07-27 sweep).** The open question was credit-based per-stream flow control, and it
  was scoped by its own caveat: *"revisit if large messages starve a multiplexed
  connection"*. There is no longer a multiplexed custom-`Frame` connection to starve — one
  WebSocket carries exactly one stream. Where multiplexing does exist it is **real HTTP/2
  over h2ts**, so per-stream flow control is the genuine article (`WINDOW_UPDATE`,
  end-to-end between the browser's H2 stack and tonic/grpc-go), not something this layer
  should reimplement. What the custom path does have is a bounded chain that propagates to
  TCP: request `mpsc::channel(16)` and outbound `mpsc::channel(64)`, both with awaited
  sends (`src/ws.rs`); the Go server mirrors it with `chan []byte, 16` / `chan wsOutbound,
  64` plus an unbuffered `io.Pipe` into grpc-go, so a slow service blocks the socket read
  rather than accumulating. Same reasoning that closed "stream large WS message payloads
  without buffering" under Protocol. The client half closed the same way on 2026-07-28 —
  see the TypeScript section.
- [x] ~~**Trailing vs initial metadata fidelity (unary).**~~ **Done.** Fetch unary now
  separates the two: initial metadata goes out as HTTP response headers, trailing metadata
  into the `Trailer` block alongside the status — including the trailers-only case, where
  the metadata rides in the upstream's headers block and is routed to the trailer
  (`fetch.rs`, `unary_proto`). Pinned by the conformance case `error-with-trailers`, which
  asserts a non-OK status *and* its trailing metadata on every transport and both server
  implementations.
- [x] ~~Retry (unary) — REMOVED (2026-07-04).~~ A `RetryPolicy` was briefly on the
  proxy, then removed on principle: retry belongs in the **client** (gRPC service
  config). A protocol-level wire proxy fans many clients into one upstream, so
  proxy-side retry amplifies load exactly when the upstream is failing (retry storms)
  and compounds with client retries. Removing it also unblocked response streaming
  (retry forced buffering to replay the request / peek the status). Not planned.
- [x] ~~Max-concurrent-streams cap per WS.~~ Built as a `max_concurrent_streams` knob,
  then **removed with multiplexing**: a WebSocket now carries exactly one stream, so the
  cap is 1 by construction and a second `Subscribe` is answered
  `Reset{INVALID_ARGUMENT, "stream already open"}`. Concurrency limits for the h2ts path
  are HTTP/2's `SETTINGS_MAX_CONCURRENT_STREAMS`, set on the H2 server.
- [x] ~~Deadline enforcement proxy-side.~~ The proxy now drops the call at the
  deadline (DEADLINE_EXCEEDED) on both unary and streaming, and forwards
  `grpc-timeout` downstream with a grace backstop. Covered by `proxy/tests/deadline.rs`.
- [x] ~~Same-port native gRPC coexistence.~~ `application/grpc` is forwarded to the
  upstream untouched (README #9). Covered by `proxy/tests/passthrough.rs`.
- [x] ~~`+json` termination (2026-07-05).~~ The proxy transcodes `+json` to/from the
  upstream's binary protobuf on both Fetch and WebSocket, reusing the core `Transcoder`
  (identical output to the native server). Descriptors come from **upstream reflection**
  (v1 → v1alpha fallback), a **bundled `FileDescriptorSet`**, or
  **both** — `SchemaSource::{Reflection, Bundled, ReflectionOrBundled}`, the last being
  reflection-primary with the bundle serving immediately and as a fallback when the
  upstream has no reflection. No source → `UNIMPLEMENTED`. Binary `+proto` stays
  schema-agnostic. Reflection is loaded **eagerly and whole** (`list_services` +
  fetch-all into one snapshot; `crates/proxy/src/reflect.rs`), refreshed on a TTL
  (`reflection_ttl`, default 4h), with a `POST` management endpoint (`admin_reload_path`)
  + `Schema::reload()` to force a reload. The binary composes the source from
  `SCHEMA` × `DESCRIPTOR_SET` env. Covered by `proxy/tests/json.rs`.
- [x] ~~REST / `google.api.http` annotation routing on the proxy (2026-07-05).~~ Both
  surfaces resolve annotation URLs against the proxy's transcoder (bundle or eager
  reflection snapshot): Fetch via `transcode_http_request` (`handle_json_fetch` tries a
  REST binding before the main method path), WebSocket via `match_ws` at upgrade
  (single-stream JSON, method from the binding, requests built from the URL — GET-style
  no-body streams inject one empty payload + half-close). Covered by `proxy/tests/json.rs`
  (`rest_*`, `ws_rest_*`). **Caveat:** REST over *reflection* needs the upstream's
  reflection to preserve custom options — the proxy frames raw descriptor bytes verbatim
  so options survive, but tonic-reflection round-trips through prost and strips
  `google.api.http` (Go/Java/C++ preserve it). Bundled descriptors always carry them.
  Filed upstream: [grpc/grpc-rust#2719](https://github.com/grpc/grpc-rust/issues/2719)
  (root cause + proposed fix; PR offered).

## Native server library

Serves native gRPC pass-through + grpc-webnext unary + streaming on one port, backed by a
tonic `Routes` (Rust) or a `*grpc.Server` (Go). Deferred:

- [x] ~~**`+json` is binary-only (UNIMPLEMENTED).**~~ **Done** — descriptor-based
  transcoding via `ServerConfig::transcoder`, which is what the Codec section below
  already recorded. (This entry contradicted it; closed in the 2026-07-27 sweep.)
- [x] ~~**Graceful shutdown / drain.**~~ **Done (2026-07-27)**, in Rust and Go, across all
  four surfaces. Rust adds `serve_in_process_with_shutdown` / `serve_proxy_with_shutdown`
  (the returned future *is* the drain, so `tokio::time::timeout` bounds it and dropping it
  force-closes — no drain-timeout knob to get wrong); Go adds `webnext.NewServer` with
  `Serve`/`Shutdown(ctx)`, mirroring `http.Server`. Fetch and native gRPC drain via hyper's
  own `graceful_shutdown` / `http.Server.Shutdown`; h2ts tunnels get a real `GOAWAY`,
  because both servers now own the HTTP/2 connection running inside the tunnel; a
  custom-`Frame` WebSocket with no stream yet is closed `1001`, and one with a live stream
  runs to its terminal frame. Covered by `tests/inproc_shutdown.rs` and
  `webnext/shutdown_test.go`. **Not drainable by design:** the *proxy's* h2ts tunnels are a
  byte-transparent pump, so there is no `GOAWAY` to inject without parsing the traffic the
  proxy exists not to parse — those run until the caller's deadline.
- [x] ~~**Per-WS max-concurrent-streams cap** and idle cleanup.~~ **Moot** — a WebSocket
  carries exactly one stream, so the cap is 1 by construction (a second `Subscribe` is
  answered `Reset{INVALID_ARGUMENT, "stream already open"}`). Idle cleanup is covered by
  the two mechanisms that replaced it: server-driven keepalive drops a dead peer
  (`ws_keepalive` + `ws_keepalive_timeout`), and single-stream teardown closes the socket
  as soon as its one stream reaches a terminal frame.
- [x] ~~**Unary buffers the whole response** before framing.~~ **Done** — the `+proto`
  Fetch response is streamed, not buffered: the status rides in the trailer block *after*
  the message, so the inner gRPC frame is piped to the socket (minus its compression-flag
  byte) and the trailer block appended at the end (`fetch.rs` `async_stream` + `StreamBody`;
  Go: `fetch.go` chunked copy + flush). A large blob is never malloc'd. `+json` still
  buffers by necessity — the transcode is whole-message and the status must precede the body.
- Note: deadlines *are* enforced here (grpc-timeout is forwarded and the inner
  tonic server honors it), and client cancellation drops the inner call future.

## Auth

- [x] ~~WebSocket connection-time auth (`connect_auth`).~~ **Removed** — a connection-scoped
  app-credential gate is non-canonical (the ecosystem gates connections only on network
  identity, e.g. mTLS) and can't be uniform (Fetch has no connection), so it was a footgun.
  With it went the `bearer.<token>` subprotocol machinery (`ws_bearer_token`, the client's
  subprotocol derivation). Auth is now purely per-RPC at the router; a browser credential
  needed at the edge travels as request metadata (or dynamic metadata via an Envoy filter,
  below). The `4000 + code` handshake close remains for codec/surface rejections. See
  `spec/PROTOCOL.md` "Auth".
- [x] ~~Per-stream authorization.~~ **Removed** — a grpc-webnext-specific `stream_auth`
  hook was redundant with a tonic interceptor, which the request already reaches on every
  transport (and which, unlike the hook, also covers the native/h2ts path). Per-RPC auth is
  now a router interceptor (in-process) / the upstream server (proxy). Pinned by
  `tests/inproc_auth.rs::tonic_interceptor_guards_both_grpc_webnext_surfaces`.
- [x] ~~Proxy-side per-stream auth hooks~~ — moot: proxy per-RPC auth is the upstream
  server's interceptors; the proxy forwards metadata opaquely.

## Envoy integration (dynamic-module filters)

Goal: a user runs **stock Envoy** — no sidecar process, no custom Envoy build — and the
grpc-webnext browser transports are terminated **inside** Envoy by runtime-loaded native
**dynamic modules** (Rust SDK; `.so` located via `ENVOY_DYNAMIC_MODULES_SEARCH_PATH`).
After termination the traffic is ordinary HTTP/2 gRPC, so Envoy does routing, `ext_authz`,
rate-limit, LB, and tracing natively and grpc-webnext does **zero** L7 — the same clean split
the proxy already has (it already translates every path to clean gRPC; see `src/h2ts.rs`,
`fetch.rs`, `ws.rs`). This is the ecosystem analog of Envoy's own `grpc_web` filter; the wire
contract is `spec/PROTOCOL.md`, so each filter is a **port of the existing translation logic**
(and h2ts's wslay bridge), not new semantics. Dynamic modules confirmed to support HTTP **and
network** filters (docs), but are "under active development", so treat the ABI specifics as
needing verification.

- [x] ~~Spike: confirm the load-bearing network-filter ABI capabilities.~~ **Confirmed** in
  the SDK (`EnvoyNetworkFilter` trait, envoyproxy/envoy Rust SDK, verified against the pinned
  rev). Read path: `get_read_buffer_chunks` + `drain_read_buffer` consume the inbound WS bytes;
  **`inject_read_data(data, end_stream)` — "inject data into the read filter chain (after this
  filter)"** forwards the de-framed h2c to the next filter (HCM). Downstream write:
  **`write(data, end_stream)` — "write directly to the connection (downstream)"** emits the
  `101` + outbound WS frames; response re-framing uses `get_write_buffer_chunks` /
  `drain_write_buffer` / `inject_write_data`. Lifecycle via `on_new_connection` / `on_event` /
  `close`. **Bonus:** the filter can stash handshake material — SNI (`get_requested_server_name`),
  cert SANs (`get_ssl_uri_sans`), a subprotocol token — into **dynamic metadata**
  (`set_dynamic_metadata_*`) for Envoy `ext_authz`/RBAC, a clean bridge for browser-handshake
  credentials. **Caveat:** the SDK is **version-locked** to Envoy (git dep pinned to a rev,
  strict ABI compat) — the module builds against a matching Envoy release.
- [ ] **Fetch (unary / server-stream) → HTTP filter.** grpc-web-shaped body translation:
  rewrite the length-prefixed request into gRPC and buffer the response trailer into the
  `[msg][trailer]` body (browsers can't read trailers). 1 request = 1 stream → an in-place
  transform. Easiest path; directly analogous to `grpc_web`. Likely viable in Wasm too, but
  a Rust dynamic module keeps one toolchain.
- [ ] **Custom-Frame WS → filter (single-stream 1:1).** Decode `Frame` protobufs off the WS
  upgrade and map the one WS to one gRPC stream (Subscribe→HEADERS, Message→DATA, response→
  Header/Message/Trailer frames). Retiring multiplexing made this a clean 1:1 map.
- [ ] **h2ts → network filter before HCM.** Run the wslay de-frame (own the WS handshake +
  framing) as a **network** filter and hand the inner h2c byte stream to a downstream
  `HttpConnectionManager` (codec `HTTP2`), which natively demuxes it into N routed streams —
  so "1 WS → N streams" is HCM's job, not the filter's. Essentially h2ts-server's `accept` +
  `bridge` ported to the Envoy net-filter ABI — **confirmed viable** by the spike above
  (`inject_read_data` forwards h2c to HCM; `write` emits the `101`/WS frames).
- [ ] **Deployment doc + client-profile guidance.** Topology: browser → stock Envoy (these
  filters terminate) → routing/authz/LB. Document the h2c details the filters must set for
  Envoy routing/authz (`:path`, `:authority`, `content-type`, `te`, metadata pass-through),
  and the transport split: behind a mesh any client profile is terminable; for
  **direct-to-server** (no Envoy) h2ts stays the default. Note Wasm can host network filters
  too but is impractical for a high-throughput H2 tunnel (per-byte VM boundary) — dynamic
  modules (Rust, native) are the chosen mechanism.

## HTTP transcoding (`google.api.http`)

- [x] ~~REST transcoding on the Fetch path.~~ `crates/core/src/httprule.rs` compiles
  `google.api.http` bindings from the descriptor pool (via prost-reflect's extension
  reading) and maps `(HTTP method, path)` onto a gRPC method, binding path segments,
  query params, and the body into the request message. The native server tries a REST
  match first and falls back to a direct `/pkg.Service/Method` JSON call. Covered by
  `server/tests/json.rs` (`transcode_*`). The google/api protos are vendored **once**, at
  `/proto/google/api/`, and included from there by every consumer (the Rust test service,
  the conformance protos, and the Go/TS codegen) — moved there on 2026-07-27 when the
  conformance suite started needing them too.
- [x] ~~**Unsupported HttpRule bits.**~~ **Closed 2026-07-28.** Audited in full on
  2026-07-27 alongside the Go port, then finished: `response_body`, multi-segment captures
  (`{name=shelves/*/books/*}`), bare `*`/`**` wildcards, `custom { kind: "*" }`, `bytes` and
  well-known types from a query, `body:` naming any top-level field, and field names
  resolving by JSON spelling. Every one landed in Rust and Go together.

  **There are no functional gaps left** in the `google.api.http` method option. What remains
  is one *declined* input format — `HttpRule.selector` / service-config rule lists, a new
  configuration surface rather than a parsing fix, which the conformance matrix could not
  cover without every implementation also growing service-config loading — plus a set of
  recorded decisions where the spec itself forbids something. Both are written up, with
  their reasoning and the tests that pin them, in [doc/HTTPRULE_GAPS.md](HTTPRULE_GAPS.md).

  *A pattern worth carrying forward: three of these ("`body:` on a scalar", "`bytes` from a
  query", "well-known types from a query") looked like three features and were one idea —
  stop hand-writing conversions and hand the value to the JSON decoder, which already knows
  every protobuf-JSON spelling. That is `response_body` in reverse. Reach for it before
  writing a parser.*

- [x] ~~**REST transcoding in the Go server.**~~ **Done (2026-07-27.)** `go/webnext/httprule.go`
  is a structurally parallel port of `httprule.rs`, wired into both surfaces: Fetch tries a
  REST binding before the main method path (and before the implicit-codec gate, since
  annotated endpoints accept plain HTTP unconditionally), and an annotation-matching WS
  upgrade becomes a text-locked single-stream JSON route. `go/webnext` now serves **every**
  spec surface. See `doc/GO_SERVER.md`.
- [x] ~~**REST conformance cases.**~~ **Done (2026-07-27.)** `conformance/cases/rest.yaml`
  — 18 cases × 2 servers — covers `body:"*"` on both surfaces, path/query binding,
  `additional_bindings`, binding precedence, status/metadata fidelity, and both
  wrong-surface rejections. Both anticipated blockers were paid rather than dodged:
  `conformance.proto` now imports `google/api/annotations.proto`, vendored **once** at
  `/proto/google/api` and included from there by all three toolchains; and the driver runs
  a `rest:` case with a **raw HTTP client** instead of the grpc-webnext one, which is the
  claim being tested (an annotated URL should need no SDK). A `rest:` case therefore runs
  once, not once per transport/codec profile. Found a build bug on its first run — see
  `conformance/README.md`.

## WebSocket streams / multiplexing

- [x] ~~Multiplexing off by default; human-readable single-stream JSON.~~ Superseded by
  the h2ts pivot: the custom `Frame` path is **always one WebSocket per stream**, connected
  to the method's URL — frames carry no `streamId` and no `method` (both implied by the
  route), and the first inbound frame opens the stream. The opt-in `+multi` subprotocol and
  the client's `multiplex`/`poolSize` options were **deleted outright**, along with
  `stream_id` (the proto field is removed, not reserved). Multiplexing, when you want it,
  is real HTTP/2 over h2ts. Verified gone: no `stream_id` / `streamId` / `poolSize` /
  `+multi` / `max_concurrent_streams` anywhere in the tree.
- [x] ~~WebSocket handshake auth gate (method-scoped).~~ **Removed** with `connect_auth`
  (above) — auth is per-RPC at the router on every transport, with no WS-handshake gate.
  (This item also predated single-stream; the `?method=` multiplex variant is gone too.)
  (`connect_gate_*`, `multiplex_auth_*`, `no_credential_opens_the_connection`).
- [x] ~~**WS pool never reaps idle connections** (multiplex mode).~~ **Moot** — there is
  no pool. One WebSocket per stream, closed as soon as that stream terminates.

## Protocol

- [x] ~~Keepalive~~ — done as native **WebSocket ping/pong** on a timer
  (`ServerConfig::ws_keepalive` / `ProxyConfig::ws_keepalive`), with gRPC-style
  pong-timeout drop (`ws_keepalive_timeout`). The old app-level `Ping`/`Pong` frame
  kinds were removed (field numbers reserved). See `doc/STATUS.md`.
- [x] ~~**Fragmentation** (README point 11 "another day").~~ **Resolved by the h2ts
  integration** — the same way backpressure and large-payload streaming were. Both halves of
  the original motivation are gone on the default path:
  - *Fairness/interleaving* — moot twice over: one WebSocket carries one stream, and where
    streams do share a connection it is real HTTP/2, which interleaves DATA frames natively.
  - *Peak memory* — HTTP/2 **is** the fragmentation mechanism. A large gRPC message is split
    across bounded, flow-controlled DATA frames (`SETTINGS_MAX_FRAME_SIZE`, 16 KiB default),
    and h2ts passes them through **sub-frame**: wslay runs with `no_buffering` and never holds
    a whole WebSocket frame (`h2ts-server/src/wslay.rs`), and the Go `*Conn` presents the
    tunnel as a continuous byte stream. So the transport never materializes the message; only
    the gRPC codec does, at the message boundary — exactly as native gRPC does.

  A bespoke `fragment` frame kind would therefore be reimplementing H2 DATA framing, worse and
  only for the opt-out paths. **Deliberately not pursued** for those: on the custom `Frame`
  path one message is still one WebSocket message, materialized at both ends and bounded by
  `max_message_bytes` (4 MiB default) — and `json` could never be incremental regardless, since
  it transcodes whole messages. If you need very large messages, the default path already
  handles them; that is the answer, not a new frame kind.
- [x] ~~**Proxy: stream large WS message payloads without buffering the whole frame.**~~
  **Resolved by the h2ts integration.** The motivation was a proxy-only peak-memory win: the
  proxy forwards `+proto` opaquely, so it never needs the whole message — only tungstenite's
  read-frame-into-memory forced materialization. The fix envisioned here (drive **wslay**'s
  `on_frame_recv_chunk_callback` to pipe payload chunks straight through) is now exactly what
  the **default proto path** does: it runs over h2ts, and the proxy forwards it with
  `h2ts_server::bridge` (`src/h2ts.rs`) — an opaque, zero-buffer, sub-frame byte pump to the
  h2c upstream (wslay `no_buffering`; never holds a whole message). The old blocker ("wslay is
  C with no crate — needs a thin FFI wrapper") is gone: it's vendored in `wslay-sys`, pulled in
  via `h2ts-server`. **Deliberately not pursued** for the remaining custom-`Frame` proxy paths:
  `proto` + `streaming:"ws"` still materializes each message, but it's an opt-out from the h2ts
  default (use the default for the opaque win) — and `WsByteStream` can't serve it directly
  because it erases the WS message boundaries the `Frame` protocol uses as delimiters (would
  need an upstream h2ts API that preserves them). The `+json` proxy path can never be
  incremental — it must transcode each message, so it materializes by definition.

## TypeScript client

Two client flavors ship: callback/EventEmitter (`makeClient`) and promise/async-iterable
(`makePromiseClient`). All four cardinalities + AbortSignal cancellation are covered
end-to-end. Remaining:

- [x] ~~**No retry / reconnect.**~~ **Done (2026-07-28).** Two separate things wearing one
  bullet, and the reconnect half was the urgent one — it was three live bugs, not a
  missing feature.

  *Reconnect.* Every call rides one lazily-opened `H2Connection`, cached in a field that
  was **never cleared**. So a tunnel that died stayed cached and every later call failed
  forever; a client that started before its server cached the *rejected dial* and stayed
  broken even once the server came up — and nothing fires a `closed` event for that one, so
  it could never recover. On top of which the failure was reported `UNKNOWN` rather than
  `UNAVAILABLE`, which is both wrong (UNKNOWN means "the server said something we could not
  interpret") and load-bearing, since it made the failure unretryable under any sane policy.
  The cache now evicts on close *and* on dial failure, never hands out a dead entry, and a
  request handed to an already-dead tunnel is replayed once — gRPC's **transparent** retry,
  which is not a policy decision and so lives in the transport. Pinned by
  `reconnect.test.ts`, which cuts the tunnel with a TCP relay while leaving the server up,
  so a failure to recover is unambiguously the client's.

  *Retry.* gRPC's design: jittered exponential backoff, `retryableStatusCodes`,
  `grpc-retry-pushback-ms` (including the negative "stop" case), and the token-bucket
  throttle. **Off unless configured**, matching gRPC and matching this file's own position
  on retry storms — the reason proxy-side retry was removed. Streams follow the commit
  rule (retry until the first message reaches the caller); client-streaming and bidi are
  excluded because replaying them needs an unbounded buffer of written messages, which is
  the same thing the flow-control work declined to grow on the response side. Deviations
  from gRPC, both deliberate and written up in `spec/COMPATIBILITY.md`: config is a client
  option rather than per-method service config, and hedging is not implemented.

  That compatibility table, incidentally, had been claiming retries were `✅ client-side
  policy` on every column the whole time — including hedging. Fixed; the table now says
  what is true.
- [x] ~~**`ClientReadableStream` has no backpressure / pause.**~~ **Done (2026-07-28),
  and it split into two very different halves.**

  *Real backpressure, on the default path.* `deliver()` returns a promise while the
  consumer is behind and the h2ts read loop awaits it. That is the entire mechanism: h2ts
  hands out response bodies as `highWaterMark: 0`, consumption-driven streams, so not
  reading withholds `WINDOW_UPDATE` and **tonic/grpc-go blocks**. Pinned by
  `flow-control.test.ts`, whose load-bearing assertion is not the queue depth but that a
  2.5 MiB response *cannot terminate* while the consumer is away — with the `await`
  removed, 39 messages arrive and the RPC completes. Once again the answer was "the
  default path already has it": the fourth item in this file to close that way.

  *Not backpressure, on the custom path — and that is the answer, not a TODO.* A browser
  `WebSocket` has no receive-side flow control: no `pause()`, no unread side, `onmessage`
  fires regardless. A credit frame would be a protocol the client could not honor from the
  one place it matters, and would reimplement HTTP/2 flow control for the opt-out
  transport — the same reasoning that rejected a bespoke `fragment` frame kind above. So
  messages queue, as in every browser WebSocket library, and the client instead **exposes**
  the backlog (`readableLength`) and can stop it growing (`pause()`/`resume()`). Written up
  in `spec/COMPATIBILITY.md` and pinned by a test asserting the queue *does* grow past the
  mark there — so if this ever changes, that test says so.

  *A real bug fell out.* The stream fed its own iterator queue **as well as** emitting
  `data`, so a callback consumer retained every message it had already handled, for the
  life of the stream. Attaching a `data` listener is now flowing mode (Node's rule) and
  retains nothing. Left alone it would have become a deadlock rather than a leak once
  backpressure landed — a flowing consumer never drains that queue, so it would have parked
  the transport at the high-water mark forever. Mutation-checked: reinstating it hangs the
  suite.
- [x] ~~Deadlines sent but not locally enforced~~ — a client-side timer (`context.ts`)
  now fires DEADLINE_EXCEEDED on both the Fetch and WebSocket paths.
- [x] ~~Server/client-streaming untested~~ — covered via the promise-client e2e
  (Greeter server-stream, client-stream, bidi).
- [x] ~~AbortSignal → WebSocket cancel~~ — `signal` now sends a `Reset` and locally
  terminates the stream with CANCELLED (deadline aborts report DEADLINE_EXCEEDED).

### A REST helper in the client — **declined** (2026-07-28)

Calling a method through its `google.api.http` URL from the typed client, rather than
`POST /pkg.Service/Method`. Not deferred — **declined**, because the feature argues against
itself:

- **The client already reaches every service with JSON**, over the base endpoints. There is
  no capability the helper would unlock, only a different URL.
- **The annotated URL exists for consumers who have no SDK.** That is the claim
  `conformance/cases/rest.yaml` deliberately tests by driving `rest:` cases with a *raw HTTP
  client*. A helper is the SDK reaching for the thing defined as the no-SDK path.
- **The typed part is already free.** ts-proto emits `fromJSON` for every message, so a
  cacheable annotated `GET` — the one thing the base endpoints genuinely cannot offer, since
  they are all POSTs — is already two lines:
  `ConformancePayload.fromJSON(await (await fetch("/v1/rest/5")).json())`. The helper would
  save the URL template, nothing more.
- **The cost is permanent.** It is a *third* HttpRule implementation, running backwards
  (message → URL, where both servers do URL → message), and
  [`HTTPRULE_GAPS.md`](HTTPRULE_GAPS.md) already requires the two existing routers to change
  in lockstep. Trading a maintained inverse implementation for a saved string is the wrong
  side of that bargain — and an inverse is exactly where drift hides.

Reversible if a deployment turns up that can *only* be reached through annotated paths, in
which case the right shape is probably a REST-specific generated client, not a helper bolted
onto the gRPC one.

**Correction to the record:** this item had been carried as blocked on codegen — "ts-proto
emits no annotation information". That is **false**. ts-proto does not *resolve* custom
options, so it preserves them as unknown fields, and the annotation is in the generated
output today: `options._unknownFields[578365826]` (tag for field 72295728,
`google.api.http`). All six annotated conformance methods carry theirs intact, nested
`additional_bindings`, `response_body` and multi-segment captures included. The blocker was
imaginary; the decision above is made on merits instead.

## Codec

- [x] ~~JSON support~~ — the native server transcodes `+json` <-> protobuf via a
  descriptor-set `Transcoder` (`ServerConfig::transcoder`). JSON is **native on the
  wire**: Fetch responds with a bare JSON body + status in HTTP headers; WebSocket
  uses JSON **text** frames (native message, not base64) — the WS text/binary type
  selects the codec. The TS client has a `codec: "json"` option. Covered by
  `server/tests/json.rs` and `clients/typescript/test/json.test.ts`.
- [x] ~~**`Subscribe.json` flag is now vestigial.**~~ **Removed (2026-07-28.)** It turned out
  to be even less load-bearing than this entry claimed: no server read it, and it never
  reached the wire in a meaningful state either — Rust and Go set it only inside their
  JSON-frame→proto converters (JSON frames are text, so that `Subscribe` is never
  serialized), and the TS client set `json: false`, which proto3 omits as a default. So the
  removal was a pure cleanup with **zero wire impact**, not the field-number retirement this
  entry braced for. Field 5 is `reserved` anyway, matching how the removed `ping`/`pong`
  numbers were handled.
- [x] ~~**Binary metadata (`-bin`) is omitted from JSON frames** (ASCII only).~~ Not a
  gap — a **recorded decision**, now normative: *"Binary (`-bin`) metadata is dropped
  crossing into the JSON codec — JSON frames carry ASCII metadata only"*
  (`spec/PROTOCOL.md`, "JSON frame edge semantics"). Base64-in-JSON would make the frame
  less readable, which is the JSON path's whole point; a client needing binary metadata
  uses the binary codec, where it round-trips as raw bytes (pinned by the
  `metadata-roundtrip-websocket` conformance case).
- [x] ~~**JSON in the proxy** remains out (binary-only).~~ **Done (2026-07-05)** — the
  proxy terminates `+json` via `ProxyConfig::schema` (reflection, a bundled descriptor set,
  or both), producing output identical to the native server's. (This entry contradicted the
  proxy section above; closed in the 2026-07-27 sweep.)

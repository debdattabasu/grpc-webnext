# Proxy / native-server unification audit

*2026-07-05. Why the proxy and the native server were two near-duplicate codebases,
whether they needed to be, and what it took to merge them.*

> **Status: done (2026-07-05).** All phases are complete — server, proxy, transport, and
> core are now a single `grpc-webnext` crate (library + proxy binary) whose only
> per-surface difference is a two-variant `Backend` enum. The analysis below is retained as
> the rationale; see "Proposed path" for what was actually done.

## TL;DR

We do **not** structurally need two codebases. The proxy and the native server already
share every *wire primitive* (all of `grpc_webnext_core`), and differ in exactly one
thing: **where the gRPC call goes.** The server dispatches into an in-process
`tonic::service::Routes`; the proxy forwards to a remote `tonic::transport::Channel`.
Both are `tower::Service<http::Request, Response = http::Response>` invoked the same way —
`.oneshot(req)`. Everything wrapped around that one call — WebSocket frame handling, codec
negotiation, JSON transcoding, keepalive, deadlines, size limits, stream-level errors,
metadata mapping — is ~70–80% duplicated glue that drifts independently. This session alone
found and fixed **four** proxy/server divergences (see `STATUS.md`), every one of them a
copy that fell out of sync. That is the cost of the split, and it will recur.

The fix is to extract the shared translation layer into one module generic over a small
`Backend` trait, with `Routes` and `Channel` as the two implementations. The blockers below
are all tractable; none is fundamental.

## Evidence: the two crates are structural twins

Line counts (orchestration glue only — core primitives excluded):

| | `lib.rs` | `ws.rs` | other |
|---|---|---|---|
| server | 704 | 513 | — |
| proxy | 678 | 503 | `schema.rs` 214, `reflect.rs` 253 (both `+json`-only) |

The function inventories line up almost 1:1:

| Responsibility | server | proxy |
|---|---|---|
| accept loop | `serve` / `bind_and_serve` | `serve` / `bind_and_serve` |
| request router | `handle` | `Proxy::handle` |
| native gRPC forward | `passthrough` | `Proxy::passthrough` |
| `+proto` unary | `unary` | `handle_unary` |
| `+json` / REST unary | `json_fetch` → `json_unary_call` | `handle_json_fetch` → `unary_json_upstream` |
| request-body framing | `frame_upstream_request` | `frame_upstream_request` |
| JSON Fetch response | `json_fetch_response` / `webnext_error` | `json_fetch_response` / `json_error` / `fetch_status` |
| WS connection loop | `ws::serve` | `ws::serve` |
| WS frame decode | `decode_binary` / `decode_text` | `decode_binary` / `decode_text` |
| WS frame encode | `to_tung` | `to_tung` |
| WS dispatch | `handle_frame` | `handle_frame` |
| WS per-stream pump | `run_stream` | `run_stream` |
| WS reset/trailer | `send_reset` | `send_reset` / `send_trailer` |
| keepalive helpers | `keepalive_interval` / `next_tick` / `sleep_until` | `keepalive_interval` / `next_tick` / `sleep_until` *(identical)* |

`decode_binary`, `decode_text`, `to_tung`, `keepalive_interval`, `next_tick`,
`sleep_until`, and `frame_upstream_request` are **character-for-character identical** (or
differ only in an error-helper name). They are pure copies.

## The one real difference: local dispatch vs remote call

Compare `server::unary` (`crates/server/src/lib.rs:525`) and `proxy::handle_unary`
(`crates/proxy/src/lib.rs:307`) — pre-unification paths, cited as prose rather than links
because the merge this document describes is what deleted those files. Both:

1. parse the method path,
2. stream the length-prefixed request body into a gRPC frame (`frame_upstream_request`),
3. build a gRPC request (forward metadata minus the hop-by-hop denylist, force
   `application/grpc`, `te: trailers`, forward `grpc-timeout`),
4. **dispatch**,
5. map initial metadata → response headers,
6. stream the message block (drop the 1-byte compression flag) and append the trailer
   block built from the terminal status — with the same `EMPTY_MESSAGE_BLOCK` fallback for
   a trailers-only response.

Steps 1–3 and 5–6 are identical. Step 4 is the entire divergence:

```rust
// server                                  // proxy
routes.oneshot(grpc_req).await             channel.oneshot(grpc_req).await
    .unwrap_or_else(|e| match e {})            // fallible: BAD_GATEWAY / Unavailable
```

`Routes::Error` is `Infallible` (`match e {}`); `Channel::Error` is a transport error. Both
take an `http::Request<_>` and return an `http::Response<_>`. **That is the whole reason
there are two codebases** — and it's a difference in one associated type, not in the
protocol logic.

## Blockers to unification (all tractable)

| # | Blocker | Why it looks fundamental | Actually | Resolution |
|---|---|---|---|---|
| 1 | Backend type: `Routes` vs `Channel`; different `Error` + response-`Body` assoc types | "One is a server, one is a client" | Both are `tower::Service<Request<TonicBody>, Response = Response<B>>`; both are already reduced to `.oneshot()` + `.map(\|b\| b.map_err(Into::into).boxed_unsync())` | A `Backend` trait (or a generic bound `S: Service<…, Error: Into<BoxError>>, B: Body<Data = Bytes>`). `Infallible: Into<BoxError>` already holds. |
| 2 | Transcoder source: server holds `Option<Arc<Transcoder>>` (sync, in-process); proxy holds `Schema` (async: reflection/bundled/TTL/reload) | Reflection machinery is proxy-only | The server's transcoder is a strict **subset** of `Schema` (a permanent `Bundled`/`None`) | Move `Schema` into core; the server uses `Bundled`/`None`. `Schema::transcoder` is already async — a synchronous in-process transcoder is just an always-ready future. `reflect.rs` stays feature-gated to the proxy. |
| 3 | Auth hooks (`connect_auth`, `stream_auth`) exist only on the server | Proxy has no auth model | The shared layer takes them as `Option<…>`; the proxy passes `None` | Optional fields on the shared config. (Both were later **removed** — auth is a per-RPC router interceptor on every transport; grpc-webnext ships no auth hooks.) |
| 4 | Deadline enforcement: proxy runs a local timer + forwards `grpc-timeout + grace`; server delegates to the in-process router | Remote calls need client-side cancellation; local ones don't | Local enforcement is *harmless* for the in-process case (the router also honours the header) | Always enforce locally in the shared path, or gate it with one `Backend` flag (`enforces_own_deadline`). |
| 5 | Proxy-only knobs: `max_concurrent_streams`, `admin_reload_path`, upstream URI; retry deliberately removed | Proxy-specific policy | These are config values, not protocol | Live on the backend-specific config extension; the shared loop reads `max_streams: Option<usize>` etc. |
| 6 | `ServerConfig` vs `ProxyConfig` | Two structs | Overlap is `max_message_bytes`, `ws_keepalive`, `ws_keepalive_timeout`, `allow_implicit_codec` | Factor a shared `CoreConfig`; each crate wraps it with its extras. |
| 7 | Style: server is free fns `(routes, config)`; proxy is `&self` methods on a `Proxy` struct | — | Cosmetic | Pick one (a `Server<B: Backend>` holder reads best). |

Nothing on this list is a design contradiction. #1 is the only one with real Rust friction
(async-trait ergonomics, body-type erasure, `Send`/`'static` bounds on the generic), and
even that is a well-trodden pattern (`tower::Service` + `BoxBody`).

## What is already shared (so it is *not* a blocker)

All wire primitives live in `grpc_webnext_core` and are used verbatim by both crates:
`Transcoder`, `json_frame::{json_frame_to_proto, proto_frame_to_json, json_open_to_subscribe,
decode_json_frame, encode_json_frame}`, `metadata::*`, `httprule` / `WsBinding`, `Deframer`,
`BytesCodec`, `decode_frame` / `encode_frame`, `grpc_frame`, and the generated `pb` types.
The protocol *vocabulary* is unified already; only the *orchestration* is duplicated.

## Proposed path (phased, lowest-risk first)

- **Phase 0 — dedupe the pure copies (no abstraction).** ✅ **Done 2026-07-05.** Created
  the `grpc-webnext-transport` crate (`crates/transport`) and moved the character-identical
  WebSocket helpers there: `decode_binary`, `decode_text`, `to_tung`, `keepalive_interval`,
  `next_tick`, `sleep_until` — the frame codec and keepalive timing, i.e. exactly where the
  divergences kept appearing. They now have a single home; both `ws.rs` files shrank ~80
  lines each. (A new crate rather than `core` because these touch `hyper-tungstenite` /
  `tokio::time`, which don't belong in the pure wire-primitives crate — and this is the
  eventual home for Phase 1.) Still-pending pure copies that carry a crate-local `ResBody`
  in their signature — `frame_upstream_request`, `text_response`, `boxed_full`, the
  trailer-block streaming builder, `EMPTY_MESSAGE_BLOCK` — fold in with Phase 1, once a
  canonical `ResBody`/`BoxError` lives in the transport crate.
- **Phase 1 — the dispatch enum + shared handlers.** ✅ **Done 2026-07-05.** Rather than a
  generic `Backend` trait (async-trait ergonomics, `Send`/`'static` bounds threaded through
  the call graph — the "when not to" risk below), an **enum** — there are exactly two
  backends:
  ```rust
  enum Backend { InProcess(tonic::service::Routes), Upstream(tonic::transport::Channel) }
  impl Backend { async fn call(&self, req: Request<TonicBody>) -> Result<Response<ResBody>, BoxError> { … } }
  ```
  The two backends' response bodies differ in type; the `match` in `call` boxes both into
  one `ResBody`, so everything downstream is monomorphic — no generics, no async-trait. The
  Fetch (`handle`/`unary`/`json_fetch`) and WebSocket (`serve`/`handle_frame`/`run_stream`)
  handlers now live once in the crate and dispatch through `Backend::call`; `run_stream`
  uses `Backend::call` + a shared `Deframer` for both surfaces (the proxy dropped its
  `client::Grpc`/`BytesCodec` path). A protocol change is now written **once**.
- **Phase 2 — one crate.** ✅ **Done 2026-07-05.** `grpc-webnext-server`,
  `grpc-webnext-proxy`, the transitional `grpc-webnext-transport`, and `grpc-webnext-core`
  are **collapsed into a single `grpc-webnext` crate** (library + proxy binary), published
  as one. Two entry points build one internal `Runtime` over the shared handlers:
  `serve_in_process(routes, ServerConfig)` and `serve_proxy(ProxyConfig)` (plus
  `bind_and_serve_*`). `Schema` gained `from_transcoder` for the in-process case (a static
  transcoder, no channel); reflection stays proxy-only. The full server + proxy test suites
  moved into the crate (`tests/inproc_*` + `tests/proxy_*`) — 90 tests, all green.

## Outcome (the "when not to" risk did not materialize)

The enum sidestepped the generic/async-trait friction the trait approach would have hit, so
there was no reason to stop early. One deliberate **behavior consolidation** fell out: the
WebSocket handshake now uniformly rejects a blank codec subprotocol on a main endpoint
unless `allow_implicit_codec` (the documented server behavior) — the proxy's old lenient
"blank ⇒ binary" default is gone. Net: ~1000 fewer lines, and the four divergences that
kept recurring (see `STATUS.md`) can no longer happen — the code that produced them is now
written once.

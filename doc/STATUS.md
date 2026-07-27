# Protocol conformance status

> **Note (2026-07-27):** the **Go** in-process server is implemented and joined the
> conformance matrix, which now runs every case against both server implementations.
> Writing the second implementation surfaced two real Rust bugs — `-bin` metadata dropped
> on the WebSocket path, and a size-limit `Reset` that left the socket open — both fixed,
> both now pinned by tests. See `doc/GO_SERVER.md`.

> **Note (2026-07-05):** the `grpc-webnext-core`, `-server`, `-proxy`, and `-transport`
> crates were unified into a single `grpc-webnext` crate (library + proxy binary) — see
> `doc/UNIFICATION.md`. Dated entries below cite the pre-unification paths
> (`crates/server/…`, `crates/proxy/…`, `crates/core/…`); those files now live under
> `crates/grpc-webnext/src/` (the two `ws.rs`/`lib.rs` pairs became `ws.rs` + `fetch.rs`),
> and the migrated tests under `crates/grpc-webnext/tests/{inproc,proxy}_*.rs`.

Audit of `PROTOCOL.md` against the implementation and test suites (2026-07-03).
Verdict: the doc is largely accurate — routing rules, close codes, frame shapes,
transcoding, and the auth handshake all match the code, with solid coverage on the
happy paths. Three real behavioral drifts, one internal doc contradiction, and a
cluster of test gaps concentrated in the httprule subset and the proxy's JSON
handling. Drift 2 (client close-code reconstruction) and drift 3 (keepalive) are
fixed as of 2026-07-04. Drift 1 is resolved as of 2026-07-05 — by *implementing*
proxy `+json` (transcoding), not by rejecting it. As of 2026-07-05 PROTOCOL.md is
reconciled: the stale "v1 proxy rejects `+json`" section is rewritten, the internal
contradiction is fixed, and every previously-undocumented wire-observable behavior
is now written up under "Limits & error surfaces". Verifying those behaviors against
the code turned up four **proxy/native-server divergences**, all **harmonized in code as
of 2026-07-05** — see "Divergences to reconcile" below. That they kept appearing is itself
a signal: the proxy and native-server WS/JSON paths are near-duplicate code that drifts
independently — audited in `doc/UNIFICATION.md`, which finds the split is not structural
(both backends are a `tower::Service`) and proposes a phased merge.

## Drift: doc says X, code does Y

### 1. The proxy did not handle `+json` correctly — RESOLVED (2026-07-05)

PROTOCOL.md said the v1 proxy "rejects `+json` with `UNIMPLEMENTED`". The original
finding: Fetch returned an HTTP `501` plain-text body (not a gRPC trailer), and
WebSocket didn't reject at all — it echoed `grpc-webnext+json`/`+json+multi` back,
negotiated the connection, then forwarded JSON payloads opaquely to an upstream that
expects binary protobuf, so the client got garbage errors instead of a clean close.

Resolved by making the proxy **terminate `+json`** rather than reject it. When a
descriptor source is configured (`ProxyConfig::schema`), the proxy transcodes the
JSON request to the upstream's binary protobuf and the binary response back to JSON,
on both Fetch and WebSocket, reusing the same core `Transcoder` as the native server
(so a client can't tell the two apart). Descriptors come from **upstream gRPC
reflection** (v1 with a v1alpha fallback), a **bundled `FileDescriptorSet`**, or **both**
(`SchemaSource::{Reflection, Bundled, ReflectionOrBundled}` — the last is reflection with
the bundle as an immediate fallback). With no source (`SchemaSource::None`, the default)
`+json` returns a gRPC `UNIMPLEMENTED` status — now a proper status-in-header response on
both surfaces, not an HTTP 501.

- Binary `+proto` is untouched: still schema-agnostic, still streamed opaquely.
- Reflection is loaded **eagerly and whole** (2026-07-05): on startup the proxy
  `list_services` + fetches every service's transitive closure into one snapshot,
  refreshed on a TTL (`ProxyConfig::reflection_ttl`, default 4h). An optional
  management endpoint (`admin_reload_path`, `POST`) forces an immediate reload;
  `Schema::reload()` exposes the same programmatically. Requests block (bounded) only
  for the first load. See `crates/proxy/src/reflect.rs` / `schema.rs`.
- Unknown methods surface as `UNIMPLEMENTED` uniformly (`Transcoder::has_method`).
- Covered by `crates/proxy/tests/json.rs` (bundled + reflection × Fetch + WS, plus
  no-schema, unknown-method, and admin-reload cases).

REST / `google.api.http` annotation routing also works on the proxy (2026-07-05), on
both Fetch (`handle_json_fetch` tries a REST binding before the main method path) and
WebSocket (`match_ws` at upgrade). Caveat: REST over reflection needs the upstream's
reflection to preserve custom options — the proxy frames raw descriptor bytes so options
survive, but tonic-reflection strips `google.api.http` (Go/Java/C++ don't); bundled
descriptors always carry them. See BACKLOG.md.

Follow-up: ~~update PROTOCOL.md — the "v1 proxy rejects `+json`" line is now stale.~~
**Done 2026-07-05** — the "Proxy vs native library" section now documents `+json`/REST
termination, the `SchemaSource` table, eager reflection + TTL + reload, and the
raw-descriptor / tonic-reflection caveat.

### 2. The client never reconstructs a `Status` from the close frame — RESOLVED (2026-07-04)

PROTOCOL.md: the JS client "reads `CloseEvent.code`/`.reason` and reconstructs a
`Status`". Server side was already implemented and tested — close before the read
loop (`crates/server/src/ws.rs:76-80`), `4000 + code` with truncated reason
(`ws.rs:192-198`), asserted by `crates/server/tests/auth.rs:89-99`.

Client side was not: both close listeners discarded the event and `onClose()`
always emitted `UNAVAILABLE "websocket closed"`. An auth rejection was
indistinguishable from the server being down, and UNAVAILABLE is conventionally
retryable while UNAUTHENTICATED is not — retry loops would hammer a hopeless auth
failure.

Fixed in `clients/typescript/src/ws-transport.ts`: a `statusForClose(event)`
helper maps a private close code (`4000 + gRPC code`, range 4000..=4016) to the
gRPC status with `event.reason` as the message; any other close (normal 1000,
abnormal 1006, or an `error` event with no CloseEvent) falls back to
UNAVAILABLE. Both `SingleStreamConn.onClose` and `MultiplexConn.onClose` now
pass the `CloseEvent` through it; the multiplex path fans the reconstructed
status out to every open stream. Covered by
`clients/typescript/test/ws-close-status.test.ts` — mock-driven cases for both
transport modes (each gRPC code, out-of-range codes, error events, fan-out) plus
one real `ws` server that closes with `4016` to exercise a genuine
`CloseEvent.code`/`.reason`.

### 3. App-level `Ping`/`Pong` exist on paper only — RESOLVED (2026-07-04)

PROTOCOL.md presented them as app-level keepalive on both codecs, but they were
declared only in the binary envelope with no JSON wire form, the server's
dispatcher fell through on them, and nothing ever sent one.

Resolved by dropping the app-level frames and using **native WebSocket ping/pong
control frames** (RFC 6455 §5.5.2) for keepalive — the right layer, since a browser
can't send an app frame on an idle timer from JS but *does* auto-answer a server
ping with a pong. Changes:

- Removed `Ping`/`Pong` from `proto/grpc_webnext.proto` and reserved field numbers
  6/7 (`reserved 6, 7;`) so they can't be silently reused; regenerated the Rust
  (build.rs/prost) and TS (`npm run gen`) bindings.
- Added `ServerConfig::ws_keepalive` and `ProxyConfig::ws_keepalive`
  (`Option<Duration>`, default `None`). When set, the connection's writer task
  (`crates/server/src/ws.rs`, `crates/proxy/src/ws.rs`) `select!`s a ticker and
  emits a `TungMessage::Ping` each period; the peer's automatic pong is the return
  traffic that keeps an idle-timeout proxy/LB from dropping a quiet stream. The
  ticker's first tick is one period out and missed ticks are skipped (no ping
  bursts after a busy period).
- **Dead-peer detection (gRPC-style), added 2026-07-04.** `ws_keepalive_timeout`
  (`Duration`, default 20s — gRPC's default) bounds the wait for a response. The
  read loop keeps a liveness deadline that any inbound frame (the pong, or ordinary
  data) pushes out; if nothing arrives for `ws_keepalive + ws_keepalive_timeout`,
  the connection is dropped, surfacing `UNAVAILABLE` to its streams. This detects
  half-open connections in seconds instead of waiting out the OS TCP timeout.
  Notably this needed **no new shared state**: pongs already arrive in the read loop
  and the drop is just its `break`, so the writer keeps pinging while the read loop
  owns detection.
- No client change needed: browsers and the Node `ws` package auto-answer pings.
- Covered by `crates/server/tests/keepalive.rs` and `crates/proxy/tests/keepalive.rs`:
  pings arrive when enabled and not when disabled; a peer that stops answering (a
  client that stops polling, so tokio-tungstenite stops auto-ponging) is dropped
  within the window; a peer that keeps answering stays connected. PROTOCOL.md's
  keepalive paragraph documents both the ping mechanism and the timeout.

### Smaller inaccuracies — RESOLVED (2026-07-04)

- **stream_id "omitted entirely"** (doc fix): the stream_id section contradicted
  the binary single-stream note (which says `stream_id` is fixed to `1`). Reworded
  to: JSON omits it from the wire, binary carries it fixed at `1` (protobuf has no
  field omission), and the server ignores the wire value in single-stream mode
  either way.
- **"Fetch is the same" for auth** (code fix, now **superseded**): `stream_auth` used to
  run only on the WebSocket `Subscribe` path, so a server that registered it had an
  **unauthenticated Fetch surface**; the fix ran the same hook on Fetch too. `stream_auth`
  was later **removed entirely** — it was redundant with a tonic interceptor, which the
  request already reaches on every transport (and which, unlike the hook, also covers the
  native/h2ts path). Per-RPC auth is now a router interceptor / the upstream server; pinned
  by `tests/inproc_auth.rs::tonic_interceptor_guards_both_grpc_webnext_surfaces`.
- **`Reset{ UNAUTHENTICATED }` isn't hardcoded** (doc fix): clarified that the
  Reset (WS) / `grpc-status` (Fetch) carries whatever `Status` the hook returns —
  any code, e.g. `PERMISSION_DENIED` for a valid-but-unauthorized token.

## Internal doc contradiction — RESOLVED (2026-07-05)

The content-type table said `grpc-webnext+json` also works on annotated REST
endpoints; the REST section said annotated endpoints accept "never the
grpc-webnext content-types". The code implements the table's version — all
JSON-ish content types route through the same transcode-first path
(`crates/server/src/lib.rs:351-357`), with a test asserting `+json` transcodes
on a REST URL (`crates/server/tests/json.rs:192`). Only `+proto` is rejected on
annotated URLs (415, `"REST-annotated endpoints are JSON-only"`). Fixed: the
REST-section sentence now says annotated endpoints are JSON-only — plain HTTP
*and* `+json`, rejecting only `+proto`.

## Improvements

- **Streaming `+proto` Fetch responses (native server), 2026-07-04.** The binary
  Fetch response is now streamed rather than buffered: `unary` in
  `crates/server/src/lib.rs` no longer `collect()`s the inner gRPC response. Since
  the status lives in the trailer block *after* the message (not in a header), the
  server never needs the whole message up front — it drops the inner gRPC frame's
  1-byte compression flag (turning `[flag][u32 len][msg]` into our `[u32 len][msg]`
  block), pipes that straight through a `StreamBody`, and appends the trailer block
  once the inner call's trailers arrive. A large binary blob is no longer malloc'd
  on the server. Wire format is byte-identical (all existing decode tests pass).
  New coverage in `crates/server/tests/unary.rs`: `large_response_streams_intact`
  (3 MiB multi-chunk), `empty_ok_message_streams`, `error_response_is_trailers_only`
  (trailers-only → synthesized empty message block). JSON still buffers (it must
  transcode and put status in a header).
- **Streaming `+proto` Fetch responses (proxy) + retry removed, 2026-07-04.** The
  same streaming now applies to the proxy's `handle_unary`: it forwards the upstream
  gRPC frame opaquely (minus the flag byte) via a `StreamBody` and appends the trailer,
  instead of `client.unary()` materializing the whole message. This required
  **removing the proxy's retry policy** — retry belongs in the client (a wire proxy
  fanning many clients into one upstream turns a blip into a retry storm, and compounds
  with client retries), and it was also what forced buffering (replay the request /
  peek the status). Deadline handling is preserved: the establish is bounded by the
  local deadline (→ clean `DEADLINE_EXCEEDED`, cancels upstream), and the body stream
  is bounded too. Shared `read_status`/`percent_*` helpers moved to core metadata.
  New coverage in `crates/proxy/tests/unary.rs`: `large_response_streams_intact`,
  `upstream_error_is_trailers_only`; existing `deadline.rs`/`cancel.rs` still pass.
- **Streaming `+proto` Fetch *requests* (uploads), 2026-07-04.** The Fetch `+proto`
  request wire format is now **length-prefixed** — `[u32 len | message]`, mirroring the
  response's message block. The client (`encodeFetchRequestBody` in
  `clients/typescript/src/frame.ts`) prepends the length it already knows, so the server
  and proxy (`frame_upstream_request`) peek only the 4-byte prefix for the size-limit
  check, then stream `[flag] + body` into the upstream gRPC frame — a large upload is no
  longer buffered to measure. JSON requests stay bare (they transcode). Genuinely
  unknown-length streamed uploads are a WebSocket concern, not Fetch. New coverage:
  `frame.test.ts` (client framing) plus every `+proto` Fetch test now round-trips the
  prefixed body; `large_response_streams_intact` sends 3 MiB through.

- **WS payload copy-elimination (prost `Bytes`), 2026-07-05.** The WS path materializes
  each message whole (inherent — one message per WS frame, no fragmentation; fragmenting
  a single large message stays deferred). It was also copying each payload ~3× per side.
  Switched prost to decode `bytes` fields as `Bytes` (`crates/core/build.rs` `.bytes(["."])`)
  so `Message.payload`/`initial_payload` are sliced, not copied: dropped the
  `payload.to_vec()` in both `run_stream`s and the redundant `Bytes::from(...)` wrappers,
  leaving only the one unavoidable envelope-serialize per outbound message. Rust-only —
  no wire or TS change (all suites still pass).

## Undocumented wire-observable behavior — DOCUMENTED (2026-07-05)

All of the below now live in PROTOCOL.md under **"Limits & error surfaces
(wire-observable)"** (plus the REST-precedence bullets in the REST section).
Verification against current code corrected several of these from their first
draft — the corrections are folded in here and in the doc.

- **Request-size limit** (`max_message_bytes`, default 4 MiB). Fetch `+proto` (server +
  proxy) → HTTP **413**; Fetch `+json` (server + proxy) → **`grpc-status: 8` in a 200**
  (harmonized 2026-07-05); WebSocket (server + proxy) → `Reset{RESOURCE_EXHAUSTED}` per
  stream (added 2026-07-05). See PROTOCOL.md "Limits & error surfaces".
- ~~Proxy unary retry policy~~ — **removed 2026-07-04.** Retry belongs in the
  client, not a wire proxy (retry storms; see the Improvements section and
  `doc/BACKLOG.md`). Removing it also unblocked proxy response streaming.
- **Stream-level error cases** (in-band frames, not close codes): duplicate
  `stream_id` → `Reset{INVALID_ARGUMENT, "stream_id already in use"}`; `+json` without a
  transcoder → `grpc-status: 12` in a 200 on **Fetch** and `Reset{UNIMPLEMENTED}` on **WS**
  (both surfaces, harmonized 2026-07-05); exceeding the **proxy's** `max_concurrent_streams`
  → `Reset{RESOURCE_EXHAUSTED}` (server has no cap).
- **JSON frame edge semantics**: field-presence priority is
  `method`→`status`→`halfClose`→`message`→none, so a frame with no recognized
  field (e.g. `{}`) decodes as a half-close (a typo'd field name silently ends the
  send side), and a frame with both `halfClose` and `message` drops the message. A
  multi-mode open may combine `{streamId, method, message}` (message becomes the
  initial payload). `-bin` metadata is silently dropped crossing into the JSON codec.
- **Encoding details**: `grpc-message` headers are percent-encoded (alnum + space
  `-_./:` pass, else `%XX`) on the JSON Fetch path only; close reasons truncate to
  123 bytes on a UTF-8 boundary (native server only — the proxy has no close path).
  (The `bearer.<token>` subprotocol was later removed with `connect_auth`.)
- **Pool behavior**: in `+multi` mode the client opens a new socket per stream until
  `poolSize` is reached, then round-robins — the doc described steady state only.
- **Query params are ignored when `body: "*"`** in REST transcoding; path vars
  still overlay.

## Divergences to reconcile (code, not doc)

Verifying the undocumented behaviors surfaced four places where the **proxy and native
server disagreed** on a wire-observable rejection. Three are harmonized in code as of
2026-07-05 (proxy's status-in-header behavior chosen as canonical for `+json`); one
minor one remains.

1. **WS ignored `max_message_bytes`.** ✅ **Fixed 2026-07-05** — both
   `crates/server/src/ws.rs` and `crates/proxy/src/ws.rs` now reject an inbound message
   (or a `Subscribe`'s inline `initial_payload`) over the limit with
   `Reset{RESOURCE_EXHAUSTED, "request message exceeds size limit"}`, terminating just that
   stream (the connection and its other streams continue). tungstenite's frame defaults
   remain a coarser transport backstop. Covered by `ws_json_over_size_is_reset` (server) /
   `ws_json_over_size_is_resource_exhausted` (proxy).
2. **Fetch `+json` over-size.** ✅ **Fixed 2026-07-05** — the native server now returns
   `RESOURCE_EXHAUSTED` in the `grpc-status` header (HTTP 200), matching the proxy, instead
   of HTTP 413 (`+proto` still uses 413 on both — it's a pre-framing check and they already
   agreed). Covered by `fetch_json_over_size_is_resource_exhausted` (server).
3. **Fetch `+json` with no transcoder/schema.** ✅ **Fixed 2026-07-05** — the native server
   now returns `UNIMPLEMENTED` in the `grpc-status` header (HTTP 200), matching the proxy,
   instead of HTTP 501. Covered by `fetch_json_without_transcoder_is_unimplemented` (server).
4. **`+json`-without-schema WS frame kind.** ✅ **Fixed 2026-07-05** — the proxy now sends a
   `Reset` for a capability-gap rejection (matching the server), reserving `Trailer` for
   statuses the upstream RPC actually returned; the duplicate-`stream_id` message is unified
   to `"stream_id already in use"` on both. This is wire-invisible on JSON (both render to
   `{status}`) but makes the Reset-vs-Trailer convention consistent across the two crates —
   see PROTOCOL.md "Limits & error surfaces". The convention itself is now documented there.

## Test coverage gaps

- **httprule has zero unit tests.** `crates/core/src/httprule.rs` implements
  verbs, path templates, `{field=**}` rest-captures, dotted paths, body rules,
  and query coercion with no `#[cfg(test)]` module. All coverage is indirect
  via `crates/server/tests/json.rs`, whose echo.proto annotations use only
  GET/POST, single-segment `{message}`, and `body:"*"`/none. Never executed
  under test: `put/delete/patch/custom`, `{field=**}`, dotted field paths,
  `body:"<field>"`, repeated-field query params, and the three documented
  rejection paths (`response_body`, regex patterns, non-scalar query). Highest
  value place to add tests — pure functions, easy to unit-test.
- **`+json+multi` on annotated WS routes**: the guard is `proto || multi` but
  only the `proto` half has a test (`json.rs:489`); the `multi` half could be
  dropped without a test failing.
- **Proxy negative paths**: the 415 unknown-content-type branch, the 413
  size limit, duplicate-stream_id reset. (Proxy `+json`, formerly drift 1, is now
  covered end-to-end by `crates/proxy/tests/json.rs`.)
- **Client-side limits and errors**: `maxMessageBytes` overflow (Rust core
  limit is tested, TS client's isn't), the `FetchTransport.startStream` throw,
  `poolSize > 1` round-robin distribution, `grpc-timeout` header emission on
  Fetch unary (deadline tests all go through streaming).
- **Per-RPC auth**: `stream_auth` was removed (redundant with a tonic interceptor);
  `inproc_auth.rs` now pins that a router interceptor guards the Fetch and WS surfaces.
- **Close-event handling**: covered as of drift 2's fix
  (`test/ws-close-status.test.ts`) — the client reconstructs a gRPC status from a
  `4000 + code` handshake close. (The connect-auth reject path that also exercised this was
  since removed; the close now carries codec/surface rejections.)

The through-line: happy paths are well covered on both sides of the wire;
almost every gap is a rejection or limit branch — exactly the branches that
define protocol behavior for non-conforming or misconfigured clients.

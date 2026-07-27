# h2ts integration plan

> **Historical design record — not a specification.** This is the plan as written on
> 2026-07-07, kept for its rationale: what was weighed, what was decided, and why. The
> resulting path is **normatively specified** in
> [`/spec/PROTOCOL_H2TS.md`](../spec/PROTOCOL_H2TS.md); where the two disagree, that one
> wins. Phases 0–3 are done (see Phasing); the status line below is the original.
>
> Notably superseded: "the binary/h2ts path for Go/Node needs an h2ts gateway per runtime —
> not yet built" was true when written; **Go shipped one on 2026-07-27**, and the current
> per-implementation status lives in the spec rather than here.

*2026-07-07. Status: **planned**, not started. Gated on the `h2ts` client publishing
to npm (`h2ts-server` is already on crates.io). Decisions locked 2026-07-07; one item
(client packaging) still open — see the end.*

## Thesis

Transport is a **per-client config** (`{ encoding, unary, streaming }`, below). The codec
plus two transport axes select the path.

- **Binary (`proto`) defaults to real gRPC over h2ts.**
  [`h2ts`](https://github.com/debdattabasu/h2ts) gives the browser a real HTTP/2 client
  tunneled over a WebSocket — real framing, trailers, flow control, native multiplexing.
  On the default the browser makes actual `application/grpc` calls and the server is
  **unmodified tonic** behind an h2ts gateway: no grpc-webnext translation at all. Binary
  can also be configured to send unary over Fetch and/or to stream over the custom
  WebSocket `Frame` protocol (one WS per stream).
- **JSON (`json`) is the current custom protocol, and only that.** Locked to Fetch unary +
  one raw WebSocket per stream, **no h2ts, no multiplexing** — JSON stays plaintext so it
  is inspectable in browser devtools. A hard requirement, and why JSON never touches h2ts
  (binary HPACK/H2 on the wire).

What actually changes: the custom `Frame` protocol is **kept** — JSON streaming *and* the
binary `streaming: ws` option both ride it — but it is now always **one stream per
WebSocket**, so `stream_id` and the WS-pool multiplexer are **deleted** (the field is
removed outright, not reserved — private repo, no users). Multiplexing, when you want it,
comes from H2 via h2ts. Net: real gRPC on the binary default, the same debuggable JSON
path, and the bespoke multiplexer gone.

## Client config

Transport is a per-client choice, not per-call:

```ts
{ encoding: "proto", unary: "h2ts",  streaming: "h2ts" }  // default
{ encoding: "proto", unary: "fetch", streaming: "h2ts" }  // only streams over h2ts
{ encoding: "proto", unary: "fetch", streaming: "ws"   }  // no h2ts — pure custom protocol
{ encoding: "json" }  // locked to { unary: "fetch", streaming: "ws" }
```

- `encoding: "json"` **forces** `unary: fetch, streaming: ws` (JSON can't ride h2ts).
- `streaming: "h2ts"` is always the shared, H2-multiplexed connection; `streaming: "ws"` is
  the custom `Frame` protocol, one WS per stream. **There is no per-stream h2ts** —
  per-stream is the `ws` path.

| Config | unary | streaming | h2ts? | custom `Frame`? | plaintext |
|---|---|---|---|---|---|
| `proto` — default | h2ts (real gRPC) | h2ts (real gRPC, multiplexed) | ✅ | — | — |
| `proto` — fetch unary | Fetch (translated) | h2ts (real gRPC, multiplexed) | ✅ streams | — | — |
| `proto` — ws streaming | Fetch (translated) | custom `Frame`, 1 WS/stream | — | ✅ binary | — |
| `json` — locked | Fetch (translated) | custom `Frame`, 1 WS/stream | — | ✅ | ✅ |

The server is agnostic to the client's choice — on one port it sees h2ts H2 connections,
Fetch requests, and/or custom-protocol WebSockets, and routes by content-type / subprotocol.

## Binary over h2ts — how each surface works

- **Client.** An h2ts `H2Connection` + a thin gRPC framing shim: a call is
  `request({ method: "POST", path: "/pkg.Svc/Method", headers: { te, content-type:
  application/grpc, grpc-timeout, …metadata }, body })`; messages are the 5-byte
  length-prefixed frames the client already builds (`frame.ts`); `grpc-status` is read
  from `response.trailers()`. Metadata → H2 headers; deadline → `grpc-timeout`; cancel →
  `AbortSignal` → RST_STREAM (h2ts wires this in `connection.ts`). The public API
  (`makeClient` / `makePromiseClient` / `Metadata`) is unchanged.
- **In-process server.** `accept(&mut req)` + `serve_h2(ws, routes)` where `routes` is the
  **same `tonic::service::Routes`** already in the `Backend` enum. tonic serves real gRPC
  over the tunnel — no grpc-webnext code on this path.
- **Proxy.** `h2ts-proxy` / `bridge` → h2c upstream. Byte-transparent, schema-agnostic,
  incremental (wslay streams sub-frame). The binary proxy becomes a byte pump.
- **Keepalive.** Browser JS `WebSocket` can't send pings; h2ts-server drives server-side
  keepalive via `BridgeConfig::keepalive`.

## Custom `Frame` path — kept, single-stream

Used by JSON streaming and binary `streaming: ws`. The existing machinery stays: the raw-WS
`Frame` handler, the `Transcoder` (JSON), metadata mapping, Reset-vs-Trailer. The only
change is that it is now strictly one stream per socket, so `stream_id` leaves the proto
(`HalfClose` becomes a bare marker) and the pool/demux bookkeeping is deleted. Binary and
JSON differ only in payload encoding (protobuf frames vs plaintext JSON frames).

## Same-port routing

- WS upgrade offering **`h2ts`** → `serve_h2` (binary real gRPC).
- WS upgrade offering **`grpc-webnext+proto`** → `Frame` handler, single-stream, binary.
- WS upgrade offering **`grpc-webnext+json`** → `Frame` handler, single-stream, JSON.
- `content-type: application/grpc-webnext+proto|+json` (no upgrade) → Fetch unary.
- `content-type: application/grpc*` → native gRPC passthrough.

## Codebase impact (relative to the restructured repo)

**Removed**
- `stream_id` (deleted from the proto, not reserved) and the WS-pool multiplexer. The
  `Frame` WS handler collapses to strictly one stream per socket.

**Kept**
- The whole custom protocol, now single-stream: Fetch unary (both codecs), the `Frame` WS
  handler (both codecs — binary for `streaming: ws`, JSON always), the `Transcoder`,
  metadata mapping, native passthrough.

**Added**
- Rust: `h2ts-server` dep; the in-process binary path = `serve_h2(routes)`; the binary
  proxy = `bridge`/`h2ts-proxy`. JSON stays in the proxy (its Fetch-unary + `Frame`-WS +
  reflection/bundle transcoding is unchanged).
- TS: an h2ts-based binary transport (H2Connection + gRPC shim). The custom paths keep
  `fetch-transport` + a single-stream `ws-transport` (now shared by `json` and binary
  `streaming: ws`).

**Polyglot (`rust/` `go/` `node/`)**
- The **binary/h2ts** path for Go/Node needs an h2ts gateway per runtime — both on the h2ts
  roadmap, **not yet built**. Until then, binary-over-h2ts is Rust-only; Go/Node can still
  serve native gRPC passthrough and the custom paths.
- This *shrinks* per-language work: each language needs (a) the small custom `Frame` path,
  (b) an h2ts gateway (reusable, h2ts's concern), (c) native gRPC (already exists) — instead
  of reimplementing a multiplexed frame protocol three times.

**Spec & conformance**
- `spec/PROTOCOL.md` splits: a binary section = "real gRPC over an h2ts tunnel" (largely a
  pointer to the h2ts + gRPC specs) and a custom section = the `Frame` path (single-stream,
  both codecs). `stream_id` language comes out.
- `conformance/`: the binary suite becomes "real gRPC over the tunnel"; the custom suite
  exercises the `Frame` path. The matrix and anti-drift rationale are unchanged.

## Phasing

- **Phase 0 — unblock.** ✅ `h2ts` published to npm (`@debdattabasu/h2ts@0.1.1`) + crates.io
  (`h2ts-server@0.1.1`); pulled in directly.
- **Phase 1 — binary default, end-to-end.** ✅ Client H2 transport + gRPC shim; in-process
  `serve_h2(routes)`; proxy `bridge`. Proven with a greeter round-trip across all four
  cardinalities (+ deadline, cancel) on both surfaces. Landed as an opt-in.
- **Phase 2 — config knobs.** ✅ Per-client `{ codec, unary, streaming }`: proto defaults to
  `h2ts`/`h2ts`, `json` locked to `fetch`/`ws`, unsupported combos rejected. h2ts is now the
  proto default; `unary: "fetch"` / `streaming: "ws"` are the opt-outs.
- **Phase 3 — retire multiplexing.** ✅ `stream_id` deleted from the proto; the `+multi`
  subprotocols, WS pool, and `max_concurrent_streams` removed across Rust + TS; the `Frame`
  path is now strictly one WebSocket per stream (JSON always, binary `streaming: "ws"`).
  Proto/spec/Go-placeholder updated. Rust 84 tests + clippy clean; TS 47 tests.
- **Phase 4 — conformance rework**, then the Go/Node h2ts gateways.

## Decisions (locked 2026-07-07)

1. **Real, unmodified gRPC on h2ts** — binary default is standard `application/grpc`; the
   server is plain tonic behind the gateway, zero grpc-webnext code on that path.
2. **`stream_id` deleted** from the `Frame` proto (removed, not reserved). The proto is
   kept; every `Frame` channel is single-stream.
3. **Transport is per-client config** `{ encoding, unary, streaming }` (four valid shapes
   above). `json` locks to `fetch`/`ws`. No per-stream h2ts; per-stream = the `ws` path.
4. **JSON stays in the proxy.**
5. **Auth is per-RPC at the router** on every path (h2ts and custom) — a tonic interceptor /
   the upstream mesh sees the request metadata; there are no grpc-webnext auth hooks.

## Open

- **Client packaging.** The default config pulls in h2ts, so most `proto` users get the
  ~9 KB H2 stack regardless — which weakens the case for a separate `@grpc-webnext/client-h2`,
  but JSON-only users still wouldn't want it. Unresolved; revisit when we wire the client
  (Phase 1).
- **Dependency sourcing pre-publish.** Until the h2ts client is on npm, pull it in as a git
  dep, a vendored copy, or a `node/packages/*` workspace member. (`h2ts-server` is on
  crates.io already.)

# grpc-webnext over h2ts — the binary default

Normative, and deliberately **short**. This document specifies the path that grpc-webnext
mostly *doesn't* specify: real gRPC over real HTTP/2, tunneled through a WebSocket by
[h2ts](https://github.com/debdattabasu/h2ts). There is no translation layer here and no
grpc-webnext frame format — the browser makes genuine `application/grpc` calls and the
server is an unmodified gRPC server.

So most of what you need is written down elsewhere, and this document's job is to say
*which* elsewhere, and to pin the handful of seams grpc-webnext actually owns:

| Layer | Specified by |
|---|---|
| The WebSocket handshake and framing | [RFC 6455](https://www.rfc-editor.org/rfc/rfc6455) |
| Tunnelling HTTP/2 over that WebSocket | [h2ts](https://github.com/debdattabasu/h2ts) |
| HTTP/2 itself | [RFC 9113](https://www.rfc-editor.org/rfc/rfc9113) |
| gRPC on HTTP/2 | [gRPC over HTTP/2](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md) |
| **The seams below** | this document |

[`PROTOCOL.md`](PROTOCOL.md) is the sibling document, and specifies the **custom `Frame`
path** — Fetch unary plus a single-stream WebSocket protocol — used by the JSON codec always
and by the binary codec when the client opts out of h2ts. The two paths share a port and
nothing else.

## Selecting this path

Transport is a per-client config, not a per-call choice:

| Config | unary | streaming |
|---|---|---|
| `{ codec: "proto" }` — **the default** | h2ts | h2ts |
| `{ codec: "proto", unary: "fetch" }` | Fetch (translated) | h2ts |
| `{ codec: "proto", unary: "fetch", streaming: "ws" }` | Fetch (translated) | custom `Frame` |
| `{ codec: "json" }` — locked | Fetch (translated) | custom `Frame` |

`json` **cannot** ride h2ts: the point of the JSON codec is that it is plaintext and legible
in browser devtools, and an h2ts tunnel carries binary HPACK/HTTP/2 frames. There is also no
"per-stream h2ts" — h2ts is always one connection carrying many multiplexed streams; if you
want a socket per stream, that is `streaming: "ws"` on the custom path.

## The handshake

A client selects this path by opening a WebSocket offering the subprotocol **`h2ts`**. A
server that supports it echoes the subprotocol and completes the upgrade; everything after
the `101` is tunnelled HTTP/2.

Three properties are normative and easy to get wrong:

- **Same-port routing.** The `h2ts` subprotocol is checked **before** any grpc-webnext codec
  subprotocol, and a connection offering it never reaches the `Frame` handler. On one port a
  server therefore serves: `h2ts` upgrades (this document), `grpc-webnext+proto` /
  `grpc-webnext+json` upgrades and `application/grpc-webnext+…` requests
  ([`PROTOCOL.md`](PROTOCOL.md)), and `application/grpc*` requests (native gRPC, passed
  through untouched).
- **The upgrade URL is not the route.** Unlike the custom `Frame` path — where the WebSocket
  URL *is* the gRPC method — an h2ts upgrade may be at any path, conventionally the server's
  base URL. The method is `:path` on each HTTP/2 stream inside the tunnel, exactly as in
  native gRPC. A server MUST NOT derive routing from the upgrade URL.
- **Scheme and authority.** The tunnelled HTTP/2 is always **h2c** — cleartext, prior
  knowledge, no ALPN, no TLS *inside*. Transport security belongs to the outer WebSocket:
  `ws://` ↔ `http://`, `wss://` ↔ `https://`. Requests inside the tunnel set `:scheme` to
  `http` and `:authority` to the **outer** connection's host.

## Inside the tunnel

A complete HTTP/2 connection: client preface, `SETTINGS`, `HEADERS`/`DATA`/trailers,
`WINDOW_UPDATE`, `RST_STREAM`, `GOAWAY`, `PING`. Nothing is emulated and nothing is
degraded — this is why the path exists.

What that buys, relative to the custom path:

- **Real trailers**, so the gRPC status arrives where the gRPC spec says it does, with no
  buffering trick. (The Fetch path has to bury the trailer *inside* the response body,
  because browsers cannot read HTTP trailers — see `PROTOCOL.md`, "Unary — Fetch".)
- **Real multiplexing**: many concurrent streams on one WebSocket, so the browser's
  ~6-connections-per-host cap on HTTP/1.1 stops mattering.
- **Real flow control**: per-stream `WINDOW_UPDATE`, end to end between the browser's H2
  stack and the server's.
- **Real fragmentation**: a large message rides bounded `DATA` frames
  (`SETTINGS_MAX_FRAME_SIZE`), and h2ts forwards them **sub-frame** — it never holds a whole
  WebSocket message in memory. This is why grpc-webnext has no fragmentation of its own; see
  `PROTOCOL.md`, "Fragmentation: why this protocol doesn't have it".

On top of that, ordinary gRPC: `content-type: application/grpc`, `te: trailers`, messages as
`[1-byte compression flag][u32 big-endian length][message]`, status in `grpc-status` /
`grpc-message` trailers, and a trailers-only response when a call fails before any message.

Consequently the gRPC conventions apply verbatim, **including the ones that differ from the
custom path**:

- **Metadata is HTTP/2 headers.** A `-bin` key carries **base64** on the wire, per the gRPC
  spec — not the raw bytes a `Frame`'s `bin_value` holds. A client crossing between the two
  paths must encode accordingly; this exact seam has produced a real bug before
  (see [`/doc/GO_SERVER.md`](../doc/GO_SERVER.md)).
- **Deadlines are `grpc-timeout`**, not the `Subscribe.timeout_millis` envelope field.
- **Cancellation is `RST_STREAM`**, not a `Reset` frame.
- **Errors are gRPC statuses.** The `4000 + code` WebSocket close convention belongs to the
  custom path's pre-RPC rejections and has no meaning here.

## What grpc-webnext adds

Three things, and only three. Each is wire-observable, so each is normative.

### Request message size limit

`max_message_bytes` (default 4 MiB) bounds inbound **request** messages on this path as it
does on every other. A server enforces it by inspecting the gRPC length prefix as the body
streams past — nothing is buffered to measure it — and fails the stream with
`RESOURCE_EXHAUSTED` when a declared length exceeds the limit.

This is ordinary gRPC behavior (every gRPC server has a max-receive-size), so a stock client
needs no special handling. Response size is **not** bounded: the server's own gRPC stack owns
that, and unlike the custom path there is no framing reason to care.

### Keepalive

Browser JavaScript cannot send WebSocket pings — the API simply does not expose them — so
keepalive on this path is **server-driven**: the server pings an idle tunnel and closes it if
the peer does not answer. Both server implementations use h2ts's defaults: **30 s** ping
interval, **15 s** pong timeout, close `1001 Going Away` with reason `"keepalive timeout"`.

Note this is *WebSocket* keepalive on the outer connection, not HTTP/2 `PING` inside it.

### Draining

On a graceful shutdown, an in-process server sends a real HTTP/2 **`GOAWAY` down the tunnel**:
in-flight streams finish, no new ones start. That works because the server owns the HTTP/2
connection running inside the tunnel rather than delegating it.

**The proxy is the exception, by design.** A proxy's h2ts tunnel is a byte-transparent pump
to an h2c upstream — it never parses the traffic — so there is no `GOAWAY` to inject without
parsing the very thing the proxy exists not to parse. Proxy tunnels run until the caller's
deadline. This is recorded rather than papered over; see [`/doc/GO_SERVER.md`](../doc/GO_SERVER.md).

## What this path does not have

Not gaps — consequences of it being real gRPC:

- **No `+json` codec.** JSON is a grpc-webnext translation; there is nothing to translate
  here. A JSON client uses the custom path.
- **No REST / `google.api.http` routes.** Those are URL-level aliases the grpc-webnext edge
  resolves; a tunnelled gRPC call addresses `/pkg.Service/Method` directly. See
  `PROTOCOL.md`, "REST transcoding".
- **No `Frame` envelope** — no `Subscribe`, `Header`, `Trailer`, `Reset`, `HalfClose`, and no
  `stream_id` (multiplexing is HTTP/2's).
- **No `allow_implicit_codec` surface gating.** There is no content-type ambiguity to gate:
  the subprotocol already said what this connection is.

## Implementation status

| Implementation | h2ts path |
|---|---|
| TS client ([`node/packages/client`](../node/packages/client)) | ✅ default for `codec: "proto"` |
| Rust in-process server | ✅ tonic `Routes` served over the tunnel |
| Rust proxy | ✅ byte-transparent bridge to an h2c upstream (no graceful drain) |
| Go in-process server | ✅ the same `*grpc.Server` that serves native traffic |
| Node server | ⬜ skeleton — needs an h2ts gateway for the runtime |

An implementation without an h2ts gateway is still conformant: it serves native gRPC and the
custom `Frame` path, and simply does not offer the `h2ts` subprotocol. Clients opt out per
axis (`unary: "fetch"`, `streaming: "ws"`).

## Conformance

The [conformance suite](../conformance/README.md) runs every case against this path under the
`proto/h2ts` transport profile, alongside `proto/ws` and `json`, against every server
implementation. That profile is what proves the two paths deliver the *same* gRPC semantics
rather than merely both working — the trailers-only metadata bug the suite found on its first
run was visible precisely because one profile disagreed with another.

## Background

The design rationale, the alternatives weighed, and the phased rollout that produced this
path are in [`/doc/H2TS_INTEGRATION.md`](../doc/H2TS_INTEGRATION.md). That document is a
**historical design record, not a specification** — where the two disagree, this one is
normative.

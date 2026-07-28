# grpc-webnext wire protocol

Full gRPC semantics in the browser on one port. The client picks its transport with a
per-client config `{ codec, unary, streaming }`; content-type (request path) and
subprotocol (WebSocket) then disambiguate the paths on the wire.

## Transports — the binary/h2ts split

- **`proto` (binary) defaults to `{ unary: "h2ts", streaming: "h2ts" }`:** *real* gRPC over
  [h2ts](https://github.com/debdattabasu/h2ts) — real HTTP/2 tunneled over a WebSocket. The
  browser makes actual `application/grpc` calls and the server is an **unmodified gRPC
  server** behind an h2ts gateway; there is no grpc-webnext translation on this path, and H2
  supplies real framing, trailers, flow control, and many concurrent streams per connection.
  **That path is specified by [`PROTOCOL_H2TS.md`](PROTOCOL_H2TS.md)**, its sibling document,
  which is short by design — it delegates to the WebSocket, HTTP/2, and gRPC-over-HTTP/2
  specs and pins only the seams grpc-webnext owns. Binary opts out per axis: `unary: "fetch"`
  uses the translated Fetch path, and `streaming: "ws"` uses the custom `Frame` protocol below.
- **`json` is locked to `{ unary: "fetch", streaming: "ws" }`:** always the custom protocol,
  never h2ts, so it stays plaintext and inspectable in browser devtools.

**The rest of this document specifies the custom `Frame` path** — the transport used by the
JSON codec (always) and by the binary codec only when the client selects `streaming: "ws"`
(unary likewise takes the Fetch path when `unary: "fetch"`). Two transports carry it: **Fetch**
for unary and a **single-stream WebSocket** `Frame` protocol for streaming, on the same port,
disambiguated by content-type / subprotocol.

## Content types

The **URL** picks the route — a `google.api.http` binding (`/v1/…`) is an **annotated
REST endpoint**, anything else is a **main gRPC path** (`/pkg.Service/Method`). The
content-type (Fetch) / subprotocol (WebSocket) then picks the codec, under **two
rules**:

1. **Plain HTTP** (`application/json` / *blank*) reaches annotated REST endpoints
   **always**; it reaches main paths **only with `ServerConfig::allow_implicit_codec`**.
2. **grpc-webnext** is the SDK: `+proto`/`+json` work on **all main paths**; `+json`
   **also** works on annotated endpoints (it's JSON). `+proto` doesn't fit the JSON
   REST surface.

| Content-type / WS subprotocol | Main path (`/pkg.Svc/Method`) | Annotated URL (`/v1/…`) |
|---|---|---|
| `application/grpc*` | passthrough (native gRPC) | — |
| `application/grpc-webnext+proto` | ✅ binary | ✗ 415 / close 4009 |
| `application/grpc-webnext+json` | ✅ JSON | ✅ JSON |
| `application/json` | flag off → **415** · on → ✅ | ✅ JSON |
| *(blank / no codec subprotocol)* | flag off → **415 / close 4012** · on → ✅ | ✅ JSON |

A WebSocket upgrade offering the **`h2ts`** subprotocol is the binary default — real gRPC
over an h2ts tunnel, specified in [`PROTOCOL_H2TS.md`](PROTOCOL_H2TS.md) rather than here.

`allow_implicit_codec` is off by default; its whole job is rule 1's flag — *"may plain
HTTP reach the real gRPC method paths, or only the REST routes?"* Rejections are
**explicit**: Fetch → `415 Unsupported Media Type`; WebSocket → a close frame with a
private code `4000 + gRPC code` (`4012` UNIMPLEMENTED, `4009` FAILED_PRECONDITION). An
unknown content-type is `415`; a JSON call to a nonexistent method is `UNIMPLEMENTED`.

The codec is **native** on both transports: proto uses binary framing; JSON is
idiomatic JSON on the wire (never wrapped in the binary framing).

## Unary — Fetch

- **Request:** HTTP POST. Metadata → HTTP request headers; `grpc-timeout` carries the
  deadline. For **`+proto`** the body is the single message **length-prefixed** —
  `[ 4-byte big-endian length | message bytes ]`, mirroring the response's message block.
  The client already has the whole serialized message, so it prepends the length it
  knows; the server/proxy then turn it into a gRPC frame (prepend the `[1-byte flag]`)
  and **stream** it upstream without buffering to measure. For **`+json`** the body is
  the bare JSON text (buffered, since it's transcoded).
- **Response (`+proto`):** the browser cannot read HTTP trailers, so the server
  writes the body as two length-prefixed blocks:

  ```
  [ 4-byte big-endian length | message bytes ]
  [ 4-byte big-endian length | Trailer block ]
  ```

  The trailer block is an encoded `Trailer` (status + trailing metadata). The
  client buffers the whole body up to a **configurable size limit**. On the **server
  and the proxy** this response is **streamed, not buffered**: because the status rides
  in the trailer block *after* the message (not in a header), neither needs the whole
  message up front. The inner gRPC frame is already `[1-byte flag][4-byte length |
  message]`, so dropping the compression-flag byte yields our first block verbatim — the
  message is piped straight to the socket and the trailer block appended at the end. A
  large binary blob therefore isn't malloc'd. (The proxy does this opaquely — it never
  decodes the message — which is also why it does no retry: a wire proxy can't replay a
  streamed call, and retry belongs in the client anyway.)
- **Response (`+json`):** the body is the **bare JSON message**; gRPC status and
  metadata travel in HTTP response headers (`grpc-status`, `grpc-message`, plus
  metadata). No length-prefix, no trailer block — a plain JSON API you can `curl`.
  Unlike `+proto`, JSON **is** buffered on the server: it transcodes the whole protobuf
  message to JSON and the status goes in a header (which must precede the body), so the
  full message is needed before anything is written.
- Server-streaming does **not** use Fetch — it goes over WebSocket (Fetch would have
  to buffer the entire stream). Fetch is unary only. A genuinely *streamed* upload of
  unknown length also goes over WebSocket — the Fetch upload path assumes a known-length
  message (always true for protobuf), which is what lets the client supply the length
  prefix.

### REST transcoding (`google.api.http`)

Core gRPC is always `POST /pkg.Service/Method`. To get real HTTP verbs and REST
URLs, annotate methods with `google.api.http` (the grpc-gateway / Envoy standard):

```proto
rpc GetUser(GetUserReq) returns (User) {
  option (google.api.http) = {
    get: "/v1/users/{id}"
    additional_bindings { post: "/v1/users" body: "*" }
  };
}
```

The native server compiles these bindings from the descriptor set (the same
`Transcoder` used for `+json`) and, on the Fetch path, maps a matching
`(method, path)` onto the gRPC method — binding path segments, query params, and
the body into the request message, and returning the response as JSON. So
`GET /v1/users/123` and `POST /v1/users {…}` both reach `GetUser`. These annotated
endpoints are **JSON-only**: they accept plain HTTP (blank / `application/json`) *and*
`grpc-webnext+json` — all three transcode through the same path — but reject `+proto`
with **415** (`"REST-annotated endpoints are JSON-only"`), since binary framing doesn't
fit the REST surface. A plain request whose URL matches no binding is a direct
main-endpoint call, which requires `allow_implicit_codec` (else 415).

**Streaming methods** can be annotated too — the annotation URL is then a **WebSocket**
endpoint (`ws://host/v1/watch/123`, verb-agnostic since a WS upgrade is a GET). Such a
connection is **text-locked, single-stream JSON**, accepts a blank / `application/json`
/ `grpc-webnext+json` subprotocol (proto is rejected), and its gRPC method
+ path/query bindings come from the annotation. A
`body:"*"` route takes each frame's JSON as a request message; a body-less route (GET)
builds the single request entirely from the URL and streams the responses. See
"Streaming — WebSocket" below.

An annotation only **adds** the REST alias; it does not change the RPC's main path,
which stays reachable by its grpc-webnext content-types/subprotocols (the SDK contract)
and — when `allow_implicit_codec` is on — by plain HTTP like any other main path.

Supported: verbs `get/put/post/delete/patch` + `custom` (whose `kind: "*"` means **any**
HTTP method); a trailing custom verb
(`/v1/things/{id}:cancel`), which is **matched, not stripped** — a binding that declares
one answers only URLs carrying it, and a binding that declares none never answers a URL
that carries one (a colon in path *data* is percent-encoded, so it still binds);
`additional_bindings`; path templates with literal segments, `{field}`/`{field=*}`
single-segment captures, `{field=**}` rest-captures, **multi-segment captures**
(`{name=shelves/*/books/*}`), bare `*`/`**` wildcards that match without capturing, and
dotted field paths; `body: "*"` / `body: "<top-level field>"` / none; `response_body`
(below); query-param binding to scalars, `bytes` (base64), and the well-known types with a
canonical string form (`Timestamp`, `Duration`, `FieldMask`, the `*Value` wrappers). Field
names in a path template or a query key resolve by `.proto` name **or** JSON
(lowerCamelCase) name.
Where several bindings can match one URL, the **first in descriptor order wins** — there
is no specificity ranking.

**`response_body`** names a **top-level** field of the response message, and makes the
body that field's value rather than the whole message — so `response_body: "resource"` on
a method returning `{"resource": {...}, "etag": "..."}` answers with just the `{...}`. It
is applied with the codec's own encoding, so a `bytes` field answers a base64 **string**, a
64-bit integer answers a quoted number, an enum answers its value name. When the field is
absent from the encoded message (protobuf-JSON omits defaults), the body is that field's
**JSON zero** — `""`, `0`, `"0"` for 64-bit, `false`, `[]`, `null` for an unset message —
never an omitted body. On a **stream** it applies **per message**, not once to the stream.

`body:` and `response_body:` must both name a **top-level** field, per HttpRule. A query
parameter cannot carry an arbitrary submessage — also HttpRule's own rule, since no client
library can express it; reach nested scalars with a dotted key (`nested.id=x`).

The one HttpRule feature **not** implemented is `HttpRule.selector` — bindings supplied out
of band in a service config rather than as a method option. That is declined rather than
deferred; see [`doc/HTTPRULE_GAPS.md`](../doc/HTTPRULE_GAPS.md), which also enumerates every
recorded decision above.

## Streaming — WebSocket

A WebSocket is either a **main-endpoint** connection (described here) or an **annotated
route** — if the upgrade URL matches a `google.api.http` binding, the connection is
text-locked, single-stream, JSON, accepts a blank / `application/json` /
`grpc-webnext+json` subprotocol (proto rejected), and its method +
path/query bindings come from the annotation (see the REST section). The rest of this
section describes the main-endpoint surface.

The connection's codec is chosen by the subprotocol; **every WebSocket carries exactly one
stream** (one stream per socket, never pooled):

| Subprotocol | Codec |
|---|---|
| `grpc-webnext+proto` | binary |
| `grpc-webnext+json` | JSON |

The client offers one of these alongside the base `grpc-webnext`; the server negotiates the
subprotocol back. A connection with **no** codec subprotocol (or a plain `application/json`
one) is rejected by default — closed with `UNIMPLEMENTED` (close code `4012`);
`ServerConfig::allow_implicit_codec` re-enables first-frame inference (blank ⇒ lock to first
frame's type; `application/json` ⇒ text). Either way the connection is single-codec — frames
of the other type are dropped.

### One WebSocket, one stream

The WS **URL is the route** (`ws://host/pkg.Svc/Method`), so the method is implied and
frames carry no `method`. The **first inbound frame opens** the stream (a `Subscribe`);
then `Message` / `HalfClose` / `Reset` flow on the same socket, and the server replies
`Header`, `Message`(s), `Trailer` / `Reset`. JSON is human-readable:

```jsonc
// client → server
{ "metadata": {…}, "timeoutMillis": 5000 } // open / Subscribe (optional; metadata/deadline)
{ "message": {…} }                         // data
{ "halfClose": true }                      // done sending (empty marker)
{ "status": { "code": 1, "message": "…" } }// client reset
// server → client
{ "metadata": {…} }                        // initial headers (Header)
{ "message": {…} }                         // data
{ "status": { "code": 0, "message": "" } } // terminal (trailer / reset)
```

Binary uses the same `Frame` envelope — a leading `Subscribe` (its method taken from the
URL, not read off the frame), then the same frame kinds encoded as protobuf. See
`crates/grpc-webnext/src/json_frame.rs`.

**One message per frame, no fragmentation** (both codecs).

**Keepalive** uses native **WebSocket ping/pong control frames** (RFC 6455 §5.5.2),
not application frames — HTTP/2 PING isn't reachable from browser JS, and a browser
can't send a WS ping from JS either, but it *does* auto-answer a server ping with a
pong. So the **server drives keepalive** (mirroring gRPC's `keepalive_time` /
`keepalive_timeout`):

- With `ServerConfig::ws_keepalive` (or `ProxyConfig::ws_keepalive`) set to an
  interval, an open streaming connection emits a ping each period. The peer's
  automatic pong is the return traffic that stops an idle-timeout proxy/LB from
  dropping a quiet stream.
- `ws_keepalive_timeout` (default 20s, gRPC's default) bounds the wait for a
  response. Any inbound frame — the pong, or ordinary stream data — proves liveness;
  if **nothing** arrives for `ws_keepalive + ws_keepalive_timeout`, the peer is
  presumed dead and the connection is **dropped** (the stream then surfaces
  `UNAVAILABLE`). This detects half-open connections in seconds instead of waiting
  out the OS TCP timeout.

Both are off by default (`ws_keepalive: None`). There is no application-level
ping/pong frame (the `Frame` oneof reserves the old field numbers 6/7).

### Auth

Authorization is **not** a grpc-webnext protocol feature — there are **no** auth hooks,
per-RPC or per-connection. Every request carries its metadata into the router on every
transport — HTTP request headers on Fetch, the `Subscribe` frame's metadata on WebSocket,
real HTTP/2 headers on the native/h2ts path — so authorization belongs in the **router**: a
tonic interceptor (in-process) or the upstream server / mesh (proxy). One check there covers
**all** transports uniformly, and its `Status` propagates the normal way — a WebSocket
`Trailer`, or a Fetch `grpc-status` (a denied `+proto` call still returns HTTP 200 with the
status in the trailer block; `+json` carries it in the `grpc-status` header). Per standard
HTTP/2 semantics a per-RPC rejection is a **stream-level** event: the status is returned on
that stream and the connection is **not** torn down.

This mirrors the ecosystem: application auth is per-request everywhere (gRPC interceptors,
Envoy `ext_authz`, nginx `auth_request`), and the only connection-level gate is **mTLS** — a
transport gate at connect time, which browser JS can't drive. grpc-webnext therefore ships
no connection-level auth hook. A browser credential that must be evaluated at the edge (e.g.
by a mesh) travels as ordinary request metadata to per-request authz.

## Limits & error surfaces (wire-observable)

The happy paths above are the contract; these are the rejection and limit branches a
non-conforming or oversized client will actually hit. Where the native server and the
proxy differ, both are called out — those differences are wire-observable.

### Message size limits (`max_message_bytes`, default 4 MiB)

- **Fetch `+proto`** (server and proxy): only the 4-byte length prefix is read; if the
  *declared* length exceeds the limit → **HTTP 413** with a plain-text body
  `"request message exceeds size limit"`. Nothing is buffered to measure.
- **Fetch `+json`** (server and proxy): the oversized body is rejected with
  **`RESOURCE_EXHAUSTED` in the `grpc-status` header** (HTTP 200) — *not* an HTTP 413. The
  JSON codec always carries status in the header, so both surfaces answer identically.
- **WebSocket** (server and proxy): an inbound message whose payload exceeds the limit
  (including a `Subscribe`'s inline `initial_payload`) terminates the stream with a
  `Reset{RESOURCE_EXHAUSTED, "request message exceeds size limit"}`. The transport's own
  frame limits (tungstenite's defaults) are a coarser backstop above that.

### Stream-level errors — in-band frames (WebSocket)

These arrive as ordinary frames on an already-open connection (not close codes):

- **`+json` with no transcoder/schema** → `Reset{UNIMPLEMENTED}` (server and proxy) — a
  pre-RPC capability rejection, so a `Reset`, not a terminal `Trailer`. (The message
  differs: the server says `"+json needs a transcoder"`, the proxy explains it's
  binary-only.)

The split between the two frame kinds is a **convention** worth stating: a `Reset` carries
a status for a stream rejected *before or outside* the RPC lifecycle (bad/oversized frame,
missing capability, auth failure, client cancel); a `Trailer` carries the
*terminal gRPC status of an RPC that reached the router/upstream* (normal completion or an
error status it returned). Both render to the same `{status}` JSON frame, so on a JSON
connection the distinction is invisible; it's observable only on a binary connection.

The `4000 + code` **close code** is used only for **connection-level** rejections at the
handshake (e.g. a missing codec subprotocol → `4012`), never for these per-stream errors on
an open connection.

**Single-stream teardown.** Once a stream reaches its terminal frame (`Trailer` or `Reset`),
the server closes the WebSocket with a **normal `1000`** close — a WebSocket carries exactly
one stream (proto or json), so it has served its purpose. The status has already been
delivered in the terminal frame, so the close carries no status. (This is the single-stream
analog of a stream-level teardown; the multiplexed h2ts path is a separate connection the
server never owns, so it is not closed per-stream.)

Having sent its close, the server waits only a **bounded** time for the peer's answering
close before dropping the socket. The handshake is politeness — the status was already
delivered in the terminal frame — so a client that never answers cannot pin the connection
open (and, during a drain, cannot hold shutdown open with it).

**Draining.** A server shutting down refuses *new* RPCs without cutting live ones. Because a
socket carries exactly one stream, that distinction is per-socket: one that has **not yet
opened its stream** is closed with **`1001 Going Away`**, and one carrying a live stream is
left alone to reach its terminal frame. `1001` is not a `4000 + code` status close — no RPC
ever started on that socket — and a client should read it the way it reads any transport
loss: `UNAVAILABLE`, retry elsewhere. On the h2ts path the same intent is HTTP/2's own
`GOAWAY`.

### Fetch error surfaces (no transcoder)

- `+json`/JSON with **no** transcoder/schema configured → **`UNIMPLEMENTED` in the
  `grpc-status` header** (HTTP 200) on both the native server (no `ServerConfig::transcoder`)
  and the proxy (`SchemaSource::None`) — *not* an HTTP 501. With a transcoder present but the
  *method* unknown → likewise HTTP 200 + `grpc-status: 12`.
- Unknown content-type → **415**; `+proto` on a REST-annotated URL → **415**.

### JSON frame edge semantics (`json_frame_to_proto`)

The **first** inbound frame is the open: `json_open_to_subscribe` takes the method from the
WS URL and, if the frame carries a `message`, folds it into the `Subscribe`'s
`initial_payload` (open + first data together). **Every later** frame's kind is chosen by
which field is present, in priority order `status` → `halfClose` → `message` → *(none)*.
Consequences:

- A frame with **no recognized field** (`{}`, or a mistyped field name) decodes as a
  **half-close** — a typo silently ends the send side.
- Because `halfClose` outranks `message`, a frame carrying **both** ends the stream and
  drops the message.
- **Binary (`-bin`) metadata is dropped** crossing into the JSON codec — JSON frames carry
  ASCII metadata only.

### Encoding details

- **`grpc-message`** header values are **percent-encoded** — ASCII alphanumerics plus
  `` (space) `-` `_` `.` `/` `:` `` pass through, everything else becomes `%XX` — on the
  JSON Fetch path. The `+proto` paths carry status in a protobuf `Trailer`, so no header
  encoding applies there.
- **WebSocket close reasons** truncate to **123 bytes** on a UTF-8 char boundary
  (WebSocket caps the reason at 123 bytes). The `4000 + code` close is sent for any
  handshake-time rejection (e.g. a missing codec subprotocol).

### REST binding precedence

- **Path variables** always overlay the message (applied unconditionally).
- **Query parameters** bind only when `body` is **not** `"*"`. With `body: "*"` (the whole
  body *is* the message) query params are **ignored entirely**; path vars still overlay.
  For a non-wildcard body, a query param naming a field already set by a path var is
  skipped.
- **Bindings resolve in declaration order** — the first whose verb and template match wins.
  There is deliberately no specificity ranking, so a later binding must not describe a URL
  shape an earlier one already covers (`POST /v1/x/{id}` declared before `POST /v1/x/body`
  swallows the latter, binding `id="body"`).

## In-process vs proxy: what needs schemas

grpc-webnext ships as **one crate** with two entry points over identical inbound handling:
`serve_in_process` wraps a local `tonic::service::Routes` (you own the service);
`serve_proxy` fronts a remote upstream over a `Channel`. Internally the only difference is
a `Backend` enum (`InProcess` / `Upstream`) — both are `Service::oneshot(request)` — so a
client can't tell a wrapped response from a proxied one. Either mode always parses the
WebSocket `Frame` envelope (headers, frame kind; the method comes from the WS URL) — that is
the protocol. Whether it must decode the **application payload** depends on the codec:

- `+proto` upstream `application/grpc` — payload forwarded **opaquely**, no schema. The
  proxy mode stays fully schema-agnostic here, fronting any gRPC server (Go, Java, …) with
  zero `.proto` knowledge.
- `+json` (and REST) — needs message descriptors to transcode JSON ↔ protobuf. Both modes
  **terminate** these: JSON request → binary protobuf, binary response → JSON back, via the
  same `Transcoder`, so the two modes are byte-identical.

The in-process mode gets its transcoder directly (`ServerConfig::transcoder`); the proxy
mode gets it from a configurable **schema source** (`ProxyConfig::schema`):

| `SchemaSource` | Descriptors from |
|---|---|
| `None` (default) | — `+json`/REST answer `UNIMPLEMENTED` |
| `Reflection` | upstream gRPC server reflection (v1, `v1alpha` fallback) |
| `Bundled(fds)` | a bundled `FileDescriptorSet` |
| `ReflectionOrBundled(fds)` | reflection, with the bundle as an immediate fallback |

Reflection is loaded **eagerly and whole** at startup — `list_services` plus each
service's transitive file closure, assembled into one snapshot — and refreshed on a TTL
(`reflection_ttl`, default 4h); an optional `admin_reload_path` (`POST`) forces an
immediate reload. Requests block (bounded) only for the very first load. The proxy frames
the **raw** descriptor bytes verbatim so custom options (e.g. `google.api.http`) survive —
but the upstream's reflection must itself preserve them; tonic-reflection currently strips
custom options ([grpc/grpc-rust#2719](https://github.com/grpc/grpc-rust/issues/2719), see
`doc/BACKLOG.md`), so REST-over-reflection against a tonic upstream needs a bundled set.
With `None` (or, in-process, no `transcoder`), `+json`/REST return `UNIMPLEMENTED` as a
proper status-in-header (Fetch) / `Reset` frame (WS) — never an HTTP 501, on either mode
(see "Limits & error surfaces").

## Fragmentation: why this protocol doesn't have it

Splitting one large message across frames is **intentionally out of scope**, and not merely
deferred: on the binary default the problem is already solved a layer down. That path is real
HTTP/2 over h2ts, where a large gRPC message is carried in bounded, flow-controlled **DATA
frames** (`SETTINGS_MAX_FRAME_SIZE`, 16 KiB default) and the tunnel forwards them
**sub-frame** — h2ts never holds a whole WebSocket frame in memory. The transport therefore
never materializes the message; only the gRPC codec does, at the message boundary, exactly as
native gRPC does. A `fragment` frame kind here would be a worse reimplementation of that.

On the custom `Frame` path the rule stands: **one message per frame, no fragmentation**, in
both codecs. A very large message is one very large WebSocket message, materialized at both
ends and bounded by `max_message_bytes`. That is the trade the path exists to make — atomic
messages keep the DevTools Network → Messages panel readable and the protocol trivial to
implement — and the JSON codec could not stream incrementally anyway, since it transcodes
whole messages. **If you need very large messages, use the default (h2ts) path.**

Should this ever change, it would be additive: a new `Frame` kind (e.g. `fragment`), leaving
existing frames untouched. Not built.

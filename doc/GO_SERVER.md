# Go in-process server

*2026-07-27. Status: **implemented and in the conformance matrix**. The Go server serves
every surface the Rust in-process server does, REST transcoding included, and every case
in `conformance/cases/` runs against **both** server implementations.*

## What was built

`go/webnext` went from a skeleton (a content-type router with `TODO(spec)` bodies and no
dependencies) to a full implementation:

| Surface | File | Notes |
|---|---|---|
| native gRPC passthrough | `server.go` | h2c preface detection via `x/net/http2/h2c`, so grpc-go clients share the port |
| h2ts (real HTTP/2 tunnel) | `h2ts.go` | `h2ts.Accept` + `ServeH2` straight to the gRPC handler; no translation |
| Fetch unary `+proto` | `fetch.go` | streamed both ways; only the length prefix is read to enforce the size limit |
| Fetch unary `+json` | `fetch.go` | buffered (the transcode is whole-message, status precedes the body) |
| WebSocket streaming | `ws.go` | one stream per socket, both codecs, keepalive, deadlines, single-stream teardown |
| JSON ⇄ protobuf | `transcode.go` | `protodesc` + `dynamicpb` + `protojson` over a `FileDescriptorSet` |
| REST (`google.api.http`) | `httprule.go` | added 2026-07-27; both surfaces (Fetch `/v1/…` and the WebSocket form) |

Plus `internal/conformance` (the `ConformanceService` implementation, shared by the binary
and the tests), `internal/testecho` (the Echo service carrying the shared REST
annotations), `examples/conformance-server`, and `examples/greeter`.

### The one structural difference from Rust

The Rust crate needs a `Backend` enum (`InProcess(Routes)` | `Upstream(Channel)`) so its two
entry points can share one set of inbound handlers. In Go that abstraction collapses: a
`*grpc.Server` **is** an `http.Handler`, so the backend is just `http.Handler` and
"dispatch" is an in-process HTTP round trip (`dispatch.go`). Two details make it a *gRPC*
round trip rather than an ordinary one, and both are load-bearing:

- grpc-go's `http.Handler` transport refuses `r.ProtoMajor != 2` and requires an
  `http.Flusher`, so the synthetic request is stamped HTTP/2 and the response writer
  flushes. It also relies on `Flush()` to force the header/trailer split *before* any body
  is written, which is how a trailers-only-shaped response still yields clean initial
  metadata.
- Response trailers — where the gRPC status lives — arrive through the *header* map, per
  the net/http contract: names pre-declared in `Trailer` get values later, and undeclared
  ones are added under the `Trailer:` prefix. Both are harvested once the handler returns,
  before the response body is EOF'd, so a reader that has seen EOF can rely on them.

Proxy mode is deliberately out of scope: the standalone schema-agnostic proxy is a single
implementation, the Rust `grpc-webnext-proxy` binary.

## Findings — divergences the port surfaced

Writing a second implementation against the same spec is itself an audit. Two real bugs
fell out, both in Rust, both now fixed with tests.

### 1. Binary (`-bin`) metadata was dropped on the Rust WebSocket path

**Wire-observable, and a silent data loss.** A `-bin` metadatum carries **raw bytes** in a
`Frame` but is **base64** on the HTTP wire. `metadata_vec_to_metadata` pushed the raw bytes
straight into a `HeaderValue`, which rejects control bytes — so `x-blob-bin: 0x01020304`
never reached the service at all. The inverse, `metadata_to_vec`, had the mirror bug: it
copied the base64 *text* into `bin_value`, so an outbound `-bin` trailer arrived at the
client as ASCII base64 bytes rather than the decoded value.

Both now go through tonic's binary metadata API (`MetadataValue::from_bytes` /
`MetadataValue::<Binary>::to_bytes`), which is the canonical encoding — unpadded base64,
matching grpc-go. The Go server does the same at its own header boundary.

*Why it survived this long:* the conformance suite's `metadata-roundtrip` case is a
**unary** RPC, and a unary call takes the **Fetch** path under every transport profile —
including `proto/ws`, where only *streaming* uses the WebSocket. So the frame-metadata
conversion was never executed by the matrix. Fixed by adding
`metadata-roundtrip-websocket`, a `ServerStream` case with the same metadata, which failed
on Rust and passed on Go the moment it was written. Unit-level backstops:
`metadata_unit.rs::binary_metadata_round_trips_through_headers` and
`wire_test.go::TestBinaryMetadataRoundTrip`.

### 2. A size-limit `Reset` did not tear down the WebSocket

PROTOCOL.md is unambiguous: *"Once a stream reaches its terminal frame (`Trailer` or
`Reset`), the server closes the WebSocket with a normal `1000` close."* Rust honored that
for the transcoder-gap `Reset` and for every `Trailer`, but the `handle_frame` rejections —
an oversized `Message`, an oversized `Subscribe.initial_payload`, an unparseable method —
sent the `Reset` and left the socket open with no stream on it. A client could only resolve
that by timing out.

Fixed with a `reject_stream` helper that pairs the `Reset` with the close, in Rust and Go
alike. The one `Reset` that deliberately does *not* close is "stream already open": it
rejects a duplicate `Subscribe`, not the live stream, which is still running and will
deliver its own terminal frame. Covered by
`inproc_ws_close.rs::proto_ws_closes_after_size_limit_reset` and
`server_test.go::TestWSOversizeMessageResets` / `TestWSOversizeInitialPayloadResets`.

## Graceful shutdown (2026-07-27)

Added afterwards, in both languages at once, since a drain has to mean the same thing on
every surface or it is not one:

- **Rust** — `serve_in_process_with_shutdown` / `serve_proxy_with_shutdown`. There is
  deliberately no drain-timeout config knob: the returned future *is* the drain, so
  `tokio::time::timeout` bounds it and dropping it force-closes. One knob fewer to
  mis-set, and it composes.
- **Go** — `webnext.NewServer` with `Serve` / `Shutdown(ctx)`, mirroring `http.Server`.

Per surface: Fetch and native gRPC use the HTTP layer's own graceful shutdown; **h2ts
tunnels get a real `GOAWAY`**, because both servers now own the HTTP/2 connection running
inside the tunnel (Rust drives `hyper` over h2ts's `WsByteStream` rather than calling
`serve_h2`; Go hands h2ts the server's own `*http2.Server`, which `http2.ConfigureServer`
has wired to `Shutdown`); and a custom-`Frame` WebSocket with no stream yet is closed
`1001 Going Away`, while one carrying a live stream runs to its terminal frame.

The proxy's h2ts tunnels are the one surface that cannot drain gracefully: they are a
byte-transparent pump, so there is no `GOAWAY` to inject without parsing the very traffic
the proxy exists not to parse. Recorded rather than papered over.

### Two more findings

- **Rust waited forever for a client's close echo.** After sending its close frame the
  server kept reading until the peer answered — so a client that receives the close and
  stops reading pinned the connection open indefinitely, and during a drain held shutdown
  open with it. Now bounded (`CLOSE_GRACE`, 5s), matching what the Go server already did.
  The close handshake is politeness; the status was already delivered in the terminal
  frame. Found because the drain test hung on exactly that.
- **A `sync.WaitGroup` is the wrong tool for counting live connections in Go.** `Add` from
  zero while `Wait` is running is documented misuse, and an HTTP/2 connection does exactly
  that when a new stream arrives mid-drain — the race detector caught it on the first
  `-race` run. Replaced with an explicit mutex-guarded counter that signals once draining
  has begun and the count reaches zero.

## REST transcoding (2026-07-27)

The last Rust-only surface, closed the same way the rest of the port was: `httprule.go` is
a **structurally parallel** port of `httprule.rs` — same segment/body model, same matching
order, same coercion table — so the two can be read side by side. Both surfaces are wired:
Fetch tries a REST binding before the main method path (and before the implicit-codec
gate, since annotated endpoints accept plain HTTP unconditionally), and a WebSocket
upgrade whose URL matches a binding becomes a text-locked single-stream JSON route,
rejecting a `+proto` subprotocol with close `4009`.

Three details are load-bearing and easy to get wrong in Go specifically:

- **`r.URL.EscapedPath()`, not `r.URL.Path`.** net/http has already percent-decoded
  `Path`, so an encoded `%2F` inside a segment would arrive as a segment *separator* and
  split the path differently than the template expects. The router does its own decoding,
  after splitting, exactly as Rust does.
- **Binding order is taken from the `FileDescriptorSet`, not from the registry.** First
  match wins, so precedence must be deterministic — and `protoregistry.Files.RangeFiles`
  iterates a map. Walking `fds.GetFile()` in order restores both determinism and parity
  with Rust's descriptor-pool order.
- **The `google.api.http` extension resolves through the global type registry.**
  `httprule.go` imports the annotations package for that side effect; without it the
  option lands in unknown fields and every annotation silently disappears. `startEcho` in
  the tests asserts `HasHTTPRules()` up front so that failure mode can't masquerade as
  "no routes configured".

### The finding: a custom verb was stripped rather than matched

Writing the port surfaced a **mis-routing bug present in both implementations**, which is
exactly what a second implementation is for. `parse_template` cut everything after the
first `:` and discarded it, so a binding for `/v1/things/{id}:cancel` compiled to the same
template as `/v1/things/{id}` — meaning `GET /v1/things/5` reached the `:cancel` binding,
`GET /v1/things/5:cancel` bound `id = "5:cancel"`, and two custom verbs on one resource
collided on the first-declared. Fixed in Rust and Go together: the verb rides on the
binding and is matched in both directions. Full write-up, plus every remaining gap and
what it actually does, in [HTTPRULE_GAPS.md](HTTPRULE_GAPS.md).

### Coverage

Two layers, and they answer different questions.

The **conformance suite** now covers REST (`conformance/cases/rest.yaml`, added the same
day): `conformance.proto` carries `google.api.http` annotations, and the driver hits them
with a **raw HTTP client** rather than the grpc-webnext one — which is the claim being
tested, since an annotated URL is supposed to need no SDK. That is one harness proving both
servers answer alike, which is what the matrix is for.

The **per-language tests** cover what the matrix structurally cannot: the unsupported
HttpRule features (a shared subset, pinned per language — see `HTTPRULE_GAPS.md`), and the
router internals. The Go REST tests additionally drive the **same** `echo.proto` annotations
the Rust tests drive, through the same URLs, each naming its Rust counterpart — belt and
braces on top of the matrix rather than, as it was for a few hours, a substitute for it.

## Recorded decisions (differences that are not bugs)

- **Trailers-only responses.** tonic emits a gRPC error carrying no message as a genuine
  trailers-only response (status in the headers block) — the shape that produced the
  dropped-trailing-metadata bug the conformance suite found on its first run. grpc-go's
  `http.Handler` transport always splits headers from trailers, so the Go server produces
  an empty initial-metadata block plus a trailer block. Clients read the same status and
  the same metadata either way; the difference does not reach the wire contract. The Go
  reader still falls back to the headers block for the status, so it handles both shapes.
- **`grpc-timeout` digit cap.** The Go parser applies the gRPC spec's 8-digit limit, as
  grpc-go does when it re-parses the header downstream; Rust's parser has no cap. Only
  absurd inputs differ, and both then behave as "no usable deadline".
- **`trailer` on the metadata denylist.** Go needs it because response trailers are
  surfaced through the header map, so grpc-go's `Trailer: Grpc-Status, …` declaration would
  otherwise leak to the client as metadata. It is hop-by-hop in both directions, so it was
  added to the Rust denylist too — keeping the two lists identical, which is the point.
- **A malformed JSON message frame forwards as the default message.** Matches the Rust
  server rather than inventing a new error surface.

## Coverage

- `go test ./...` — unit tests for the wire primitives (framing, deframing across arbitrary
  chunk boundaries, metadata, `grpc-timeout`, percent coding, JSON frame-kind priority) and
  end-to-end tests over real sockets covering all four RPC cardinalities on both codecs,
  deadlines, size limits, metadata fidelity, handshake rejection and codec inference,
  keepalive pings, native gRPC passthrough, graceful shutdown, and REST transcoding on both
  surfaces. Run under `-race` in CI, since a WebSocket connection has a reader, a writer,
  and a keepalive ticker on it — and since that is what caught the WaitGroup misuse above.
- The REST unit tests (`webnext/httprule_test.go`) bind against a **synthetic** message
  built from a `FileDescriptorProto` in the test itself, so every coercion branch — each
  scalar width, enum by name and by number, bytes, repeated, nested — is reachable without
  bending the shared `echo.proto` into a test fixture. The last section of that file pins
  the *unsupported* HttpRule features: those tests assert current behavior deliberately, so
  implementing one of them breaks its test, which is the tripwire keeping
  `HTTPRULE_GAPS.md` honest.
- The shutdown suites (`webnext/shutdown_test.go`, `tests/inproc_shutdown.rs`) test the two
  failure modes that matter: a drain that returns too early cuts live RPCs, and one that
  returns too late hangs a deploy. So every case asserts *both* that in-flight work
  finished and that the drain actually ended.
- The end-to-end tests drive the server with a **hand-written wire client**
  (`webnext/wireclient_test.go`) that re-implements the framing independently, so a bug in
  the server's codec cannot cancel itself out against the test's.
- The cross-language matrix runs every case against Rust **and** Go via the TypeScript
  client driver, across `proto/h2ts`, `proto/ws`, and `json`.

## Not implemented

- **Proxy mode**, by design (above). Every *spec surface* is now served.
- The HttpRule subset has gaps, but they are **shared with Rust**, not Go-specific:
  [HTTPRULE_GAPS.md](HTTPRULE_GAPS.md).

# Go in-process server

*2026-07-27. Status: **implemented and in the conformance matrix**. The Go server serves
every surface the Rust in-process server does except REST transcoding, and every case in
`conformance/cases/` now runs against **both** server implementations.*

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

Plus `internal/conformance` (the `ConformanceService` implementation, shared by the binary
and the tests), `examples/conformance-server`, and `examples/greeter`.

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
  keepalive pings, and native gRPC passthrough. Run under `-race` in CI, since a WebSocket
  connection has a reader, a writer, and a keepalive ticker on it.
- The end-to-end tests drive the server with a **hand-written wire client**
  (`webnext/wireclient_test.go`) that re-implements the framing independently, so a bug in
  the server's codec cannot cancel itself out against the test's.
- The cross-language matrix runs every case against Rust **and** Go via the TypeScript
  client driver, across `proto/h2ts`, `proto/ws`, and `json`.

## Not implemented

- **REST transcoding** of `google.api.http` annotations (`/v1/…` routes and their WebSocket
  form). The `+json` codec itself is fully supported on both surfaces. This is the only
  spec surface the Go server does not serve; the conformance suite does not cover REST
  routes yet either (see `conformance/README.md` "Not yet covered"), so adding them is one
  piece of work spanning the suite and this server.
- **Proxy mode**, by design (above).

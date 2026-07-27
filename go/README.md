# grpc-webnext — Go

In-process grpc-webnext server for Go: serve full gRPC semantics to browsers — unary over
Fetch, streaming over WebSocket, and **real HTTP/2 over an [h2ts](https://github.com/debdattabasu/h2ts)
tunnel** — in front of a native `grpc-go` server, on the same port as native gRPC.

> **Status: implemented and in the conformance matrix.** Every case in
> [`/conformance/cases`](../conformance/cases) runs against this server via the TypeScript
> client driver, across every transport and codec, alongside the Rust server. The one
> Rust-only surface is REST transcoding of `google.api.http` annotations (see
> [Not implemented](#not-implemented)).

## Usage

A `*grpc.Server` already implements `http.Handler`, so it *is* the backend — there is no
adapter to construct:

```go
grpcServer := grpc.NewServer()
pb.RegisterGreeterServer(grpcServer, svc)

addr, run, err := webnext.BindAndServe("127.0.0.1:8080", grpcServer, webnext.ServerConfig{})
if err != nil { log.Fatal(err) }
log.Printf("LISTENING http://%s", addr)
log.Fatal(run())
```

`webnext.Handler(backend, cfg)` returns the same router as a plain `http.Handler` if you
would rather run your own `http.Server`. It has to be that server's **root** handler:
native gRPC clients are detected from the HTTP/2 cleartext connection preface, and the
WebSocket upgrades hijack the connection — mounting it under a mux breaks both.

```go
import "github.com/grpc-webnext/grpc-webnext/go/webnext"
```

### Graceful shutdown

`webnext.NewServer` mirrors `http.Server`, including `Shutdown`:

```go
srv := webnext.NewServer(grpcServer, webnext.ServerConfig{})
go srv.Serve(listener)

<-sigint
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
srv.Shutdown(ctx)   // refuse new RPCs, let in-flight ones finish
```

Draining means the same thing on every surface — refuse *new* RPCs, never cut a live one
— using each one's native mechanism:

| Surface | On drain |
|---|---|
| Fetch, native gRPC | HTTP/2 `GOAWAY`, or no-more-requests on HTTP/1 |
| h2ts tunnel | a real `GOAWAY` down the tunnel — the same HTTP/2 server runs inside it |
| custom-`Frame` WebSocket | no stream opened yet → closed with `1001 Going Away`; a live stream runs to its terminal frame, which closes the socket anyway |

A long-lived stream holds the drain open until `ctx` expires — that is what graceful
means. `Shutdown` returning `context.DeadlineExceeded` reports that stragglers are still
there; it does not kill them. The package-level `Serve` / `BindAndServe` helpers have no
drain; use `NewServer` when you need one.

Enable the `+json` codec by supplying message descriptors:

```go
cfg := webnext.ServerConfig{}
cfg.Transcoder, err = webnext.NewTranscoder(fileDescriptorSetBytes)
```

`protoc --descriptor_set_out` produces that blob; so does
`protodesc.ToFileDescriptorProto` over the descriptors already compiled into your generated
package — see [`internal/conformance/service.go`](internal/conformance/service.go) for the
no-side-car-file version. Without a transcoder, `+json` answers `UNIMPLEMENTED` as a proper
gRPC status (never an HTTP 501), exactly as the Rust server does.

## What runs on which surface

| Client speaks | Reaches | Handled by |
|---|---|---|
| `application/grpc*` | your `*grpc.Server`, byte for byte | passthrough |
| `h2ts` WS subprotocol | your `*grpc.Server`, over real HTTP/2 in a tunnel | [`h2ts.go`](webnext/h2ts.go) |
| `application/grpc-webnext+proto` (POST) | unary, `[len\|message][len\|trailer]` | [`fetch.go`](webnext/fetch.go) |
| `application/grpc-webnext+json` (POST) | unary, bare JSON + `grpc-status` header | [`fetch.go`](webnext/fetch.go) |
| `grpc-webnext+proto` / `+json` WS subprotocol | streaming, one stream per socket | [`ws.go`](webnext/ws.go) |

## Layout

```
go/
  go.mod                      module github.com/grpc-webnext/grpc-webnext/go
  generate.sh                 regenerate the checked-in protobuf bindings
  webnext/                    the library
    doc.go                    package overview
    config.go                 ServerConfig; content-type + subprotocol constants
    server.go                 Server (Serve/Shutdown), the surface router, drain tracking
    fetch.go                  unary over Fetch (+proto and +json)
    ws.go                     streaming over the custom `Frame` WebSocket protocol
    h2ts.go                   the real-HTTP/2 tunnel + its request-size limit
    dispatch.go               the in-process gRPC round trip (request/response/trailers)
    framing.go                Fetch blocks and gRPC message frames
    metadata.go               header <-> metadata, grpc-timeout, the denylist
    jsonframe.go              the native-JSON frame codec
    transcode.go              JSON <-> protobuf via descriptors
    status.go                 WS close codes, grpc-message percent coding
    pb/                       generated from /proto/grpc_webnext.proto
  internal/conformance/       the ConformanceService implementation + its bindings
  examples/greeter/           the shared Greeter demo service
  examples/conformance-server/  the conformance-matrix entry point
```

## Build & test

```bash
cd go
go build ./... && go vet ./...
go test ./...           # unit + end-to-end over real sockets
go test -race ./...     # the WebSocket path has concurrent readers and writers
```

The end-to-end tests serve the same `ConformanceService` the cross-language matrix uses and
drive it with a hand-written wire client ([`wireclient_test.go`](webnext/wireclient_test.go))
that re-implements the framing independently — so a bug in the server's codec cannot cancel
itself out against the test's.

The cross-language matrix (this server × the TypeScript client × every transport and codec)
runs from the Node workspace:

```bash
cd node/packages/client && npx vitest run test/conformance.test.ts
```

## Codegen

The `.proto` files at the repo root are the shared contract and the only place to edit them.
Go, like Node, **checks the generated bindings in**, so `go build` needs no protoc and the
module never ships a `.proto`. Regenerate with:

```bash
./generate.sh          # needs protoc, protoc-gen-go, protoc-gen-go-grpc
```

Each proto's Go import path is supplied on the protoc command line (`M<file>=<path>`) rather
than as a `go_package` option, so the shared contract stays language-neutral.

## Not implemented

- **REST transcoding** of `google.api.http` annotations (the `/v1/…` routes and their
  WebSocket form). The `+json` *codec* is fully supported on both surfaces; only the
  annotated REST aliases are Rust-only. A plain-HTTP request that matches no binding
  behaves as the spec says it should: it reaches a main gRPC path only with
  `AllowImplicitCodec`, else `415`.
- **Proxy mode.** The standalone, schema-agnostic proxy is a single implementation, the
  Rust `grpc-webnext-proxy` binary. The abstraction Rust needs for it (a `Backend` enum
  over in-process and upstream) collapses to nothing in Go, where a gRPC server already
  *is* an `http.Handler`.

## Behavioral notes vs. the Rust server

Both implementations are held to the same wire by the conformance matrix; these are the
places where the *language runtime*, not the protocol, differs:

- **Trailers-only responses.** tonic emits a gRPC error with no message as a genuine
  trailers-only response (status in the headers block). grpc-go's `http.Handler` transport
  always splits headers from trailers, so the Go server produces an empty initial-metadata
  block plus a trailer block. Clients read the same status and the same metadata either way;
  the difference is invisible above the transport.
- **`grpc-timeout` parsing.** The Go server applies the gRPC spec's 8-digit cap (as grpc-go
  does when it re-parses the header downstream), so a longer value reads as no deadline.

## Releasing

This module lives in a subdirectory of the polyglot monorepo, so its import path carries the
`/go` suffix and its release tags are prefixed `go/vX.Y.Z` (e.g. `go/v0.1.0`).

## License

Dual-licensed under [MIT](../LICENSE-MIT) or [Apache-2.0](../LICENSE-APACHE), at your option.

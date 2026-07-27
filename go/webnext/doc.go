// Package webnext is the Go implementation of grpc-webnext: it serves full gRPC
// semantics to browsers — unary over Fetch, streaming over WebSocket, and real
// HTTP/2 over an h2ts tunnel — in front of an in-process grpc-go server, on the
// same port as native gRPC.
//
// It is the Go sibling of the Rust `grpc-webnext` crate and the Node
// `@grpc-webnext/server` package. All three implement the one wire format defined
// in /proto/grpc_webnext.proto and /spec/PROTOCOL.md, and are held to it by the
// cross-language conformance suite in /conformance.
//
// # Usage
//
// A *grpc.Server already implements http.Handler, so it is the backend directly:
//
//	grpcServer := grpc.NewServer()
//	pb.RegisterGreeterServer(grpcServer, svc)
//
//	addr, run, err := webnext.BindAndServe("127.0.0.1:8080", grpcServer, webnext.ServerConfig{})
//	if err != nil {
//		log.Fatal(err)
//	}
//	log.Printf("LISTENING http://%s", addr)
//	log.Fatal(run())
//
// # The four surfaces
//
// One port; the URL picks the route and the content-type (Fetch) or subprotocol
// (WebSocket) picks the codec:
//
//   - `application/grpc*` — native gRPC, passed through untouched, so existing
//     gRPC clients keep working against the same address.
//   - `h2ts` WebSocket subprotocol — real HTTP/2 tunneled over a WebSocket, served
//     directly to the gRPC handler. No translation at all; this is the binary
//     default for browser clients.
//   - `application/grpc-webnext+proto` / `+json` over Fetch — unary only, with the
//     response written as `[len|message][len|trailer]` (browsers cannot read HTTP
//     trailers) or, for JSON, a bare body with the status in headers.
//   - `grpc-webnext+proto` / `+json` WebSocket subprotocols — streaming over the
//     custom `Frame` protocol, one stream per socket.
//   - `google.api.http` annotation URLs (`/v1/…`) — REST aliases for annotated
//     methods, JSON-only, over Fetch or (for streaming methods) a WebSocket.
//
// # Differences from the Rust implementation
//
// The Rust crate has two entry points over identical inbound handling —
// `serve_in_process` (wrap a service you own) and `serve_proxy` (front a remote
// upstream). This package ships the in-process shape only; the standalone,
// schema-agnostic proxy is a single implementation, the Rust
// `grpc-webnext-proxy` binary. The abstraction the Rust crate needs for that
// (its `Backend` enum) collapses to nothing here, because a gRPC server in Go
// already *is* an http.Handler.
//
// Every *spec surface* is served here, proxy mode aside. The HttpRule subset does
// have gaps, but they are shared with the Rust router rather than Go-specific —
// see ../../doc/HTTPRULE_GAPS.md, and ../README.md for the current matrix.
package webnext

package webnext

import (
	"net"
	"net/http"
	"strings"

	h2ts "github.com/debdattabasu/h2ts/go/server"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Handler builds the grpc-webnext router in front of backend — a native gRPC
// handler, typically a *grpc.Server, which already implements http.Handler.
//
// The returned handler serves all four surfaces on one port:
//
//   - native gRPC (`application/grpc*`) — passed through to backend untouched;
//   - `+proto` / `+json` unary over Fetch;
//   - streaming over a WebSocket carrying the custom `Frame` protocol;
//   - the `h2ts` subprotocol — real HTTP/2 tunneled over a WebSocket, served
//     straight to backend with no grpc-webnext translation at all.
//
// It must be the http.Server's *root* Handler: native gRPC clients speak HTTP/2
// cleartext, which is detected from the connection preface (the returned handler
// is wrapped in h2c for exactly that), and the WebSocket upgrades hijack the
// connection. Mounting it under a mux breaks both.
func Handler(backend http.Handler, cfg ServerConfig) http.Handler {
	return h2c.NewHandler(&handler{backend: backend, cfg: cfg}, &http2.Server{})
}

// Serve runs a grpc-webnext server on l until it errors, dispatching translated
// calls to backend.
func Serve(l net.Listener, backend http.Handler, cfg ServerConfig) error {
	srv := &http.Server{Handler: Handler(backend, cfg)}
	return srv.Serve(l)
}

// BindAndServe binds addr and returns the bound address plus a run func that
// serves until error (mirroring the Rust `bind_and_serve_in_process` shape). The
// caller typically prints the address, then calls run().
//
// Passing port 0 binds an ephemeral port, which is why the bound address is
// returned rather than assumed.
func BindAndServe(addr string, backend http.Handler, cfg ServerConfig) (net.Addr, func() error, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	return l.Addr(), func() error { return Serve(l, backend, cfg) }, nil
}

type handler struct {
	backend http.Handler
	cfg     ServerConfig
}

// ServeHTTP routes one inbound request to its surface. The URL picks the route
// and the content-type (Fetch) / subprotocol (WebSocket) picks the codec; see
// spec/PROTOCOL.md "Content types".
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h2ts.IsUpgradeRequest(r) {
		h.handleUpgrade(w, r)
		return
	}

	// Exact matches, and native gRPC by prefix — checked last, because
	// `application/grpc-webnext+…` also starts with `application/grpc`.
	switch ct := r.Header.Get("Content-Type"); {
	case ct == CTProto:
		h.unaryProto(w, r)
	case ct == CTJSON:
		h.unaryJSON(w, r, true)
	case strings.HasPrefix(ct, CTGRPC):
		h.backend.ServeHTTP(w, r) // native gRPC, byte-for-byte passthrough
	case ct == "application/json" || ct == "":
		// Plain JSON reaches a main gRPC path only with AllowImplicitCodec;
		// unaryJSON enforces that (and answers UNIMPLEMENTED with no transcoder).
		h.unaryJSON(w, r, false)
	default:
		http.Error(w, "unsupported content-type: "+ct, http.StatusUnsupportedMediaType)
	}
}

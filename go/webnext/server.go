package webnext

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"

	h2ts "github.com/debdattabasu/h2ts/go/server"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Server serves grpc-webnext in front of a native gRPC handler — typically a
// *grpc.Server, which already implements http.Handler — with graceful shutdown
// wired through every surface. It mirrors [net/http.Server]'s shape:
//
//	srv := webnext.NewServer(grpcServer, webnext.ServerConfig{})
//	go srv.Serve(listener)
//	<-sigint
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	srv.Shutdown(ctx)
//
// All four surfaces share one port:
//
//   - native gRPC (`application/grpc*`) — passed through untouched, so existing
//     gRPC clients keep working against the same address;
//   - the `h2ts` WebSocket subprotocol — real HTTP/2 tunneled over a WebSocket,
//     served directly to the backend with no translation at all;
//   - `+proto` / `+json` unary over Fetch;
//   - `grpc-webnext+proto` / `+json` WebSockets — streaming over the custom
//     `Frame` protocol, one stream per socket.
type Server struct {
	http *http.Server
	// h2 serves every HTTP/2 connection: native gRPC cleartext (via h2c) and the
	// inside of each h2ts tunnel. Keeping one instance is what lets a single
	// Shutdown reach all of them — see NewServer.
	h2 *http2.Server

	draining  chan struct{}
	drainOnce sync.Once
	// live counts in-flight handler calls, which for the hijacking surfaces means
	// whole connections: net/http stops tracking a connection the moment it is
	// hijacked, and http.Server.Shutdown explicitly neither closes nor waits for
	// those, so Shutdown waits on this instead.
	live *liveConns
}

// liveConns counts in-flight handler calls and lets a drain wait for them.
//
// This deliberately is not a sync.WaitGroup: `Add` from zero while `Wait` is
// running is documented misuse, and that is precisely what an HTTP/2 connection
// does when a new stream arrives mid-drain — the race detector catches it.
type liveConns struct {
	mu       sync.Mutex
	n        int
	draining bool
	idle     chan struct{}
}

func newLiveConns() *liveConns { return &liveConns{idle: make(chan struct{})} }

func (l *liveConns) add() {
	l.mu.Lock()
	l.n++
	l.mu.Unlock()
}

func (l *liveConns) done() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.n--
	l.signalIfIdle()
}

// wait blocks until nothing is in flight, or ctx expires.
func (l *liveConns) wait(ctx context.Context) error {
	l.mu.Lock()
	l.draining = true
	l.signalIfIdle()
	l.mu.Unlock()
	select {
	case <-l.idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// signalIfIdle reports "nothing left" once draining has begun. Caller holds the lock.
func (l *liveConns) signalIfIdle() {
	if !l.draining || l.n != 0 {
		return
	}
	select {
	case <-l.idle: // already signalled
	default:
		close(l.idle)
	}
}

// NewServer builds a Server. It does not listen; call [Server.Serve].
func NewServer(backend http.Handler, cfg ServerConfig) *Server {
	s := &Server{h2: &http2.Server{}, draining: make(chan struct{}), live: newLiveConns()}
	root := &handler{backend: backend, cfg: cfg, srv: s}
	s.http = &http.Server{Handler: h2c.NewHandler(root, s.h2)}

	// Registers s.http's shutdown hook on the HTTP/2 server, which is what makes
	// Shutdown send a graceful GOAWAY on every HTTP/2 connection it is running —
	// native gRPC clients and the inside of each h2ts tunnel alike. It cannot fail
	// for a server with no TLS config, which is all this one ever is (the tunnel's
	// TLS, if any, terminates on the outer HTTP/1.1 connection).
	_ = http2.ConfigureServer(s.http, s.h2)
	return s
}

// Serve accepts connections on l until it errors or [Server.Shutdown] is called,
// in which case it returns [net/http.ErrServerClosed].
func (s *Server) Serve(l net.Listener) error { return s.http.Serve(l) }

// Shutdown drains the server: it stops accepting connections, tells every open one
// to finish, waits for in-flight RPCs, and returns once the last connection is gone
// or ctx expires — whichever comes first.
//
// What "tell it to finish" means differs by surface, but the intent is uniform —
// refuse *new* RPCs, never cut a live one:
//
//   - Fetch and native gRPC: an HTTP/2 GOAWAY, or no-more-requests on HTTP/1.
//   - h2ts tunnels: a real GOAWAY down the tunnel, since the same HTTP/2 server
//     runs inside it.
//   - Custom-`Frame` WebSockets: a socket carries exactly one stream, so one that
//     has not opened its stream yet is closed (`1001 Going Away`), and one with a
//     live stream runs to its terminal frame, which closes the socket anyway.
//
// A long-lived stream therefore holds the drain open until ctx expires; that is
// what graceful means. Returning on ctx expiry does not kill the stragglers — it
// reports that they are still there. Close the listener's process, or keep a
// reference and force-close, if you need them gone.
func (s *Server) Shutdown(ctx context.Context) error {
	s.drainOnce.Do(func() { close(s.draining) })

	// Stops accepting, closes idle HTTP/1 connections, fires the GOAWAY hook
	// registered in NewServer, and waits for tracked connections to go idle.
	err := s.http.Shutdown(ctx)

	// Hijacked connections are invisible to the above, so wait for them here.
	if waitErr := s.live.wait(ctx); waitErr != nil {
		return waitErr
	}
	return err
}

// Handler builds the grpc-webnext router as a plain http.Handler, for callers that
// want to run their own [net/http.Server]. Graceful shutdown is *not* wired up on
// this path — use [NewServer] for that.
//
// It must be the server's **root** handler: native gRPC clients are detected from
// the HTTP/2 cleartext connection preface (the returned handler is wrapped in h2c
// for exactly that), and the WebSocket upgrades hijack the connection. Mounting it
// under a mux breaks both.
func Handler(backend http.Handler, cfg ServerConfig) http.Handler {
	return NewServer(backend, cfg).http.Handler
}

// Serve runs a grpc-webnext server on l until it errors, dispatching translated
// calls to backend. It cannot be drained; use [NewServer] if you need that.
func Serve(l net.Listener, backend http.Handler, cfg ServerConfig) error {
	return NewServer(backend, cfg).Serve(l)
}

// BindAndServe binds addr and returns the bound address plus a run func that serves
// until error (mirroring the Rust `bind_and_serve_in_process` shape). The caller
// typically prints the address, then calls run().
//
// Passing port 0 binds an ephemeral port, which is why the bound address is
// returned rather than assumed. Like [Serve], this shape cannot be drained.
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
	srv     *Server
}

// ServeHTTP routes one inbound request to its surface. The URL picks the route and
// the content-type (Fetch) / subprotocol (WebSocket) picks the codec; see
// spec/PROTOCOL.md "Content types".
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Counted for the whole handler, which for the hijacking surfaces is the whole
	// connection: those never return to net/http, so this is what Shutdown waits on.
	h.srv.live.add()
	defer h.srv.live.done()

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

// The binary default: real HTTP/2, tunneled through a WebSocket by
// [h2ts](https://github.com/debdattabasu/h2ts).
//
// When a WebSocket handshake offers the `h2ts` subprotocol the client is speaking
// real gRPC — HTTP/2 with genuine framing, trailers, flow control, and many
// multiplexed streams — over the tunnel, not the custom `Frame` protocol. So
// there is nothing to translate: the tunnel is handed straight to the same
// gRPC handler that serves native `application/grpc` traffic on this port.
//
// The only thing wrapped around it is the request-size limit, so this path
// honors MaxMessageBytes like the Fetch and WebSocket paths do.

package webnext

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	h2ts "github.com/debdattabasu/h2ts/go/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serveH2TS completes the h2ts handshake and serves real gRPC over the tunnel.
// It blocks for the tunnel's lifetime; the connection has been hijacked by then,
// so net/http no longer owns it.
func (h *handler) serveH2TS(w http.ResponseWriter, r *http.Request) {
	conn, err := h2ts.Accept(w, r)
	if err != nil {
		return // Accept has already written the rejection
	}
	max := h.cfg.maxMessageBytes()
	backend := h.backend
	keepalive := h2ts.DefaultKeepAlive()
	// Serving the tunnel with the *server's* HTTP/2 server, rather than letting
	// h2ts make its own, is what puts this connection within reach of
	// Server.Shutdown: it sends a real GOAWAY down the tunnel, so in-flight
	// streams finish and no new ones start.
	_ = h2ts.ServeH2With(conn, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &grpcSizeLimitReader{inner: r.Body, max: max}
		backend.ServeHTTP(w, r)
	}), h2ts.ServeConfig{KeepAlive: &keepalive, Server: h.srv.h2})
}

// grpcSizeLimitReader passes a gRPC request body through unchanged but fails the
// stream if any length-prefixed message declares a size over max — giving the
// real-gRPC h2ts path the same request limit the custom paths enforce, without
// buffering anything to measure it.
type grpcSizeLimitReader struct {
	inner io.ReadCloser
	max   int

	// gRPC frame parse state: a 5-byte prefix (1 compression flag + u32
	// big-endian length), then that many message bytes.
	headerSeen    int
	lenBuf        [4]byte
	bodyRemaining int
}

func (g *grpcSizeLimitReader) Read(p []byte) (int, error) {
	n, err := g.inner.Read(p)
	if n > 0 {
		if inspectErr := g.inspect(p[:n]); inspectErr != nil {
			return n, inspectErr
		}
	}
	return n, err
}

func (g *grpcSizeLimitReader) Close() error { return g.inner.Close() }

// inspect walks data tracking frame boundaries, erroring if a declared message
// length exceeds the limit.
func (g *grpcSizeLimitReader) inspect(data []byte) error {
	for i := 0; i < len(data); {
		if g.headerSeen < grpcHeaderLen {
			if g.headerSeen >= 1 {
				g.lenBuf[g.headerSeen-1] = data[i]
			}
			g.headerSeen++
			i++
			if g.headerSeen == grpcHeaderLen {
				n := int64(binary.BigEndian.Uint32(g.lenBuf[:]))
				if n > int64(g.max) {
					return status.Error(codes.ResourceExhausted,
						fmt.Sprintf("request message exceeds size limit (%d bytes)", g.max))
				}
				g.bodyRemaining = int(n)
			}
			continue
		}
		take := min(g.bodyRemaining, len(data)-i)
		g.bodyRemaining -= take
		i += take
		if g.bodyRemaining == 0 {
			g.headerSeen = 0
		}
	}
	return nil
}

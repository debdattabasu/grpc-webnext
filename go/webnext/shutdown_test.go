// Graceful shutdown: stop accepting, let in-flight RPCs finish, close idle
// connections, and return once the last one is gone.
//
// The interesting cases are all "does the drain wait for the right thing" — a drain
// that returns too early cuts live RPCs, and one that returns too late hangs a deploy.

package webnext_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grpc-webnext/grpc-webnext/go/internal/conformance"
	cpb "github.com/grpc-webnext/grpc-webnext/go/internal/conformance/conformancepb"
	"github.com/grpc-webnext/grpc-webnext/go/webnext"
)

// drainableServer is a running Server plus the knobs a shutdown test needs.
type drainableServer struct {
	baseURL string
	addr    string
	srv     *webnext.Server
	serveNo chan error // the Serve goroutine's return value
}

func startDrainable(t *testing.T, cfg webnext.ServerConfig) *drainableServer {
	t.Helper()

	if cfg.Transcoder == nil {
		fds, err := conformance.DescriptorSet()
		if err != nil {
			t.Fatalf("descriptor set: %v", err)
		}
		if cfg.Transcoder, err = webnext.NewTranscoder(fds); err != nil {
			t.Fatalf("transcoder: %v", err)
		}
	}

	grpcServer := grpc.NewServer()
	conformance.Register(grpcServer)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := webnext.NewServer(grpcServer, cfg)
	serveNo := make(chan error, 1)
	go func() { serveNo <- srv.Serve(l) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		grpcServer.Stop()
	})

	addr := l.Addr().String()
	return &drainableServer{baseURL: "http://" + addr, addr: addr, srv: srv, serveNo: serveNo}
}

// shutdown drains with a bound, returning how long it took.
func (s *drainableServer) shutdown(t *testing.T, within time.Duration) (time.Duration, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	start := time.Now()
	err := s.srv.Shutdown(ctx)
	return time.Since(start), err
}

// An idle server drains at once — there is nothing to wait for — and Serve reports
// the shutdown rather than an error.
func TestShutdownIdleServerReturnsImmediately(t *testing.T) {
	s := startDrainable(t, webnext.ServerConfig{})

	elapsed, err := s.shutdown(t, 5*time.Second)
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("an idle drain took %v, want ~immediate", elapsed)
	}

	select {
	case err := <-s.serveNo:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve returned %v, want http.ErrServerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Serve did not return after Shutdown")
	}
}

// After the drain the listener is closed, so a new RPC cannot be served.
func TestShutdownStopsAccepting(t *testing.T) {
	s := startDrainable(t, webnext.ServerConfig{})

	// Prove the address served before the drain.
	got := fetchUnaryProto(t, s.baseURL, unaryPath,
		unaryRequest(t, "hi", &cpb.ResponseDefinition{Payload: []byte("hi")}), callOptions{})
	if got.statusCode != 0 {
		t.Fatalf("pre-drain call failed: %d %q", got.statusCode, got.statusMsg)
	}

	if _, err := s.shutdown(t, 5*time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if conn, err := net.DialTimeout("tcp", s.addr, time.Second); err == nil {
		// Some platforms hand back a connection from the accept backlog; it must
		// still be dead rather than serving.
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := conn.Read(make([]byte, 1)); err == nil {
			t.Error("the listener still served a connection after the drain")
		}
		conn.Close()
	}
}

// A WebSocket that has not opened its stream is idle, so the drain closes it — with
// `1001 Going Away`, not a gRPC status, because no RPC ever started.
func TestShutdownClosesIdleWebSocket(t *testing.T) {
	s := startDrainable(t, webnext.ServerConfig{})

	c, _, err := dialWS(t, s.baseURL, serverStream, false)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(ctx)
	}()

	got := c.collect(5 * time.Second)
	if got.closeCode != websocket.CloseGoingAway {
		t.Errorf("close code = %d, want %d (1001 Going Away)", got.closeCode, websocket.CloseGoingAway)
	}
	if got.terminal != nil {
		t.Errorf("draining an unused socket must not fabricate a status, got %v", got.terminal)
	}
}

// A WebSocket with a live stream is not cut: the RPC completes, delivers its terminal
// frame, and only then does the socket — and the drain — finish.
func TestShutdownWaitsForInFlightStream(t *testing.T) {
	s := startDrainable(t, webnext.ServerConfig{})

	c, _, err := dialWS(t, s.baseURL, serverStream, false)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	// Three messages, each delayed, so the stream is provably still running when the
	// drain starts.
	req := &cpb.ServerStreamRequest{ResponseDefinition: &cpb.ResponseDefinition{
		StreamMessages: []*cpb.StreamMessage{
			{Payload: []byte("a")},
			{Payload: []byte("b"), DelayMs: 150},
			{Payload: []byte("c"), DelayMs: 150},
		},
	}}
	c.subscribe(nil, 0, encodeRequest(t, req, false))
	c.halfClose()

	// Wait for the first message, then drain underneath the running stream.
	deadline := time.Now().Add(5 * time.Second)
	_ = c.conn.SetReadDeadline(deadline)
	for {
		messageType, data, err := c.conn.ReadMessage()
		if err != nil {
			t.Fatalf("read first message: %v", err)
		}
		frame, err := c.decode(messageType, data)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if frame.GetMessage() != nil {
			break
		}
	}

	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		drained <- s.srv.Shutdown(ctx)
	}()

	got := c.collect(10 * time.Second)
	// The remaining messages must still arrive, followed by the terminal status and a
	// normal (1000) close — not the going-away code an idle socket gets.
	if n := len(got.messages); n != 2 {
		t.Errorf("got %d messages after the drain started, want the remaining 2", n)
	}
	assertTerminalOK(t, got)
	if got.closeCode != websocket.CloseNormalClosure {
		t.Errorf("close code = %d, want 1000 — the stream ended normally", got.closeCode)
	}

	select {
	case err := <-drained:
		if err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("Shutdown did not return after the stream finished")
	}
}

// A unary Fetch call already in flight when the drain starts still gets its response:
// the server stops taking *new* requests, it does not cut the one it is serving.
func TestShutdownWaitsForInFlightUnary(t *testing.T) {
	s := startDrainable(t, webnext.ServerConfig{})

	type result struct {
		got fetchResult
	}
	done := make(chan result, 1)
	go func() {
		done <- result{fetchUnaryProto(t, s.baseURL, unaryPath,
			unaryRequest(t, "x", &cpb.ResponseDefinition{Payload: []byte("x"), DelayMs: 400}),
			callOptions{})}
	}()

	// Let the call get established, then drain out from under it.
	time.Sleep(100 * time.Millisecond)
	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		drained <- s.srv.Shutdown(ctx)
	}()

	select {
	case r := <-done:
		if r.got.statusCode != 0 {
			t.Fatalf("the in-flight call was cut by the drain: %d %q", r.got.statusCode, r.got.statusMsg)
		}
		if p := string(decodePayload(t, r.got.body).GetPayload()); p != "x" {
			t.Errorf("payload = %q, want %q", p, "x")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the in-flight call never completed")
	}

	if err := <-drained; err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// A native gRPC client sharing the port gets a GOAWAY, so its in-flight call finishes
// too — this is the surface where "graceful" is HTTP/2's own mechanism.
func TestShutdownWaitsForInFlightNativeGRPC(t *testing.T) {
	s := startDrainable(t, webnext.ServerConfig{})

	conn, err := grpc.NewClient(s.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := cpb.NewConformanceServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := client.Unary(ctx, &cpb.UnaryRequest{
			Payload:            []byte("native"),
			ResponseDefinition: &cpb.ResponseDefinition{Payload: []byte("native"), DelayMs: 400},
		})
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	drained := make(chan error, 1)
	go func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		drained <- s.srv.Shutdown(dctx)
	}()

	if err := <-done; err != nil {
		t.Fatalf("the in-flight native gRPC call was cut by the drain: %v", err)
	}
	if err := <-drained; err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// A stream that never ends holds the drain open, so the caller's context is what
// bounds it — Shutdown reports the deadline rather than hanging forever.
func TestShutdownBoundedByContext(t *testing.T) {
	s := startDrainable(t, webnext.ServerConfig{})

	c, _, err := dialWS(t, s.baseURL, bidiStreamPath, false)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	// A bidi stream that is opened and never half-closed: the server has no reason
	// to end it, so the drain has to wait.
	c.subscribe(nil, 0, encodeRequest(t, &cpb.BidiStreamRequest{Payload: []byte("a")}, false))
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		messageType, data, err := c.conn.ReadMessage()
		if err != nil {
			t.Fatalf("read echo: %v", err)
		}
		frame, err := c.decode(messageType, data)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if frame.GetMessage() != nil {
			break
		}
	}

	elapsed, err := s.shutdown(t, 300*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("Shutdown took %v, want it bounded by the 300ms context", elapsed)
	}
}

// Shutdown is safe to call more than once, which matters when both a signal handler
// and a defer reach for it.
func TestShutdownIsIdempotent(t *testing.T) {
	s := startDrainable(t, webnext.ServerConfig{})

	if _, err := s.shutdown(t, 5*time.Second); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if _, err := s.shutdown(t, 5*time.Second); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("second Shutdown: %v", err)
	}
}

// The status codes a drained server hands back are ordinary gRPC ones — nothing here
// invents a new surface. Guards against a drain that starts answering with, say, an
// HTTP error instead of a status.
func TestShutdownLeavesStatusCodesAlone(t *testing.T) {
	s := startDrainable(t, webnext.ServerConfig{})

	got := fetchUnaryProto(t, s.baseURL, unaryPath, unaryRequest(t, "x", &cpb.ResponseDefinition{
		StatusCode: uint32(codes.PermissionDenied), StatusMessage: "denied",
	}), callOptions{})
	if got.statusCode != uint32(codes.PermissionDenied) {
		t.Fatalf("status = %d, want PERMISSION_DENIED", got.statusCode)
	}

	if _, err := s.shutdown(t, 5*time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

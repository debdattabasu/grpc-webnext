// Dispatching a translated call into the gRPC handler.
//
// Everything else in this package is inbound protocol translation (Fetch +
// WebSocket <-> gRPC); this is where the translated call lands. In Go that needs
// no abstraction at all: a gRPC server already *is* an [net/http.Handler]
// (*grpc.Server.ServeHTTP), so "dispatch" is an in-process HTTP round trip
// against it — the Go analog of calling a tonic `Routes` as a tower service.
//
// Two details make it a *gRPC* round trip rather than an ordinary one:
//
//   - grpc-go's http.Handler transport requires HTTP/2 (it refuses
//     r.ProtoMajor != 2) and a ResponseWriter implementing http.Flusher, so the
//     synthetic request is stamped HTTP/2 and the writer below flushes.
//   - Response trailers — where the gRPC status lives — arrive through the
//     *header* map, per the net/http contract: names pre-declared in `Trailer`
//     get their values later, and undeclared ones are added under the
//     `Trailer:` prefix. Both are collected once the handler returns.

package webnext

import (
	"context"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
)

// grpcCall is one in-flight gRPC call dispatched into the backend handler.
//
// The request body and the response body are both live streams, so all four
// call types work: request messages are written as they arrive off the wire, and
// response messages are read as the service produces them.
type grpcCall struct {
	w *grpcResponseWriter

	// reqBody is the write end of the dispatched request's body. Close it (with
	// no error) to half-close the request stream.
	reqBody *io.PipeWriter
	// respBody streams the gRPC response body; it reaches EOF when the handler
	// returns, after which Trailers is populated.
	respBody *io.PipeReader

	cancel context.CancelFunc
	// done is closed once the backend handler has returned.
	done chan struct{}
}

// dispatch starts a gRPC call against backend and returns immediately; the
// handler runs on its own goroutine. `header` supplies the request metadata (the
// caller has already filtered the denylist and set grpc-timeout).
//
// `origin` is the inbound HTTP request, used only for the peer address and
// authority — the dispatched request is built fresh.
func dispatch(ctx context.Context, backend http.Handler, origin *http.Request, path string, header http.Header) *grpcCall {
	ctx, cancel := context.WithCancel(ctx)
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()

	w := &grpcResponseWriter{header: http.Header{}, body: respW, ready: make(chan struct{})}
	call := &grpcCall{w: w, reqBody: reqW, respBody: respR, cancel: cancel, done: make(chan struct{})}

	pathOnly, rawQuery, _ := strings.Cut(path, "?")
	req := (&http.Request{
		Method:     http.MethodPost,
		URL:        &url.URL{Path: pathOnly, RawQuery: rawQuery},
		Proto:      "HTTP/2.0",
		ProtoMajor: 2,
		ProtoMinor: 0,
		Header:     header,
		Body:       reqR,
		Host:       origin.Host,
		RemoteAddr: origin.RemoteAddr,
		RequestURI: path,
	}).WithContext(ctx)

	go func() {
		defer close(call.done)
		defer w.finish()
		backend.ServeHTTP(w, req)
	}()
	return call
}

// awaitHeaders blocks until the response head is available (the service wrote a
// message, sent initial metadata, or finished), the call is cancelled, or ctx
// ends. It reports whether the head arrived.
func (c *grpcCall) awaitHeaders(ctx context.Context) bool {
	select {
	case <-c.w.ready:
		return true
	case <-ctx.Done():
		return false
	}
}

// headers returns the response's initial metadata. Valid once awaitHeaders
// reports true.
func (c *grpcCall) headers() http.Header { return c.w.snapshot() }

// trailers returns the response's trailing metadata and status. Valid once the
// response body has reached EOF.
func (c *grpcCall) trailers() http.Header { return c.w.trailerSnapshot() }

// halfClose signals that the client is done sending.
func (c *grpcCall) halfClose() { _ = c.reqBody.Close() }

// close abandons the call: cancels the handler and tears down both pipes. Safe
// to call more than once.
func (c *grpcCall) close() {
	c.cancel()
	_ = c.reqBody.CloseWithError(context.Canceled)
	_ = c.respBody.CloseWithError(context.Canceled)
}

// grpcResponseWriter is the http.ResponseWriter grpc-go writes the response
// into: headers are snapshotted the first time they are committed, the body is
// streamed through a pipe, and trailers are harvested from the header map once
// the handler returns.
type grpcResponseWriter struct {
	header http.Header
	body   *io.PipeWriter

	commitOnce sync.Once
	ready      chan struct{}

	mu       sync.Mutex
	initial  http.Header
	trailing http.Header
}

func (w *grpcResponseWriter) Header() http.Header { return w.header }

func (w *grpcResponseWriter) WriteHeader(int) { w.commit() }

func (w *grpcResponseWriter) Write(b []byte) (int, error) {
	w.commit()
	return w.body.Write(b)
}

// Flush is required by grpc-go's handler transport, which uses it both to push
// each message out and — before any write — to force the header/trailer split.
// The pipe is already unbuffered, so committing the headers is all it does.
func (w *grpcResponseWriter) Flush() { w.commit() }

// commit freezes the initial metadata. Trailer values are not set yet at this
// point (grpc-go writes them after the body), and the names it pre-declared are
// excluded so they cannot be mistaken for initial metadata.
func (w *grpcResponseWriter) commit() {
	w.commitOnce.Do(func() {
		declared := declaredTrailers(w.header)
		initial := make(http.Header, len(w.header))
		for k, v := range w.header {
			if _, isTrailer := declared[k]; isTrailer || strings.HasPrefix(k, http.TrailerPrefix) {
				continue
			}
			initial[k] = append([]string(nil), v...)
		}
		w.mu.Lock()
		w.initial = initial
		w.mu.Unlock()
		close(w.ready)
	})
}

// finish runs once the backend handler has returned: it harvests the trailers
// and then EOFs the response body, in that order, so a reader that has seen EOF
// can rely on the trailers being there.
func (w *grpcResponseWriter) finish() {
	w.commit() // a handler that wrote nothing at all still has a response head
	trailing := http.Header{}
	for name := range declaredTrailers(w.header) {
		if v, ok := w.header[name]; ok {
			trailing[name] = v
		}
	}
	for k, v := range w.header {
		if name, ok := strings.CutPrefix(k, http.TrailerPrefix); ok {
			trailing[textproto.CanonicalMIMEHeaderKey(name)] = v
		}
	}
	w.mu.Lock()
	w.trailing = trailing
	w.mu.Unlock()
	_ = w.body.Close()
}

func (w *grpcResponseWriter) snapshot() http.Header {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.initial
}

func (w *grpcResponseWriter) trailerSnapshot() http.Header {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.trailing == nil {
		return http.Header{}
	}
	return w.trailing
}

// declaredTrailers is the set of canonical header names listed in the `Trailer`
// header — the net/http way of announcing trailers up front, which is how
// grpc-go carries grpc-status/grpc-message.
func declaredTrailers(h http.Header) map[string]struct{} {
	out := map[string]struct{}{}
	for _, list := range h["Trailer"] {
		for _, name := range strings.Split(list, ",") {
			if name = strings.TrimSpace(name); name != "" {
				out[textproto.CanonicalMIMEHeaderKey(name)] = struct{}{}
			}
		}
	}
	return out
}

// Unary RPCs over Fetch.
//
// Streaming never comes through here — it is WebSocket-only, because Fetch would
// have to buffer an entire stream. See spec/PROTOCOL.md "Unary — Fetch".

package webnext

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/grpc-webnext/grpc-webnext/go/webnext/pb"
	"google.golang.org/grpc/codes"
)

// unaryProto serves `+proto` unary: the length-prefixed request is streamed into
// a gRPC frame, dispatched, and the response written back as
// `[u32 len | message][u32 len | Trailer]` — streamed, never buffered. Because
// the status rides in the trailer block *after* the message rather than in a
// header, a large blob is piped straight to the socket.
func (h *handler) unaryProto(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Only the declared length is read: nothing is buffered to measure it.
	var lead [lenPrefix]byte
	if _, err := io.ReadFull(r.Body, lead[:]); err != nil {
		textResponse(w, http.StatusBadRequest, "request body missing length prefix")
		return
	}
	if int64(binary.BigEndian.Uint32(lead[:])) > int64(h.cfg.maxMessageBytes()) {
		textResponse(w, http.StatusRequestEntityTooLarge, "request message exceeds size limit")
		return
	}

	deadline, hasDeadline := parseGRPCTimeout(r.Header)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	call := dispatch(ctx, h.backend, r, r.URL.RequestURI(), h.requestHeaders(r.Header, deadline, hasDeadline))
	defer call.close()

	// Feed the request body: the gRPC compression flag, then the client's block
	// verbatim (its `[u32 len | message]` layout is already the frame's tail).
	go func() {
		defer call.halfClose()
		if _, err := call.reqBody.Write([]byte{0}); err != nil {
			return
		}
		if _, err := call.reqBody.Write(lead[:]); err != nil {
			return
		}
		_, _ = io.Copy(call.reqBody, r.Body)
	}()

	// The edge owns the client-facing deadline: expiry abandons the call, and the
	// status is synthesized here rather than waited for.
	var timedOut atomic.Bool
	if hasDeadline {
		t := time.AfterFunc(deadline, func() {
			timedOut.Store(true)
			call.close()
		})
		defer t.Stop()
	}

	if !call.awaitHeaders(ctx) || timedOut.Load() {
		fetchStatus(w, codes.DeadlineExceeded, "deadline exceeded")
		return
	}

	respHeaders := call.headers()
	header := w.Header()
	copyMetadataHeaders(header, respHeaders)
	header.Set("Content-Type", CTProto)
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	// Forward the response body opaquely: drop the leading gRPC compression flag
	// and the remainder is our message block, byte for byte.
	sawMessage := false
	buf := make([]byte, 32*1024)
	skip := 1
	for {
		n, err := call.respBody.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if skip > 0 {
				drop := min(skip, len(chunk))
				chunk, skip = chunk[drop:], skip-drop
			}
			if len(chunk) > 0 {
				sawMessage = true
				if _, werr := w.Write(chunk); werr != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
		}
		if err != nil {
			break
		}
	}

	// A deadline firing mid-message cannot be signalled cleanly — the block has
	// already promised a length — so stop and let the client see a truncated body.
	// Before any message there is still room for a clean status.
	if timedOut.Load() && sawMessage {
		return
	}
	if !sawMessage {
		if _, err := w.Write(emptyMessageBlock); err != nil {
			return
		}
	}

	trailer := &pb.Trailer{}
	if timedOut.Load() {
		trailer.StatusCode, trailer.StatusMessage = uint32(codes.DeadlineExceeded), "deadline exceeded"
	} else {
		trailing := call.trailers()
		trailer.StatusCode, trailer.StatusMessage = readStatus(trailing, respHeaders)
		trailer.Trailers = headersToMetadataList(trailing)
	}
	_, _ = w.Write(encodeTrailerBlock(trailer))
}

// unaryJSON serves the `+json` codec, REST-annotated URLs, and — with
// AllowImplicitCodec — plain `application/json` on main paths: JSON in, JSON out,
// with the gRPC status in the `grpc-status` / `grpc-message` response headers.
// Unlike `+proto` this path buffers — the transcode is whole-message and the
// status must precede the body.
//
// sdkJSON distinguishes the SDK content-type (`+json`, allowed on any main path)
// from plain JSON (which needs AllowImplicitCodec to reach a main path). Neither
// gate applies to a REST URL: annotated endpoints accept plain HTTP always.
func (h *handler) unaryJSON(w http.ResponseWriter, r *http.Request, sdkJSON bool) {
	defer r.Body.Close()

	respCT := CTJSON
	if !sdkJSON {
		respCT = "application/json"
	}
	if h.cfg.Transcoder == nil {
		// A capability gap is a gRPC status, never an HTTP 501.
		jsonResponse(w, respCT, nil, uint32(codes.Unimplemented), "+json needs a transcoder", nil, nil)
		return
	}

	// Read the body before routing: a REST binding may need it to build the
	// request message, and the size limit applies either way.
	body, err := readBounded(r.Body, h.cfg.maxMessageBytes())
	if errors.Is(err, errTooLarge) {
		// Not an HTTP 413: the JSON codec always carries status in the header, so
		// both Fetch surfaces answer identically.
		jsonResponse(w, respCT, nil, uint32(codes.ResourceExhausted), errTooLarge.Error(), nil, nil)
		return
	} else if err != nil {
		jsonResponse(w, respCT, nil, uint32(codes.Internal), "read body: "+err.Error(), nil, nil)
		return
	}

	deadline, hasDeadline := parseGRPCTimeout(r.Header)

	// 1) A REST annotation binding? Path, query, and body build the method's
	//    request message. Annotated routes always answer `application/json`,
	//    whichever JSON content-type asked.
	call, matched, err := h.cfg.Transcoder.transcodeHTTPRequest(
		r.Method, r.URL.EscapedPath(), r.URL.RawQuery, body)
	switch {
	case err != nil:
		jsonResponse(w, "application/json", nil, uint32(codes.InvalidArgument),
			"bad REST request: "+err.Error(), nil, nil)
		return
	case matched:
		h.jsonUpstream(w, r, call.grpcMethod, call.message, "application/json", deadline, hasDeadline)
		return
	}

	// 2) A main gRPC method path. `+json` is the SDK contract; plain JSON needs
	//    the flag.
	if !sdkJSON && !h.cfg.AllowImplicitCodec {
		textResponse(w, http.StatusUnsupportedMediaType,
			"this gRPC method path requires content-type "+CTJSON+
				" (or set AllowImplicitCodec to accept plain JSON here)")
		return
	}
	path := r.URL.RequestURI()
	if !h.cfg.Transcoder.HasMethod(path) {
		jsonResponse(w, respCT, nil, uint32(codes.Unimplemented), "no such method: "+path, nil, nil)
		return
	}
	message, err := h.cfg.Transcoder.RequestJSONToProto(path, body)
	if err != nil {
		jsonResponse(w, respCT, nil, uint32(codes.InvalidArgument), "bad json request: "+err.Error(), nil, nil)
		return
	}
	h.jsonUpstream(w, r, path, message, respCT, deadline, hasDeadline)
}

// jsonUpstream dispatches an already-encoded binary request and renders the
// binary response as native JSON (bare body, status in `grpc-status` /
// `grpc-message`). Both the `+json` main path and REST routes land here — they
// differ only in how the request message was built.
func (h *handler) jsonUpstream(
	w http.ResponseWriter, r *http.Request,
	path string, message []byte, respCT string,
	deadline time.Duration, hasDeadline bool,
) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	call := dispatch(ctx, h.backend, r, path, h.requestHeaders(r.Header, deadline, hasDeadline))
	defer call.close()
	go func() {
		defer call.halfClose()
		_, _ = call.reqBody.Write(grpcFrame(message))
	}()

	var timedOut atomic.Bool
	if hasDeadline {
		t := time.AfterFunc(deadline, func() {
			timedOut.Store(true)
			call.close()
		})
		defer t.Stop()
	}

	headersOK := call.awaitHeaders(ctx)
	respBody, readErr := io.ReadAll(call.respBody)
	if timedOut.Load() || !headersOK {
		jsonResponse(w, respCT, nil, uint32(codes.DeadlineExceeded), "deadline exceeded", nil, nil)
		return
	}
	if readErr != nil {
		jsonResponse(w, respCT, nil, uint32(codes.Unavailable), "upstream body: "+readErr.Error(), nil, nil)
		return
	}

	initial, trailing := call.headers(), call.trailers()
	code, statusMessage := readStatus(trailing, initial)

	var jsonBody []byte
	if messages := deframeAll(respBody); code == 0 && len(messages) > 0 {
		encoded, err := h.cfg.Transcoder.ResponseProtoToJSON(path, messages[0])
		if err != nil {
			jsonResponse(w, respCT, nil, uint32(codes.Internal), "bad json response: "+err.Error(), nil, nil)
			return
		}
		jsonBody = encoded
	}
	jsonResponse(w, respCT, jsonBody, code, statusMessage, initial, trailing)
}

// requestHeaders builds the headers for the dispatched gRPC call: the caller's
// metadata minus the denylist, plus gRPC framing and the forwarded deadline.
func (h *handler) requestHeaders(src http.Header, deadline time.Duration, hasDeadline bool) http.Header {
	out := http.Header{}
	copyMetadataHeaders(out, src)
	out.Set("Content-Type", CTGRPC)
	out.Set("Te", "trailers")
	if hasDeadline {
		out.Set("Grpc-Timeout", formatGRPCTimeout(deadline+deadlineGrace))
	}
	return out
}

// fetchStatus writes a `+proto` response carrying only a status: an empty
// message block plus the trailer block. Used for failures that happen before any
// response body exists.
func fetchStatus(w http.ResponseWriter, code codes.Code, message string) {
	body := encodeResponseBody(nil, &pb.Trailer{StatusCode: uint32(code), StatusMessage: message})
	w.Header().Set("Content-Type", CTProto)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// jsonResponse writes a native-JSON Fetch response: a bare JSON body (empty on
// error), the status in `grpc-status` / `grpc-message`, and metadata in headers.
// A denied call is still HTTP 200 — the gRPC status is the answer.
func jsonResponse(w http.ResponseWriter, contentType string, message []byte, code uint32, statusMessage string, initial, trailing http.Header) {
	header := w.Header()
	copyMetadataHeaders(header, initial)
	copyMetadataHeaders(header, trailing)
	header.Set("Content-Type", contentType)
	header.Set("Grpc-Status", strconv.FormatUint(uint64(code), 10))
	if statusMessage != "" {
		header.Set("Grpc-Message", percentEncode(statusMessage))
	}
	if code != 0 {
		message = nil
	}
	header.Set("Content-Length", strconv.Itoa(len(message)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(message)
}

func textResponse(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(message)))
	w.WriteHeader(status)
	_, _ = io.WriteString(w, message)
}

// errTooLarge reports a request message over MaxMessageBytes.
var errTooLarge = errors.New("request message exceeds size limit")

// readBounded buffers r into memory, refusing anything over max.
func readBounded(r io.Reader, max int) ([]byte, error) {
	// Read one byte past the limit so an exactly-at-limit body still succeeds.
	body, err := io.ReadAll(io.LimitReader(r, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(body) > max {
		return nil, errTooLarge
	}
	return body, nil
}

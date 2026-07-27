// Streaming over WebSocket — the custom `Frame` protocol.
//
// One WebSocket carries exactly ONE gRPC stream, so the WS URL *is* the route and
// frames carry no method and no stream id. The first inbound frame opens the
// stream (a Subscribe, its method taken from the URL); then Message / HalfClose /
// Reset flow on the same socket, and the server replies Header, Message(s),
// Trailer / Reset. Exactly one Frame per WebSocket message, no fragmentation.
//
// The binary default runs real HTTP/2 over an h2ts tunnel instead; see h2ts.go.

package webnext

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	h2ts "github.com/debdattabasu/h2ts/go/server"
	"github.com/gorilla/websocket"
	"github.com/grpc-webnext/grpc-webnext/go/webnext/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// wsReadLimit is a coarse transport backstop on a single WebSocket message,
// deliberately well above MaxMessageBytes: the size limit itself must surface as
// an in-band Reset{RESOURCE_EXHAUSTED}, not as a connection teardown. It mirrors
// tungstenite's default, which plays the same role on the Rust server.
const wsReadLimit = 64 << 20

// wsCloseGrace bounds how long the connection lingers after the server has sent
// its close frame, waiting for the peer's close to complete the handshake.
const wsCloseGrace = 5 * time.Second

var upgrader = websocket.Upgrader{
	ReadBufferSize:  32 * 1024,
	WriteBufferSize: 32 * 1024,
	// The subprotocol is negotiated by hand (see handleUpgrade), so the Upgrader
	// echoes whatever the response header carries.
	//
	// Origin is deliberately not checked: grpc-webnext is a cross-origin API
	// surface like any other, and authorization is per-RPC metadata (see
	// spec/PROTOCOL.md "Auth"), not a connect-time origin gate. This mirrors the
	// Rust server, and nginx, which do no Origin validation either.
	CheckOrigin: func(*http.Request) bool { return true },
}

// handleUpgrade resolves the codec from the offered subprotocols, runs the
// connection gate, then upgrades and serves the one stream.
func (h *handler) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	offered := h2ts.OfferedProtocols(r)
	has := func(s string) bool {
		for _, p := range offered {
			if p == s {
				return true
			}
		}
		return false
	}

	// The binary default: the client speaks real gRPC over an h2ts tunnel, so
	// there is nothing to translate and no codec to negotiate.
	if has(WSSubprotocolH2TS) {
		h.serveH2TS(w, r)
		return
	}

	// codec: nil = infer from the first frame (only reachable with implicit
	// codecs); explicit = a grpc-webnext codec subprotocol was offered.
	var codec *bool
	explicit := false
	switch {
	case has(WSSubprotocolProto):
		codec, explicit = ptr(false), true
	case has(WSSubprotocolJSON):
		codec, explicit = ptr(true), true
	case has("application/json"):
		codec = ptr(true)
	}

	// A main-endpoint WebSocket needs a grpc-webnext codec subprotocol unless
	// implicit codecs are allowed. A rejected handshake still completes, then
	// closes with a `4000 + code` frame so browser JS can read the status.
	rejected := !explicit && !h.cfg.AllowImplicitCodec

	respHeader := http.Header{}
	for _, p := range []string{WSSubprotocolProto, WSSubprotocolJSON, WSSubprotocol, "application/json"} {
		if has(p) {
			respHeader.Set("Sec-Websocket-Protocol", p)
			break
		}
	}

	conn, err := upgrader.Upgrade(w, r, respHeader)
	if err != nil {
		return // Upgrade has already written the rejection
	}
	defer conn.Close()

	if rejected {
		closeWithStatus(conn, codes.Unimplemented,
			"this websocket requires a grpc-webnext+proto or grpc-webnext+json subprotocol")
		return
	}
	h.serveWS(conn, r, codec)
}

func ptr[T any](v T) *T { return &v }

// wsOutbound is one ready-to-send WebSocket message. A frame is encoded in the
// connection's codec at send time, which is what lets a connection whose codec is
// inferred from the first frame work.
type wsOutbound struct {
	messageType int
	data        []byte
	// closeNormal marks the terminal message: the writer emits a normal (1000)
	// close frame and stops.
	closeNormal bool
}

// wsConn is the write side of one connection. A single goroutine owns the socket
// writer, so frames, keepalive pings, and the close frame never interleave.
type wsConn struct {
	conn       *websocket.Conn
	out        chan wsOutbound
	connDone   chan struct{} // closed when the read loop ends
	writerDone chan struct{} // closed when the writer goroutine ends
}

// send queues a message, reporting false if the writer has already stopped (so a
// stream never blocks forever on a dead socket).
func (c *wsConn) send(m wsOutbound) bool {
	select {
	case c.out <- m:
		return true
	case <-c.writerDone:
		return false
	}
}

func (c *wsConn) sendFrame(f *pb.Frame, jsonCodec bool) bool {
	m, ok := encodeOutbound(f, jsonCodec)
	return ok && c.send(m)
}

// encodeOutbound renders a Frame as a WebSocket message in the connection codec:
// a text frame of native JSON, or a binary protobuf `Frame`.
func encodeOutbound(f *pb.Frame, jsonCodec bool) (wsOutbound, bool) {
	if jsonCodec {
		jf := protoFrameToJSON(f)
		if jf == nil {
			return wsOutbound{}, false
		}
		return wsOutbound{messageType: websocket.TextMessage, data: encodeJSONFrame(jf)}, true
	}
	data, err := proto.Marshal(f)
	if err != nil {
		return wsOutbound{}, false
	}
	return wsOutbound{messageType: websocket.BinaryMessage, data: data}, true
}

// serveWS runs one connection: a writer goroutine owning the socket (plus
// keepalive), and this goroutine reading frames and driving the single stream.
func (h *handler) serveWS(conn *websocket.Conn, r *http.Request, codec *bool) {
	conn.SetReadLimit(wsReadLimit)

	c := &wsConn{
		conn:       conn,
		out:        make(chan wsOutbound, 64),
		connDone:   make(chan struct{}),
		writerDone: make(chan struct{}),
	}
	defer close(c.connDone)
	go h.runWriter(c)

	// Keepalive: any inbound frame — a pong or ordinary stream data — proves
	// liveness, so the read deadline *is* the silence timer. If nothing arrives
	// for WSKeepalive + WSKeepaliveTimeout the peer is presumed dead and the
	// connection is dropped, which surfaces the stream as UNAVAILABLE.
	silence := h.cfg.maxSilence()
	refresh := func() {
		if silence > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(silence))
		}
	}
	conn.SetPongHandler(func(string) error { refresh(); return nil })
	refresh()

	methodURL := r.URL.RequestURI()
	opened := false
	var stream *wsStream
	defer func() {
		if stream != nil {
			stream.abort()
		}
	}()

	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		refresh()

		var frame *pb.Frame
		switch messageType {
		case websocket.BinaryMessage:
			if codec == nil {
				codec = ptr(false)
			}
			if *codec {
				continue // connection is locked to JSON/text
			}
			frame = decodeBinaryFrame(data, methodURL, &opened)
		case websocket.TextMessage:
			if codec == nil {
				codec = ptr(true)
			}
			if !*codec {
				continue // connection is locked to proto/binary
			}
			frame = decodeTextFrame(data, methodURL, &opened)
		default:
			continue
		}
		if frame == nil {
			continue
		}
		h.handleFrame(r, c, frame, *codec, &stream)
	}
}

// runWriter owns every write to the socket: queued frames, keepalive pings, and
// the terminal close frame.
func (h *handler) runWriter(c *wsConn) {
	defer close(c.writerDone)

	var tick <-chan time.Time
	if h.cfg.WSKeepalive > 0 {
		t := time.NewTicker(h.cfg.WSKeepalive)
		defer t.Stop()
		tick = t.C
	}

	for {
		select {
		case m := <-c.out:
			if m.closeNormal {
				// The stream's status was already delivered in its terminal
				// frame, so this close carries none. Bound the wait for the
				// peer's close echo so the read loop cannot linger.
				_ = c.conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
					time.Now().Add(wsCloseGrace))
				_ = c.conn.SetReadDeadline(time.Now().Add(wsCloseGrace))
				return
			}
			if err := c.conn.WriteMessage(m.messageType, m.data); err != nil {
				return
			}
		case <-tick:
			// The peer's automatic pong is the return traffic that stops an
			// idle-timeout proxy from dropping a quiet stream.
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsCloseGrace)); err != nil {
				return
			}
		case <-c.connDone:
			return
		}
	}
}

// decodeBinaryFrame decodes an inbound protobuf `Frame`. The first frame must be
// a Subscribe (its method comes from the WS URL, never from the frame); anything
// else before the stream opens is dropped.
func decodeBinaryFrame(data []byte, methodURL string, opened *bool) *pb.Frame {
	var frame pb.Frame
	if err := proto.Unmarshal(data, &frame); err != nil {
		return nil
	}
	if sub := frame.GetSubscribe(); sub != nil {
		sub.Method = methodURL
		*opened = true
		return &frame
	}
	if !*opened {
		return nil
	}
	return &frame
}

// decodeTextFrame decodes an inbound JSON text frame. The first frame opens the
// one stream; later frames are messages / half-close / reset.
func decodeTextFrame(data []byte, methodURL string, opened *bool) *pb.Frame {
	jf, err := decodeJSONFrame(data)
	if err != nil {
		return nil
	}
	if !*opened {
		*opened = true
		return &pb.Frame{Kind: &pb.Frame_Subscribe{Subscribe: jsonOpenToSubscribe(jf, methodURL)}}
	}
	return jsonFrameToProto(jf)
}

// wsStream is the single gRPC stream a connection carries.
type wsStream struct {
	// requests feeds request payloads to the dispatch goroutine; closed on
	// half-close.
	requests chan []byte
	// halfClosed guards `requests` against a send after close. Only the read
	// loop touches it, so it needs no lock.
	halfClosed bool
	// aborted marks a client Reset (or connection teardown): the stream stops
	// silently, emitting no terminal frame, because the client already knows.
	aborted atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
}

func (s *wsStream) abort() {
	s.aborted.Store(true)
	s.cancel()
}

// handleFrame processes one inbound Frame: open the stream on Subscribe, feed
// request messages, half-close, or reset. A rejected Subscribe is answered with
// a Reset on the same (still-open) connection — per-RPC authorization lives in
// the service, so there is no auth-driven connection teardown here.
func (h *handler) handleFrame(r *http.Request, c *wsConn, frame *pb.Frame, jsonCodec bool, stream **wsStream) {
	switch kind := frame.GetKind().(type) {
	case *pb.Frame_Subscribe:
		sub := kind.Subscribe
		if *stream != nil {
			// Rejecting the duplicate open, not the live stream — which is
			// still running and will deliver its own terminal frame and close.
			sendReset(c, jsonCodec, codes.InvalidArgument, "stream already open")
			return
		}
		// An opening frame may carry the first message inline; hold it to the limit.
		if len(sub.GetInitialPayload()) > h.cfg.maxMessageBytes() {
			sendReset(c, jsonCodec, codes.ResourceExhausted, "request message exceeds size limit")
			c.send(wsOutbound{closeNormal: true})
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		s := &wsStream{requests: make(chan []byte, 16), ctx: ctx, cancel: cancel}
		if payload := sub.GetInitialPayload(); len(payload) > 0 {
			s.requests <- payload // the buffer is empty, so this cannot block
		}
		*stream = s
		go h.runStream(r, c, s, sub, jsonCodec)

	case *pb.Frame_Message:
		payload := kind.Message.GetPayload()
		if len(payload) > h.cfg.maxMessageBytes() {
			sendReset(c, jsonCodec, codes.ResourceExhausted, "request message exceeds size limit")
			if s := *stream; s != nil {
				s.abort()
				*stream = nil
			}
			// The stream has reached its terminal frame, so the socket — which
			// carries exactly this one stream — has served its purpose.
			c.send(wsOutbound{closeNormal: true})
			return
		}
		if s := *stream; s != nil && !s.halfClosed {
			// Blocking here is deliberate backpressure; it unblocks if the
			// stream ends (its context is cancelled) or the connection dies.
			select {
			case s.requests <- payload:
			case <-s.ctx.Done():
			case <-c.connDone:
			}
		}

	case *pb.Frame_HalfClose:
		if s := *stream; s != nil && !s.halfClosed {
			s.halfClosed = true
			close(s.requests)
		}

	case *pb.Frame_Reset_:
		if s := *stream; s != nil {
			s.abort()
			*stream = nil
		}
	}
}

// runStream dispatches the one stream and pumps the gRPC response back out as
// frames, closing the socket when it terminates.
func (h *handler) runStream(r *http.Request, c *wsConn, s *wsStream, sub *pb.Subscribe, jsonCodec bool) {
	path := sub.GetMethod()

	// JSON needs a transcoder. A capability gap (no schema, or an unknown
	// method) rejects the stream with a Reset *before* the RPC starts — it is a
	// pre-RPC rejection, not a terminal status.
	var tc *Transcoder
	if jsonCodec {
		switch {
		case h.cfg.Transcoder == nil:
			h.rejectStream(c, s, jsonCodec, codes.Unimplemented, "+json needs a transcoder")
			return
		case !h.cfg.Transcoder.HasMethod(path):
			h.rejectStream(c, s, jsonCodec, codes.Unimplemented, "no such method: "+path)
			return
		}
		tc = h.cfg.Transcoder
	}

	deadline := timeoutFromMillis(sub.GetTimeoutMillis())
	header := metadataListToHeaders(sub.GetHeaders())
	header.Set("Content-Type", CTGRPC)
	header.Set("Te", "trailers")
	if deadline > 0 {
		header.Set("Grpc-Timeout", formatGRPCTimeout(deadline+deadlineGrace))
	}

	call := dispatch(s.ctx, h.backend, r, path, header)
	defer call.close()

	// Request side: each inbound payload becomes one gRPC frame (transcoded
	// first on a JSON connection). Closing `requests` half-closes the RPC; the
	// context ends it if the stream is torn down without a half-close.
	go func() {
		defer call.halfClose()
		for {
			var payload []byte
			select {
			case p, ok := <-s.requests:
				if !ok {
					return
				}
				payload = p
			case <-s.ctx.Done():
				return
			}
			message := payload
			if tc != nil {
				// A malformed JSON message forwards as the default message,
				// matching the Rust server rather than inventing a new error.
				message, _ = tc.RequestJSONToProto(path, payload)
			}
			if _, err := call.reqBody.Write(grpcFrame(message)); err != nil {
				return
			}
		}
	}()

	var timedOut atomic.Bool
	if deadline > 0 {
		t := time.AfterFunc(deadline, func() {
			timedOut.Store(true)
			call.close()
		})
		defer t.Stop()
	}

	if !call.awaitHeaders(s.ctx) {
		h.finishStream(c, s, jsonCodec, &pb.Trailer{
			StatusCode: uint32(codes.Unavailable), StatusMessage: "stream aborted",
		})
		return
	}
	respHeaders := call.headers()
	if !c.sendFrame(&pb.Frame{Kind: &pb.Frame_Header{Header: &pb.Header{
		Headers: headersToMetadataList(respHeaders),
	}}}, jsonCodec) {
		return
	}

	var d deframer
	buf := make([]byte, 32*1024)
	for {
		n, readErr := call.respBody.Read(buf)
		if n > 0 {
			d.push(buf[:n])
			for {
				message, ok := d.next()
				if !ok {
					break
				}
				if tc != nil {
					encoded, err := tc.ResponseProtoToJSON(path, message)
					if err != nil {
						h.rejectStream(c, s, jsonCodec, codes.Internal, "json encode: "+err.Error())
						return
					}
					message = encoded
				}
				if !c.sendFrame(&pb.Frame{Kind: &pb.Frame_Message{
					Message: &pb.Message{Payload: message},
				}}, jsonCodec) {
					return
				}
			}
		}
		if readErr != nil {
			break
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
	h.finishStream(c, s, jsonCodec, trailer)
}

// finishStream delivers the terminal Trailer and closes the socket. A WebSocket
// carries exactly one stream, so once that stream has ended the connection has
// served its purpose: it closes with a normal 1000, carrying no status (the
// status was already delivered in the Trailer).
func (h *handler) finishStream(c *wsConn, s *wsStream, jsonCodec bool, trailer *pb.Trailer) {
	if s.aborted.Load() {
		// The client reset the stream; it already has its status and owns the
		// connection's fate.
		return
	}
	if !c.sendFrame(&pb.Frame{Kind: &pb.Frame_Trailer{Trailer: trailer}}, jsonCodec) {
		return
	}
	c.send(wsOutbound{closeNormal: true})
}

// rejectStream ends the stream with a Reset — the status for a stream rejected
// before or outside the RPC lifecycle — and then closes the socket.
func (h *handler) rejectStream(c *wsConn, s *wsStream, jsonCodec bool, code codes.Code, message string) {
	if s.aborted.Load() {
		return
	}
	sendReset(c, jsonCodec, code, message)
	c.send(wsOutbound{closeNormal: true})
}

func sendReset(c *wsConn, jsonCodec bool, code codes.Code, message string) {
	c.sendFrame(&pb.Frame{Kind: &pb.Frame_Reset_{Reset_: &pb.Reset{
		StatusCode: uint32(code), StatusMessage: message,
	}}}, jsonCodec)
}

// closeWithStatus rejects a connection at the handshake with a private close
// code carrying the gRPC status (4000 + code), readable from browser JS. Used
// only for connection-level rejections, never for per-stream errors.
func closeWithStatus(conn *websocket.Conn, code codes.Code, message string) {
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(WSCloseCode(code), truncateUTF8(message, maxCloseReason)),
		time.Now().Add(wsCloseGrace))
}

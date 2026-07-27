// A minimal grpc-webnext *client*, used only by this package's end-to-end tests.
//
// It re-implements the wire format from spec/PROTOCOL.md by hand rather than
// reusing the server's internals, so a framing bug cannot cancel itself out: the
// tests assert the bytes an independent implementation would produce and expect.

package webnext_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/grpc-webnext/grpc-webnext/go/webnext"
	"github.com/grpc-webnext/grpc-webnext/go/webnext/pb"
	"google.golang.org/protobuf/proto"
)

// --- Fetch ------------------------------------------------------------------

// fetchResult is one unary Fetch response, decoded per the codec in play.
type fetchResult struct {
	httpStatus int
	headers    http.Header
	body       []byte // `+proto`: the message block; `+json`: the bare JSON body
	statusCode uint32
	statusMsg  string
	trailers   []*pb.Metadatum // `+proto` only: the Trailer block's metadata
}

// fetchUnaryProto sends a `+proto` unary request: `[u32 len | message]` in, and
// `[u32 len | message][u32 len | Trailer]` back.
func fetchUnaryProto(t *testing.T, baseURL, path string, message []byte, opts callOptions) fetchResult {
	t.Helper()

	body := make([]byte, 4, 4+len(message))
	binary.BigEndian.PutUint32(body, uint32(len(message)))
	body = append(body, message...)

	resp := doFetch(t, baseURL, path, webnext.CTProto, body, opts)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	out := fetchResult{httpStatus: resp.StatusCode, headers: resp.Header}
	if resp.StatusCode != http.StatusOK {
		out.body = raw
		return out
	}

	message, rest, err := takeBlock(raw)
	if err != nil {
		t.Fatalf("decode message block: %v (body %d bytes)", err, len(raw))
	}
	trailerBytes, _, err := takeBlock(rest)
	if err != nil {
		t.Fatalf("decode trailer block: %v", err)
	}
	var trailer pb.Trailer
	if err := proto.Unmarshal(trailerBytes, &trailer); err != nil {
		t.Fatalf("decode Trailer: %v", err)
	}
	out.body = message
	out.statusCode, out.statusMsg, out.trailers = trailer.GetStatusCode(), trailer.GetStatusMessage(), trailer.GetTrailers()
	return out
}

// fetchUnaryJSON sends a JSON unary request. The status rides in the
// `grpc-status` / `grpc-message` response headers and the body is bare JSON.
func fetchUnaryJSON(t *testing.T, baseURL, path, contentType string, message []byte, opts callOptions) fetchResult {
	t.Helper()

	resp := doFetch(t, baseURL, path, contentType, message, opts)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	code, _ := strconv.ParseUint(resp.Header.Get("grpc-status"), 10, 32)
	return fetchResult{
		httpStatus: resp.StatusCode,
		headers:    resp.Header,
		body:       raw,
		statusCode: uint32(code),
		statusMsg:  percentDecodeTest(resp.Header.Get("grpc-message")),
	}
}

type callOptions struct {
	timeout  time.Duration
	metadata http.Header
}

func doFetch(t *testing.T, baseURL, path, contentType string, body []byte, opts callOptions) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	for k, vs := range opts.metadata {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if opts.timeout > 0 {
		req.Header.Set("grpc-timeout", strconv.FormatInt(opts.timeout.Milliseconds(), 10)+"m")
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("fetch %s: %v", path, err)
	}
	return resp
}

// takeBlock reads one `[u32 len | bytes]` block, returning it and the remainder.
func takeBlock(b []byte) (block, rest []byte, err error) {
	if len(b) < 4 {
		return nil, nil, fmt.Errorf("truncated: want 4 length bytes, have %d", len(b))
	}
	n := int(binary.BigEndian.Uint32(b))
	if len(b) < 4+n {
		return nil, nil, fmt.Errorf("truncated: declared %d bytes, have %d", n, len(b)-4)
	}
	return b[4 : 4+n], b[4+n:], nil
}

// percentDecodeTest mirrors the gRPC `grpc-message` decoding, independently of
// the server's copy.
func percentDecodeTest(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); {
		if s[i] == '%' && i+2 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
				out = append(out, byte(n))
				i += 3
				continue
			}
		}
		out = append(out, s[i])
		i++
	}
	return string(out)
}

// --- WebSocket --------------------------------------------------------------

// wsClient drives one stream over one WebSocket, in either codec.
type wsClient struct {
	t          *testing.T
	conn       *websocket.Conn
	jsonCodec  bool
	subscribed bool
}

// dialWS opens a WebSocket for `path`, offering the codec's subprotocol. The
// `extra` subprotocols replace the default offer, which is how the tests
// exercise handshake rejection.
func dialWS(t *testing.T, baseURL, path string, jsonCodec bool, extra ...string) (*wsClient, *http.Response, error) {
	t.Helper()

	protocols := extra
	if protocols == nil {
		codec := webnext.WSSubprotocolProto
		if jsonCodec {
			codec = webnext.WSSubprotocolJSON
		}
		protocols = []string{webnext.WSSubprotocol, codec}
	}
	dialer := websocket.Dialer{
		Subprotocols:     protocols,
		HandshakeTimeout: 10 * time.Second,
	}
	conn, resp, err := dialer.Dial("ws"+strings.TrimPrefix(baseURL, "http")+path, nil)
	if err != nil {
		return nil, resp, err
	}
	return &wsClient{t: t, conn: conn, jsonCodec: jsonCodec}, resp, nil
}

func (c *wsClient) close() { _ = c.conn.Close() }

// send writes one client frame. The first one opens the stream; on the JSON
// codec an opening frame carries `metadata` / `timeoutMillis` instead of a
// Subscribe envelope, and the method always comes from the URL.
func (c *wsClient) send(frame *pb.Frame) {
	c.t.Helper()

	if !c.jsonCodec {
		data, err := proto.Marshal(frame)
		if err != nil {
			c.t.Fatalf("encode frame: %v", err)
		}
		if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
			c.t.Fatalf("write frame: %v", err)
		}
		return
	}

	obj := map[string]any{}
	switch kind := frame.GetKind().(type) {
	case *pb.Frame_Subscribe:
		if md := jsonMeta(kind.Subscribe.GetHeaders()); len(md) > 0 {
			obj["metadata"] = md
		}
		if ms := kind.Subscribe.GetTimeoutMillis(); ms > 0 {
			obj["timeoutMillis"] = ms
		}
		if p := kind.Subscribe.GetInitialPayload(); len(p) > 0 {
			obj["message"] = json.RawMessage(p)
		}
	case *pb.Frame_Message:
		obj["message"] = json.RawMessage(kind.Message.GetPayload())
	case *pb.Frame_HalfClose:
		obj["halfClose"] = true
	case *pb.Frame_Reset_:
		obj["status"] = map[string]any{
			"code": kind.Reset_.GetStatusCode(), "message": kind.Reset_.GetStatusMessage(),
		}
	}
	data, err := json.Marshal(obj)
	if err != nil {
		c.t.Fatalf("encode json frame: %v", err)
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.t.Fatalf("write json frame: %v", err)
	}
}

// subscribe opens the stream. `payload` is the optional inline first message.
func (c *wsClient) subscribe(md []*pb.Metadatum, timeoutMillis uint32, payload []byte) {
	c.send(&pb.Frame{Kind: &pb.Frame_Subscribe{Subscribe: &pb.Subscribe{
		Headers: md, TimeoutMillis: timeoutMillis, InitialPayload: payload, Json: c.jsonCodec,
	}}})
	c.subscribed = true
}

func (c *wsClient) message(payload []byte) {
	c.send(&pb.Frame{Kind: &pb.Frame_Message{Message: &pb.Message{Payload: payload}}})
}

func (c *wsClient) halfClose() {
	c.send(&pb.Frame{Kind: &pb.Frame_HalfClose{HalfClose: &pb.HalfClose{}}})
}

func (c *wsClient) reset(code uint32, message string) {
	c.send(&pb.Frame{Kind: &pb.Frame_Reset_{Reset_: &pb.Reset{StatusCode: code, StatusMessage: message}}})
}

// wsResult is everything a stream delivered before the socket closed.
type wsResult struct {
	header    []*pb.Metadatum
	messages  [][]byte
	terminal  *pb.Frame // the Trailer or Reset frame, if one arrived
	closeCode int       // the WebSocket close code, or -1 if the socket just ended
}

// collect reads until the stream terminates or the socket closes.
func (c *wsClient) collect(timeout time.Duration) wsResult {
	c.t.Helper()

	out := wsResult{closeCode: -1}
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		messageType, data, err := c.conn.ReadMessage()
		if err != nil {
			var closeErr *websocket.CloseError
			if errors.As(err, &closeErr) {
				out.closeCode = closeErr.Code
			}
			return out
		}
		frame, err := c.decode(messageType, data)
		if err != nil {
			c.t.Fatalf("decode server frame: %v (%q)", err, data)
		}
		switch kind := frame.GetKind().(type) {
		case *pb.Frame_Header:
			out.header = kind.Header.GetHeaders()
		case *pb.Frame_Message:
			out.messages = append(out.messages, kind.Message.GetPayload())
		case *pb.Frame_Trailer, *pb.Frame_Reset_:
			out.terminal = frame
		}
	}
}

// decode turns a server WebSocket message back into the internal Frame shape.
// On the JSON codec that means reading which field is present, exactly as the
// spec describes; a `status` with metadata is a Trailer, without one a Reset —
// indistinguishable on a JSON connection, so the tests treat it as a Trailer.
func (c *wsClient) decode(messageType int, data []byte) (*pb.Frame, error) {
	if !c.jsonCodec {
		if messageType != websocket.BinaryMessage {
			return nil, fmt.Errorf("expected a binary frame, got type %d", messageType)
		}
		var frame pb.Frame
		if err := proto.Unmarshal(data, &frame); err != nil {
			return nil, err
		}
		return &frame, nil
	}
	if messageType != websocket.TextMessage {
		return nil, fmt.Errorf("expected a text frame, got type %d", messageType)
	}
	var jf struct {
		Metadata map[string]string `json:"metadata"`
		Message  json.RawMessage   `json:"message"`
		Status   *struct {
			Code    uint32 `json:"code"`
			Message string `json:"message"`
		} `json:"status"`
	}
	if err := json.Unmarshal(data, &jf); err != nil {
		return nil, err
	}
	switch {
	case jf.Status != nil:
		return &pb.Frame{Kind: &pb.Frame_Trailer{Trailer: &pb.Trailer{
			StatusCode:    jf.Status.Code,
			StatusMessage: jf.Status.Message,
			Trailers:      metaList(jf.Metadata),
		}}}, nil
	case len(jf.Message) > 0:
		return &pb.Frame{Kind: &pb.Frame_Message{Message: &pb.Message{Payload: jf.Message}}}, nil
	default:
		return &pb.Frame{Kind: &pb.Frame_Header{Header: &pb.Header{Headers: metaList(jf.Metadata)}}}, nil
	}
}

// --- metadata helpers -------------------------------------------------------

func asciiMeta(key, value string) *pb.Metadatum {
	return &pb.Metadatum{Key: key, Value: &pb.Metadatum_AsciiValue{AsciiValue: value}}
}

func binMeta(key string, value []byte) *pb.Metadatum {
	return &pb.Metadatum{Key: key, Value: &pb.Metadatum_BinValue{BinValue: value}}
}

func metaList(m map[string]string) []*pb.Metadatum {
	var out []*pb.Metadatum
	for k, v := range m {
		out = append(out, asciiMeta(k, v))
	}
	return out
}

func jsonMeta(items []*pb.Metadatum) map[string]string {
	out := map[string]string{}
	for _, m := range items {
		if v, ok := m.GetValue().(*pb.Metadatum_AsciiValue); ok {
			out[m.GetKey()] = v.AsciiValue
		}
	}
	return out
}

// findASCII returns the first ASCII value for key, and whether it was present.
func findASCII(items []*pb.Metadatum, key string) (string, bool) {
	for _, m := range items {
		if m.GetKey() == key {
			if v, ok := m.GetValue().(*pb.Metadatum_AsciiValue); ok {
				return v.AsciiValue, true
			}
		}
	}
	return "", false
}

// findBin returns the first binary value for key, and whether it was present.
func findBin(items []*pb.Metadatum, key string) ([]byte, bool) {
	for _, m := range items {
		if m.GetKey() == key {
			if v, ok := m.GetValue().(*pb.Metadatum_BinValue); ok {
				return v.BinValue, true
			}
		}
	}
	return nil, false
}

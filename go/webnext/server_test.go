// End-to-end tests: a real listener, real sockets, real grpc-go behind the edge.
//
// The service under test is the same ConformanceService every implementation
// serves for the cross-language matrix, so these tests and the matrix exercise
// identical server behavior — these just do it in-language, over the wire, with
// a hand-written client (see wireclient_test.go).

package webnext_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/grpc-webnext/grpc-webnext/go/internal/conformance"
	cpb "github.com/grpc-webnext/grpc-webnext/go/internal/conformance/conformancepb"
	"github.com/grpc-webnext/grpc-webnext/go/webnext"
	"github.com/grpc-webnext/grpc-webnext/go/webnext/pb"
)

const (
	svc            = "/grpc.webnext.conformance.v1.ConformanceService"
	unaryPath      = svc + "/Unary"
	serverStream   = svc + "/ServerStream"
	clientStream   = svc + "/ClientStream"
	bidiStreamPath = svc + "/BidiStream"
)

// --- harness ----------------------------------------------------------------

type testServer struct {
	baseURL string
	addr    string
}

// startServer runs the conformance service behind grpc-webnext on an ephemeral
// port, torn down with the test. A transcoder is installed unless the config
// already carries one (the capability-gap tests pass a config that has none by
// using startServerWithout).
func startServer(t *testing.T, cfg webnext.ServerConfig) *testServer {
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
	return serve(t, cfg)
}

// startServerWithoutTranscoder runs the same service with the `+json` capability
// deliberately absent.
func startServerWithoutTranscoder(t *testing.T, cfg webnext.ServerConfig) *testServer {
	t.Helper()
	cfg.Transcoder = nil
	return serve(t, cfg)
}

func serve(t *testing.T, cfg webnext.ServerConfig) *testServer {
	return serveBackend(t, cfg, conformance.Register)
}

// serveBackend is the same harness over an arbitrary gRPC service — the REST
// tests drive the Echo service instead, since that is where the shared
// `google.api.http` annotations live.
func serveBackend(t *testing.T, cfg webnext.ServerConfig, register func(grpc.ServiceRegistrar)) *testServer {
	t.Helper()

	grpcServer := grpc.NewServer()
	register(grpcServer)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: webnext.Handler(grpcServer, cfg)}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		grpcServer.Stop()
	})

	addr := l.Addr().String()
	return &testServer{baseURL: "http://" + addr, addr: addr}
}

// unaryRequest builds a binary UnaryRequest with the given response definition.
func unaryRequest(t *testing.T, payload string, rd *cpb.ResponseDefinition) []byte {
	t.Helper()
	body, err := proto.Marshal(&cpb.UnaryRequest{Payload: []byte(payload), ResponseDefinition: rd})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return body
}

func decodePayload(t *testing.T, b []byte) *cpb.ConformancePayload {
	t.Helper()
	var out cpb.ConformancePayload
	if err := proto.Unmarshal(b, &out); err != nil {
		t.Fatalf("decode ConformancePayload: %v", err)
	}
	return &out
}

func decodeJSONPayload(t *testing.T, b []byte) *cpb.ConformancePayload {
	t.Helper()
	var out cpb.ConformancePayload
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatalf("decode JSON ConformancePayload from %q: %v", b, err)
	}
	return &out
}

func jsonRequest(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := protojson.Marshal(m)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return b
}

// --- native gRPC passthrough -------------------------------------------------

// A native gRPC client must reach the same service on the same port, untouched.
// grpc-go speaks HTTP/2 cleartext, so this also pins the h2c preface detection.
func TestNativeGRPCPassthrough(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	conn, err := grpc.NewClient(s.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := cpb.NewConformanceServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Unary(ctx, &cpb.UnaryRequest{Payload: []byte("native")})
	if err != nil {
		t.Fatalf("unary: %v", err)
	}
	if got := string(resp.GetPayload()); got != "native" {
		t.Errorf("payload = %q, want %q", got, "native")
	}

	stream, err := client.ServerStream(ctx, &cpb.ServerStreamRequest{
		ResponseDefinition: &cpb.ResponseDefinition{StreamMessages: []*cpb.StreamMessage{
			{Payload: []byte("a")}, {Payload: []byte("b")},
		}},
	})
	if err != nil {
		t.Fatalf("server stream: %v", err)
	}
	var got []string
	for {
		msg, err := stream.Recv()
		if err != nil {
			break
		}
		got = append(got, string(msg.GetPayload()))
	}
	if strings.Join(got, ",") != "a,b" {
		t.Errorf("stream payloads = %v, want [a b]", got)
	}
}

// --- Fetch, +proto -----------------------------------------------------------

func TestFetchUnaryProtoOK(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	got := fetchUnaryProto(t, s.baseURL, unaryPath,
		unaryRequest(t, "hello", &cpb.ResponseDefinition{Payload: []byte("hello")}), callOptions{})

	if got.httpStatus != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200 (body %q)", got.httpStatus, got.body)
	}
	if got.statusCode != 0 {
		t.Fatalf("gRPC status = %d %q, want OK", got.statusCode, got.statusMsg)
	}
	if p := string(decodePayload(t, got.body).GetPayload()); p != "hello" {
		t.Errorf("payload = %q, want %q", p, "hello")
	}
	if ct := got.headers.Get("Content-Type"); ct != webnext.CTProto {
		t.Errorf("content-type = %q, want %q", ct, webnext.CTProto)
	}
}

// An empty response payload still produces a well-formed two-block body.
func TestFetchUnaryProtoEmptyPayload(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	got := fetchUnaryProto(t, s.baseURL, unaryPath,
		unaryRequest(t, "", &cpb.ResponseDefinition{}), callOptions{})

	if got.statusCode != 0 {
		t.Fatalf("gRPC status = %d %q, want OK", got.statusCode, got.statusMsg)
	}
	if p := decodePayload(t, got.body).GetPayload(); len(p) != 0 {
		t.Errorf("payload = %q, want empty", p)
	}
}

// A non-OK status and its trailing metadata must survive into the trailer block.
func TestFetchUnaryProtoErrorWithTrailers(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	got := fetchUnaryProto(t, s.baseURL, unaryPath, unaryRequest(t, "x", &cpb.ResponseDefinition{
		StatusCode:    uint32(codes.FailedPrecondition),
		StatusMessage: "nope",
		Trailers:      []*cpb.Metadatum{{Key: "x-detail", Value: &cpb.Metadatum_AsciiValue{AsciiValue: "denied"}}},
	}), callOptions{})

	if got.httpStatus != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200 — a gRPC error is still HTTP 200", got.httpStatus)
	}
	if got.statusCode != uint32(codes.FailedPrecondition) {
		t.Errorf("status = %d, want %d", got.statusCode, codes.FailedPrecondition)
	}
	if !strings.Contains(got.statusMsg, "nope") {
		t.Errorf("status message = %q, want it to contain %q", got.statusMsg, "nope")
	}
	if v, ok := findASCII(got.trailers, "x-detail"); !ok || v != "denied" {
		t.Errorf("trailing metadata x-detail = %q (present %v), want %q", v, ok, "denied")
	}
}

// Initial metadata must arrive as Fetch response headers, not in the trailer.
func TestFetchUnaryProtoResponseHeaders(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	got := fetchUnaryProto(t, s.baseURL, unaryPath, unaryRequest(t, "x", &cpb.ResponseDefinition{
		Payload: []byte("x"),
		Headers: []*cpb.Metadatum{{Key: "x-greeting", Value: &cpb.Metadatum_AsciiValue{AsciiValue: "hi"}}},
	}), callOptions{})

	if v := got.headers.Get("x-greeting"); v != "hi" {
		t.Errorf("response header x-greeting = %q, want %q", v, "hi")
	}
}

// ASCII and binary (`-bin`) request metadata must reach the service unchanged.
// On Fetch, `-bin` values are base64 on the wire; the service sees raw bytes.
func TestFetchUnaryProtoMetadataRoundtrip(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	md := http.Header{}
	md.Set("x-ascii", "plain")
	md.Set("x-blob-bin", "AQIDBA==") // 0x01020304

	got := fetchUnaryProto(t, s.baseURL, unaryPath,
		unaryRequest(t, "x", &cpb.ResponseDefinition{Payload: []byte("x")}),
		callOptions{metadata: md})

	echoed := decodePayload(t, got.body).GetRequestInfo().GetRequestHeaders()
	var sawASCII, sawBin bool
	for _, m := range echoed {
		switch v := m.GetValue().(type) {
		case *cpb.Metadatum_AsciiValue:
			sawASCII = sawASCII || (m.GetKey() == "x-ascii" && v.AsciiValue == "plain")
		case *cpb.Metadatum_BinValue:
			sawBin = sawBin || (m.GetKey() == "x-blob-bin" && string(v.BinValue) == "\x01\x02\x03\x04")
		}
	}
	if !sawASCII {
		t.Errorf("service did not observe x-ascii: %v", echoed)
	}
	if !sawBin {
		t.Errorf("service did not observe x-blob-bin as raw bytes: %v", echoed)
	}
}

// A deadline the service blows past terminates the call with DEADLINE_EXCEEDED
// rather than returning the delayed payload.
func TestFetchUnaryProtoDeadlineExceeded(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	start := time.Now()
	got := fetchUnaryProto(t, s.baseURL, unaryPath,
		unaryRequest(t, "x", &cpb.ResponseDefinition{Payload: []byte("x"), DelayMs: 3000}),
		callOptions{timeout: 150 * time.Millisecond})

	if got.statusCode != uint32(codes.DeadlineExceeded) {
		t.Fatalf("status = %d %q, want DEADLINE_EXCEEDED", got.statusCode, got.statusMsg)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %v — the edge should have abandoned the call at the deadline", elapsed)
	}
}

// A deadline that is NOT exceeded still completes normally.
func TestFetchUnaryProtoWithinDeadline(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	got := fetchUnaryProto(t, s.baseURL, unaryPath,
		unaryRequest(t, "x", &cpb.ResponseDefinition{Payload: []byte("x"), DelayMs: 50}),
		callOptions{timeout: 5 * time.Second})

	if got.statusCode != 0 {
		t.Fatalf("status = %d %q, want OK", got.statusCode, got.statusMsg)
	}
	if p := string(decodePayload(t, got.body).GetPayload()); p != "x" {
		t.Errorf("payload = %q, want %q", p, "x")
	}
}

// Only the declared length is read, so an oversize request is refused with HTTP
// 413 before anything is buffered.
func TestFetchUnaryProtoOversizeRequest(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{MaxMessageBytes: 1024})

	got := fetchUnaryProto(t, s.baseURL, unaryPath,
		unaryRequest(t, strings.Repeat("z", 4096), &cpb.ResponseDefinition{}), callOptions{})

	if got.httpStatus != http.StatusRequestEntityTooLarge {
		t.Fatalf("HTTP status = %d, want 413", got.httpStatus)
	}
	if want := "request message exceeds size limit"; string(got.body) != want {
		t.Errorf("body = %q, want %q", got.body, want)
	}
}

// A large response under the limit passes through intact — the response path is
// streamed, not bounded.
func TestFetchUnaryProtoLargeResponse(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{MaxMessageBytes: 4 << 20})

	got := fetchUnaryProto(t, s.baseURL, unaryPath,
		unaryRequest(t, "x", &cpb.ResponseDefinition{OversizeResponseBytes: 1 << 20}), callOptions{})

	if got.statusCode != 0 {
		t.Fatalf("status = %d %q, want OK", got.statusCode, got.statusMsg)
	}
	if n := len(decodePayload(t, got.body).GetPayload()); n != 1<<20 {
		t.Errorf("payload = %d bytes, want %d", n, 1<<20)
	}
}

// --- Fetch, +json ------------------------------------------------------------

func TestFetchUnaryJSONOK(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	body := jsonRequest(t, &cpb.UnaryRequest{
		Payload:            []byte("hello"),
		ResponseDefinition: &cpb.ResponseDefinition{Payload: []byte("hello")},
	})
	got := fetchUnaryJSON(t, s.baseURL, unaryPath, webnext.CTJSON, body, callOptions{})

	if got.statusCode != 0 {
		t.Fatalf("status = %d %q, want OK (body %q)", got.statusCode, got.statusMsg, got.body)
	}
	if p := string(decodeJSONPayload(t, got.body).GetPayload()); p != "hello" {
		t.Errorf("payload = %q, want %q", p, "hello")
	}
	// The JSON surface is meant to be readable in a browser's Network tab.
	if !json.Valid(got.body) {
		t.Errorf("response body is not valid JSON: %q", got.body)
	}
}

// On the JSON codec the status travels in headers and the body stays empty.
func TestFetchUnaryJSONError(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	body := jsonRequest(t, &cpb.UnaryRequest{ResponseDefinition: &cpb.ResponseDefinition{
		StatusCode: uint32(codes.PermissionDenied), StatusMessage: "no way",
	}})
	got := fetchUnaryJSON(t, s.baseURL, unaryPath, webnext.CTJSON, body, callOptions{})

	if got.httpStatus != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200", got.httpStatus)
	}
	if got.statusCode != uint32(codes.PermissionDenied) {
		t.Errorf("status = %d, want %d", got.statusCode, codes.PermissionDenied)
	}
	if got.statusMsg != "no way" {
		t.Errorf("status message = %q, want %q", got.statusMsg, "no way")
	}
	if len(got.body) != 0 {
		t.Errorf("body = %q, want empty on error", got.body)
	}
}

// The `grpc-message` header is percent-encoded, so a message with reserved
// characters survives the round trip.
func TestFetchUnaryJSONPercentEncodedMessage(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	const message = "bad \"input\"; try 100%"
	body := jsonRequest(t, &cpb.UnaryRequest{ResponseDefinition: &cpb.ResponseDefinition{
		StatusCode: uint32(codes.InvalidArgument), StatusMessage: message,
	}})
	got := fetchUnaryJSON(t, s.baseURL, unaryPath, webnext.CTJSON, body, callOptions{})

	if got.statusMsg != message {
		t.Errorf("status message = %q, want %q", got.statusMsg, message)
	}
}

// A capability gap is a gRPC status, never an HTTP 501.
func TestFetchUnaryJSONWithoutTranscoder(t *testing.T) {
	s := startServerWithoutTranscoder(t, webnext.ServerConfig{})

	got := fetchUnaryJSON(t, s.baseURL, unaryPath, webnext.CTJSON, []byte(`{}`), callOptions{})

	if got.httpStatus != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200", got.httpStatus)
	}
	if got.statusCode != uint32(codes.Unimplemented) {
		t.Errorf("status = %d, want UNIMPLEMENTED", got.statusCode)
	}
}

func TestFetchUnaryJSONUnknownMethod(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	got := fetchUnaryJSON(t, s.baseURL, svc+"/NoSuchMethod", webnext.CTJSON, []byte(`{}`), callOptions{})

	if got.statusCode != uint32(codes.Unimplemented) {
		t.Errorf("status = %d, want UNIMPLEMENTED", got.statusCode)
	}
}

// An oversize JSON body answers RESOURCE_EXHAUSTED in the header — not a 413 —
// because the JSON codec always carries status in the header.
func TestFetchUnaryJSONOversizeRequest(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{MaxMessageBytes: 512})

	body := jsonRequest(t, &cpb.UnaryRequest{Payload: []byte(strings.Repeat("z", 4096))})
	got := fetchUnaryJSON(t, s.baseURL, unaryPath, webnext.CTJSON, body, callOptions{})

	if got.httpStatus != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200", got.httpStatus)
	}
	if got.statusCode != uint32(codes.ResourceExhausted) {
		t.Errorf("status = %d, want RESOURCE_EXHAUSTED", got.statusCode)
	}
}

// Plain `application/json` reaches a main gRPC path only with AllowImplicitCodec.
func TestFetchPlainJSONNeedsImplicitCodec(t *testing.T) {
	body := jsonRequest(t, &cpb.UnaryRequest{
		Payload:            []byte("hi"),
		ResponseDefinition: &cpb.ResponseDefinition{Payload: []byte("hi")},
	})

	strict := startServer(t, webnext.ServerConfig{})
	got := fetchUnaryJSON(t, strict.baseURL, unaryPath, "application/json", body, callOptions{})
	if got.httpStatus != http.StatusUnsupportedMediaType {
		t.Errorf("strict server: HTTP status = %d, want 415", got.httpStatus)
	}

	lax := startServer(t, webnext.ServerConfig{AllowImplicitCodec: true})
	got = fetchUnaryJSON(t, lax.baseURL, unaryPath, "application/json", body, callOptions{})
	if got.httpStatus != http.StatusOK || got.statusCode != 0 {
		t.Fatalf("lax server: HTTP %d, gRPC %d %q, want 200/OK", got.httpStatus, got.statusCode, got.statusMsg)
	}
	if p := string(decodeJSONPayload(t, got.body).GetPayload()); p != "hi" {
		t.Errorf("payload = %q, want %q", p, "hi")
	}
}

func TestFetchUnsupportedContentType(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	resp := doFetch(t, s.baseURL, unaryPath, "application/xml", []byte("<x/>"), callOptions{})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("HTTP status = %d, want 415", resp.StatusCode)
	}
}

// --- WebSocket streaming -----------------------------------------------------

func TestWSServerStream(t *testing.T) {
	for _, jsonCodec := range []bool{false, true} {
		t.Run(codecName(jsonCodec), func(t *testing.T) {
			s := startServer(t, webnext.ServerConfig{})
			c, _, err := dialWS(t, s.baseURL, serverStream, jsonCodec)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer c.close()

			req := &cpb.ServerStreamRequest{ResponseDefinition: &cpb.ResponseDefinition{
				StreamMessages: []*cpb.StreamMessage{
					{Payload: []byte("a")}, {Payload: []byte("b")}, {Payload: []byte("c")},
				},
			}}
			c.subscribe(nil, 0, encodeRequest(t, req, jsonCodec))
			c.halfClose()

			got := c.collect(10 * time.Second)
			if n := len(got.messages); n != 3 {
				t.Fatalf("got %d messages, want 3", n)
			}
			for i, want := range []string{"a", "b", "c"} {
				if p := string(payloadOf(t, got.messages[i], jsonCodec)); p != want {
					t.Errorf("message %d = %q, want %q", i, p, want)
				}
			}
			assertTerminalOK(t, got)
			// One WebSocket carries one stream, so it closes once that stream ends.
			if got.closeCode != websocket.CloseNormalClosure {
				t.Errorf("close code = %d, want 1000", got.closeCode)
			}
		})
	}
}

// Messages already delivered must stay delivered when the stream then fails.
func TestWSServerStreamMessagesThenError(t *testing.T) {
	for _, jsonCodec := range []bool{false, true} {
		t.Run(codecName(jsonCodec), func(t *testing.T) {
			s := startServer(t, webnext.ServerConfig{})
			c, _, err := dialWS(t, s.baseURL, serverStream, jsonCodec)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer c.close()

			req := &cpb.ServerStreamRequest{ResponseDefinition: &cpb.ResponseDefinition{
				StatusCode:     uint32(codes.ResourceExhausted),
				StatusMessage:  "quota",
				StreamMessages: []*cpb.StreamMessage{{Payload: []byte("a")}, {Payload: []byte("b")}},
			}}
			c.subscribe(nil, 0, encodeRequest(t, req, jsonCodec))
			c.halfClose()

			got := c.collect(10 * time.Second)
			if n := len(got.messages); n != 2 {
				t.Fatalf("got %d messages, want 2", n)
			}
			trailer := got.terminal.GetTrailer()
			if trailer == nil {
				t.Fatalf("terminal frame = %v, want a Trailer", got.terminal)
			}
			if trailer.GetStatusCode() != uint32(codes.ResourceExhausted) {
				t.Errorf("status = %d, want RESOURCE_EXHAUSTED", trailer.GetStatusCode())
			}
			if !strings.Contains(trailer.GetStatusMessage(), "quota") {
				t.Errorf("status message = %q, want it to contain %q", trailer.GetStatusMessage(), "quota")
			}
		})
	}
}

func TestWSClientStream(t *testing.T) {
	for _, jsonCodec := range []bool{false, true} {
		t.Run(codecName(jsonCodec), func(t *testing.T) {
			s := startServer(t, webnext.ServerConfig{})
			c, _, err := dialWS(t, s.baseURL, clientStream, jsonCodec)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer c.close()

			c.subscribe(nil, 0, encodeRequest(t, &cpb.ClientStreamRequest{
				Payload:            []byte("one"),
				ResponseDefinition: &cpb.ResponseDefinition{Payload: []byte("ok")},
			}, jsonCodec))
			c.message(encodeRequest(t, &cpb.ClientStreamRequest{Payload: []byte("two")}, jsonCodec))
			c.message(encodeRequest(t, &cpb.ClientStreamRequest{Payload: []byte("three")}, jsonCodec))
			c.halfClose()

			got := c.collect(10 * time.Second)
			if n := len(got.messages); n != 1 {
				t.Fatalf("got %d messages, want 1", n)
			}
			var resp cpb.ClientStreamResponse
			decodeMessage(t, got.messages[0], jsonCodec, &resp)
			if resp.GetReceivedCount() != 3 {
				t.Errorf("received_count = %d, want 3", resp.GetReceivedCount())
			}
			if p := string(resp.GetPayload().GetPayload()); p != "ok" {
				t.Errorf("payload = %q, want %q", p, "ok")
			}
			assertTerminalOK(t, got)
		})
	}
}

func TestWSBidiEcho(t *testing.T) {
	for _, jsonCodec := range []bool{false, true} {
		t.Run(codecName(jsonCodec), func(t *testing.T) {
			s := startServer(t, webnext.ServerConfig{})
			c, _, err := dialWS(t, s.baseURL, bidiStreamPath, jsonCodec)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer c.close()

			c.subscribe(nil, 0, encodeRequest(t, &cpb.BidiStreamRequest{
				Payload: []byte("a"), ResponseDefinition: &cpb.ResponseDefinition{},
			}, jsonCodec))
			c.message(encodeRequest(t, &cpb.BidiStreamRequest{Payload: []byte("b")}, jsonCodec))
			c.halfClose()

			got := c.collect(10 * time.Second)
			if n := len(got.messages); n != 2 {
				t.Fatalf("got %d messages, want 2", n)
			}
			for i, want := range []string{"a", "b"} {
				if p := string(payloadOf(t, got.messages[i], jsonCodec)); p != want {
					t.Errorf("echo %d = %q, want %q", i, p, want)
				}
			}
			assertTerminalOK(t, got)
		})
	}
}

// A client Reset mid-stream cancels the RPC. The client already has its status,
// so the server emits no terminal frame and leaves the connection to the client.
func TestWSClientReset(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})
	c, _, err := dialWS(t, s.baseURL, bidiStreamPath, false)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	c.subscribe(nil, 0, encodeRequest(t, &cpb.BidiStreamRequest{Payload: []byte("a")}, false))

	// Wait for the echo, then cancel while the stream is still open.
	_ = c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
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
	c.reset(uint32(codes.Canceled), "cancelled")

	// Deliberately a short wait for *silence*: the server should send nothing.
	got := c.collect(750 * time.Millisecond)
	if got.terminal != nil {
		t.Errorf("server sent a terminal frame after the client reset: %v", got.terminal)
	}
}

// Deadline expiry mid-stream ends the stream with DEADLINE_EXCEEDED, after the
// messages that did make it.
func TestWSServerStreamDeadline(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})
	c, _, err := dialWS(t, s.baseURL, serverStream, false)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	req := &cpb.ServerStreamRequest{ResponseDefinition: &cpb.ResponseDefinition{
		StreamMessages: []*cpb.StreamMessage{
			{Payload: []byte("a")},
			{Payload: []byte("b"), DelayMs: 5000},
		},
	}}
	c.subscribe(nil, 250, encodeRequest(t, req, false))
	c.halfClose()

	start := time.Now()
	got := c.collect(10 * time.Second)
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("took %v — the deadline should have fired at ~250ms", elapsed)
	}
	trailer := got.terminal.GetTrailer()
	if trailer == nil || trailer.GetStatusCode() != uint32(codes.DeadlineExceeded) {
		t.Fatalf("terminal frame = %v, want Trailer{DEADLINE_EXCEEDED}", got.terminal)
	}
	if len(got.messages) != 1 {
		t.Errorf("got %d messages, want the 1 that beat the deadline", len(got.messages))
	}
}

// Metadata — including `-bin`, whose raw bytes ride in the frame rather than
// base64 — must reach the service unchanged over the WebSocket surface too.
func TestWSMetadataRoundtrip(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})
	c, _, err := dialWS(t, s.baseURL, serverStream, false)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	md := []*pb.Metadatum{
		asciiMeta("x-ascii", "plain"),
		binMeta("x-blob-bin", []byte{1, 2, 3, 4}),
	}
	req := &cpb.ServerStreamRequest{ResponseDefinition: &cpb.ResponseDefinition{
		StreamMessages: []*cpb.StreamMessage{{Payload: []byte("a")}},
		Headers:        []*cpb.Metadatum{{Key: "x-greeting", Value: &cpb.Metadatum_AsciiValue{AsciiValue: "hi"}}},
	}}
	c.subscribe(md, 0, encodeRequest(t, req, false))
	c.halfClose()

	got := c.collect(10 * time.Second)
	if len(got.messages) == 0 {
		t.Fatalf("no messages; terminal = %v", got.terminal)
	}
	echoed := decodePayload(t, got.messages[0]).GetRequestInfo().GetRequestHeaders()

	var sawASCII, sawBin bool
	for _, m := range echoed {
		switch v := m.GetValue().(type) {
		case *cpb.Metadatum_AsciiValue:
			sawASCII = sawASCII || (m.GetKey() == "x-ascii" && v.AsciiValue == "plain")
		case *cpb.Metadatum_BinValue:
			sawBin = sawBin || (m.GetKey() == "x-blob-bin" && string(v.BinValue) == "\x01\x02\x03\x04")
		}
	}
	if !sawASCII {
		t.Errorf("service did not observe x-ascii: %v", echoed)
	}
	if !sawBin {
		t.Errorf("service did not observe x-blob-bin as raw bytes: %v", echoed)
	}
	// Initial response metadata arrives as the Header frame.
	if v, ok := findASCII(got.header, "x-greeting"); !ok || v != "hi" {
		t.Errorf("Header frame x-greeting = %q (present %v), want %q", v, ok, "hi")
	}
	// Binary metadata has no JSON form, but it must survive on a binary
	// connection, which is the only place the distinction is observable.
	if _, ok := findBin(got.header, "x-nonexistent-bin"); ok {
		t.Errorf("unexpected binary metadata in the Header frame")
	}
}

// An oversize inbound message ends the stream with an in-band Reset, not a close
// code: the size limit is a per-stream error on an open connection.
func TestWSOversizeMessageResets(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{MaxMessageBytes: 1024})
	c, _, err := dialWS(t, s.baseURL, bidiStreamPath, false)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	c.subscribe(nil, 0, encodeRequest(t, &cpb.BidiStreamRequest{Payload: []byte("a")}, false))
	c.message(encodeRequest(t, &cpb.BidiStreamRequest{Payload: make([]byte, 4096)}, false))

	got := c.collect(10 * time.Second)
	reset := got.terminal.GetReset_()
	if reset == nil {
		t.Fatalf("terminal frame = %v, want a Reset", got.terminal)
	}
	if reset.GetStatusCode() != uint32(codes.ResourceExhausted) {
		t.Errorf("reset status = %d, want RESOURCE_EXHAUSTED", reset.GetStatusCode())
	}
	// The terminal frame ends the one stream this socket carries, so the socket
	// closes normally right behind it.
	if got.closeCode != websocket.CloseNormalClosure {
		t.Errorf("close code = %d, want 1000", got.closeCode)
	}
}

// An oversize inline payload on the opening Subscribe is caught the same way.
func TestWSOversizeInitialPayloadResets(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{MaxMessageBytes: 1024})
	c, _, err := dialWS(t, s.baseURL, unaryPath, false)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	c.subscribe(nil, 0, make([]byte, 4096))

	got := c.collect(10 * time.Second)
	if reset := got.terminal.GetReset_(); reset == nil || reset.GetStatusCode() != uint32(codes.ResourceExhausted) {
		t.Fatalf("terminal frame = %v, want Reset{RESOURCE_EXHAUSTED}", got.terminal)
	}
	if got.closeCode != websocket.CloseNormalClosure {
		t.Errorf("close code = %d, want 1000", got.closeCode)
	}
}

// `+json` against a server with no transcoder is a pre-RPC capability gap, so it
// is a Reset — not a terminal Trailer.
func TestWSJSONWithoutTranscoder(t *testing.T) {
	s := startServerWithoutTranscoder(t, webnext.ServerConfig{})
	c, _, err := dialWS(t, s.baseURL, serverStream, true)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	c.subscribe(nil, 0, []byte(`{}`))
	c.halfClose()

	got := c.collect(10 * time.Second)
	// On a JSON connection a Reset and a Trailer render identically, so this
	// asserts the status the client can actually observe.
	if got.terminal.GetTrailer().GetStatusCode() != uint32(codes.Unimplemented) {
		t.Fatalf("terminal frame = %v, want status UNIMPLEMENTED", got.terminal)
	}
}

// A WebSocket with no codec subprotocol is a connection-level rejection: the
// handshake completes, then closes with the private code 4000 + UNIMPLEMENTED.
func TestWSRejectsMissingCodecSubprotocol(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})
	c, _, err := dialWS(t, s.baseURL, serverStream, false, webnext.WSSubprotocol)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	got := c.collect(10 * time.Second)
	if want := webnext.WSCloseCode(codes.Unimplemented); got.closeCode != want {
		t.Errorf("close code = %d, want %d", got.closeCode, want)
	}
	if got.terminal != nil {
		t.Errorf("a handshake rejection must not send frames, got %v", got.terminal)
	}
}

// With AllowImplicitCodec the same handshake is accepted and the codec is
// inferred from the first frame's type.
func TestWSImplicitCodecInfersFromFirstFrame(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{AllowImplicitCodec: true})
	c, _, err := dialWS(t, s.baseURL, serverStream, false, webnext.WSSubprotocol)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	req := &cpb.ServerStreamRequest{ResponseDefinition: &cpb.ResponseDefinition{
		StreamMessages: []*cpb.StreamMessage{{Payload: []byte("a")}},
	}}
	c.subscribe(nil, 0, encodeRequest(t, req, false))
	c.halfClose()

	got := c.collect(10 * time.Second)
	if len(got.messages) != 1 {
		t.Fatalf("got %d messages, want 1 (terminal %v)", len(got.messages), got.terminal)
	}
	assertTerminalOK(t, got)
}

// The negotiated subprotocol is echoed back so the client can confirm the codec.
func TestWSHandshakeEchoesSubprotocol(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{})

	for _, tc := range []struct {
		jsonCodec bool
		want      string
	}{
		{false, webnext.WSSubprotocolProto},
		{true, webnext.WSSubprotocolJSON},
	} {
		c, resp, err := dialWS(t, s.baseURL, serverStream, tc.jsonCodec)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		if got := resp.Header.Get("Sec-Websocket-Protocol"); got != tc.want {
			t.Errorf("negotiated subprotocol = %q, want %q", got, tc.want)
		}
		c.close()
	}
}

// Keepalive drives WebSocket ping control frames from the server; the client's
// automatic pong is the return traffic that keeps a quiet stream alive.
func TestWSKeepalivePings(t *testing.T) {
	s := startServer(t, webnext.ServerConfig{WSKeepalive: 60 * time.Millisecond})
	c, _, err := dialWS(t, s.baseURL, bidiStreamPath, false)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	pings := make(chan struct{}, 8)
	c.conn.SetPingHandler(func(data string) error {
		select {
		case pings <- struct{}{}:
		default:
		}
		return c.conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})

	// Open a stream and leave it idle; reading is what surfaces control frames.
	c.subscribe(nil, 0, encodeRequest(t, &cpb.BidiStreamRequest{Payload: []byte("a")}, false))
	go func() {
		_ = c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		for {
			if _, _, err := c.conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	select {
	case <-pings:
	case <-time.After(3 * time.Second):
		t.Fatal("no keepalive ping within 3s")
	}
}

// --- helpers ----------------------------------------------------------------

func codecName(jsonCodec bool) string {
	if jsonCodec {
		return "json"
	}
	return "proto"
}

// encodeRequest renders a request message in the connection's codec.
func encodeRequest(t *testing.T, m proto.Message, jsonCodec bool) []byte {
	t.Helper()
	if jsonCodec {
		return jsonRequest(t, m)
	}
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func decodeMessage(t *testing.T, data []byte, jsonCodec bool, into proto.Message) {
	t.Helper()
	var err error
	if jsonCodec {
		err = protojson.Unmarshal(data, into)
	} else {
		err = proto.Unmarshal(data, into)
	}
	if err != nil {
		t.Fatalf("decode message %q: %v", data, err)
	}
}

func payloadOf(t *testing.T, data []byte, jsonCodec bool) []byte {
	t.Helper()
	var p cpb.ConformancePayload
	decodeMessage(t, data, jsonCodec, &p)
	return p.GetPayload()
}

func assertTerminalOK(t *testing.T, got wsResult) {
	t.Helper()
	trailer := got.terminal.GetTrailer()
	if trailer == nil {
		t.Fatalf("terminal frame = %v, want a Trailer", got.terminal)
	}
	if trailer.GetStatusCode() != 0 {
		t.Fatalf("status = %d %q, want OK", trailer.GetStatusCode(), trailer.GetStatusMessage())
	}
}

// keep the grpc status/metadata imports honest for readers of this file
var (
	_ = status.Code
	_ = metadata.MD{}
)

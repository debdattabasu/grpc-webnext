// REST transcoding end to end: `google.api.http` annotations on both surfaces.
//
// These are deliberate ports of the Rust crate's REST tests
// (`rust/crates/grpc-webnext/tests/inproc_json.rs`), driving the SAME Echo
// service through the SAME annotated URLs. Two implementations of one wire
// contract only stay one contract if something asserts they answer identically;
// until REST cases exist in `conformance/`, this pairing is that something, and
// each test below names its Rust counterpart.

package webnext_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grpc-webnext/grpc-webnext/go/internal/testecho"
	"github.com/grpc-webnext/grpc-webnext/go/webnext"
)

// startEcho serves echo.v1.Echo — the service carrying the shared REST
// annotations — behind grpc-webnext.
func startEcho(t *testing.T, allowImplicitCodec bool) *testServer {
	t.Helper()
	fds, err := testecho.DescriptorSet()
	if err != nil {
		t.Fatalf("descriptor set: %v", err)
	}
	tc, err := webnext.NewTranscoder(fds)
	if err != nil {
		t.Fatalf("transcoder: %v", err)
	}
	if !tc.HasHTTPRules() {
		t.Fatal("no google.api.http bindings compiled from echo.proto — " +
			"the annotations did not survive the descriptor round trip")
	}
	cfg := webnext.ServerConfig{Transcoder: tc, AllowImplicitCodec: allowImplicitCodec}
	return serveBackend(t, cfg, func(s grpc.ServiceRegistrar) { testecho.Register(s) })
}

// restCall issues one plain-HTTP REST request and returns the status line bits a
// grpc-webnext JSON response carries.
type restResponse struct {
	httpStatus  int
	contentType string
	grpcStatus  string
	body        []byte
}

func restCall(t *testing.T, method, url, contentType, body string) restResponse {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return restResponse{
		httpStatus:  resp.StatusCode,
		contentType: resp.Header.Get("Content-Type"),
		grpcStatus:  resp.Header.Get("Grpc-Status"),
		body:        out,
	}
}

// field pulls one string field out of a JSON response body.
func field(t *testing.T, body []byte, name string) string {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("decode json %q: %v", body, err)
	}
	s, _ := obj[name].(string)
	return s
}

// --- Fetch ------------------------------------------------------------------

// GET /v1/echo/{message} -> Unary, binding `message` from the path.
// Rust: transcode_get_binds_path_param.
func TestRESTGetBindsPathParam(t *testing.T) {
	s := startEcho(t, false)
	resp := restCall(t, http.MethodGet, s.baseURL+"/v1/echo/hello%20world", "", "")

	if resp.httpStatus != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", resp.httpStatus, resp.body)
	}
	if resp.contentType != "application/json" {
		t.Errorf("content-type = %q, want application/json", resp.contentType)
	}
	if resp.grpcStatus != "0" {
		t.Errorf("grpc-status = %q, want 0", resp.grpcStatus)
	}
	// The path segment is percent-decoded before it binds.
	if got := field(t, resp.body, "message"); got != "hello world" {
		t.Errorf("message = %q, want %q", got, "hello world")
	}
}

// POST /v1/echo with `body: "*"` -> the whole JSON body is the request message.
// Rust: transcode_post_body_wildcard.
func TestRESTPostBodyWildcard(t *testing.T) {
	s := startEcho(t, false)
	resp := restCall(t, http.MethodPost, s.baseURL+"/v1/echo", "application/json", `{"message":"posted"}`)

	if resp.httpStatus != http.StatusOK || resp.grpcStatus != "0" {
		t.Fatalf("status = %d / grpc-status %q, want 200/0 (body %q)", resp.httpStatus, resp.grpcStatus, resp.body)
	}
	if got := field(t, resp.body, "message"); got != "posted" {
		t.Errorf("message = %q, want %q", got, "posted")
	}
}

// GET /v1/sleep?millis=0 -> Sleep, coercing a uint32 field from a query param.
// Rust: transcode_get_binds_query_number.
func TestRESTGetBindsQueryNumber(t *testing.T) {
	s := startEcho(t, false)
	resp := restCall(t, http.MethodGet, s.baseURL+"/v1/sleep?millis=0", "", "")

	if resp.httpStatus != http.StatusOK || resp.grpcStatus != "0" {
		t.Fatalf("status = %d / grpc-status %q, want 200/0 (body %q)", resp.httpStatus, resp.grpcStatus, resp.body)
	}
	if got := field(t, resp.body, "message"); got != "awake" {
		t.Errorf("message = %q, want %q", got, "awake")
	}
}

// With the implicit-codec flag, a plain-JSON URL matching no REST binding falls
// back to a direct /pkg.Service/Method call.
// Rust: transcode_unmatched_path_falls_back_to_direct_when_implicit.
func TestRESTUnmatchedPathFallsBackToDirectWhenImplicit(t *testing.T) {
	s := startEcho(t, true)
	// FlakyUnary is the one un-annotated unary method, so it can only be reached
	// by the main path.
	resp := restCall(t, http.MethodPost, s.baseURL+"/echo.v1.Echo/FlakyUnary",
		"application/json", `{"message":"direct"}`)

	if resp.grpcStatus != "0" {
		t.Fatalf("grpc-status = %q, want 0 (body %q)", resp.grpcStatus, resp.body)
	}
	if got := field(t, resp.body, "message"); got != "direct" {
		t.Errorf("message = %q, want %q", got, "direct")
	}
}

// Without the flag, that same main-path plain-JSON call is a 415 — the REST
// fallback must not become a back door onto main endpoints.
func TestRESTUnmatchedPathIsRejectedWithoutImplicitCodec(t *testing.T) {
	s := startEcho(t, false)
	resp := restCall(t, http.MethodPost, s.baseURL+"/echo.v1.Echo/FlakyUnary",
		"application/json", `{"message":"direct"}`)

	if resp.httpStatus != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415 (body %q)", resp.httpStatus, resp.body)
	}
}

// Binary on a REST-annotated URL is the wrong surface -> explicit 415.
// Rust: fetch_proto_on_rest_url_is_rejected.
func TestRESTProtoOnRESTURLIsRejected(t *testing.T) {
	s := startEcho(t, false)
	resp := restCall(t, http.MethodPost, s.baseURL+"/v1/echo", webnext.CTProto, "")

	if resp.httpStatus != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415 (body %q)", resp.httpStatus, resp.body)
	}
}

// grpc-webnext+json is JSON, so it works on a REST URL too — and answers as the
// REST surface does, with a plain `application/json` content-type.
// Rust: fetch_grpc_webnext_json_transcodes_on_rest_url.
func TestRESTGRPCWebnextJSONTranscodesOnRESTURL(t *testing.T) {
	s := startEcho(t, false)
	resp := restCall(t, http.MethodPost, s.baseURL+"/v1/echo", webnext.CTJSON, `{"message":"sdkjson"}`)

	if resp.httpStatus != http.StatusOK || resp.grpcStatus != "0" {
		t.Fatalf("status = %d / grpc-status %q, want 200/0 (body %q)", resp.httpStatus, resp.grpcStatus, resp.body)
	}
	if got := field(t, resp.body, "message"); got != "sdkjson" {
		t.Errorf("message = %q, want %q", got, "sdkjson")
	}
}

// An annotation only *adds* the REST alias: the RPC's main path stays reachable
// by the SDK content-type.
// Rust: implicit_codec_flag_allows_plain_json_on_annotated_rpc_main_path (the
// `+json` half, which needs no flag).
func TestRESTAnnotatedMethodMainPathStillWorks(t *testing.T) {
	s := startEcho(t, false)
	resp := restCall(t, http.MethodPost, s.baseURL+"/echo.v1.Echo/Unary",
		webnext.CTJSON, `{"message":"mainpath"}`)

	if resp.grpcStatus != "0" {
		t.Fatalf("grpc-status = %q, want 0 (body %q)", resp.grpcStatus, resp.body)
	}
	if got := field(t, resp.body, "message"); got != "mainpath" {
		t.Errorf("message = %q, want %q", got, "mainpath")
	}
}

// A body that does not parse as the request message is INVALID_ARGUMENT, not a
// 500 and not a silent default message.
func TestRESTBadBodyIsInvalidArgument(t *testing.T) {
	s := startEcho(t, false)
	resp := restCall(t, http.MethodPost, s.baseURL+"/v1/echo", "application/json", `{"message":42}`)

	if resp.httpStatus != http.StatusOK {
		t.Fatalf("status = %d, want 200 (a gRPC status, not an HTTP error)", resp.httpStatus)
	}
	if want := itoa(codes.InvalidArgument); resp.grpcStatus != want {
		t.Errorf("grpc-status = %q, want %q (body %q)", resp.grpcStatus, want, resp.body)
	}
}

// A REST route the verb does not match is not a REST call at all: it falls
// through to the main-path rules, which reject plain JSON without the flag.
func TestRESTWrongVerbFallsThrough(t *testing.T) {
	s := startEcho(t, false)
	// `/v1/echo` is bound for POST only; DELETE matches no binding.
	resp := restCall(t, http.MethodDelete, s.baseURL+"/v1/echo", "application/json", "")

	if resp.httpStatus != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415 (body %q)", resp.httpStatus, resp.body)
	}
}

func itoa(c codes.Code) string {
	const digits = "0123456789"
	if c < 10 {
		return string(digits[c])
	}
	return string(digits[c/10]) + string(digits[c%10])
}

// --- WebSocket ---------------------------------------------------------------

// A streaming method reached via its annotation URL: GET /v1/repeat/{message}?count=N.
// Annotation routes accept a blank subprotocol, lock to text, and build the one
// request entirely from the URL (path `message`, query `count`).
// Rust: ws_annotation_server_stream.
func TestRESTWSAnnotationServerStream(t *testing.T) {
	s := startEcho(t, false)
	got := collectAnnotationStream(t, s, "/v1/repeat/hi?count=3", nil, func(c *wsClient) {
		// A bare `{}` open frame starts the stream; the server builds the one
		// request from the URL and half-closes on the client's behalf.
		c.subscribe(nil, 0, nil)
	})
	assertMessages(t, got, []string{"hi", "hi", "hi"})
}

// A bidi method reached via `post: "/v1/chat" body:"*"`: each text frame's body
// is a request message.
// Rust: ws_annotation_bidi_with_body.
func TestRESTWSAnnotationBidiWithBody(t *testing.T) {
	s := startEcho(t, false)
	got := collectAnnotationStream(t, s, "/v1/chat", []string{"application/json"}, func(c *wsClient) {
		// The first frame opens the stream and carries the first message inline;
		// `body:"*"` makes each frame's JSON the request message.
		c.subscribe(nil, 0, []byte(`{"message":"a"}`))
		c.message([]byte(`{"message":"b"}`))
		c.halfClose()
	})
	assertMessages(t, got, []string{"a", "b"})
}

// grpc-webnext+json is JSON, so it is accepted on a REST route like a blank or
// application/json subprotocol.
// Rust: ws_annotation_accepts_grpc_webnext_json.
func TestRESTWSAnnotationAcceptsGRPCWebnextJSON(t *testing.T) {
	s := startEcho(t, false)
	got := collectAnnotationStream(t, s, "/v1/repeat/hi?count=2",
		[]string{webnext.WSSubprotocolJSON}, func(c *wsClient) {
			c.subscribe(nil, 0, nil)
		})
	assertMessages(t, got, []string{"hi", "hi"})
}

// Annotation routes are single-stream JSON: a binary subprotocol is the wrong
// surface -> close 4009 (FAILED_PRECONDITION).
// Rust: ws_annotation_rejects_proto_subprotocol.
func TestRESTWSAnnotationRejectsProtoSubprotocol(t *testing.T) {
	s := startEcho(t, false)
	c, _, err := dialWS(t, s.baseURL, "/v1/repeat/hi?count=1", false, webnext.WSSubprotocolProto)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.close()

	got := c.collect(5 * time.Second)
	if want := 4000 + int(codes.FailedPrecondition); got.closeCode != want {
		t.Errorf("close code = %d, want %d", got.closeCode, want)
	}
}

// collectAnnotationStream dials an annotation route, runs `drive`, and reads the
// stream to its terminal frame.
func collectAnnotationStream(t *testing.T, s *testServer, path string, protocols []string, drive func(*wsClient)) wsResult {
	t.Helper()
	// A blank offer is spelled as an explicit empty list, which dialWS
	// distinguishes from "use the default codec subprotocol".
	if protocols == nil {
		protocols = []string{}
	}
	c, _, err := dialWS(t, s.baseURL, path, true, protocols...)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	defer c.close()
	drive(c)
	return c.collect(10 * time.Second)
}

// assertMessages checks the payloads a JSON annotation stream delivered, and
// that it ended with an OK status rather than a Reset.
func assertMessages(t *testing.T, got wsResult, want []string) {
	t.Helper()
	trailer := got.terminal.GetTrailer()
	if trailer == nil {
		t.Fatalf("stream did not end with a Trailer: %v", got.terminal)
	}
	if trailer.GetStatusCode() != 0 {
		t.Fatalf("status = %d %q, want OK", trailer.GetStatusCode(), trailer.GetStatusMessage())
	}
	if len(got.messages) != len(want) {
		t.Fatalf("got %d messages, want %d (%q)", len(got.messages), len(want), got.messages)
	}
	for i, raw := range got.messages {
		if m := field(t, raw, "message"); m != want[i] {
			t.Errorf("message[%d] = %q, want %q", i, m, want[i])
		}
	}
}

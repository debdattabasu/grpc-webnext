// Unit tests for the wire primitives, in-package so they can reach the
// unexported codecs the surfaces are built from.

package webnext

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/grpc-webnext/grpc-webnext/go/webnext/pb"
	"google.golang.org/protobuf/proto"
)

// --- Fetch + gRPC framing ----------------------------------------------------

// Dropping the gRPC compression-flag byte must yield the Fetch message block
// verbatim. That identity is what lets the unary response stream through
// without a copy, so it is worth pinning directly.
func TestGRPCFrameTailIsTheFetchMessageBlock(t *testing.T) {
	message := []byte("hello world")

	frame := grpcFrame(message)
	block := encodeResponseBody(message, &pb.Trailer{})

	if !bytes.Equal(frame[1:], block[:len(frame)-1]) {
		t.Errorf("gRPC frame tail %x != Fetch message block %x", frame[1:], block[:len(frame)-1])
	}
}

func TestEncodeResponseBodyRoundTrip(t *testing.T) {
	trailer := &pb.Trailer{
		StatusCode:    9,
		StatusMessage: "nope",
		Trailers:      []*pb.Metadatum{{Key: "x-d", Value: &pb.Metadatum_AsciiValue{AsciiValue: "v"}}},
	}
	body := encodeResponseBody([]byte("payload"), trailer)

	message, rest := readBlock(t, body)
	if string(message) != "payload" {
		t.Errorf("message = %q, want %q", message, "payload")
	}
	trailerBytes, rest := readBlock(t, rest)
	if len(rest) != 0 {
		t.Errorf("%d trailing bytes after the trailer block, want 0", len(rest))
	}
	var got pb.Trailer
	if err := proto.Unmarshal(trailerBytes, &got); err != nil {
		t.Fatalf("decode Trailer: %v", err)
	}
	if got.GetStatusCode() != 9 || got.GetStatusMessage() != "nope" {
		t.Errorf("trailer = %v, want {9 nope}", &got)
	}
}

// An empty message still needs its (zero-length) block ahead of the trailer, so
// a client can always read exactly two blocks.
func TestEmptyMessageBlock(t *testing.T) {
	body := encodeResponseBody(nil, &pb.Trailer{StatusCode: 12})

	if !bytes.HasPrefix(body, emptyMessageBlock) {
		t.Fatalf("body %x does not start with an empty message block", body)
	}
	message, rest := readBlock(t, body)
	if len(message) != 0 {
		t.Errorf("message = %q, want empty", message)
	}
	if _, rest = readBlock(t, rest); len(rest) != 0 {
		t.Errorf("%d trailing bytes, want 0", len(rest))
	}
}

func readBlock(t *testing.T, b []byte) (block, rest []byte) {
	t.Helper()
	if len(b) < lenPrefix {
		t.Fatalf("truncated block: %d bytes", len(b))
	}
	n := int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	if len(b) < lenPrefix+n {
		t.Fatalf("block declares %d bytes, only %d available", n, len(b)-lenPrefix)
	}
	return b[lenPrefix : lenPrefix+n], b[lenPrefix+n:]
}

// The deframer must reassemble messages across arbitrary chunk boundaries — a
// stream never delivers whole frames on demand.
func TestDeframerAcrossChunkBoundaries(t *testing.T) {
	stream := append(grpcFrame([]byte("one")), grpcFrame([]byte("two"))...)
	stream = append(stream, grpcFrame(nil)...)

	for _, chunk := range []int{1, 2, 3, 5, 7, 64} {
		var d deframer
		var got []string
		for i := 0; i < len(stream); i += chunk {
			d.push(stream[i:min(i+chunk, len(stream))])
			for {
				m, ok := d.next()
				if !ok {
					break
				}
				got = append(got, string(m))
			}
		}
		want := []string{"one", "two", ""}
		if len(got) != len(want) {
			t.Fatalf("chunk %d: got %q, want %q", chunk, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("chunk %d: message %d = %q, want %q", chunk, i, got[i], want[i])
			}
		}
	}
}

// A partial frame must yield nothing rather than a truncated message.
func TestDeframerHoldsIncompleteFrames(t *testing.T) {
	full := grpcFrame([]byte("abcdef"))
	for _, n := range []int{0, 1, 4, 5, len(full) - 1} {
		var d deframer
		d.push(full[:n])
		if _, ok := d.next(); ok {
			t.Errorf("%d of %d bytes produced a message", n, len(full))
		}
	}
}

// --- metadata ----------------------------------------------------------------

// `-bin` metadata is raw bytes in a frame and base64 on the HTTP wire, so the
// two conversions must be exact inverses — including for bytes that could never
// appear literally in a header.
func TestBinaryMetadataRoundTrip(t *testing.T) {
	raw := []byte{0x00, 0x01, 0x02, 0xFF}
	items := []*pb.Metadatum{
		{Key: "x-ascii", Value: &pb.Metadatum_AsciiValue{AsciiValue: "plain"}},
		{Key: "x-blob-bin", Value: &pb.Metadatum_BinValue{BinValue: raw}},
	}

	headers := metadataListToHeaders(items)
	if got := headers.Get("x-blob-bin"); got != "AAEC/w" {
		t.Errorf("wire value = %q, want unpadded base64 %q", got, "AAEC/w")
	}

	back := headersToMetadataList(headers)
	ascii, bin := findMeta(back, "x-ascii"), findMeta(back, "x-blob-bin")
	if v, ok := ascii.GetValue().(*pb.Metadatum_AsciiValue); !ok || v.AsciiValue != "plain" {
		t.Errorf("x-ascii = %v, want ascii %q", ascii, "plain")
	}
	if v, ok := bin.GetValue().(*pb.Metadatum_BinValue); !ok || !bytes.Equal(v.BinValue, raw) {
		t.Errorf("x-blob-bin = %v, want raw %x", bin, raw)
	}
}

// Padded base64 must decode too: gRPC permits either form on the wire.
func TestBinaryMetadataAcceptsPaddedBase64(t *testing.T) {
	h := http.Header{}
	h.Set("x-blob-bin", "AQIDBA==")

	got := findMeta(headersToMetadataList(h), "x-blob-bin")
	v, ok := got.GetValue().(*pb.Metadatum_BinValue)
	if !ok || !bytes.Equal(v.BinValue, []byte{1, 2, 3, 4}) {
		t.Errorf("decoded %v, want raw 01020304", got)
	}
}

// Framing headers must not leak into metadata in either direction. "trailer" is
// on the list because Go surfaces response trailers through the header map.
func TestDenylistedHeaders(t *testing.T) {
	for _, key := range []string{"content-type", "te", "trailer", "grpc-status", "grpc-timeout", "Content-Type"} {
		if !IsDeniedHeader(key) {
			t.Errorf("IsDeniedHeader(%q) = false, want true", key)
		}
	}
	for _, key := range []string{"x-custom", "authorization", "x-blob-bin"} {
		if IsDeniedHeader(key) {
			t.Errorf("IsDeniedHeader(%q) = true, want false", key)
		}
	}

	src := http.Header{}
	src.Set("content-type", "application/grpc")
	src.Set("trailer", "Grpc-Status")
	src.Set("x-custom", "v")
	dst := http.Header{}
	copyMetadataHeaders(dst, src)
	if dst.Get("content-type") != "" || dst.Get("trailer") != "" {
		t.Errorf("framing headers leaked into metadata: %v", dst)
	}
	if dst.Get("x-custom") != "v" {
		t.Errorf("user metadata dropped: %v", dst)
	}
}

// A frame must not be able to inject headers into the dispatched call.
func TestMetadataListRejectsIllegalEntries(t *testing.T) {
	items := []*pb.Metadatum{
		{Key: "x-bad\r\nx-injected", Value: &pb.Metadatum_AsciiValue{AsciiValue: "v"}},
		{Key: "x-ok", Value: &pb.Metadatum_AsciiValue{AsciiValue: "line1\r\nx-injected: yes"}},
		{Key: "content-type", Value: &pb.Metadatum_AsciiValue{AsciiValue: "text/plain"}},
	}

	h := metadataListToHeaders(items)
	if len(h) != 0 {
		t.Errorf("headers = %v, want all entries rejected", h)
	}
}

func findMeta(items []*pb.Metadatum, key string) *pb.Metadatum {
	for _, m := range items {
		if m.GetKey() == key {
			return m
		}
	}
	return nil
}

// --- grpc-timeout ------------------------------------------------------------

func TestParseGRPCTimeout(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want time.Duration
		ok   bool
	}{
		{"100m", 100 * time.Millisecond, true},
		{"5S", 5 * time.Second, true},
		{"2M", 2 * time.Minute, true},
		{"1H", time.Hour, true},
		{"250u", 250 * time.Microsecond, true},
		{"7n", 7 * time.Nanosecond, true},
		{"", 0, false},
		{"m", 0, false},
		{"abcm", 0, false},
		{"100x", 0, false},
		{"-5S", 0, false},
		// The gRPC spec caps the value at 8 digits; longer is malformed.
		{"999999999S", 0, false},
		// The largest legal hour value still overflows an int64 of nanoseconds,
		// so it clamps rather than wrapping into a negative (already-expired)
		// deadline.
		{"99999999H", maxTimeout, true},
	} {
		h := http.Header{}
		if tc.raw != "" {
			h.Set("grpc-timeout", tc.raw)
		}
		got, ok := parseGRPCTimeout(h)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("parseGRPCTimeout(%q) = %v, %v; want %v, %v", tc.raw, got, ok, tc.want, tc.ok)
		}
	}
}

// The forwarded deadline carries grace so the service's own enforcement is a
// backstop rather than a timer racing the edge's.
func TestFormatGRPCTimeout(t *testing.T) {
	if got := formatGRPCTimeout(100*time.Millisecond + deadlineGrace); got != "600m" {
		t.Errorf("formatGRPCTimeout = %q, want %q", got, "600m")
	}
	// A sub-millisecond deadline must not round down to "0m", which would read
	// as an already-expired call.
	if got := formatGRPCTimeout(100 * time.Microsecond); got != "1m" {
		t.Errorf("formatGRPCTimeout(100us) = %q, want %q", got, "1m")
	}
}

// --- status encoding ---------------------------------------------------------

func TestPercentCoding(t *testing.T) {
	for _, s := range []string{
		"", "plain message", "a-b_c.d/e:f", `bad "input"; 100%`, "unicode ☃ here", "\n\t",
	} {
		if got := percentDecode(percentEncode(s)); got != s {
			t.Errorf("round trip of %q gave %q", s, got)
		}
	}
	if got := percentEncode(`a"b`); got != "a%22b" {
		t.Errorf("percentEncode(`a\"b`) = %q, want %q", got, "a%22b")
	}
	// A malformed escape is left alone rather than mangled.
	if got := percentDecode("100%"); got != "100%" {
		t.Errorf("percentDecode(%q) = %q, want it unchanged", "100%", got)
	}
}

// WebSocket caps a close reason at 123 bytes, and the cut must not split a rune.
func TestTruncateUTF8(t *testing.T) {
	if got := truncateUTF8("hello", maxCloseReason); got != "hello" {
		t.Errorf("short string was modified: %q", got)
	}
	long := ""
	for len(long) < 200 {
		long += "☃" // 3 bytes each, so the 123-byte cut lands mid-rune
	}
	got := truncateUTF8(long, maxCloseReason)
	if len(got) > maxCloseReason {
		t.Errorf("truncated to %d bytes, want <= %d", len(got), maxCloseReason)
	}
	if !json.Valid([]byte(`"` + got + `"`)) {
		t.Errorf("truncation split a rune: %q", got)
	}
}

func TestWSCloseCode(t *testing.T) {
	// UNIMPLEMENTED (12) is the handshake rejection browsers actually see.
	if got := WSCloseCode(12); got != 4012 {
		t.Errorf("WSCloseCode(12) = %d, want 4012", got)
	}
}

func TestReadStatusFallsBackToHeaders(t *testing.T) {
	trailers, headers := http.Header{}, http.Header{}
	headers.Set("grpc-status", "5")
	headers.Set("grpc-message", "not%20found")

	// A trailers-only response carries its status in the headers block.
	if code, msg := readStatus(trailers, headers); code != 5 || msg != "not found" {
		t.Errorf("readStatus = %d %q, want 5 \"not found\"", code, msg)
	}
	// Trailers win when both are present.
	trailers.Set("grpc-status", "7")
	if code, _ := readStatus(trailers, headers); code != 7 {
		t.Errorf("readStatus code = %d, want the trailer's 7", code)
	}
}

// --- JSON frames -------------------------------------------------------------

// The kind of a post-open JSON frame is chosen by which field is present, in
// priority order status -> halfClose -> message -> (none). The last two arms are
// the sharp edges spec/PROTOCOL.md calls out explicitly.
func TestJSONFrameKindPriority(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		check      func(*pb.Frame) bool
	}{
		{"status wins", `{"status":{"code":1},"halfClose":true,"message":{}}`,
			func(f *pb.Frame) bool { return f.GetReset_().GetStatusCode() == 1 }},
		{"halfClose outranks message", `{"halfClose":true,"message":{"a":1}}`,
			func(f *pb.Frame) bool { return f.GetHalfClose() != nil }},
		{"message", `{"message":{"a":1}}`,
			func(f *pb.Frame) bool { return string(f.GetMessage().GetPayload()) == `{"a":1}` }},
		{"empty object is a half-close", `{}`,
			func(f *pb.Frame) bool { return f.GetHalfClose() != nil }},
		{"unknown field is a half-close", `{"mesage":{"a":1}}`,
			func(f *pb.Frame) bool { return f.GetHalfClose() != nil }},
	} {
		jf, err := decodeJSONFrame([]byte(tc.text))
		if err != nil {
			t.Fatalf("%s: decode %s: %v", tc.name, tc.text, err)
		}
		if got := jsonFrameToProto(jf); !tc.check(got) {
			t.Errorf("%s: %s decoded to %v", tc.name, tc.text, got)
		}
	}
}

// The opening frame takes its method from the URL and folds any inline message
// into initial_payload.
func TestJSONOpenToSubscribe(t *testing.T) {
	jf, err := decodeJSONFrame([]byte(`{"metadata":{"x-a":"1"},"timeoutMillis":5000,"message":{"n":1}}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	sub := jsonOpenToSubscribe(jf, "/pkg.Svc/Method")
	if sub.GetMethod() != "/pkg.Svc/Method" {
		t.Errorf("method = %q, want it from the URL", sub.GetMethod())
	}
	if sub.GetTimeoutMillis() != 5000 {
		t.Errorf("timeout = %d, want 5000", sub.GetTimeoutMillis())
	}
	if string(sub.GetInitialPayload()) != `{"n":1}` {
		t.Errorf("initial payload = %q, want the inline message", sub.GetInitialPayload())
	}
	// There is deliberately no codec flag to check: the WebSocket frame type
	// already settled that (text = JSON), before this Subscribe was built.
	if v, ok := sub.GetHeaders()[0].GetValue().(*pb.Metadatum_AsciiValue); !ok || v.AsciiValue != "1" {
		t.Errorf("headers = %v, want x-a=1", sub.GetHeaders())
	}
}

// Binary metadata has no JSON form and is dropped crossing into the JSON codec.
func TestJSONFrameDropsBinaryMetadata(t *testing.T) {
	header := &pb.Header{Headers: []*pb.Metadatum{
		{Key: "x-ascii", Value: &pb.Metadatum_AsciiValue{AsciiValue: "v"}},
		{Key: "x-blob-bin", Value: &pb.Metadatum_BinValue{BinValue: []byte{1, 2}}},
	}}
	frame := &pb.Frame{Kind: &pb.Frame_Header{Header: header}}

	jf := protoFrameToJSON(frame)
	if jf.Metadata["x-ascii"] != "v" {
		t.Errorf("ascii metadata = %v, want x-ascii=v", jf.Metadata)
	}
	if _, ok := jf.Metadata["x-blob-bin"]; ok {
		t.Errorf("binary metadata survived into a JSON frame: %v", jf.Metadata)
	}
}

// A server Trailer renders as `{status, metadata}` and a Reset as `{status}` —
// indistinguishable on a JSON connection, which is exactly what the spec says.
func TestProtoFrameToJSONShapes(t *testing.T) {
	trailer := protoFrameToJSON(&pb.Frame{Kind: &pb.Frame_Trailer{Trailer: &pb.Trailer{
		StatusCode: 8, StatusMessage: "quota",
	}}})
	if trailer.Status.Code != 8 || trailer.Status.Message != "quota" {
		t.Errorf("trailer = %+v, want {8 quota}", trailer.Status)
	}

	encoded := encodeJSONFrame(trailer)
	if string(encoded) != `{"status":{"code":8,"message":"quota"}}` {
		t.Errorf("encoded = %s", encoded)
	}

	// Client-only kinds have no JSON form.
	if got := protoFrameToJSON(&pb.Frame{Kind: &pb.Frame_HalfClose{HalfClose: &pb.HalfClose{}}}); got != nil {
		t.Errorf("HalfClose rendered as %v, want no JSON form", got)
	}
}

// --- transcoder ---------------------------------------------------------------

// An empty JSON body is the default message, which is what a body-less call
// sends.
func TestTranscoderRejectsUnknownMethod(t *testing.T) {
	tc := &Transcoder{methods: nil}
	if tc.HasMethod("/pkg.Svc/Nope") {
		t.Error("HasMethod returned true for an empty transcoder")
	}
	if _, err := tc.RequestJSONToProto("/pkg.Svc/Nope", []byte(`{}`)); err == nil {
		t.Error("RequestJSONToProto succeeded on an unknown method")
	}
}

func TestNewTranscoderRejectsGarbage(t *testing.T) {
	if _, err := NewTranscoder([]byte("not a descriptor set")); err == nil {
		t.Error("NewTranscoder accepted garbage")
	}
}

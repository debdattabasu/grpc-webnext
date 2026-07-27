// HTTP header <-> gRPC metadata conversion and grpc-timeout parsing, shared by
// the Fetch and WebSocket surfaces.

package webnext

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-webnext/grpc-webnext/go/webnext/pb"
)

// deniedHeaders must not cross between HTTP headers and gRPC metadata: hop-by-hop
// headers, framing, and the fields the gRPC stack owns. Mirrors the Rust crate's
// denylist (`metadata::DENY`) so the same request produces the same metadata in
// either implementation.
//
// "trailer" is on the list because Go, unlike tonic, surfaces a response's
// trailers through the *same* header map (pre-declared via `Trailer:` — see
// dispatch.go); without it, grpc-go's declaration would leak to the client as a
// metadata entry. It is hop-by-hop either way, so denying it is correct on both
// directions and in both languages.
var deniedHeaders = map[string]struct{}{
	"host":                    {},
	"connection":              {},
	"content-length":          {},
	"content-type":            {},
	"keep-alive":              {},
	"proxy-connection":        {},
	"transfer-encoding":       {},
	"te":                      {},
	"trailer":                 {},
	"upgrade":                 {},
	"grpc-timeout":            {},
	"grpc-status":             {},
	"grpc-message":            {},
	"grpc-status-details-bin": {},
	"grpc-encoding":           {},
	"grpc-accept-encoding":    {},
}

// IsDeniedHeader reports whether a header/metadata key is framing rather than
// user metadata. Exported so a service can filter echoed request metadata the
// same way the transport does (the conformance server does exactly that).
func IsDeniedHeader(name string) bool {
	_, ok := deniedHeaders[strings.ToLower(name)]
	return ok
}

// isBinKey reports whether a metadata key carries binary values (gRPC's `-bin`
// suffix convention). Binary values are base64 on the HTTP wire and raw bytes in
// a protobuf `Metadatum`.
func isBinKey(key string) bool { return strings.HasSuffix(strings.ToLower(key), "-bin") }

// copyMetadataHeaders copies user metadata from src into dst, dropping the
// denylist. Header keys are case-insensitive, so canonical-vs-lowercase does not
// matter on the wire; grpc-go lowercases them again when building metadata.
func copyMetadataHeaders(dst, src http.Header) {
	for name, values := range src {
		if IsDeniedHeader(name) {
			continue
		}
		for _, v := range values {
			dst.Add(name, v)
		}
	}
}

// headersToMetadataList converts HTTP headers into the protobuf metadata list a
// `Frame` carries. `-bin` values are base64 on the HTTP wire and *raw bytes* in
// the frame, so they are decoded here (a value that is not valid base64 is
// dropped rather than shipped as bogus bytes).
func headersToMetadataList(h http.Header) []*pb.Metadatum {
	var out []*pb.Metadatum
	for name, values := range h {
		if IsDeniedHeader(name) {
			continue
		}
		key := strings.ToLower(name)
		for _, v := range values {
			if isBinKey(key) {
				raw, err := decodeBinHeader(v)
				if err != nil {
					continue
				}
				out = append(out, &pb.Metadatum{Key: key, Value: &pb.Metadatum_BinValue{BinValue: raw}})
			} else {
				out = append(out, &pb.Metadatum{Key: key, Value: &pb.Metadatum_AsciiValue{AsciiValue: v}})
			}
		}
	}
	return out
}

// metadataListToHeaders converts a frame's protobuf metadata list into HTTP
// headers for the dispatched gRPC request — the inverse of
// headersToMetadataList, so `-bin` raw bytes are base64-encoded back.
func metadataListToHeaders(items []*pb.Metadatum) http.Header {
	h := http.Header{}
	for _, m := range items {
		key := strings.ToLower(m.GetKey())
		if key == "" || IsDeniedHeader(key) || !validHeaderKey(key) {
			continue
		}
		switch v := m.GetValue().(type) {
		case *pb.Metadatum_AsciiValue:
			if validHeaderValue(v.AsciiValue) {
				h.Add(key, v.AsciiValue)
			}
		case *pb.Metadatum_BinValue:
			// Unpadded, matching grpc-go and tonic. gRPC permits either form and
			// decodeBinHeader accepts both.
			h.Add(key, base64.RawStdEncoding.EncodeToString(v.BinValue))
		}
	}
	return h
}

// decodeBinHeader decodes a `-bin` metadata value. gRPC permits padded and
// unpadded base64 on the wire, so both are accepted.
func decodeBinHeader(v string) ([]byte, error) {
	if len(v)%4 == 0 {
		return base64.StdEncoding.DecodeString(v)
	}
	return base64.RawStdEncoding.DecodeString(v)
}

// validHeaderKey reports whether key is a legal HTTP field name (RFC 7230 token).
func validHeaderKey(key string) bool {
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c <= ' ' || c >= 0x7F || strings.IndexByte(":()<>@,;\\\"/[]?={}", c) >= 0 {
			return false
		}
	}
	return true
}

// validHeaderValue rejects values that cannot appear in a header field (control
// characters), so a hostile frame cannot inject headers into the dispatched call.
func validHeaderValue(v string) bool {
	for i := 0; i < len(v); i++ {
		if c := v[i]; c < ' ' && c != '\t' || c == 0x7F {
			return false
		}
	}
	return true
}

// parseGRPCTimeout reads the gRPC `grpc-timeout` header (e.g. "100m", "5S").
//
// The gRPC spec caps the value at 8 digits, so anything longer is malformed and
// read as no deadline at all — the same rule grpc-go applies when it parses this
// header downstream. That cap also means only the hour unit can overflow an
// int64 of nanoseconds, and it is clamped rather than wrapped.
func parseGRPCTimeout(h http.Header) (time.Duration, bool) {
	raw := h.Get("grpc-timeout")
	if len(raw) < 2 || len(raw) > 9 {
		return 0, false
	}
	var unit time.Duration
	switch raw[len(raw)-1] {
	case 'H':
		unit = time.Hour
	case 'M':
		unit = time.Minute
	case 'S':
		unit = time.Second
	case 'm':
		unit = time.Millisecond
	case 'u':
		unit = time.Microsecond
	case 'n':
		unit = time.Nanosecond
	default:
		return 0, false
	}
	n, err := strconv.ParseInt(raw[:len(raw)-1], 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	if n > int64(maxTimeout/unit) {
		return maxTimeout, true
	}
	return time.Duration(n) * unit, true
}

// maxTimeout is the longest representable deadline (an int64 of nanoseconds).
const maxTimeout = time.Duration(1<<63 - 1)

// formatGRPCTimeout renders a deadline as a `grpc-timeout` value in milliseconds.
func formatGRPCTimeout(d time.Duration) string {
	ms := d.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return strconv.FormatInt(ms, 10) + "m"
}

// timeoutFromMillis converts a `Subscribe.timeout_millis` (0 = no deadline).
func timeoutFromMillis(ms uint32) time.Duration {
	if ms == 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// readStatus reads the gRPC status from a response's trailers, falling back to
// its headers (a "trailers-only" response carries the status in the headers
// block). `grpc-message` is percent-decoded.
func readStatus(trailers, headers http.Header) (uint32, string) {
	get := func(name string) string {
		if v := trailers.Get(name); v != "" {
			return v
		}
		return headers.Get(name)
	}
	code := uint64(0)
	if v := get("grpc-status"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			code = n
		}
	}
	return uint32(code), percentDecode(get("grpc-message"))
}

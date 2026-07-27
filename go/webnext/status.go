package webnext

import (
	"strings"

	"google.golang.org/grpc/codes"
)

// gRPC status codes are canonical, so this package uses grpc-go's
// [google.golang.org/grpc/codes].Code rather than redeclaring the set. Only the
// wire encodings specific to grpc-webnext live here.

// WSCloseCode maps a gRPC status onto the WebSocket close code used for a
// *connection-level* rejection at the handshake: the private range 4000 + code
// (gRPC 0..=16 => 4000..=4016). Per-stream errors on an already-open connection
// are in-band Reset/Trailer frames, never close codes.
// (See spec/PROTOCOL.md "Limits & error surfaces".)
func WSCloseCode(c codes.Code) int { return 4000 + int(c) }

// maxCloseReason is the RFC 6455 cap on a close frame's reason: the whole
// control-frame payload is 125 bytes, 2 of which are the close code.
const maxCloseReason = 123

// truncateUTF8 shortens s to at most max bytes without splitting a rune.
func truncateUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	end := max
	// Back up off any UTF-8 continuation byte (10xxxxxx).
	for end > 0 && s[end]&0xC0 == 0x80 {
		end--
	}
	return s[:end]
}

// percentEncode is the gRPC `grpc-message` header encoding: ASCII alphanumerics
// plus " -_./:" pass through, everything else becomes %XX. Applied on the JSON
// Fetch path, which carries the status message in a header (the +proto paths
// carry it in a protobuf Trailer, where no header encoding applies).
func percentEncode(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z',
			c == ' ', c == '-', c == '_', c == '.', c == '/', c == ':':
			b.WriteByte(c)
		default:
			const hex = "0123456789ABCDEF"
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0x0F])
		}
	}
	return b.String()
}

// percentDecode reverses percentEncode (`%XX`), leaving malformed escapes alone.
func percentDecode(s string) string {
	if !strings.ContainsRune(s, '%') {
		return s
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); {
		if s[i] == '%' && i+2 < len(s) {
			if b, ok := unhex(s[i+1], s[i+2]); ok {
				out = append(out, b)
				i += 3
				continue
			}
		}
		out = append(out, s[i])
		i++
	}
	return string(out)
}

func unhex(hi, lo byte) (byte, bool) {
	h, ok1 := hexVal(hi)
	l, ok2 := hexVal(lo)
	return h<<4 | l, ok1 && ok2
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

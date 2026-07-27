// The two length-prefixed framings grpc-webnext moves between.
//
//   - Fetch (unary) response: browsers cannot read HTTP trailers, so the body is
//     two 4-byte big-endian length-prefixed blocks —
//     `[u32 len | message][u32 len | Trailer]` — the second carrying the gRPC
//     status and trailing metadata. The `+proto` request body is the same
//     `[u32 len | message]` block, supplied by the client (which already knows
//     the length), so a server can turn it into a gRPC frame and stream it on
//     without buffering to measure.
//   - gRPC wire: `[1-byte compression flag][u32 len][message]`. Dropping the flag
//     byte from a gRPC frame yields the Fetch message block verbatim, which is
//     what lets the unary response stream through without a copy.

package webnext

import (
	"encoding/binary"

	"github.com/grpc-webnext/grpc-webnext/go/webnext/pb"
	"google.golang.org/protobuf/proto"
)

const (
	lenPrefix     = 4 // Fetch block length prefix
	grpcHeaderLen = 5 // gRPC frame header: 1 flag + 4 length
)

// emptyMessageBlock is `[u32 len = 0]`: a response carrying only a status still
// needs the leading message block before the trailer block.
var emptyMessageBlock = []byte{0, 0, 0, 0}

// encodeTrailerBlock encodes the trailing `[u32 len | Trailer]` block. The
// streaming unary path forwards the message block straight from the gRPC frame
// and only appends this at the end.
func encodeTrailerBlock(t *pb.Trailer) []byte {
	body, err := proto.Marshal(t)
	if err != nil {
		// Trailer holds only scalars and metadata; marshaling cannot fail. Fall
		// back to an empty block rather than dropping the frame entirely.
		body = nil
	}
	out := make([]byte, lenPrefix+len(body))
	binary.BigEndian.PutUint32(out, uint32(len(body)))
	copy(out[lenPrefix:], body)
	return out
}

// encodeResponseBody encodes a complete buffered unary response body:
// `[u32 len | message][u32 len | Trailer]`.
func encodeResponseBody(message []byte, t *pb.Trailer) []byte {
	head := make([]byte, lenPrefix+len(message))
	binary.BigEndian.PutUint32(head, uint32(len(message)))
	copy(head[lenPrefix:], message)
	return append(head, encodeTrailerBlock(t)...)
}

// grpcFrame frames one message for the gRPC wire (uncompressed).
func grpcFrame(message []byte) []byte {
	out := make([]byte, grpcHeaderLen+len(message))
	out[0] = 0 // compression flag: none
	binary.BigEndian.PutUint32(out[1:], uint32(len(message)))
	copy(out[grpcHeaderLen:], message)
	return out
}

// deframer turns a stream of gRPC body bytes back into whole messages: push
// chunks, pull complete messages.
type deframer struct {
	buf []byte
}

func (d *deframer) push(chunk []byte) { d.buf = append(d.buf, chunk...) }

// next returns the next complete message, or nil/false if one is not yet
// fully buffered.
func (d *deframer) next() ([]byte, bool) {
	if len(d.buf) < grpcHeaderLen {
		return nil, false
	}
	n := int(binary.BigEndian.Uint32(d.buf[1:grpcHeaderLen]))
	if len(d.buf) < grpcHeaderLen+n {
		return nil, false
	}
	msg := make([]byte, n)
	copy(msg, d.buf[grpcHeaderLen:grpcHeaderLen+n])
	d.buf = d.buf[grpcHeaderLen+n:]
	return msg, true
}

// deframeAll splits a fully buffered gRPC body into its messages.
func deframeAll(body []byte) [][]byte {
	d := &deframer{buf: body}
	var out [][]byte
	for {
		msg, ok := d.next()
		if !ok {
			return out
		}
		out = append(out, msg)
	}
}

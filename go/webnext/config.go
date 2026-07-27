package webnext

import "time"

// Content-type tokens (Fetch) and subprotocols (WebSocket) that disambiguate
// grpc-webnext from native gRPC on one port. See spec/PROTOCOL.md "Content types".
const (
	CTProto = "application/grpc-webnext+proto"
	CTJSON  = "application/grpc-webnext+json"
	CTGRPC  = "application/grpc" // native gRPC (passthrough), matched as a prefix

	// WSSubprotocol is the base entry a client offers alongside a codec entry.
	WSSubprotocol      = "grpc-webnext"
	WSSubprotocolProto = "grpc-webnext+proto"
	WSSubprotocolJSON  = "grpc-webnext+json"

	// WSSubprotocolH2TS selects the binary default: real gRPC over an h2ts
	// tunnel, with no grpc-webnext translation at all.
	WSSubprotocolH2TS = "h2ts"
)

// DefaultMaxMessageBytes mirrors the Rust server/proxy default (4 MiB).
const DefaultMaxMessageBytes = 4 * 1024 * 1024

// DefaultWSKeepaliveTimeout is gRPC's `keepalive_timeout` default.
const DefaultWSKeepaliveTimeout = 20 * time.Second

// deadlineGrace is the slack added to the `grpc-timeout` forwarded to the
// service. The edge owns the client-facing deadline — it drops the call exactly
// at the deadline and surfaces DEADLINE_EXCEEDED — so the service's own
// enforcement should be a later backstop rather than a racing timer.
const deadlineGrace = 500 * time.Millisecond

// ServerConfig configures an in-process grpc-webnext server. Field names and
// defaults mirror the Rust `ServerConfig` so behavior is portable across
// implementations (and identical under the conformance suite).
//
// There are deliberately no auth hooks, per-RPC or per-connection: every request
// carries its metadata into the service on every transport, so authorization
// belongs in a grpc-go interceptor — one check that covers Fetch, WebSocket, the
// h2ts tunnel, and native gRPC alike. See spec/PROTOCOL.md "Auth".
type ServerConfig struct {
	// MaxMessageBytes bounds inbound *request* messages on every path. An
	// oversize message is rejected with RESOURCE_EXHAUSTED (HTTP 413 on the
	// `+proto` Fetch path, where only the length prefix has been read). 0 uses
	// DefaultMaxMessageBytes.
	MaxMessageBytes int

	// Transcoder enables the `+json` codec (and plain-JSON requests) by supplying
	// the message descriptors needed to convert JSON <-> protobuf. nil means
	// `+json` answers UNIMPLEMENTED — as a proper gRPC status, never an HTTP 501.
	Transcoder *Transcoder

	// AllowImplicitCodec lets plain `application/json` / a blank content-type
	// reach *main* gRPC method paths, and lets a WebSocket with no codec
	// subprotocol infer its codec from the first frame. Off by default: without
	// it, those requests are rejected (Fetch 415 / WS close 4012).
	AllowImplicitCodec bool

	// WSKeepalive, when non-zero, makes an open streaming connection emit a
	// WebSocket ping every interval; the peer's automatic pong is the return
	// traffic that stops an idle-timeout proxy from dropping a quiet stream.
	// Zero (the default) disables keepalive.
	WSKeepalive time.Duration

	// WSKeepaliveTimeout bounds the wait for a response. Any inbound frame — the
	// pong or ordinary stream data — proves liveness; if nothing arrives for
	// WSKeepalive + WSKeepaliveTimeout the peer is presumed dead and the
	// connection is dropped. 0 uses DefaultWSKeepaliveTimeout.
	WSKeepaliveTimeout time.Duration
}

func (c ServerConfig) maxMessageBytes() int {
	if c.MaxMessageBytes <= 0 {
		return DefaultMaxMessageBytes
	}
	return c.MaxMessageBytes
}

// maxSilence is how long a streaming connection may go without any inbound frame
// before the peer is presumed dead. Zero when keepalive is disabled.
func (c ServerConfig) maxSilence() time.Duration {
	if c.WSKeepalive <= 0 {
		return 0
	}
	timeout := c.WSKeepaliveTimeout
	if timeout <= 0 {
		timeout = DefaultWSKeepaliveTimeout
	}
	return c.WSKeepalive + timeout
}

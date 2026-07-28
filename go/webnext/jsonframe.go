// Native-JSON WebSocket frame format for the `+json` codec.
//
// JSON WebSocket messages are *text* frames carrying a flat object; which field
// is present tells you the frame kind. One WebSocket carries exactly one stream,
// so the WS URL *is* the route — frames carry neither `method` nor a stream id:
//
//	{ "metadata": {…}, "timeoutMillis": 5000 } // open (optional; metadata/deadline)
//	{ "message": {…} }                         // data message
//	{ "halfClose": true }                      // client done sending
//	{ "status": { "code": 0, "message": "" } } // terminal (trailer / reset)
//
// The application `message` is a *native* JSON value, not base64 bytes. Proto
// mode uses binary frames carrying the protobuf `Frame` envelope instead.

package webnext

import (
	"encoding/json"

	"github.com/grpc-webnext/grpc-webnext/go/webnext/pb"
)

// jsonStatus is the terminal status carried by a JSON frame.
type jsonStatus struct {
	Code    uint32 `json:"code"`
	Message string `json:"message,omitempty"`
}

// jsonFrame is one WebSocket text frame. The kind is chosen by which of
// `message` / `halfClose` / `status` is present (or a bare `metadata` open).
type jsonFrame struct {
	Metadata      map[string]string `json:"metadata,omitempty"`
	TimeoutMillis *uint32           `json:"timeoutMillis,omitempty"`
	Message       json.RawMessage   `json:"message,omitempty"`
	HalfClose     *bool             `json:"halfClose,omitempty"`
	Status        *jsonStatus       `json:"status,omitempty"`
}

func decodeJSONFrame(text []byte) (*jsonFrame, error) {
	var f jsonFrame
	if err := json.Unmarshal(text, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

func encodeJSONFrame(f *jsonFrame) []byte {
	out, err := json.Marshal(f)
	if err != nil {
		return []byte("{}")
	}
	return out
}

// jsonOpenToSubscribe builds the opening Subscribe from the first text frame,
// taking the method from the WS route (the frame itself carries none) and
// folding any inline first message into initial_payload.
func jsonOpenToSubscribe(f *jsonFrame, method string) *pb.Subscribe {
	sub := &pb.Subscribe{
		Method:  method,
		Headers: jsonMetaToList(f.Metadata),
	}
	if f.TimeoutMillis != nil {
		sub.TimeoutMillis = *f.TimeoutMillis
	}
	if len(f.Message) > 0 {
		sub.InitialPayload = f.Message
	}
	return sub
}

// jsonFrameToProto converts a post-open client text frame into the internal
// Frame. The kind is chosen by which field is present, in priority order
// status -> halfClose -> message -> (none). A frame with no recognized field is
// a half-close; see spec/PROTOCOL.md "JSON frame edge semantics".
func jsonFrameToProto(f *jsonFrame) *pb.Frame {
	switch {
	case f.Status != nil:
		// A terminal status from the client is a cancel/reset.
		return &pb.Frame{Kind: &pb.Frame_Reset_{Reset_: &pb.Reset{
			StatusCode: f.Status.Code, StatusMessage: f.Status.Message,
		}}}
	case f.HalfClose != nil && *f.HalfClose:
		return &pb.Frame{Kind: &pb.Frame_HalfClose{HalfClose: &pb.HalfClose{}}}
	case len(f.Message) > 0:
		return &pb.Frame{Kind: &pb.Frame_Message{Message: &pb.Message{Payload: f.Message}}}
	default:
		return &pb.Frame{Kind: &pb.Frame_HalfClose{HalfClose: &pb.HalfClose{}}}
	}
}

// protoFrameToJSON converts an internal server Frame into a JSON text frame.
// Returns nil for kinds with no JSON form (client-only kinds).
func protoFrameToJSON(f *pb.Frame) *jsonFrame {
	switch k := f.GetKind().(type) {
	case *pb.Frame_Message:
		return &jsonFrame{Message: rawJSON(k.Message.GetPayload())}
	case *pb.Frame_Header:
		meta := listToJSONMeta(k.Header.GetHeaders())
		if meta == nil {
			meta = map[string]string{}
		}
		return &jsonFrame{Metadata: meta}
	case *pb.Frame_Trailer:
		return &jsonFrame{
			Status:   &jsonStatus{Code: k.Trailer.GetStatusCode(), Message: k.Trailer.GetStatusMessage()},
			Metadata: listToJSONMeta(k.Trailer.GetTrailers()),
		}
	case *pb.Frame_Reset_:
		return &jsonFrame{
			Status: &jsonStatus{Code: k.Reset_.GetStatusCode(), Message: k.Reset_.GetStatusMessage()},
		}
	default:
		return nil
	}
}

// rawJSON wraps an already-encoded JSON message, falling back to `null` when the
// payload is empty (a message frame always has a `message` field).
func rawJSON(payload []byte) json.RawMessage {
	if len(payload) == 0 {
		return json.RawMessage("null")
	}
	return json.RawMessage(payload)
}

// listToJSONMeta renders protobuf metadata as a JSON object. Binary (`-bin`)
// metadata has no JSON form and is dropped; nil when nothing remains.
func listToJSONMeta(items []*pb.Metadatum) map[string]string {
	var out map[string]string
	for _, m := range items {
		v, ok := m.GetValue().(*pb.Metadatum_AsciiValue)
		if !ok {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[m.GetKey()] = v.AsciiValue
	}
	return out
}

func jsonMetaToList(meta map[string]string) []*pb.Metadatum {
	if len(meta) == 0 {
		return nil
	}
	out := make([]*pb.Metadatum, 0, len(meta))
	for k, v := range meta {
		out = append(out, &pb.Metadatum{Key: k, Value: &pb.Metadatum_AsciiValue{AsciiValue: v}})
	}
	return out
}

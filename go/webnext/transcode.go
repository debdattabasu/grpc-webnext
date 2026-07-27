// JSON <-> protobuf transcoding for the `+json` codec.
//
// grpc-webnext carries opaque message bytes; the envelope (frames, trailers) is
// always protobuf, but the *application message* may be JSON. Turning that JSON
// into the binary protobuf a gRPC handler expects (and back) needs the message
// descriptors, so a Transcoder is built from a compiled FileDescriptorSet — the
// same input the Rust crate's `Transcoder::from_file_descriptor_set` takes.
//
// The JSON dialect is canonical protobuf JSON (protojson defaults): lowerCamelCase
// field names, default values omitted, 64-bit integers as strings, bytes as
// base64, unknown fields rejected. Those are exactly prost-reflect's defaults on
// the Rust side, so the two implementations produce interchangeable JSON.

package webnext

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Transcoder converts application messages between JSON and binary protobuf,
// keyed by gRPC method path (`/pkg.Service/Method`), and maps `google.api.http`
// REST bindings onto gRPC methods.
type Transcoder struct {
	files   *protoregistry.Files
	methods map[string]protoreflect.MethodDescriptor
	router  *httpRouter
}

// NewTranscoder builds a Transcoder from an encoded FileDescriptorSet.
//
// Two ways to get one: `protoc --descriptor_set_out=... --include_imports` (the
// flag matters — the set must be self-contained or building the pool fails), or
// `protodesc.ToFileDescriptorProto` over the descriptors already compiled into
// your generated package, which needs no side-car file at all.
func NewTranscoder(fileDescriptorSet []byte) (*Transcoder, error) {
	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(fileDescriptorSet, &fds); err != nil {
		return nil, fmt.Errorf("webnext: decode descriptor set: %w", err)
	}
	files, err := protodesc.NewFiles(&fds)
	if err != nil {
		return nil, fmt.Errorf("webnext: build descriptor pool: %w", err)
	}
	t := &Transcoder{files: files, methods: map[string]protoreflect.MethodDescriptor{}}
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			svc := services.Get(i)
			methods := svc.Methods()
			for j := 0; j < methods.Len(); j++ {
				m := methods.Get(j)
				t.methods["/"+string(svc.FullName())+"/"+string(m.Name())] = m
			}
		}
		return true
	})
	t.router = newHTTPRouter(files, &fds)
	return t, nil
}

// HasHTTPRules reports whether any `google.api.http` REST bindings were compiled
// from the descriptor set — i.e. whether this transcoder serves REST routes at
// all, on top of the `+json` codec it always serves.
func (t *Transcoder) HasHTTPRules() bool { return !t.router.isEmpty() }

// transcodeHTTPRequest maps a REST request onto a gRPC call. The bool reports
// whether any binding matched; a miss is not an error, it means "this is a main
// gRPC method path, not a REST URL".
func (t *Transcoder) transcodeHTTPRequest(method, path, query string, body []byte) (*httpCall, bool, error) {
	return t.router.transcode(method, path, query, body)
}

// matchWS resolves a WebSocket annotation route from its upgrade path, or nil if
// the path matches no binding.
func (t *Transcoder) matchWS(path, query string) *wsBinding {
	return t.router.matchWS(path, query)
}

// HasMethod reports whether `path` (`/pkg.Service/Method`) resolves to a method
// this transcoder knows. Callers use it to tell "unknown method" (UNIMPLEMENTED)
// apart from a genuine transcode failure (INVALID_ARGUMENT).
func (t *Transcoder) HasMethod(path string) bool {
	_, ok := t.method(path)
	return ok
}

func (t *Transcoder) method(path string) (protoreflect.MethodDescriptor, bool) {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	m, ok := t.methods[path]
	return m, ok
}

// RequestJSONToProto converts a JSON request message into binary protobuf. Empty
// input is the default message (a body-less call).
func (t *Transcoder) RequestJSONToProto(path string, jsonBytes []byte) ([]byte, error) {
	m, ok := t.method(path)
	if !ok {
		return nil, fmt.Errorf("webnext: unknown method %s", path)
	}
	msg := dynamicpb.NewMessage(m.Input())
	if len(strings.TrimSpace(string(jsonBytes))) > 0 {
		if err := (protojson.UnmarshalOptions{}).Unmarshal(jsonBytes, msg); err != nil {
			return nil, fmt.Errorf("webnext: decode json request: %w", err)
		}
	}
	return proto.Marshal(msg)
}

// ResponseProtoToJSON converts a binary protobuf response message into JSON.
func (t *Transcoder) ResponseProtoToJSON(path string, protoBytes []byte) ([]byte, error) {
	m, ok := t.method(path)
	if !ok {
		return nil, fmt.Errorf("webnext: unknown method %s", path)
	}
	msg := dynamicpb.NewMessage(m.Output())
	if err := proto.Unmarshal(protoBytes, msg); err != nil {
		return nil, fmt.Errorf("webnext: decode proto response: %w", err)
	}
	return (protojson.MarshalOptions{}).Marshal(msg)
}

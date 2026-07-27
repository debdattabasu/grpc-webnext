// `google.api.http` transcoding: map REST-style `(HTTP method, path)` requests
// onto gRPC methods, binding path segments, query params, and the request body
// into the request message.
//
// This is a port of the Rust crate's `src/httprule.rs`, deliberately kept
// structurally parallel to it — same segment/body model, same matching order,
// same coercion table — because the two implementations are one wire contract
// and the cheapest way to keep them from drifting is for the code to be
// comparable side by side. The supported subset is therefore identical:
//
//   - verbs `get/put/post/delete/patch` and `custom{kind}`,
//   - a trailing custom verb (`/v1/things/{id}:cancel`), matched not stripped,
//   - `additional_bindings` (one level),
//   - path templates: literal segments, `{field}` / `{field=*}` single-segment
//     captures, and `{field=**}` capturing the rest, with dotted field paths
//     (`{a.b}`) binding nested fields,
//   - `body: "*"` (whole message), `body: "<field>"` (a sub-message field), or none,
//   - query params bound to (possibly nested) scalar/repeated fields.
//
// The unsupported surface is identical too, and enumerated in
// doc/HTTPRULE_GAPS.md: `response_body`, `HttpRule.selector` (service-config
// rules), path patterns beyond a bare `*`/`**`, non-scalar query binding, and
// scalar/repeated/dotted body fields.

package webnext

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// segmentKind distinguishes the three path-template segment forms.
type segmentKind int

const (
	// segLiteral is a fixed path component that must match exactly.
	segLiteral segmentKind = iota
	// segSingle is `{field}` / `{field=*}` — captures exactly one component.
	segSingle
	// segRest is `{field=**}` — captures the remaining components, slashes kept.
	segRest
)

type segment struct {
	kind    segmentKind
	literal string
	// field is the dotted field path a capture binds to (`{a.b}` -> ["a","b"]).
	field []string
}

// bodyKind is how the request body maps onto the message.
type bodyKind int

const (
	// bodyNone: the binding takes no body.
	bodyNone bodyKind = iota
	// bodyWildcard: `body: "*"` — the whole JSON body is the request message.
	bodyWildcard
	// bodyField: `body: "<field>"` — the JSON body is a single message field.
	bodyField
)

// binding is one compiled HTTP binding: `(verb, template)` -> a gRPC method plus
// how to build its request message.
type binding struct {
	httpMethod string // upper-case, e.g. "GET"
	segments   []segment
	// customVerb is the template's trailing `:verb` (without the colon), empty if
	// it has none. It is matched, not merely stripped: see matchSegments.
	customVerb string
	body       bodyKind
	bodyField  string // set when body == bodyField
	grpcMethod string // "/pkg.Service/Method"
	input      protoreflect.MessageDescriptor
}

// pathVar is a captured path variable: the dotted field path it binds to and its
// decoded string value — e.g. `{["user","id"], "123"}` for `{user.id}`.
type pathVar struct {
	field []string
	value string
}

// httpCall is a transcoded REST call: which gRPC method to invoke, and the
// encoded request message.
type httpCall struct {
	grpcMethod string
	message    []byte
}

// wsBinding is a WebSocket route resolved from an annotation URL: the target
// gRPC method plus the path/query bindings used to build each request message.
type wsBinding struct {
	binding *binding
	vars    []pathVar
	query   string
}

// grpcMethod is the method this annotation route maps to.
func (b *wsBinding) method() string { return b.binding.grpcMethod }

// hasBody reports whether the route takes a request body. False for GET-style
// server-streams, where the request comes entirely from the URL (path + query).
func (b *wsBinding) hasBody() bool { return b.binding.body != bodyNone }

// buildMessage builds a request message from a body payload, overlaying the URL
// path/query bindings.
func (b *wsBinding) buildMessage(body []byte) ([]byte, error) {
	return buildMessage(b.binding, b.vars, b.query, body)
}

// httpRouter is a table of HTTP bindings compiled from a descriptor set.
type httpRouter struct {
	bindings []*binding
}

// newHTTPRouter compiles every `google.api.http` binding in the set. It walks
// `fds` in file order (rather than ranging over `files`, whose iteration order is
// a map's) so binding precedence is deterministic and matches the Rust router's,
// which follows descriptor-pool order.
func newHTTPRouter(files *protoregistry.Files, fds *descriptorpb.FileDescriptorSet) *httpRouter {
	r := &httpRouter{}
	for _, file := range fds.GetFile() {
		fd, err := files.FindFileByPath(file.GetName())
		if err != nil {
			continue
		}
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			svc := services.Get(i)
			methods := svc.Methods()
			for j := 0; j < methods.Len(); j++ {
				m := methods.Get(j)
				rule := httpRuleOf(m)
				if rule == nil {
					continue
				}
				grpcMethod := "/" + string(svc.FullName()) + "/" + string(m.Name())
				r.collect(rule, grpcMethod, m.Input())
			}
		}
	}
	return r
}

// httpRuleOf reads the `google.api.http` option off a method, or nil if it has
// none. The extension resolves because this file imports the annotations
// package, which registers it in the global type registry that both the
// descriptor-set unmarshal and the lazy options unmarshal consult.
func httpRuleOf(m protoreflect.MethodDescriptor) *annotations.HttpRule {
	opts, ok := m.Options().(*descriptorpb.MethodOptions)
	if !ok || opts == nil || !proto.HasExtension(opts, annotations.E_Http) {
		return nil
	}
	rule, _ := proto.GetExtension(opts, annotations.E_Http).(*annotations.HttpRule)
	return rule
}

func (r *httpRouter) isEmpty() bool { return len(r.bindings) == 0 }

// collect pushes the top rule and any `additional_bindings` (one level).
func (r *httpRouter) collect(rule *annotations.HttpRule, grpcMethod string, input protoreflect.MessageDescriptor) {
	r.push(rule, grpcMethod, input)
	for _, extra := range rule.GetAdditionalBindings() {
		r.push(extra, grpcMethod, input)
	}
}

func (r *httpRouter) push(rule *annotations.HttpRule, grpcMethod string, input protoreflect.MessageDescriptor) {
	verb, template, ok := verbAndPath(rule)
	if !ok {
		return
	}
	body, field := bodyRule(rule)
	segments, customVerb := parseTemplate(template)
	r.bindings = append(r.bindings, &binding{
		httpMethod: verb,
		segments:   segments,
		customVerb: customVerb,
		body:       body,
		bodyField:  field,
		grpcMethod: grpcMethod,
		input:      input,
	})
}

// verbAndPath extracts the verb + path template from an HttpRule's pattern oneof.
func verbAndPath(rule *annotations.HttpRule) (verb, template string, ok bool) {
	switch p := rule.GetPattern().(type) {
	case *annotations.HttpRule_Get:
		verb, template = "GET", p.Get
	case *annotations.HttpRule_Put:
		verb, template = "PUT", p.Put
	case *annotations.HttpRule_Post:
		verb, template = "POST", p.Post
	case *annotations.HttpRule_Delete:
		verb, template = "DELETE", p.Delete
	case *annotations.HttpRule_Patch:
		verb, template = "PATCH", p.Patch
	case *annotations.HttpRule_Custom:
		// A custom binding is identified by its kind; its path may be empty.
		if kind := p.Custom.GetKind(); kind != "" {
			return strings.ToUpper(kind), p.Custom.GetPath(), true
		}
		return "", "", false
	default:
		return "", "", false
	}
	return verb, template, template != ""
}

func bodyRule(rule *annotations.HttpRule) (bodyKind, string) {
	switch body := rule.GetBody(); body {
	case "":
		return bodyNone, ""
	case "*":
		return bodyWildcard, ""
	default:
		return bodyField, body
	}
}

// parseTemplate parses a path template into segments plus its trailing custom
// verb (`/v1/things/{id}:cancel` -> the `cancel`), which the caller matches
// rather than discards.
func parseTemplate(template string) ([]segment, string) {
	path, customVerb, _ := strings.Cut(template, ":")
	var out []segment
	for _, seg := range strings.Split(strings.Trim(path, "/"), "/") {
		if seg == "" {
			continue
		}
		if inner, ok := strings.CutPrefix(seg, "{"); ok {
			if inner, ok := strings.CutSuffix(inner, "}"); ok {
				field, pattern, hasPattern := strings.Cut(inner, "=")
				if !hasPattern {
					pattern = "*"
				}
				kind := segSingle
				if pattern == "**" {
					kind = segRest
				}
				out = append(out, segment{kind: kind, field: strings.Split(field, ".")})
				continue
			}
		}
		out = append(out, segment{kind: segLiteral, literal: seg})
	}
	return out, customVerb
}

// matchSegments matches a request path against a template, returning the
// captured variables. The whole path must be consumed.
//
// `customVerb` is the template's trailing `:verb`. It is part of the match, in
// both directions: a binding that declares one matches only paths carrying it
// (and the verb is stripped before the last segment binds), and a binding that
// declares none never matches a path that carries one. Stripping the verb from
// the template alone would make `/v1/things/{id}:cancel` match the bare
// `/v1/things/5` *and* capture `id = "5:cancel"` from `/v1/things/5:cancel`,
// with every custom verb on a resource colliding on one template. A genuine
// colon in path data is percent-encoded, so it survives this check.
func matchSegments(segments []segment, customVerb, path string) ([]pathVar, bool) {
	var parts []string
	for _, p := range strings.Split(strings.Trim(path, "/"), "/") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) > 0 {
		last := len(parts) - 1
		head, verb, hasVerb := strings.Cut(parts[last], ":")
		if verb != customVerb || (hasVerb && head == "") {
			return nil, false
		}
		parts[last] = head
	} else if customVerb != "" {
		return nil, false
	}
	var vars []pathVar
	i := 0
	for _, seg := range segments {
		switch seg.kind {
		case segLiteral:
			if i >= len(parts) || parts[i] != seg.literal {
				return nil, false
			}
			i++
		case segSingle:
			if i >= len(parts) {
				return nil, false
			}
			vars = append(vars, pathVar{field: seg.field, value: percentDecode(parts[i])})
			i++
		case segRest:
			rest := make([]string, 0, len(parts)-i)
			for _, p := range parts[i:] {
				rest = append(rest, percentDecode(p))
			}
			vars = append(vars, pathVar{field: seg.field, value: strings.Join(rest, "/")})
			i = len(parts)
		}
	}
	if i != len(parts) {
		return nil, false
	}
	return vars, true
}

// matchWS matches a WebSocket upgrade path against the bindings. A WS upgrade is
// always an HTTP GET, so this matches on the path only (verb-agnostic).
func (r *httpRouter) matchWS(path, query string) *wsBinding {
	for _, b := range r.bindings {
		if vars, ok := matchSegments(b.segments, b.customVerb, path); ok {
			return &wsBinding{binding: b, vars: vars, query: query}
		}
	}
	return nil
}

// matchRequest finds a binding matching `(verb, path)` plus its captured vars.
func (r *httpRouter) matchRequest(method, path string) (*binding, []pathVar, bool) {
	want := strings.ToUpper(method)
	for _, b := range r.bindings {
		if b.httpMethod != want {
			continue
		}
		if vars, ok := matchSegments(b.segments, b.customVerb, path); ok {
			return b, vars, true
		}
	}
	return nil, nil, false
}

// transcode maps a REST request onto a gRPC call, or reports false if no binding
// matches (which is not an error — the caller falls back to the main path).
func (r *httpRouter) transcode(method, path, query string, body []byte) (*httpCall, bool, error) {
	b, vars, ok := r.matchRequest(method, path)
	if !ok {
		return nil, false, nil
	}
	message, err := buildMessage(b, vars, query, body)
	if err != nil {
		return nil, true, err
	}
	return &httpCall{grpcMethod: b.grpcMethod, message: message}, true, nil
}

// buildMessage builds the encoded request message from a matched binding.
//
// Precedence (spec/PROTOCOL.md "REST binding precedence"): the body seeds the
// message, path variables always overlay it, and query params bind only when the
// body is not the wildcard.
func buildMessage(b *binding, vars []pathVar, query string, body []byte) ([]byte, error) {
	msg := dynamicpb.NewMessage(b.input)

	switch b.body {
	case bodyWildcard:
		if err := unmarshalJSON(msg, body); err != nil {
			return nil, err
		}
	case bodyField:
		if len(body) > 0 {
			if err := setMessageField(msg, b.bodyField, body); err != nil {
				return nil, err
			}
		}
	case bodyNone:
	}

	for _, v := range vars {
		if err := setByPath(msg, v.field, v.value); err != nil {
			return nil, err
		}
	}

	if b.body != bodyWildcard && query != "" {
		for _, kv := range parseQuery(query) {
			field := strings.Split(kv.key, ".")
			if boundByPath(vars, field) {
				continue // already set from the path
			}
			if err := setByPath(msg, field, kv.value); err != nil {
				return nil, err
			}
		}
	}

	return proto.Marshal(msg)
}

// boundByPath reports whether a path variable already bound this field path.
func boundByPath(vars []pathVar, field []string) bool {
	for _, v := range vars {
		if len(v.field) == len(field) {
			same := true
			for i := range field {
				if v.field[i] != field[i] {
					same = false
					break
				}
			}
			if same {
				return true
			}
		}
	}
	return false
}

// unmarshalJSON parses a JSON body into msg. Empty input leaves the default
// message, which is what a body-less call means.
func unmarshalJSON(msg protoreflect.ProtoMessage, body []byte) error {
	if len(body) == 0 {
		return nil
	}
	if err := (protojson.UnmarshalOptions{}).Unmarshal(body, msg); err != nil {
		return fmt.Errorf("decode json body: %w", err)
	}
	return nil
}

// setMessageField parses a JSON body into the (message-typed) field `name`.
func setMessageField(msg protoreflect.Message, name string, body []byte) error {
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return fmt.Errorf("unknown body field: %s", name)
	}
	if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
		return fmt.Errorf("body field %s must be a message", name)
	}
	if fd.IsList() || fd.IsMap() {
		return fmt.Errorf("body field %s must be a message", name)
	}
	return unmarshalJSON(msg.Mutable(fd).Message().Interface(), body)
}

// setByPath sets a (possibly nested, possibly repeated) scalar field from a
// string value, walking a dotted field path.
func setByPath(msg protoreflect.Message, path []string, raw string) error {
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(path[0]))
	if fd == nil {
		return fmt.Errorf("unknown field: %s", path[0])
	}

	if len(path) == 1 {
		value, err := coerce(fd, raw)
		if err != nil {
			return err
		}
		if fd.IsList() {
			msg.Mutable(fd).List().Append(value)
			return nil
		}
		msg.Set(fd, value)
		return nil
	}

	isMessage := fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind
	if !isMessage || fd.IsList() || fd.IsMap() {
		return fmt.Errorf("field %s is not a message", path[0])
	}
	return setByPath(msg.Mutable(fd).Message(), path[1:], raw)
}

// coerce turns a string into a scalar value per the field's protobuf kind.
func coerce(fd protoreflect.FieldDescriptor, raw string) (protoreflect.Value, error) {
	invalid := func() (protoreflect.Value, error) {
		return protoreflect.Value{}, fmt.Errorf("invalid value for %s: %q", fd.Name(), raw)
	}
	switch fd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(raw), nil
	case protoreflect.BoolKind:
		switch raw {
		case "true", "1":
			return protoreflect.ValueOfBool(true), nil
		case "false", "0":
			return protoreflect.ValueOfBool(false), nil
		}
		return protoreflect.Value{}, fmt.Errorf("invalid bool: %q", raw)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return invalid()
		}
		return protoreflect.ValueOfInt32(int32(n)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return invalid()
		}
		return protoreflect.ValueOfInt64(n), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return invalid()
		}
		return protoreflect.ValueOfUint32(uint32(n)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return invalid()
		}
		return protoreflect.ValueOfUint64(n), nil
	case protoreflect.FloatKind:
		f, err := strconv.ParseFloat(raw, 32)
		if err != nil {
			return invalid()
		}
		return protoreflect.ValueOfFloat32(float32(f)), nil
	case protoreflect.DoubleKind:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return invalid()
		}
		return protoreflect.ValueOfFloat64(f), nil
	case protoreflect.EnumKind:
		if n, err := strconv.ParseInt(raw, 10, 32); err == nil {
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(n)), nil
		}
		v := fd.Enum().Values().ByName(protoreflect.Name(raw))
		if v == nil {
			return protoreflect.Value{}, fmt.Errorf("unknown enum value: %q", raw)
		}
		return protoreflect.ValueOfEnum(v.Number()), nil
	case protoreflect.BytesKind:
		return protoreflect.Value{}, fmt.Errorf("bytes fields cannot bind from path/query")
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot bind a scalar to a message field")
	}
}

type queryParam struct{ key, value string }

// parseQuery splits a query string into decoded key/value pairs.
func parseQuery(query string) []queryParam {
	var out []queryParam
	for _, pair := range strings.Split(query, "&") {
		if pair == "" {
			continue
		}
		key, value, _ := strings.Cut(pair, "=")
		out = append(out, queryParam{key: decodeQuery(key), value: decodeQuery(value)})
	}
	return out
}

// decodeQuery decodes one query-string token: `+` is a space, then `%XX`
// (percentDecode, shared with the `grpc-message` header decoding in status.go —
// the escape syntax is the same).
func decodeQuery(s string) string { return percentDecode(strings.ReplaceAll(s, "+", " ")) }

// `google.api.http` transcoding: map REST-style `(HTTP method, path)` requests
// onto gRPC methods, binding path segments, query params, and the request body
// into the request message.
//
// This implements HttpRule; doc/HTTPRULE_GAPS.md is the authoritative statement of
// the boundary, and as of 2026-07-28 there are no functional gaps in the method
// option — only `HttpRule.selector` (service-config rules), declined.
//
// This file is a deliberate port of the Rust crate's `src/httprule.rs` — same
// segment/body model, same matching order, same coercion table — so the two
// implementations can be compared side by side. **Change one, change the other in
// the same commit.**
//
// Two ideas carry most of the weight:
//
//   - Captures are patterns, not segments. segCapture holds its own sub-pattern, so
//     `{f}`, `{f=**}` and `{f=shelves/*/books/*}` are one case rather than three.
//     Splitting is brace-aware for exactly that reason.
//   - Conversions belong to the JSON decoder. Anything without a scalar form —
//     `bytes`, the well-known types, a `body:` naming a non-message field — is set
//     by handing its protobuf-JSON text to the decoder rather than parsed here.
//     See setFromJSON.

package webnext

import (
	"encoding/json"
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

// segmentKind distinguishes the path-template segment forms.
//
// Note there is no separate "single capture" / "rest capture": both are a
// segCapture over a sub-pattern, which is what lets a capture span several
// segments (`{name=shelves/*/books/*}`). `{f}` is sugar for `{f=*}`.
type segmentKind int

const (
	// segLiteral is a fixed path component that must match exactly.
	segLiteral segmentKind = iota
	// segAnyOne is a bare `*` — matches exactly one component, capturing nothing.
	segAnyOne
	// segAnyRest is a bare `**` — matches the remaining components, capturing nothing.
	segAnyRest
	// segCapture is `{field=<sub-pattern>}` — binds whatever the sub-pattern
	// consumed, joined by `/`, to a (dotted) field path. Sub-segments are never
	// captures themselves; the grammar does not nest them.
	segCapture
)

type segment struct {
	kind    segmentKind
	literal string
	// field is the dotted field path a capture binds to (`{a.b}` -> ["a","b"]).
	field []string
	// sub is a capture's own pattern.
	sub []segment
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
	// responseBody is `response_body` — the top-level response field to return
	// instead of the whole message. Empty means the whole message.
	responseBody string
	grpcMethod   string // "/pkg.Service/Method"
	input        protoreflect.MessageDescriptor
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
	// responseBody is the binding's `response_body`, empty for the whole message.
	// Carried on the call because the response is encoded long after the binding
	// is matched.
	responseBody string
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

// responseBody is the binding's `response_body` — the top-level response field to
// return instead of the whole message. Empty means the whole message.
func (b *wsBinding) responseBody() string { return b.binding.responseBody }

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
		httpMethod:   verb,
		segments:     segments,
		customVerb:   customVerb,
		body:         body,
		bodyField:    field,
		responseBody: rule.GetResponseBody(),
		grpcMethod:   grpcMethod,
		input:        input,
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

// splitOutsideBraces splits s on sep, ignoring separators inside `{...}`.
//
// This is the whole reason multi-segment captures work: splitting the raw template
// on `/` first would tear `{name=shelves/*}` into pieces that no longer look like a
// capture, which is precisely how they used to compile into dead literal routes.
func splitOutsideBraces(s string, sep byte) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		case sep:
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}

// parseTemplate parses a path template into segments plus its trailing custom
// verb (`/v1/things/{id}:cancel` -> the `cancel`), which the caller matches
// rather than discards.
func parseTemplate(template string) ([]segment, string) {
	parts := splitOutsideBraces(template, ':')
	path, customVerb := parts[0], ""
	if len(parts) > 1 {
		customVerb = strings.Join(parts[1:], ":")
	}

	var out []segment
	for _, seg := range splitOutsideBraces(strings.Trim(path, "/"), '/') {
		if seg == "" {
			continue
		}
		out = append(out, parseSegment(seg))
	}
	return out, customVerb
}

// parseSegment parses one template segment: a capture (with its own sub-pattern)
// or a plain one.
func parseSegment(seg string) segment {
	if inner, ok := strings.CutPrefix(seg, "{"); ok {
		if inner, ok := strings.CutSuffix(inner, "}"); ok {
			field, pattern, hasPattern := strings.Cut(inner, "=")
			if !hasPattern {
				pattern = "*"
			}
			var sub []segment
			for _, p := range strings.Split(strings.Trim(pattern, "/"), "/") {
				if p != "" {
					sub = append(sub, plainSegment(p))
				}
			}
			return segment{kind: segCapture, field: strings.Split(field, "."), sub: sub}
		}
	}
	return plainSegment(seg)
}

// plainSegment is a non-capturing segment: a wildcard or a literal.
func plainSegment(seg string) segment {
	switch seg {
	case "*":
		return segment{kind: segAnyOne}
	case "**":
		return segment{kind: segAnyRest}
	default:
		return segment{kind: segLiteral, literal: seg}
	}
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
	end, ok := matchList(segments, parts, 0, &vars)
	if !ok || end != len(parts) {
		return nil, false
	}
	return vars, true
}

// matchList matches segments against parts[i:], collecting captures. It returns
// the index just past what was consumed, or false if the pattern does not fit.
func matchList(segments []segment, parts []string, i int, vars *[]pathVar) (int, bool) {
	for _, seg := range segments {
		switch seg.kind {
		case segLiteral:
			if i >= len(parts) || parts[i] != seg.literal {
				return 0, false
			}
			i++
		case segAnyOne:
			if i >= len(parts) {
				return 0, false
			}
			i++
		case segAnyRest:
			i = len(parts)
		case segCapture:
			start := i
			next, ok := matchList(seg.sub, parts, i, vars)
			if !ok {
				return 0, false
			}
			i = next
			// The captured value is everything the sub-pattern consumed. Each
			// component is decoded separately, then joined — so an encoded `%2F`
			// inside a component never reads as a separator.
			decoded := make([]string, 0, i-start)
			for _, p := range parts[start:i] {
				decoded = append(decoded, percentDecode(p))
			}
			*vars = append(*vars, pathVar{field: seg.field, value: strings.Join(decoded, "/")})
		}
	}
	return i, true
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
		// `custom { kind: "*" }` is HttpRule's "leave the HTTP method unspecified
		// for this rule" — it matches any verb, not a literal `*`.
		if b.httpMethod != want && b.httpMethod != "*" {
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
	return &httpCall{grpcMethod: b.grpcMethod, message: message, responseBody: b.responseBody}, true, nil
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

// fieldByAnyName resolves a field by its `.proto` name, falling back to its JSON
// (lowerCamelCase) name. URLs are written by hand — in an annotation template by
// the service author, in a query string by the caller — and both conventions turn
// up in practice, so both resolve. grpc-gateway does the same. The proto name wins
// on a collision, which can only happen if a message deliberately names one field
// like another's JSON name.
func fieldByAnyName(desc protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	fields := desc.Fields()
	if fd := fields.ByName(protoreflect.Name(name)); fd != nil {
		return fd
	}
	return fields.ByJSONName(name)
}

// setFromJSON sets one field by handing its protobuf-JSON text to the *decoder*,
// rather than converting by hand.
//
// This is the same move `response_body` makes on the way out, in reverse: the
// library already knows how every field shape is spelled in JSON — base64 for
// `bytes`, RFC 3339 for a `Timestamp`, `"a,b"` for a `FieldMask`, `"3.5s"` for a
// `Duration` — and re-deriving those rules by hand in two languages is how the
// implementations would drift. So build `{"<jsonName>": <text>}`, decode it into a
// scratch message, and lift the field across.
func setFromJSON(msg protoreflect.Message, fd protoreflect.FieldDescriptor, jsonText string) error {
	name, _ := json.Marshal(fd.JSONName())
	doc := []byte("{" + string(name) + ":" + jsonText + "}")
	scratch := dynamicpb.NewMessage(msg.Descriptor())
	if err := (protojson.UnmarshalOptions{}).Unmarshal(doc, scratch); err != nil {
		return fmt.Errorf("invalid value for %s: %w", fd.Name(), err)
	}
	msg.Set(fd, scratch.Get(fd))
	return nil
}

// wktJSONShape reports whether a message field has a canonical protobuf-JSON form
// a bare URL value can be bound to, and whether that form is a string (so the raw
// value needs quoting). These are the well-known types a REST URL realistically
// carries — `?update_mask=a,b`, `?ttl=3.5s`, `?since=2026-01-01T00:00:00Z`. Any
// other message is still refused: a query parameter cannot carry an arbitrary
// submessage.
func wktJSONShape(md protoreflect.MessageDescriptor) (quoted, ok bool) {
	switch md.FullName() {
	// Encoded as a JSON string — quote the raw value.
	case "google.protobuf.Timestamp", "google.protobuf.Duration", "google.protobuf.FieldMask",
		"google.protobuf.StringValue", "google.protobuf.BytesValue",
		"google.protobuf.Int64Value", "google.protobuf.UInt64Value":
		return true, true
	// Encoded as a bare JSON number/bool — pass the raw value through.
	case "google.protobuf.BoolValue", "google.protobuf.Int32Value", "google.protobuf.UInt32Value",
		"google.protobuf.FloatValue", "google.protobuf.DoubleValue":
		return false, true
	}
	return false, false
}

// setMessageField parses a JSON body into the top-level field `name`.
func setMessageField(msg protoreflect.Message, name string, body []byte) error {
	fd := fieldByAnyName(msg.Descriptor(), name)
	if fd == nil {
		return fmt.Errorf("unknown body field: %s", name)
	}
	// Any top-level field, not just a message: `body: "content"` on a `string` takes
	// a JSON string, `body: "items"` on a repeated field takes an array. HttpRule
	// requires only that the field be top-level.
	return setFromJSON(msg, fd, string(body))
}

// setByPath sets a (possibly nested, possibly repeated) scalar field from a
// string value, walking a dotted field path.
func setByPath(msg protoreflect.Message, path []string, raw string) error {
	fd := fieldByAnyName(msg.Descriptor(), path[0])
	if fd == nil {
		return fmt.Errorf("unknown field: %s", path[0])
	}

	if len(path) == 1 {
		// Kinds with no scalar value form bind through the JSON decoder instead:
		// `bytes` (base64) and the string-shaped well-known types.
		switch {
		case fd.Kind() == protoreflect.BytesKind:
			quoted, _ := json.Marshal(raw)
			return setFromJSON(msg, fd, string(quoted))
		case (fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind) &&
			!fd.IsList() && !fd.IsMap():
			quote, ok := wktJSONShape(fd.Message())
			if !ok {
				return fmt.Errorf("cannot bind a path/query value to message field %s (%s)",
					fd.Name(), fd.Message().FullName())
			}
			text := raw
			if quote {
				b, _ := json.Marshal(raw)
				text = string(b)
			}
			return setFromJSON(msg, fd, text)
		}
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
	// Unreachable: setByPath routes this to the JSON decoder above. Kept so the
	// switch stays exhaustive over the scalar kinds rather than falling through.
	case protoreflect.BytesKind:
		return protoreflect.Value{}, fmt.Errorf("bytes must bind through the JSON decoder")
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

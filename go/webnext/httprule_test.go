// Unit tests for the `google.api.http` router, in-package so they can reach the
// template parser, the matcher, and the scalar coercion table directly.
//
// The end-to-end tests (httprule_e2e_test.go) prove the two annotated URLs on
// echo.proto work; these pin the edges that no realistic annotation reaches —
// and, at the bottom, the *unsupported* HttpRule features, so doc/HTTPRULE_GAPS.md
// is backed by executable statements of what actually happens rather than prose.

package webnext

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/grpc-webnext/grpc-webnext/go/internal/protoset"
	testechopb "github.com/grpc-webnext/grpc-webnext/go/internal/testecho/testechopb"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	// Registered so the well-known-type fixture below can resolve its imports.
	// Production pools get these from the user's own descriptor set, which
	// `protoc --include_imports` carries.
	_ "google.golang.org/protobuf/types/known/durationpb"
	_ "google.golang.org/protobuf/types/known/fieldmaskpb"
	_ "google.golang.org/protobuf/types/known/timestamppb"
	_ "google.golang.org/protobuf/types/known/wrapperspb"
)

// --- fixture ----------------------------------------------------------------

// fixtureMessage is a synthetic request message covering every field shape the
// binder has a branch for: each scalar kind, an enum, bytes, a repeated field,
// and a nested message (for dotted paths and `body: "<field>"`).
func fixtureMessage(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	field := func(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name: proto.String(name), Number: proto.Int32(number), Label: &optional, Type: &kind,
		}
	}
	typed := func(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type, typeName string) *descriptorpb.FieldDescriptorProto {
		f := field(name, number, kind)
		f.TypeName = proto.String(typeName)
		return f
	}

	tags := field("tags", 20, descriptorpb.FieldDescriptorProto_TYPE_STRING)
	tags.Label = &repeated

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("httprule_fixture.proto"),
		Package: proto.String("webnext.httprule.test"),
		Syntax:  proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Color"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("COLOR_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: proto.String("RED"), Number: proto.Int32(1)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name:  proto.String("Nested"),
				Field: []*descriptorpb.FieldDescriptorProto{field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING)},
			},
			{
				Name: proto.String("Req"),
				Field: []*descriptorpb.FieldDescriptorProto{
					field("name", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
					field("count", 2, descriptorpb.FieldDescriptorProto_TYPE_UINT32),
					field("big", 3, descriptorpb.FieldDescriptorProto_TYPE_INT64),
					field("ratio", 4, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE),
					field("flag", 5, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
					field("blob", 6, descriptorpb.FieldDescriptorProto_TYPE_BYTES),
					field("some_field", 9, descriptorpb.FieldDescriptorProto_TYPE_STRING),
					typed("color", 7, descriptorpb.FieldDescriptorProto_TYPE_ENUM, ".webnext.httprule.test.Color"),
					typed("nested", 8, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".webnext.httprule.test.Nested"),
					tags,
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("build fixture descriptor: %v", err)
	}
	return fd.Messages().ByName("Req")
}

// build runs a binding end to end and returns the decoded request message.
func build(t *testing.T, b *binding, path, query string, body string) protoreflect.Message {
	t.Helper()
	vars, ok := matchSegments(b.segments, b.customVerb, path)
	if !ok {
		t.Fatalf("path %q did not match the template", path)
	}
	encoded, err := buildMessage(b, vars, query, []byte(body))
	if err != nil {
		t.Fatalf("build message: %v", err)
	}
	msg := dynamicpb.NewMessage(b.input)
	if err := proto.Unmarshal(encoded, msg); err != nil {
		t.Fatalf("decode built message: %v", err)
	}
	return msg
}

// tmpl compiles a verb-less template for a test binding.
func tmpl(s string) []segment {
	segments, _ := parseTemplate(s)
	return segments
}

func str(msg protoreflect.Message, name string) string {
	return msg.Get(msg.Descriptor().Fields().ByName(protoreflect.Name(name))).String()
}

// --- path templates ----------------------------------------------------------

func TestParseTemplate(t *testing.T) {
	// A capture is `{field=<sub-pattern>}`; `{f}` is sugar for `{f=*}`. The table
	// checks the segment kinds plus, for captures, the field path and the shape of
	// the sub-pattern — which is what distinguishes `{f}` from `{f=**}` now that
	// both are the same kind.
	cases := []struct {
		template string
		kinds    []segmentKind
		fields   [][]string // captures only, in order
		subKinds [][]segmentKind
	}{
		{"/v1/echo", []segmentKind{segLiteral, segLiteral}, nil, nil},
		{"/v1/echo/{message}",
			[]segmentKind{segLiteral, segLiteral, segCapture},
			[][]string{{"message"}},
			[][]segmentKind{{segAnyOne}}},
		{"/v1/{a=*}", []segmentKind{segLiteral, segCapture}, [][]string{{"a"}}, [][]segmentKind{{segAnyOne}}},
		{"/v1/{a=**}", []segmentKind{segLiteral, segCapture}, [][]string{{"a"}}, [][]segmentKind{{segAnyRest}}},
		// A dotted capture binds a nested field.
		{"/v1/{user.id}", []segmentKind{segLiteral, segCapture}, [][]string{{"user", "id"}}, [][]segmentKind{{segAnyOne}}},
		// A multi-segment sub-pattern survives the split, which is the whole point.
		{"/v1/{name=shelves/*/books/*}",
			[]segmentKind{segLiteral, segCapture},
			[][]string{{"name"}},
			[][]segmentKind{{segLiteral, segAnyOne, segLiteral, segAnyOne}}},
		// A trailing `:verb` is not part of the path.
		{"/v1/things/{id}:cancel",
			[]segmentKind{segLiteral, segLiteral, segCapture},
			[][]string{{"id"}},
			[][]segmentKind{{segAnyOne}}},
		// Bare wildcards, and redundant slashes collapsing.
		{"/v1/*/things/**", []segmentKind{segLiteral, segAnyOne, segLiteral, segAnyRest}, nil, nil},
		{"//v1//echo//", []segmentKind{segLiteral, segLiteral}, nil, nil},
	}
	for _, tc := range cases {
		got, _ := parseTemplate(tc.template)
		if len(got) != len(tc.kinds) {
			t.Errorf("%q: %d segments, want %d (%+v)", tc.template, len(got), len(tc.kinds), got)
			continue
		}
		var captures int
		for i, seg := range got {
			if seg.kind != tc.kinds[i] {
				t.Errorf("%q segment %d kind = %v, want %v", tc.template, i, seg.kind, tc.kinds[i])
				continue
			}
			if seg.kind != segCapture {
				continue
			}
			if captures >= len(tc.fields) {
				t.Errorf("%q: more captures than expected", tc.template)
				break
			}
			if strings.Join(seg.field, ".") != strings.Join(tc.fields[captures], ".") {
				t.Errorf("%q capture %d field = %v, want %v", tc.template, captures, seg.field, tc.fields[captures])
			}
			want := tc.subKinds[captures]
			if len(seg.sub) != len(want) {
				t.Errorf("%q capture %d sub = %+v, want %d segments", tc.template, captures, seg.sub, len(want))
			} else {
				for j := range seg.sub {
					if seg.sub[j].kind != want[j] {
						t.Errorf("%q capture %d sub[%d] = %v, want %v", tc.template, captures, j, seg.sub[j].kind, want[j])
					}
				}
			}
			captures++
		}
	}
}

func TestMatchSegments(t *testing.T) {
	single, _ := parseTemplate("/v1/echo/{message}")
	rest, _ := parseTemplate("/v1/files/{path=**}")

	if _, ok := matchSegments(single, "", "/v1/echo/hi"); !ok {
		t.Error("/v1/echo/hi should match /v1/echo/{message}")
	}
	// A capture is exactly one segment: neither a missing nor an extra one matches.
	if _, ok := matchSegments(single, "", "/v1/echo"); ok {
		t.Error("/v1/echo should not match a template with a required capture")
	}
	if _, ok := matchSegments(single, "", "/v1/echo/a/b"); ok {
		t.Error("/v1/echo/a/b should not match a single-segment capture")
	}
	// `**` swallows the remainder, slashes included.
	vars, ok := matchSegments(rest, "", "/v1/files/a/b/c.txt")
	if !ok || len(vars) != 1 || vars[0].value != "a/b/c.txt" {
		t.Errorf("rest capture = %+v (ok=%v), want a/b/c.txt", vars, ok)
	}
	// Each segment is percent-decoded after splitting, so an encoded slash stays
	// inside its segment rather than becoming a separator.
	vars, ok = matchSegments(single, "", "/v1/echo/a%2Fb")
	if !ok || len(vars) != 1 || vars[0].value != "a/b" {
		t.Errorf("percent-decoded capture = %+v (ok=%v), want a/b", vars, ok)
	}
}

// A trailing `:verb` is matched, not merely stripped from the template. Without
// that, `{id}:cancel` would also answer the bare resource URL and would capture
// `id = "5:cancel"` — and every custom verb on one resource would collide.
func TestMatchSegmentsCustomVerb(t *testing.T) {
	cancel, verb := parseTemplate("/v1/things/{id}:cancel")
	if verb != "cancel" {
		t.Fatalf("custom verb = %q, want cancel", verb)
	}

	// The verb must be present, and it does not leak into the captured variable.
	vars, ok := matchSegments(cancel, verb, "/v1/things/5:cancel")
	if !ok || len(vars) != 1 || vars[0].value != "5" {
		t.Errorf("captured %+v (ok=%v), want id=5", vars, ok)
	}
	// The bare resource URL belongs to a different binding.
	if _, ok := matchSegments(cancel, verb, "/v1/things/5"); ok {
		t.Error("/v1/things/5 must not match a :cancel binding")
	}
	// A different verb on the same resource is a different route.
	if _, ok := matchSegments(cancel, verb, "/v1/things/5:archive"); ok {
		t.Error("/v1/things/5:archive must not match a :cancel binding")
	}
	// And symmetrically, a verb-less binding does not swallow a custom-verb URL.
	plain, plainVerb := parseTemplate("/v1/things/{id}")
	if _, ok := matchSegments(plain, plainVerb, "/v1/things/5:cancel"); ok {
		t.Error("a verb-less binding must not match /v1/things/5:cancel")
	}
	// A percent-encoded colon is data, not a verb separator, so it still binds.
	vars, ok = matchSegments(plain, plainVerb, "/v1/things/urn%3Afoo")
	if !ok || len(vars) != 1 || vars[0].value != "urn:foo" {
		t.Errorf("captured %+v (ok=%v), want id=urn:foo", vars, ok)
	}
}

// --- binding -----------------------------------------------------------------

func TestBuildMessagePathAndQuery(t *testing.T) {
	input := fixtureMessage(t)
	b := &binding{
		httpMethod: "GET",
		segments:   tmpl("/v1/things/{name}"),
		body:       bodyNone,
		grpcMethod: "/test.Svc/Get",
		input:      input,
	}

	msg := build(t, b, "/v1/things/widget", "count=7&flag=true&ratio=1.5&color=RED&big=-9", "")
	if got := str(msg, "name"); got != "widget" {
		t.Errorf("name = %q, want widget", got)
	}
	fields := input.Fields()
	if got := msg.Get(fields.ByName("count")).Uint(); got != 7 {
		t.Errorf("count = %d, want 7", got)
	}
	if got := msg.Get(fields.ByName("flag")).Bool(); !got {
		t.Error("flag = false, want true")
	}
	if got := msg.Get(fields.ByName("ratio")).Float(); got != 1.5 {
		t.Errorf("ratio = %v, want 1.5", got)
	}
	// An enum binds by name or by number.
	if got := msg.Get(fields.ByName("color")).Enum(); got != 1 {
		t.Errorf("color = %d, want 1", got)
	}
	if got := msg.Get(fields.ByName("big")).Int(); got != -9 {
		t.Errorf("big = %d, want -9", got)
	}
}

// A repeated field takes one value per query param, in order.
func TestBuildMessageRepeatedQueryParam(t *testing.T) {
	input := fixtureMessage(t)
	b := &binding{httpMethod: "GET", segments: tmpl("/v1/things"), body: bodyNone, input: input}

	msg := build(t, b, "/v1/things", "tags=a&tags=b", "")
	list := msg.Get(input.Fields().ByName("tags")).List()
	if list.Len() != 2 || list.Get(0).String() != "a" || list.Get(1).String() != "b" {
		t.Errorf("tags = %v, want [a b]", list)
	}
}

// A dotted query key binds a nested field, same as a dotted path capture.
func TestBuildMessageDottedQueryKey(t *testing.T) {
	input := fixtureMessage(t)
	b := &binding{httpMethod: "GET", segments: tmpl("/v1/things"), body: bodyNone, input: input}

	msg := build(t, b, "/v1/things", "nested.id=abc", "")
	nested := msg.Get(input.Fields().ByName("nested")).Message()
	if got := str(nested, "id"); got != "abc" {
		t.Errorf("nested.id = %q, want abc", got)
	}
}

// Precedence, per spec/PROTOCOL.md "REST binding precedence": with `body: "*"`
// the body IS the message, so query params are ignored entirely — but path
// variables still overlay it.
func TestBuildMessageWildcardBodyIgnoresQueryButNotPath(t *testing.T) {
	input := fixtureMessage(t)
	b := &binding{
		httpMethod: "POST",
		segments:   tmpl("/v1/things/{name}"),
		body:       bodyWildcard,
		input:      input,
	}

	msg := build(t, b, "/v1/things/frompath", "count=9", `{"name":"frombody","count":3}`)
	if got := str(msg, "name"); got != "frompath" {
		t.Errorf("name = %q, want frompath (the path overlays the body)", got)
	}
	if got := msg.Get(input.Fields().ByName("count")).Uint(); got != 3 {
		t.Errorf("count = %d, want 3 (the query is ignored under a wildcard body)", got)
	}
}

// For a non-wildcard body, a query param naming a field a path variable already
// set is skipped rather than overwriting it.
func TestBuildMessageQueryDoesNotOverridePathVar(t *testing.T) {
	input := fixtureMessage(t)
	b := &binding{httpMethod: "GET", segments: tmpl("/v1/things/{name}"), body: bodyNone, input: input}

	msg := build(t, b, "/v1/things/frompath", "name=fromquery", "")
	if got := str(msg, "name"); got != "frompath" {
		t.Errorf("name = %q, want frompath", got)
	}
}

// `body: "<field>"` parses the body into that one (message-typed) field.
func TestBuildMessageBodyField(t *testing.T) {
	input := fixtureMessage(t)
	b := &binding{
		httpMethod: "POST",
		segments:   tmpl("/v1/things/{name}"),
		body:       bodyField,
		bodyField:  "nested",
		input:      input,
	}

	msg := build(t, b, "/v1/things/w", "", `{"id":"inner"}`)
	if got := str(msg, "name"); got != "w" {
		t.Errorf("name = %q, want w", got)
	}
	nested := msg.Get(input.Fields().ByName("nested")).Message()
	if got := str(nested, "id"); got != "inner" {
		t.Errorf("nested.id = %q, want inner", got)
	}
}

// --- coercion errors ---------------------------------------------------------

func TestBindingErrors(t *testing.T) {
	input := fixtureMessage(t)
	newBinding := func(template string, kind bodyKind) *binding {
		return &binding{httpMethod: "GET", segments: tmpl(template), body: kind, input: input}
	}

	cases := []struct {
		name    string
		binding *binding
		path    string
		query   string
		body    string
	}{
		{"unknown field", newBinding("/v1/{nope}", bodyNone), "/v1/x", "", ""},
		{"non-numeric number", newBinding("/v1/things", bodyNone), "/v1/things", "count=abc", ""},
		{"out-of-range uint32", newBinding("/v1/things", bodyNone), "/v1/things", "count=4294967296", ""},
		{"negative uint32", newBinding("/v1/things", bodyNone), "/v1/things", "count=-1", ""},
		{"non-boolean bool", newBinding("/v1/things", bodyNone), "/v1/things", "flag=yes", ""},
		{"unknown enum value", newBinding("/v1/things", bodyNone), "/v1/things", "color=MAUVE", ""},
		// Bytes DO bind now (base64), so the error case is malformed base64.
		{"malformed base64 bytes", newBinding("/v1/things", bodyNone), "/v1/things", "blob=!!!", ""},
		// A scalar cannot land on a message field, nor a dotted path traverse a scalar.
		{"scalar onto message", newBinding("/v1/{nested}", bodyNone), "/v1/x", "", ""},
		{"path through a scalar", newBinding("/v1/{name.more}", bodyNone), "/v1/x", "", ""},
		{"malformed json body", newBinding("/v1/things", bodyWildcard), "/v1/things", "", `{"name":`},
		{"unknown json field", newBinding("/v1/things", bodyWildcard), "/v1/things", "", `{"nope":1}`},
	}
	for _, tc := range cases {
		vars, ok := matchSegments(tc.binding.segments, tc.binding.customVerb, tc.path)
		if !ok {
			t.Errorf("%s: path %q did not match", tc.name, tc.path)
			continue
		}
		if _, err := buildMessage(tc.binding, vars, tc.query, []byte(tc.body)); err == nil {
			t.Errorf("%s: expected an error, got none", tc.name)
		}
	}
}

// --- query parsing -----------------------------------------------------------

func TestParseQuery(t *testing.T) {
	got := parseQuery("a=1&b=hello+world&c=%2Fslash&bare&d=")
	want := []queryParam{
		{"a", "1"},
		{"b", "hello world"}, // `+` is a space in a query string
		{"c", "/slash"},
		{"bare", ""}, // a valueless key binds the empty string
		{"d", ""},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d params, want %d (%+v)", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("param %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// --- rule compilation --------------------------------------------------------

// compile runs one HttpRule through the router's compiler and returns the
// bindings it produced.
func compile(t *testing.T, rule *annotations.HttpRule) []*binding {
	t.Helper()
	r := &httpRouter{}
	r.collect(rule, "/test.Svc/Method", fixtureMessage(t))
	return r.bindings
}

func TestRouterCompilesVerbs(t *testing.T) {
	cases := []struct {
		rule *annotations.HttpRule
		verb string
		path string
	}{
		{&annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "/v1/a"}}, "GET", "/v1/a"},
		{&annotations.HttpRule{Pattern: &annotations.HttpRule_Put{Put: "/v1/a"}}, "PUT", "/v1/a"},
		{&annotations.HttpRule{Pattern: &annotations.HttpRule_Post{Post: "/v1/a"}}, "POST", "/v1/a"},
		{&annotations.HttpRule{Pattern: &annotations.HttpRule_Delete{Delete: "/v1/a"}}, "DELETE", "/v1/a"},
		{&annotations.HttpRule{Pattern: &annotations.HttpRule_Patch{Patch: "/v1/a"}}, "PATCH", "/v1/a"},
		// A custom verb is upper-cased; its path may be anything.
		{&annotations.HttpRule{Pattern: &annotations.HttpRule_Custom{
			Custom: &annotations.CustomHttpPattern{Kind: "watch", Path: "/v1/a"},
		}}, "WATCH", "/v1/a"},
		// `*` is HttpRule's "any HTTP method", not a literal verb — see
		// TestCustomKindStarMatchesAnyVerb.
		{&annotations.HttpRule{Pattern: &annotations.HttpRule_Custom{
			Custom: &annotations.CustomHttpPattern{Kind: "*", Path: "/v1/a"},
		}}, "*", "/v1/a"},
	}
	for _, tc := range cases {
		got := compile(t, tc.rule)
		if len(got) != 1 {
			t.Errorf("%s: got %d bindings, want 1", tc.verb, len(got))
			continue
		}
		if got[0].httpMethod != tc.verb {
			t.Errorf("verb = %q, want %q", got[0].httpMethod, tc.verb)
		}
		if _, ok := matchSegments(got[0].segments, got[0].customVerb, tc.path); !ok {
			t.Errorf("%s: compiled template does not match %q", tc.verb, tc.path)
		}
	}
}

// A rule with no pattern (or a custom rule with no kind) compiles to nothing
// rather than to a binding that matches everything.
func TestRouterSkipsPatternlessRules(t *testing.T) {
	if got := compile(t, &annotations.HttpRule{}); len(got) != 0 {
		t.Errorf("empty rule produced %d bindings, want 0", len(got))
	}
	got := compile(t, &annotations.HttpRule{Pattern: &annotations.HttpRule_Custom{
		Custom: &annotations.CustomHttpPattern{Path: "/v1/a"},
	}})
	if len(got) != 0 {
		t.Errorf("kind-less custom rule produced %d bindings, want 0", len(got))
	}
}

func TestRouterCompilesAdditionalBindings(t *testing.T) {
	got := compile(t, &annotations.HttpRule{
		Pattern: &annotations.HttpRule_Post{Post: "/v1/things"},
		Body:    "*",
		AdditionalBindings: []*annotations.HttpRule{
			{Pattern: &annotations.HttpRule_Get{Get: "/v1/things/{name}"}},
		},
	})
	if len(got) != 2 {
		t.Fatalf("got %d bindings, want 2", len(got))
	}
	if got[0].httpMethod != "POST" || got[0].body != bodyWildcard {
		t.Errorf("primary binding = %s body %v, want POST wildcard", got[0].httpMethod, got[0].body)
	}
	// An additional binding carries its own verb, template, and body rule — it
	// does not inherit the parent's `body: "*"`.
	if got[1].httpMethod != "GET" || got[1].body != bodyNone {
		t.Errorf("additional binding = %s body %v, want GET none", got[1].httpMethod, got[1].body)
	}
}

// --- unsupported HttpRule features -------------------------------------------
//
// These do not assert desirable behavior; they pin the CURRENT behavior of the
// features doc/HTTPRULE_GAPS.md lists as unsupported, so the gaps are visible
// and a future implementation has a test that must change.

// A capture may span several segments — `{name=shelves/*/books/*}`, the canonical
// Google resource-name shape. The captured value is everything the sub-pattern
// consumed, joined by `/`. Rust is pinned by the same case
// (`httprule.rs::tests::multi_segment_capture`).
func TestMultiSegmentCapture(t *testing.T) {
	segments, verb := parseTemplate("/v1/{name=shelves/*/books/*}")
	vars, ok := matchSegments(segments, verb, "/v1/shelves/1/books/2")
	if !ok || len(vars) != 1 || vars[0].value != "shelves/1/books/2" {
		t.Fatalf("captured %+v (ok=%v), want name=shelves/1/books/2", vars, ok)
	}

	// The sub-pattern is a pattern, not a prefix: the shape has to line up.
	for _, path := range []string{"/v1/shelves/1/books", "/v1/shelves/1/books/2/3", "/v1/racks/1/books/2"} {
		if _, ok := matchSegments(segments, verb, path); ok {
			t.Errorf("%s should not match /v1/{name=shelves/*/books/*}", path)
		}
	}

	// A trailing `**` inside a capture takes the remainder.
	segments, verb = parseTemplate("/v1/{name=files/**}")
	vars, ok = matchSegments(segments, verb, "/v1/files/a/b/c.txt")
	if !ok || len(vars) != 1 || vars[0].value != "files/a/b/c.txt" {
		t.Errorf("captured %+v (ok=%v), want name=files/a/b/c.txt", vars, ok)
	}

	// A custom verb still binds after a multi-segment capture, which is only
	// possible because the `:` split ignores colons inside braces.
	segments, verb = parseTemplate("/v1/{name=shelves/*}:archive")
	if verb != "archive" {
		t.Fatalf("custom verb = %q, want archive", verb)
	}
	vars, ok = matchSegments(segments, verb, "/v1/shelves/7:archive")
	if !ok || len(vars) != 1 || vars[0].value != "shelves/7" {
		t.Errorf("captured %+v (ok=%v), want name=shelves/7", vars, ok)
	}
}

// `additional_bindings` nest only one level deep: the HttpRule spec forbids
// nesting them at all, and this compiler silently drops any that appear.
func TestUnsupportedNestedAdditionalBindings(t *testing.T) {
	got := compile(t, &annotations.HttpRule{
		Pattern: &annotations.HttpRule_Post{Post: "/v1/things"},
		AdditionalBindings: []*annotations.HttpRule{{
			Pattern: &annotations.HttpRule_Get{Get: "/v1/things/{name}"},
			AdditionalBindings: []*annotations.HttpRule{
				{Pattern: &annotations.HttpRule_Delete{Delete: "/v1/things/{name}/deep"}},
			},
		}},
	})
	if len(got) != 2 {
		t.Fatalf("got %d bindings, want 2 (the nested one is dropped)", len(got))
	}
	for _, b := range got {
		if b.httpMethod == "DELETE" {
			t.Error("a second-level additional_binding was compiled; if that became " +
				"supported, update doc/HTTPRULE_GAPS.md and this test")
		}
	}
}

// `body: "<field>"` may name ANY top-level field, not only a singular message:
// HttpRule requires only that the field be top-level. The JSON decoder does the
// work, so each shape is spelled exactly as protobuf-JSON spells it.
func TestBodyMayNameAnyTopLevelField(t *testing.T) {
	input := fixtureMessage(t)

	// A scalar field takes that scalar's JSON.
	msg := dynamicpb.NewMessage(input)
	if err := setMessageField(msg, "name", []byte(`"hello"`)); err != nil {
		t.Fatalf("scalar body field: %v", err)
	}
	if got := str(msg, "name"); got != "hello" {
		t.Errorf("name = %q, want hello", got)
	}

	// A repeated field takes an array.
	msg = dynamicpb.NewMessage(input)
	if err := setMessageField(msg, "tags", []byte(`["a","b"]`)); err != nil {
		t.Fatalf("repeated body field: %v", err)
	}
	list := msg.Get(input.Fields().ByName("tags")).List()
	if list.Len() != 2 || list.Get(0).String() != "a" || list.Get(1).String() != "b" {
		t.Errorf("tags = %v, want [a b]", list)
	}

	// A message field still takes an object, as before.
	msg = dynamicpb.NewMessage(input)
	if err := setMessageField(msg, "nested", []byte(`{"id":"inner"}`)); err != nil {
		t.Fatalf("message body field: %v", err)
	}
	if got := str(msg.Get(input.Fields().ByName("nested")).Message(), "id"); got != "inner" {
		t.Errorf("nested.id = %q, want inner", got)
	}

	// A dotted path is NOT a top-level field, and the HttpRule spec says the
	// referred field must be one — so this stays refused, by the spec, not by
	// omission.
	msg = dynamicpb.NewMessage(input)
	if err := setMessageField(msg, "nested.id", []byte(`"x"`)); err == nil {
		t.Error("a dotted body path should be refused: HttpRule requires a top-level field")
	}
}

// A query param cannot carry an arbitrary submessage — but the well-known types
// that a REST URL realistically carries have a canonical string form, and those
// bind through the JSON decoder rather than a hand-rolled parser.
func TestQueryBindsWellKnownTypesButNotArbitraryMessages(t *testing.T) {
	input := fixtureMessage(t)

	// An arbitrary message is still refused, and says which type it refused.
	msg := dynamicpb.NewMessage(input)
	err := setByPath(msg, []string{"nested"}, `{"id":"x"}`)
	if err == nil {
		t.Fatal("binding an arbitrary message from a query should be refused")
	}
	if !strings.Contains(err.Error(), "Nested") {
		t.Errorf("error %q should name the offending message type", err)
	}
	// The supported spelling for the same intent:
	if err := setByPath(msg, []string{"nested", "id"}, "x"); err != nil {
		t.Errorf("dotted scalar binding should work: %v", err)
	}

	// `bytes` binds as base64 — protobuf-JSON's spelling, so no guessing.
	msg = dynamicpb.NewMessage(input)
	if err := setByPath(msg, []string{"blob"}, "AQID"); err != nil {
		t.Fatalf("bytes from query: %v", err)
	}
	if got := msg.Get(input.Fields().ByName("blob")).Bytes(); string(got) != "\x01\x02\x03" {
		t.Errorf("blob = %v, want [1 2 3]", got)
	}
	if err := setByPath(msg, []string{"blob"}, "!!!"); err == nil {
		t.Error("malformed base64 should be refused")
	}
}

// The well-known types themselves, which need a descriptor pool that has them.
// `?update_mask=a,b` is the canonical case and the one most likely to be hit.
func TestQueryBindsWellKnownTypes(t *testing.T) {
	md := wktFixture(t)
	fields := md.Fields()

	cases := []struct{ field, raw, want string }{
		{"mask", "a,b.c", "a,b.c"},
		{"ttl", "3.500s", "3.500s"},
		{"at", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"},
		{"note", "hi", `"hi"`},
		{"count", "7", "7"},
		{"on", "true", "true"},
	}
	for _, tc := range cases {
		msg := dynamicpb.NewMessage(md)
		if err := setByPath(msg, []string{tc.field}, tc.raw); err != nil {
			t.Errorf("%s=%q: %v", tc.field, tc.raw, err)
			continue
		}
		// Round-trip through protojson: the value must come back as the same
		// canonical spelling it went in as.
		encoded, err := (protojson.MarshalOptions{}).Marshal(msg)
		if err != nil {
			t.Errorf("%s: marshal: %v", tc.field, err)
			continue
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &obj); err != nil {
			t.Fatalf("%s: %v", tc.field, err)
		}
		got := strings.Trim(string(obj[fields.ByName(protoreflect.Name(tc.field)).JSONName()]), `"`)
		if want := strings.Trim(tc.want, `"`); got != want {
			t.Errorf("%s=%q round-tripped as %q, want %q", tc.field, tc.raw, got, want)
		}
	}

	// A malformed value for a well-known type is an error, not a silent default.
	msg := dynamicpb.NewMessage(md)
	if err := setByPath(msg, []string{"at"}, "not-a-timestamp"); err == nil {
		t.Error("a malformed Timestamp should be refused")
	}
}

// wktFixture is a message with one field per well-known type a URL can carry.
func wktFixture(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	msgKind := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	field := func(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name: proto.String(name), Number: proto.Int32(number),
			Label: &optional, Type: &msgKind, TypeName: proto.String(typeName),
		}
	}
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("wkt_fixture.proto"),
		Package: proto.String("webnext.wkt.test"),
		Syntax:  proto.String("proto3"),
		Dependency: []string{
			"google/protobuf/field_mask.proto",
			"google/protobuf/duration.proto",
			"google/protobuf/timestamp.proto",
			"google/protobuf/wrappers.proto",
		},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Req"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("mask", 1, ".google.protobuf.FieldMask"),
				field("ttl", 2, ".google.protobuf.Duration"),
				field("at", 3, ".google.protobuf.Timestamp"),
				field("note", 4, ".google.protobuf.StringValue"),
				field("count", 5, ".google.protobuf.Int32Value"),
				field("on", 6, ".google.protobuf.BoolValue"),
			},
		}},
	}
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build wkt fixture: %v", err)
	}
	return fd.Messages().ByName("Req")
}

// Path captures and query keys resolve by `.proto` name OR by JSON
// (lowerCamelCase) name — both conventions turn up in hand-written URLs, and
// grpc-gateway accepts both. Shared with Rust, so the two implementations agree.
func TestFieldNamesResolveByProtoOrJSONName(t *testing.T) {
	input := fixtureMessage(t)

	for _, name := range []string{"some_field", "someField"} {
		msg := dynamicpb.NewMessage(input)
		if err := setByPath(msg, []string{name}, "x"); err != nil {
			t.Errorf("%q should bind: %v", name, err)
			continue
		}
		if got := str(msg, "some_field"); got != "x" {
			t.Errorf("%q bound %q, want x", name, got)
		}
	}
	// A name that is neither still fails.
	msg := dynamicpb.NewMessage(input)
	if err := setByPath(msg, []string{"someFieldd"}, "x"); err == nil {
		t.Error("an unknown field should still be rejected")
	}
}

// Bare `*` and `**` segments — unnamed wildcards from the HttpRule grammar —
// match without capturing anything.
func TestBareWildcardSegments(t *testing.T) {
	one, _ := parseTemplate("/v1/*/things/{id}")
	if one[1].kind != segAnyOne {
		t.Fatalf("segment 1 = %v, want a single-segment wildcard", one[1].kind)
	}
	vars, ok := matchSegments(one, "", "/v1/anything/things/7")
	if !ok || len(vars) != 1 || vars[0].value != "7" {
		t.Errorf("captured %+v (ok=%v), want only id=7", vars, ok)
	}
	// It consumes exactly one segment: neither zero nor two.
	if _, ok := matchSegments(one, "", "/v1/things/7"); ok {
		t.Error("a bare `*` must not match zero segments")
	}
	if _, ok := matchSegments(one, "", "/v1/a/b/things/7"); ok {
		t.Error("a bare `*` must not match two segments")
	}

	rest, _ := parseTemplate("/v1/things/{id}/**")
	if rest[3].kind != segAnyRest {
		t.Fatalf("segment 3 = %v, want a rest wildcard", rest[3].kind)
	}
	vars, ok = matchSegments(rest, "", "/v1/things/7/a/b/c")
	if !ok || len(vars) != 1 || vars[0].value != "7" {
		t.Errorf("captured %+v (ok=%v), want only id=7", vars, ok)
	}
	// `**` also matches the empty remainder.
	if _, ok := matchSegments(rest, "", "/v1/things/7"); !ok {
		t.Error("a trailing `**` should match an empty remainder")
	}
}

// --- response_body ------------------------------------------------------------

// The zero-value table is the ONE place protobuf-JSON's rules are restated by
// hand (whole-message encoding skips defaults, so lifting a member out can come up
// empty). It is therefore the likeliest place for the two implementations to
// drift, and every kind is pinned here — with `json_zero` in
// rust/.../src/transcode.rs asserting the identical table.
func TestJSONZeroPerKind(t *testing.T) {
	input := fixtureMessage(t)
	fields := input.Fields()

	cases := map[string]string{
		"name":       `""`,                  // string
		"count":      `0`,                   // uint32
		"big":        `"0"`,                 // int64 — a JSON *string* in protobuf-JSON
		"ratio":      `0`,                   // double
		"flag":       `false`,               // bool
		"blob":       `""`,                  // bytes — the empty base64 string
		"color":      `"COLOR_UNSPECIFIED"`, // enum — by name, not number
		"nested":     `null`,                // an unset message was never there
		"tags":       `[]`,                  // repeated
		"some_field": `""`,
	}
	for name, want := range cases {
		fd := fields.ByName(protoreflect.Name(name))
		if fd == nil {
			t.Fatalf("fixture has no field %q", name)
		}
		if got := string(jsonZero(fd)); got != want {
			t.Errorf("jsonZero(%s) = %s, want %s", name, got, want)
		}
	}
}

// Extraction end to end: `response_body` returns the named field's value, encoded
// by the library's own rules, and falls back to the zero when the field is absent.
func TestResponseBodyExtraction(t *testing.T) {
	fds, err := testechoDescriptorSet()
	if err != nil {
		t.Fatalf("descriptor set: %v", err)
	}
	tc, err := NewTranscoder(fds)
	if err != nil {
		t.Fatalf("transcoder: %v", err)
	}
	const method = "/echo.v1.Echo/Unary" // returns EchoResponse{ message: string }

	// A populated field comes back as its bare JSON value, not wrapped.
	resp, err := proto.Marshal(dynamicResponse(t, tc, method, "hi"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := tc.ResponseProtoToJSONBody(method, resp, "message")
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if string(got) != `"hi"` {
		t.Errorf("body = %s, want \"hi\"", got)
	}

	// The whole message is still the default.
	if got, err = tc.ResponseProtoToJSON(method, resp); err != nil || string(got) != `{"message":"hi"}` {
		t.Errorf("whole message = %s (err %v), want {\"message\":\"hi\"}", got, err)
	}

	// An absent field falls back to its zero rather than vanishing: an empty
	// message encodes as `{}`, so there is no member to lift.
	empty, err := proto.Marshal(dynamicResponse(t, tc, method, ""))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got, err = tc.ResponseProtoToJSONBody(method, empty, "message"); err != nil || string(got) != `""` {
		t.Errorf("absent field = %s (err %v), want \"\"", got, err)
	}

	// A name that is no field at all is an error, not a silent whole-message answer.
	if _, err = tc.ResponseProtoToJSONBody(method, resp, "nope"); err == nil {
		t.Error("unknown response_body field should be an error")
	}
}

// dynamicResponse builds the method's response message with `message` set.
func dynamicResponse(t *testing.T, tc *Transcoder, method, message string) *dynamicpb.Message {
	t.Helper()
	m, ok := tc.method(method)
	if !ok {
		t.Fatalf("no method %s", method)
	}
	msg := dynamicpb.NewMessage(m.Output())
	fd := msg.Descriptor().Fields().ByName("message")
	if message != "" {
		msg.Set(fd, protoreflect.ValueOfString(message))
	}
	return msg
}

// testechoDescriptorSet is the echo.proto closure, used here for a real service's
// response types (the synthetic fixture above has no service to key a method on).
func testechoDescriptorSet() ([]byte, error) {
	return protoset.Marshal(testechopb.File_echo_proto)
}

// `custom { kind: "*" }` means "leave the HTTP method unspecified for this rule",
// so the binding answers every verb rather than a literal `*`. Rust is pinned by
// the same case (`httprule.rs::tests::custom_kind_star_matches_any_verb`).
func TestCustomKindStarMatchesAnyVerb(t *testing.T) {
	r := &httpRouter{}
	r.collect(&annotations.HttpRule{Pattern: &annotations.HttpRule_Custom{
		Custom: &annotations.CustomHttpPattern{Kind: "*", Path: "/v1/any"},
	}}, "/test.Svc/Any", fixtureMessage(t))

	for _, verb := range []string{"GET", "POST", "DELETE", "PATCH"} {
		if _, _, ok := r.matchRequest(verb, "/v1/any"); !ok {
			t.Errorf("%s /v1/any should match a `custom { kind: \"*\" }` binding", verb)
		}
	}
	// It is still a route, not a catch-all: the path must match.
	if _, _, ok := r.matchRequest("GET", "/v1/other"); ok {
		t.Error("a wildcard-method binding must still match on its path")
	}
}

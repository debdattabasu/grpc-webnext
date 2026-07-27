// Unit tests for the `google.api.http` router, in-package so they can reach the
// template parser, the matcher, and the scalar coercion table directly.
//
// The end-to-end tests (httprule_e2e_test.go) prove the two annotated URLs on
// echo.proto work; these pin the edges that no realistic annotation reaches —
// and, at the bottom, the *unsupported* HttpRule features, so doc/HTTPRULE_GAPS.md
// is backed by executable statements of what actually happens rather than prose.

package webnext

import (
	"testing"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
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
	cases := []struct {
		template string
		want     []segment
	}{
		{"/v1/echo", []segment{{kind: segLiteral, literal: "v1"}, {kind: segLiteral, literal: "echo"}}},
		{"/v1/echo/{message}", []segment{
			{kind: segLiteral, literal: "v1"},
			{kind: segLiteral, literal: "echo"},
			{kind: segSingle, field: []string{"message"}},
		}},
		// `{f=*}` is the explicit spelling of `{f}`.
		{"/v1/{a=*}", []segment{{kind: segLiteral, literal: "v1"}, {kind: segSingle, field: []string{"a"}}}},
		{"/v1/{a=**}", []segment{{kind: segLiteral, literal: "v1"}, {kind: segRest, field: []string{"a"}}}},
		// A dotted capture binds a nested field.
		{"/v1/{user.id}", []segment{{kind: segLiteral, literal: "v1"}, {kind: segSingle, field: []string{"user", "id"}}}},
		// A trailing `:verb` is not part of the path.
		{"/v1/things/{id}:cancel", []segment{
			{kind: segLiteral, literal: "v1"},
			{kind: segLiteral, literal: "things"},
			{kind: segSingle, field: []string{"id"}},
		}},
		// Redundant slashes collapse.
		{"//v1//echo//", []segment{{kind: segLiteral, literal: "v1"}, {kind: segLiteral, literal: "echo"}}},
	}
	for _, tc := range cases {
		got, _ := parseTemplate(tc.template)
		if len(got) != len(tc.want) {
			t.Errorf("%q: %d segments, want %d (%+v)", tc.template, len(got), len(tc.want), got)
			continue
		}
		for i := range got {
			if got[i].kind != tc.want[i].kind || got[i].literal != tc.want[i].literal {
				t.Errorf("%q segment %d = %+v, want %+v", tc.template, i, got[i], tc.want[i])
				continue
			}
			if len(got[i].field) != len(tc.want[i].field) {
				t.Errorf("%q segment %d field = %v, want %v", tc.template, i, got[i].field, tc.want[i].field)
				continue
			}
			for j := range got[i].field {
				if got[i].field[j] != tc.want[i].field[j] {
					t.Errorf("%q segment %d field = %v, want %v", tc.template, i, got[i].field, tc.want[i].field)
				}
			}
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
		// Bytes have no unambiguous path/query spelling, so binding one is refused
		// rather than guessed at (base64? raw? percent-decoded?).
		{"bytes from query", newBinding("/v1/things", bodyNone), "/v1/things", "blob=AQID", ""},
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

// A path pattern richer than a bare `*`/`**` — e.g. `{name=shelves/*/books/*}`,
// which the HttpRule spec allows — is not merely approximated: the template is
// split on `/` *before* braces are examined, so `{name=shelves` and `*}` become
// literal segments and the binding matches NOTHING it was meant to match.
//
// This is the sharpest of the gaps: it fails closed (a dead route) rather than
// mis-binding, but it fails silently at compile time. Rust splits in the same
// order and behaves identically, so the two implementations agree — see
// doc/HTTPRULE_GAPS.md.
func TestUnsupportedNestedPathPattern(t *testing.T) {
	segments := tmpl("/v1/{name=shelves/*/books/*}")

	if _, ok := matchSegments(segments, "", "/v1/shelves/1/books/2"); ok {
		t.Error("nested path patterns are unsupported; the intended URL unexpectedly matched — " +
			"if support was added, update doc/HTTPRULE_GAPS.md and this test")
	}
	// Nor does it degrade into a single-segment capture.
	if _, ok := matchSegments(segments, "", "/v1/anything"); ok {
		t.Error("nested pattern unexpectedly matched a single segment")
	}
	// What it compiles to: five literals, capturing nothing.
	if len(segments) != 5 {
		t.Fatalf("got %d segments, want 5 (%+v)", len(segments), segments)
	}
	for i, seg := range segments {
		if seg.kind != segLiteral {
			t.Errorf("segment %d is %v, want a literal", i, seg.kind)
		}
	}
}

// `response_body` is accepted and silently ignored: the binding compiles as if
// it were absent, so the whole response message comes back rather than the named
// sub-field. Ignoring it is a *wrong answer*, not a missing route — the loudest
// of the gaps in consequence, the quietest in symptom.
func TestUnsupportedResponseBody(t *testing.T) {
	got := compile(t, &annotations.HttpRule{
		Pattern:      &annotations.HttpRule_Get{Get: "/v1/things/{name}"},
		ResponseBody: "nested",
	})
	if len(got) != 1 {
		t.Fatalf("got %d bindings, want 1", len(got))
	}
	// Nothing in the compiled binding records it. If a `responseBody` member is
	// ever added, this test must change — and so must doc/HTTPRULE_GAPS.md.
	if got[0].httpMethod != "GET" || got[0].body != bodyNone {
		t.Errorf("binding = %s body %v, want GET none", got[0].httpMethod, got[0].body)
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

// A repeated field cannot be filled from a JSON body via `body: "<field>"`:
// the body must name a singular message field.
func TestUnsupportedRepeatedBodyField(t *testing.T) {
	input := fixtureMessage(t)
	msg := dynamicpb.NewMessage(input)
	if err := setMessageField(msg, "tags", []byte(`["a","b"]`)); err == nil {
		t.Error("expected repeated body fields to be refused; if support was added, " +
			"update doc/HTTPRULE_GAPS.md and this test")
	}
}

// Non-scalar query binding: a query param cannot carry a JSON message for a
// message-typed field — only its scalar leaves, via a dotted key.
func TestUnsupportedNonScalarQueryBinding(t *testing.T) {
	input := fixtureMessage(t)
	msg := dynamicpb.NewMessage(input)
	if err := setByPath(msg, []string{"nested"}, `{"id":"x"}`); err == nil {
		t.Error("expected message-typed query binding to be refused; if support was added, " +
			"update doc/HTTPRULE_GAPS.md and this test")
	}
	// The supported spelling for the same intent:
	if err := setByPath(msg, []string{"nested", "id"}, "x"); err != nil {
		t.Errorf("dotted scalar binding should work: %v", err)
	}
}

// Path captures and query keys match PROTO field names only, never their JSON
// (camelCase) spelling — grpc-gateway accepts both. Shared with Rust, so the two
// implementations agree; see doc/HTTPRULE_GAPS.md.
func TestUnsupportedJSONFieldNames(t *testing.T) {
	input := fixtureMessage(t)
	msg := dynamicpb.NewMessage(input)

	if err := setByPath(msg, []string{"some_field"}, "x"); err != nil {
		t.Errorf("the proto field name should bind: %v", err)
	}
	if err := setByPath(msg, []string{"someField"}, "x"); err == nil {
		t.Error("the JSON field name unexpectedly bound; if support was added, " +
			"update doc/HTTPRULE_GAPS.md and this test")
	}
}

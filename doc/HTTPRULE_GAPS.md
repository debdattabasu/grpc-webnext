# `google.api.http` — what is and isn't implemented

*Last updated 2026-07-28. Written alongside the Go REST implementation
([`go/webnext/httprule.go`](../go/webnext/httprule.go)), which is a deliberate port of
the Rust router ([`rust/crates/grpc-webnext/src/httprule.rs`](../rust/crates/grpc-webnext/src/httprule.rs)).
Auditing the two side by side is what produced this list.*

grpc-webnext implements a **practical subset** of
[HttpRule](https://github.com/googleapis/googleapis/blob/master/google/api/http.proto) —
enough for the bindings real services actually write. This document is the honest
boundary of that subset: what works, what doesn't, what happens when you use
something that doesn't, and whether it's worth fixing.

**Both implementations share every item below.** That is the point: the Go router was
written to be structurally parallel to the Rust one so a reader can diff them, and the
gaps are properties of the shared design rather than of one language. Where a gap is
pinned by a test, the test is named — those tests assert *current* behavior, so adding
support means changing them, which is the tripwire that keeps this document honest. It
has already worked: every entry closed on 2026-07-28 announced itself as a failing test,
not as someone remembering to reread this page.

## Supported

| Feature | Notes |
|---|---|
| `get` / `put` / `post` / `delete` / `patch` | |
| `custom { kind, path }` | `kind` upper-cased into the HTTP method; a rule with no `kind` compiles to nothing |
| Trailing custom verb — `/v1/things/{id}:cancel` | **Matched**, not stripped (see "Closed") |
| `additional_bindings` | One level, which is all the spec permits |
| Literal path segments | |
| `{field}` / `{field=*}` | Single-segment capture, percent-decoded |
| `{field=**}` | Captures the remainder, slashes preserved |
| Bare `*` | Matches exactly one segment, captures nothing |
| Bare `**` | Matches the remaining segments (possibly none), captures nothing |
| Dotted capture paths — `{user.id}` | Binds nested message fields |
| `body: "*"` | The whole JSON body is the request message |
| `body: "<field>"` | A singular message-typed field |
| `response_body` | The body is that top-level response field's value; **per message** on a stream |
| No body | The request is built entirely from path + query |
| Query params → scalar fields | Including nested (`a.b=x`) and repeated (`t=1&t=2`) |
| Enum query/path values | By name or by number |
| Field names | Resolved by `.proto` name **or** JSON (lowerCamelCase) name |

Precedence is normative in [`spec/PROTOCOL.md`](../spec/PROTOCOL.md) ("REST binding
precedence"): the body seeds the message, path variables always overlay it, and query
params bind only when `body` is not `"*"`.

## Closed

### A trailing custom verb was stripped rather than matched *(2026-07-27)*

**A trailing custom verb was stripped from the template but not matched against the
request path.** `parse_template` cut everything after the first `:` and threw it away,
so a binding for `/v1/things/{id}:cancel` compiled to the same template as one for
`/v1/things/{id}` — with three consequences, all wrong:

- `GET /v1/things/5` reached the `:cancel` binding, because the verb it required had
  been discarded.
- `GET /v1/things/5:cancel` bound `id = "5:cancel"` — the verb leaked into the
  captured variable and on into the request message.
- Two custom verbs on one resource (`:cancel`, `:archive`) compiled to *identical*
  templates, so whichever came first in descriptor order answered both.

This is mis-routing, not a missing feature, so it was fixed rather than filed: the verb
is now carried on the binding and matched in both directions — a binding that declares
one matches only paths carrying it, and a binding that declares none never matches a
path that carries one. A genuine colon in path data is percent-encoded (`%3A`), so it
survives the check and still binds.

Pinned by `httprule.rs::tests::custom_verb_is_part_of_the_match` and
`httprule_test.go::TestMatchSegmentsCustomVerb`.

### Bare `*` / `**` segments *(2026-07-28)*

The HttpRule grammar allows an **unnamed** wildcard — `get: "/v1/*/things"` matches any
single segment there, capturing nothing — and `**` for the remainder. Both routers treated a
segment not wrapped in braces as a literal, so those templates matched only a path containing
a literal `*`. Now implemented in both, and covered end to end by the conformance cases
`url-bare-wildcard-segment`, `url-bare-wildcard-needs-a-segment`, and `url-bare-rest-wildcard`
— which is what proves the two servers agree rather than merely each passing its own tests.

Note this changes the meaning of a template containing a literal `*`. That is the correct
reading of the grammar, and no real REST path contains one.

### Field names now resolve by JSON name too *(2026-07-28)*

Path captures and query keys matched the `.proto` field name only, so `?someField=x` against
a `some_field` field was an `INVALID_ARGUMENT` — while grpc-gateway accepts both. URLs are
hand-written (in an annotation template by the service author, in a query string by the
caller) and both conventions turn up, so both now resolve, proto name first. This is a
compatible widening: every URL that worked before still works.

### `response_body` *(2026-07-28)*

`response_body: "resource"` should return only that field of the response message. Neither
router read the option: the binding compiled as if it were absent and the **whole** message
came back — the route worked, the status was `OK`, and the JSON was simply the wrong shape.
It was the last gap that returned a *wrong answer* rather than no answer.

Two things made it more than a 20-line change, and both are worth knowing before touching
this code again:

- **The binding was thrown away before the response was encoded.** The Fetch path resolved
  it, took the method and the request message, and dropped the rest; the response step had
  only a method path. `response_body` now rides on `HttpCall`/`httpCall` and `WsBinding`/
  `wsBinding` to reach both response sites.
- **Neither JSON library can encode a lone field value.** `protojson.Marshal` takes a
  message; prost-reflect serializes `DynamicMessage` only. Re-deriving protobuf-JSON's
  scalar rules by hand — 64-bit as a *string*, bytes as base64, enums by name — twice,
  identically, is exactly how two implementations drift. So both encode the **whole**
  message with the library's own rules and lift out the one member, which reuses those
  rules verbatim.

  That leaves one hand-written rule: whole-message encoding **skips defaults**, so a field
  at its zero has no member to lift — and `response_body` promises a body, where a zero is
  not "no answer". Hence a zero-value table (`json_zero` / `jsonZero`), kept to exactly that
  case and pinned kind-for-kind on both sides
  (`transcode::tests::json_zero_per_kind`, `TestJSONZeroPerKind`). If you add a kind,
  add it to both.

Covered end to end by `response-body-field`, `response-body-field-unset`,
`response-body-error-has-no-body`, and `response-body-per-stream-message`.

## Gaps that fail closed

**Every remaining gap is in this category.** Each produces no route, or an explicit
`INVALID_ARGUMENT` — never a wrong answer. That makes them cheap to live with, and it is
the property to preserve when adding to this list.

### `HttpRule.selector` and service-config rules

Bindings are read **only** from the `google.api.http` option attached to a method. A
`google.api.Http` block — the `rules:` list in a service-config YAML, each entry naming
its target through `selector` — is invisible to both implementations. There is no
mechanism to supply one, so this is a missing input format rather than a mis-read one.
In-proto annotations are the common case by a wide margin.

### Multi-segment patterns

`{name=shelves/*/books/*}` — a capture spanning several segments — is the sharpest of
the remaining gaps, because it does not degrade gracefully. The template is split on `/`
*before* braces are examined, so `{name=shelves` and `*}` each become their own literal
segment and the binding matches **nothing** it was meant to. (The interior bare `*`s *are*
wildcards now; the braces around them are what got split apart.) It fails closed, but it
fails silently at compile time: no error, just a route that never fires.

*Pinned by `nested_path_patterns_are_unsupported` (Rust) and
`TestUnsupportedNestedPathPattern` (Go).*

### Non-scalar query binding

A query parameter can only reach a **scalar** field, directly or through a dotted path
(`nested.id=x`). A message-typed field cannot be bound from a single parameter, which
also rules out the well-known types that Google APIs conventionally spell as one query
value: `google.protobuf.Timestamp`, `Duration`, `FieldMask` (`?update_mask=a,b`), and
the `*Value` wrappers. Attempting it is an explicit `INVALID_ARGUMENT`.

*Pinned by `TestUnsupportedNonScalarQueryBinding`.*

### `body` must name a singular message field

`body: "<field>"` requires that field to be a singular message. A **scalar** body field
(`body: "content"` where `content` is a `string`), a **repeated** one, and a **dotted**
path (`body: "a.b"`) are all refused. gRPC-gateway supports the first two.

*Pinned by `TestUnsupportedRepeatedBodyField`.*

### `bytes` cannot bind from path or query

There is no unambiguous spelling — base64? raw? percent-decoded? — so binding one is
refused rather than guessed at.

## Deliberate behaviors that are not gaps

Recorded so a future reader doesn't file them as bugs.

- **Proto name wins over JSON name** when a message perversely names one field like
  another's JSON name. *Pinned by `TestFieldNamesResolveByProtoOrJSONName`.*
- **First match wins,** in descriptor-set file order. There is no longest-prefix or
  specificity ranking, so two bindings that can match the same URL resolve by
  declaration order. Go walks the `FileDescriptorSet` in file order specifically so this
  is deterministic and matches Rust's descriptor-pool order — ranging over the Go
  registry directly would have made it a map's iteration order.
- **Percent-decoding is unconditional.** `Http.fully_decode_reserved_expansion` is not
  modeled; a `{x=*}` capture decodes reserved characters inside its own segment. The
  split happens before decoding, so an encoded `%2F` never becomes a separator.
- **A query param naming a `body: "<field>"` field errors** rather than being skipped.
  The spec says query params bind fields not covered by the path *or body*; only path
  coverage is checked, and the message-typed collision then fails coercion. Failing
  closed on a contradictory binding is the safe reading.
- **WebSocket annotation matching is verb-agnostic.** A WS upgrade is always an HTTP
  GET, so a `post:`-only binding is still reachable as a WebSocket route. This is
  normative in `spec/PROTOCOL.md`, not an accident.
- **Nested `additional_bindings` are dropped.** The HttpRule spec forbids nesting them,
  so refusing to compile the second level is conformant.
  *Pinned by `TestUnsupportedNestedAdditionalBindings`.*

## If you implement one of these

Two rules, both from the way this list came about:

1. **Do it in both languages, in the same commit.** The routers are parallel by
   construction; the moment they aren't, the shared-contract claim above stops being
   true and the next audit has to rediscover it.
2. **Update the test that pins it.** Every gap named above has a test asserting the
   current behavior. Those tests failing is the signal that this document is stale —
   that's what they're for.

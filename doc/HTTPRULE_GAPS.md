# `google.api.http` — what is and isn't implemented

*Last updated 2026-07-28. Written alongside the Go REST implementation
([`go/webnext/httprule.go`](../go/webnext/httprule.go)), which is a deliberate port of
the Rust router ([`rust/crates/grpc-webnext/src/httprule.rs`](../rust/crates/grpc-webnext/src/httprule.rs)).
Auditing the two side by side is what produced this list.*

grpc-webnext implements
[HttpRule](https://github.com/googleapis/googleapis/blob/master/google/api/http.proto).
This document is the honest boundary of that: what works, what doesn't, what happens when
you use something that doesn't, and why.

**As of the 2026-07-28 pass there are no functional gaps left** — every feature of the
`google.api.http` *method option* is implemented. What remains is one **declined** input
format (`HttpRule.selector`, below) and a handful of recorded decisions where the spec
itself forbids something.

**Both implementations share every item below.** That is the point: the Go router was
written to be structurally parallel to the Rust one so a reader can diff them, and behavior
is a property of the shared design rather than of one language. Where something is pinned by
a test, the test is named — those tests assert *current* behavior, so changing it means
changing them, which is the tripwire that keeps this document honest. It has already worked:
every entry closed below announced itself as a failing test, not as someone remembering to
reread this page.

## Supported

| Feature | Notes |
|---|---|
| `get` / `put` / `post` / `delete` / `patch` | |
| `custom { kind, path }` | `kind` upper-cased into the HTTP method; a rule with no `kind` compiles to nothing |
| `custom { kind: "*" }` | "HTTP method unspecified" — the binding answers **any** verb |
| Trailing custom verb — `/v1/things/{id}:cancel` | **Matched**, not stripped |
| `additional_bindings` | One level, which is all the spec permits |
| Literal path segments | |
| `{field}` / `{field=*}` | Single-segment capture, percent-decoded |
| `{field=**}` | Captures the remainder, slashes preserved |
| **`{field=a/*/b/*}`** | Multi-segment capture — the canonical resource-name shape |
| Bare `*` | Matches exactly one segment, captures nothing |
| Bare `**` | Matches the remaining segments (possibly none), captures nothing |
| Dotted capture paths — `{user.id}` | Binds nested message fields |
| `body: "*"` | The whole JSON body is the request message |
| **`body: "<field>"`** | **Any** top-level field — message, scalar, or repeated |
| No body | The request is built entirely from path + query |
| `response_body` | The body is that top-level response field's value; **per message** on a stream |
| Query params → scalar fields | Including nested (`a.b=x`) and repeated (`t=1&t=2`) |
| **Query params → `bytes`** | base64, protobuf-JSON's spelling |
| **Query params → well-known types** | `Timestamp`, `Duration`, `FieldMask`, the `*Value` wrappers |
| Enum query/path values | By name or by number |
| Field names | Resolved by `.proto` name **or** JSON (lowerCamelCase) name |

Precedence is normative in [`spec/PROTOCOL.md`](../spec/PROTOCOL.md) ("REST binding
precedence"): the body seeds the message, path variables always overlay it, query params bind
only when `body` is not `"*"`, and bindings resolve in declaration order.

## The one thing not implemented: `HttpRule.selector`

Bindings are read **only** from the `google.api.http` option attached to a method. A
`google.api.Http` block — the `rules:` list in a service-config YAML, each entry naming its
target through `selector` — is not read, and this is **declined rather than deferred**:

- It is a missing *input format*, not a mis-read one. There is no service-config loading
  anywhere in grpc-webnext, so supporting it means a new configuration surface in two
  languages, a selector-wildcard matcher, and a precedence rule against in-proto annotations
  — for a feature nothing here can currently express.
- The conformance matrix could not cover it without every implementation *also* growing
  service-config loading, so it would land as per-language tests only.
- There is a workable substitute today: bindings come from the `FileDescriptorSet` you hand
  the `Transcoder`, and you can add annotations to that set programmatically even when you
  do not own the `.proto`.

Reversible if a real need turns up. Until then, in-proto annotations are the surface.

## Recorded decisions

Things the spec forbids, or that are deliberate. Recorded so a future reader doesn't file
them as bugs.

- **A query param cannot carry an arbitrary submessage.** Only scalars, `bytes`, and the
  well-known types with a canonical string form bind. This is the spec's own rule —
  *"Repeated message fields must not be mapped to URL query parameters, because no client
  library can support such complicated mapping"* — so refusing is correct, and the error
  names the offending type. Reach nested scalars with a dotted key (`nested.id=x`).
  *Pinned by `TestQueryBindsWellKnownTypesButNotArbitraryMessages` and
  `bytes_binds_from_a_url_and_arbitrary_messages_do_not`.*
- **`body:` must name a top-level field.** A dotted path (`body: "a.b"`) is refused, because
  HttpRule says *"the referred field must be present at the top-level of the request message
  type"*. The same rule applies to `response_body`.
  *Pinned by `TestBodyMayNameAnyTopLevelField` and `body_may_name_any_top_level_field`.*
- **Nested `additional_bindings` are dropped.** The spec forbids nesting them, so refusing to
  compile the second level is conformant. *Pinned by `TestUnsupportedNestedAdditionalBindings`.*
- **Proto name wins over JSON name** when a message perversely names one field like another's
  JSON name. *Pinned by `TestFieldNamesResolveByProtoOrJSONName`.*
- **First match wins,** in descriptor-set file order. There is no longest-prefix or
  specificity ranking, so two bindings that can match the same URL resolve by declaration
  order — a later binding must not describe a shape an earlier one already covers. Go walks
  the `FileDescriptorSet` in file order specifically so this is deterministic and matches
  Rust's descriptor-pool order.
- **Percent-decoding is unconditional.** `Http.fully_decode_reserved_expansion` is not
  modeled; a capture decodes reserved characters inside each component. The split happens
  before decoding, so an encoded `%2F` never becomes a separator.
- **A query param naming a `body: "<field>"` field errors** rather than being skipped. The
  spec says query params bind fields not covered by the path *or body*; only path coverage is
  checked, and the collision then fails. Failing closed on a contradictory binding is safe.
- **WebSocket annotation matching is verb-agnostic.** A WS upgrade is always an HTTP GET, so
  a `post:`-only binding is still reachable as a WebSocket route. Normative in
  `spec/PROTOCOL.md`, not an accident.
- **Well-known-type query binding is unit-tested, not in the conformance matrix.** Proving it
  there would put a WKT field in the *shared contract*, and every implementation then pays
  for it in codegen — ts-proto answers a single `FieldMask` reference by emitting 7 300 lines
  of `descriptor.ts`. The cost outweighed one more matrix case; see the note in
  `conformance/proto/conformance.proto`.

## Closed

A running log, newest first. Each entry says what was wrong and what it cost, because the
*shape* of these fixes turned out to repeat.

### `custom { kind: "*" }` matched nothing *(2026-07-28)*

HttpRule allows `*` as a custom kind, meaning "leave the HTTP method unspecified for this
rule". Both routers upper-cased it into the method slot and then compared it to the request's
verb, which is never the literal `*` — so such a binding answered nothing. Found by reading
the upstream doc comment while auditing the tail, not by a test. Worth remembering that the
vendored `google/api/http.proto` is a **doc-stripped subset**: the normative prose lives
upstream, and two of this pass's findings came from reading it there.

### Multi-segment captures *(2026-07-28)*

`{name=shelves/*/books/*}` — the canonical Google resource-name shape — compiled to a route
that matched **nothing**, and silently: both routers split the template on `/` *before*
looking for braces, so `{name=shelves` and `*}` each became a literal segment.

The fix generalized the model rather than special-casing it: splitting is now brace-aware,
and every capture is `Capture(field, sub-pattern)` over its own segment list. `{f}` is sugar
for `{f=*}`, and `{f=**}` is a capture over `[**]`, so the two former special cases
*disappeared* — the segment enum has fewer variants than before, not more.

### `body:` naming a scalar or repeated field; `bytes` and well-known types from a URL *(2026-07-28)*

Three separate entries that turned out to be one idea. Each was refused because there was no
hand-written conversion for it — a scalar body field, a `bytes` query param, a `FieldMask` in
`?update_mask=a,b`.

All three now **delegate to the JSON decoder**: build `{"<jsonName>": <text>}` and let the
library decode it. That is `response_body` in reverse, and it means base64, RFC 3339, `"3.5s"`
and `"a,b"` are spelled exactly as protobuf-JSON spells them, without a line of parsing here.
*Pinned by `TestBodyMayNameAnyTopLevelField`, `TestQueryBindsWellKnownTypes`, and the Rust
twins.*

### `response_body` *(2026-07-28)*

`response_body: "resource"` should return only that field of the response message. Neither
router read the option: the whole message came back — route working, status `OK`, JSON simply
the wrong shape. The last gap that returned a *wrong answer*.

Two things made it more than a small change. The binding was **thrown away before the
response was encoded**, so it now rides on `HttpCall`/`WsBinding` to reach both response
sites. And **neither JSON library can encode a lone field value**, so both encode the whole
message and lift out the one member — which leaves exactly one hand-written rule, the
zero-value table for a field that whole-message encoding skipped, pinned kind-for-kind on
both sides (`json_zero_per_kind`, `TestJSONZeroPerKind`). If you add a kind, add it to both.

### Bare `*` / `**` segments *(2026-07-28)*

The grammar's unnamed wildcards — `get: "/v1/*/things"` matches any single segment there,
capturing nothing. Both routers treated a segment not wrapped in braces as a literal, so
those templates matched only a path containing a literal `*`.

### Field names now resolve by JSON name too *(2026-07-28)*

`?someField=x` against a `some_field` field was `INVALID_ARGUMENT`, while grpc-gateway
accepts both. A compatible widening: every URL that worked before still works.

### A trailing custom verb was stripped rather than matched *(2026-07-27)*

`parse_template` cut everything after the first `:` and threw it away, so a binding for
`/v1/things/{id}:cancel` compiled to the same template as `/v1/things/{id}` — with three
consequences, all wrong: `GET /v1/things/5` reached the `:cancel` binding; `GET
/v1/things/5:cancel` bound `id = "5:cancel"`; and two custom verbs on one resource compiled
to *identical* templates, so declaration order answered both.

Mis-routing, not a missing feature, so it was fixed rather than filed. A genuine colon in
path data is percent-encoded (`%3A`), so it survives the check and still binds.

## If you change any of this

Two rules, both from the way this list came about:

1. **Do it in both languages, in the same commit.** The routers are parallel by
   construction; the moment they aren't, the shared-contract claim above stops being true and
   the next audit has to rediscover it.
2. **Update the test that pins it.** Everything named above has a test asserting current
   behavior. Those tests failing is the signal that this document is stale — that's what
   they're for.

//! `google.api.http` transcoding: map REST-style `(HTTP method, path)` requests
//! onto gRPC methods, binding path segments, query params, and the request body
//! into the request message.
//!
//! This is a practical subset of the HttpRule spec — enough for the common
//! `get: "/v1/x/{id}"` / `post: "/v1/x" body: "*"` bindings:
//!   * verbs `get/put/post/delete/patch` and `custom{kind}`,
//!   * a trailing custom verb (`/v1/things/{id}:cancel`), matched not stripped,
//!   * `additional_bindings` (one level),
//!   * path templates: literal segments, `{field}` / `{field=*}` single-segment
//!     captures, `{field=**}` (or a trailing segment) capturing the rest, bare
//!     `*` / `**` wildcards that match without capturing, and dotted field paths
//!     (`{a.b}`) binding nested fields,
//!   * `body: "*"` (whole message), `body: "<field>"` (a sub-message field), or none,
//!   * `response_body` — answer with one top-level response field (see `transcode.rs`),
//!   * query params bound to (possibly nested) scalar/repeated fields, keyed by
//!     either the `.proto` field name or its JSON (lowerCamelCase) name.
//!
//! `go/webnext/httprule.go` is the deliberately parallel port of this file — same
//! segment/body model, same matching order, same coercion table — so the two
//! implementations can be compared side by side. The unsupported surface is
//! identical and enumerated in `doc/HTTPRULE_GAPS.md`: `HttpRule.selector`
//! (service-config rules), multi-segment patterns such as `{name=shelves/*}`,
//! non-scalar query binding, and scalar/repeated/dotted body fields.

use prost_reflect::prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage, Value,
};

use crate::transcode::TranscodeError;

/// The extension full name that carries an HttpRule on a method.
const HTTP_EXT: &str = "google.api.http";

/// A parsed path-template segment.
#[derive(Debug, Clone)]
enum Segment {
    /// A fixed path component that must match exactly.
    Literal(String),
    /// `{field}` / `{field=*}` — captures exactly one path component into `field`
    /// (a dotted path into the request message).
    Single(Vec<String>),
    /// `{field=**}` — captures the remaining components (slashes preserved).
    Rest(Vec<String>),
    /// A bare `*` — matches exactly one component, capturing nothing.
    AnyOne,
    /// A bare `**` — matches the remaining components, capturing nothing.
    AnyRest,
}

/// How the request body maps onto the message.
#[derive(Debug, Clone)]
enum BodyRule {
    /// `body: "*"` — the whole JSON body is the request message.
    Wildcard,
    /// `body: "<field>"` — the JSON body is a single (message) field.
    Field(String),
    /// No body.
    None,
}

/// One compiled HTTP binding: `(method, template)` -> a gRPC method + how to build
/// its request message.
#[derive(Debug, Clone)]
struct Binding {
    http_method: String, // upper-case, e.g. "GET"
    segments: Vec<Segment>,
    /// The template's trailing `:verb` (without the colon), empty if it has none.
    /// It is *matched*, not merely stripped — see [`match_segments`].
    custom_verb: String,
    body: BodyRule,
    /// `response_body` — the top-level response field to return instead of the
    /// whole message. Empty means the whole message.
    response_body: String,
    grpc_method: String, // "/pkg.Service/Method"
    input: MessageDescriptor,
}

/// A captured path variable: the dotted field path it binds to, and its string value —
/// e.g. `(["user", "id"], "123")` for a `{user.id}` template segment.
type PathVar = (Vec<String>, String);

/// The transcoded call: which gRPC method to invoke and the encoded request.
pub struct HttpCall {
    pub grpc_method: String,
    pub message: Vec<u8>,
    /// The binding's `response_body`, empty for the whole message. Carried on the
    /// call because the response is encoded long after the binding is matched.
    pub response_body: String,
}

/// A WebSocket route resolved from an annotation URL: the target gRPC method plus
/// the path/query bindings, used to build each streamed request message.
pub struct WsBinding {
    binding: Binding,
    vars: Vec<PathVar>,
    query: Option<String>,
}

impl WsBinding {
    /// The gRPC method this annotation route maps to.
    pub fn grpc_method(&self) -> &str {
        &self.binding.grpc_method
    }

    /// Whether the route takes a request body. `false` for GET-style server-streams,
    /// where the request comes entirely from the URL (path + query).
    pub fn has_body(&self) -> bool {
        !matches!(self.binding.body, BodyRule::None)
    }

    /// The binding's `response_body` — the top-level response field to return
    /// instead of the whole message. Empty means the whole message.
    pub fn response_body(&self) -> &str {
        &self.binding.response_body
    }

    /// Build a request message from a body payload, overlaying the URL path/query.
    pub fn build_message(&self, body: &[u8]) -> Result<Vec<u8>, TranscodeError> {
        build_message(&self.binding, &self.vars, self.query.as_deref(), body)
    }
}

/// A table of HTTP bindings compiled from a descriptor pool.
#[derive(Clone, Default)]
pub struct HttpRouter {
    bindings: Vec<Binding>,
}

impl HttpRouter {
    /// Compile all `google.api.http` bindings in the pool. Empty if the pool has
    /// no annotations (or the extension isn't present).
    pub fn from_pool(pool: &DescriptorPool) -> Self {
        let mut bindings = Vec::new();
        let Some(ext) = pool.get_extension_by_name(HTTP_EXT) else {
            return Self { bindings };
        };
        for service in pool.services() {
            for method in service.methods() {
                let opts = method.options();
                if !opts.has_extension(&ext) {
                    continue;
                }
                let grpc_method = format!("/{}/{}", service.full_name(), method.name());
                let input = method.input();
                let rule = opts.get_extension(&ext);
                if let Some(rule) = rule.as_message() {
                    collect_rule(&mut bindings, rule, &grpc_method, &input);
                }
            }
        }
        Self { bindings }
    }

    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    /// Match a WebSocket upgrade path against the annotation bindings. A WS upgrade
    /// is always an HTTP GET, so this matches on the path only (verb-agnostic).
    pub fn match_ws(&self, path: &str, query: Option<&str>) -> Option<WsBinding> {
        for b in &self.bindings {
            if let Some(vars) = match_segments(&b.segments, &b.custom_verb, path) {
                return Some(WsBinding {
                    binding: b.clone(),
                    vars,
                    query: query.map(str::to_string),
                });
            }
        }
        None
    }

    /// Whether a gRPC method (`/pkg.Service/Method`) has any HTTP annotation.
    pub fn is_annotated(&self, grpc_method: &str) -> bool {
        self.bindings.iter().any(|b| b.grpc_method == grpc_method)
    }

    /// Find a binding matching `(method, path)` and return it plus the captured
    /// path variables (dotted field path -> value).
    fn match_request(&self, method: &str, path: &str) -> Option<(&Binding, Vec<PathVar>)> {
        let want = method.to_ascii_uppercase();
        for b in &self.bindings {
            if b.http_method != want {
                continue;
            }
            if let Some(vars) = match_segments(&b.segments, &b.custom_verb, path) {
                return Some((b, vars));
            }
        }
        None
    }

    /// Transcode a REST request into a gRPC call, or `None` if no binding matches.
    pub fn transcode(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: &[u8],
    ) -> Result<Option<HttpCall>, TranscodeError> {
        let Some((binding, vars)) = self.match_request(method, path) else {
            return Ok(None);
        };
        let message = build_message(binding, &vars, query, body)?;
        Ok(Some(HttpCall {
            grpc_method: binding.grpc_method.clone(),
            message,
            response_body: binding.response_body.clone(),
        }))
    }
}

/// Push the top rule and any `additional_bindings` (one level) as bindings.
fn collect_rule(bindings: &mut Vec<Binding>, rule: &DynamicMessage, grpc_method: &str, input: &MessageDescriptor) {
    push_binding(bindings, rule, grpc_method, input);
    if let Some(list) = rule.get_field_by_name("additional_bindings") {
        if let Some(items) = list.as_list() {
            for item in items {
                if let Some(m) = item.as_message() {
                    push_binding(bindings, m, grpc_method, input);
                }
            }
        }
    }
}

fn push_binding(bindings: &mut Vec<Binding>, rule: &DynamicMessage, grpc_method: &str, input: &MessageDescriptor) {
    let Some((http_method, template)) = verb_and_path(rule) else {
        return;
    };
    let (segments, custom_verb) = parse_template(&template);
    bindings.push(Binding {
        http_method,
        segments,
        custom_verb,
        body: body_rule(rule),
        response_body: rule
            .get_field_by_name("response_body")
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default(),
        grpc_method: grpc_method.to_string(),
        input: input.clone(),
    });
}

/// Extract the verb + path from an HttpRule's `pattern` oneof.
fn verb_and_path(rule: &DynamicMessage) -> Option<(String, String)> {
    for (field, verb) in [("get", "GET"), ("put", "PUT"), ("post", "POST"), ("delete", "DELETE"), ("patch", "PATCH")] {
        if let Some(v) = rule.get_field_by_name(field) {
            if let Some(s) = v.as_str() {
                if !s.is_empty() {
                    return Some((verb.to_string(), s.to_string()));
                }
            }
        }
    }
    // custom { kind, path }
    if let Some(v) = rule.get_field_by_name("custom") {
        if let Some(m) = v.as_message() {
            let kind = m.get_field_by_name("kind").and_then(|k| k.as_str().map(str::to_string)).unwrap_or_default();
            let path = m.get_field_by_name("path").and_then(|p| p.as_str().map(str::to_string)).unwrap_or_default();
            if !kind.is_empty() {
                return Some((kind.to_ascii_uppercase(), path));
            }
        }
    }
    None
}

fn body_rule(rule: &DynamicMessage) -> BodyRule {
    match rule.get_field_by_name("body").and_then(|b| b.as_str().map(str::to_string)).unwrap_or_default().as_str() {
        "" => BodyRule::None,
        "*" => BodyRule::Wildcard,
        field => BodyRule::Field(field.to_string()),
    }
}

/// Parse a path template into segments plus its trailing custom verb
/// (`/v1/things/{id}:cancel` -> the `cancel`), which the caller matches rather
/// than discards.
fn parse_template(template: &str) -> (Vec<Segment>, String) {
    let (path, custom_verb) = match template.split_once(':') {
        Some((path, verb)) => (path, verb.to_string()),
        None => (template, String::new()),
    };
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|seg| {
            if let Some(inner) = seg.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
                let (field, pattern) = inner.split_once('=').unwrap_or((inner, "*"));
                let field_path: Vec<String> = field.split('.').map(str::to_string).collect();
                if pattern == "**" {
                    Segment::Rest(field_path)
                } else {
                    Segment::Single(field_path)
                }
            } else if seg == "*" {
                Segment::AnyOne
            } else if seg == "**" {
                Segment::AnyRest
            } else {
                Segment::Literal(seg.to_string())
            }
        })
        .collect();
    (segments, custom_verb)
}

/// Match a request path against a template, returning captured `(field_path, value)`.
///
/// `custom_verb` is the template's trailing `:verb`, and it is part of the match in
/// both directions: a binding that declares one matches only paths carrying it (the
/// verb is stripped before the last segment binds), and a binding that declares none
/// never matches a path that carries one. Stripping the verb from the *template*
/// alone would make `/v1/things/{id}:cancel` match the bare `/v1/things/5` and
/// capture `id = "5:cancel"` from `/v1/things/5:cancel`, with every custom verb on a
/// resource collapsing onto one template. A genuine colon in path data is
/// percent-encoded, so it survives this check.
fn match_segments(segments: &[Segment], custom_verb: &str, path: &str) -> Option<Vec<PathVar>> {
    let mut parts: Vec<&str> = path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();
    match parts.last() {
        Some(last) => {
            let (head, verb) = last.split_once(':').unwrap_or((last, ""));
            if verb != custom_verb || (head.is_empty() && last.contains(':')) {
                return None;
            }
            let index = parts.len() - 1;
            parts[index] = head;
        }
        None if !custom_verb.is_empty() => return None,
        None => {}
    }
    let mut vars = Vec::new();
    let mut i = 0;
    for seg in segments {
        match seg {
            Segment::Literal(lit) => {
                if parts.get(i) != Some(&lit.as_str()) {
                    return None;
                }
                i += 1;
            }
            Segment::Single(field) => {
                let part = parts.get(i)?;
                vars.push((field.clone(), percent_decode(part)));
                i += 1;
            }
            Segment::Rest(field) => {
                let rest: Vec<String> = parts[i..].iter().map(|p| percent_decode(p)).collect();
                vars.push((field.clone(), rest.join("/")));
                i = parts.len();
            }
            // Unnamed wildcards: consume, bind nothing.
            Segment::AnyOne => {
                parts.get(i)?;
                i += 1;
            }
            Segment::AnyRest => i = parts.len(),
        }
    }
    if i == parts.len() {
        Some(vars)
    } else {
        None
    }
}

/// Build the encoded request message from a matched binding + inputs.
fn build_message(
    binding: &Binding,
    vars: &[PathVar],
    query: Option<&str>,
    body: &[u8],
) -> Result<Vec<u8>, TranscodeError> {
    let mut msg = match &binding.body {
        BodyRule::Wildcard => deserialize_message(binding.input.clone(), body)?,
        BodyRule::None => DynamicMessage::new(binding.input.clone()),
        BodyRule::Field(field) => {
            let mut m = DynamicMessage::new(binding.input.clone());
            if !body.is_empty() {
                set_message_field(&mut m, field, body)?;
            }
            m
        }
    };

    for (field_path, value) in vars {
        set_by_path(&mut msg, field_path, value)?;
    }

    // Query params bind fields not carried by a wildcard body.
    if !matches!(binding.body, BodyRule::Wildcard) {
        if let Some(q) = query {
            for (key, value) in parse_query(q) {
                let field_path: Vec<String> = key.split('.').map(str::to_string).collect();
                if vars.iter().any(|(fp, _)| *fp == field_path) {
                    continue; // already set from the path
                }
                set_by_path(&mut msg, &field_path, &value)?;
            }
        }
    }

    Ok(msg.encode_to_vec())
}

fn deserialize_message(desc: MessageDescriptor, json: &[u8]) -> Result<DynamicMessage, TranscodeError> {
    if json.is_empty() {
        return Ok(DynamicMessage::new(desc));
    }
    let mut de = serde_json::Deserializer::from_slice(json);
    let msg = DynamicMessage::deserialize(desc, &mut de)?;
    de.end()?;
    Ok(msg)
}

/// Resolve a field by its `.proto` name, falling back to its JSON (lowerCamelCase)
/// name. URLs are written by hand — in an annotation template by the service author,
/// in a query string by the caller — and both conventions turn up in practice, so
/// both resolve. grpc-gateway does the same. The proto name wins on a collision,
/// which can only happen if a message deliberately names one field like another's
/// JSON name.
pub(crate) fn field_by_any_name(desc: &MessageDescriptor, name: &str) -> Option<FieldDescriptor> {
    desc.get_field_by_name(name).or_else(|| desc.get_field_by_json_name(name))
}

/// Parse `json` into the (message-typed) field `name` and set it.
fn set_message_field(msg: &mut DynamicMessage, name: &str, json: &[u8]) -> Result<(), TranscodeError> {
    let field = field_by_any_name(&msg.descriptor(), name)
        .ok_or_else(|| TranscodeError::Http(format!("unknown body field: {name}")))?;
    match field.kind() {
        Kind::Message(md) => {
            let sub = deserialize_message(md, json)?;
            msg.set_field(&field, Value::Message(sub));
            Ok(())
        }
        _ => Err(TranscodeError::Http(format!("body field {name} must be a message"))),
    }
}

/// Set a (possibly nested, possibly repeated) scalar field from a string value.
fn set_by_path(msg: &mut DynamicMessage, path: &[String], raw: &str) -> Result<(), TranscodeError> {
    let field = field_by_any_name(&msg.descriptor(), &path[0])
        .ok_or_else(|| TranscodeError::Http(format!("unknown field: {}", path[0])))?;

    if path.len() == 1 {
        let value = coerce(&field, raw)?;
        if field.is_list() {
            if let Some(list) = msg.get_field_mut(&field).as_list_mut() {
                list.push(value);
            }
        } else {
            msg.set_field(&field, value);
        }
        Ok(())
    } else {
        let sub = msg
            .get_field_mut(&field)
            .as_message_mut()
            .ok_or_else(|| TranscodeError::Http(format!("field {} is not a message", path[0])))?;
        set_by_path(sub, &path[1..], raw)
    }
}

/// Coerce a string into a scalar `Value` per the field's protobuf kind.
fn coerce(field: &prost_reflect::FieldDescriptor, raw: &str) -> Result<Value, TranscodeError> {
    let num = |ok: Option<Value>| ok.ok_or_else(|| TranscodeError::Http(format!("invalid value for {}: {raw:?}", field.name())));
    Ok(match field.kind() {
        Kind::String => Value::String(raw.to_string()),
        Kind::Bool => match raw {
            "true" | "1" => Value::Bool(true),
            "false" | "0" => Value::Bool(false),
            _ => return Err(TranscodeError::Http(format!("invalid bool: {raw:?}"))),
        },
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => num(raw.parse().ok().map(Value::I32))?,
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => num(raw.parse().ok().map(Value::I64))?,
        Kind::Uint32 | Kind::Fixed32 => num(raw.parse().ok().map(Value::U32))?,
        Kind::Uint64 | Kind::Fixed64 => num(raw.parse().ok().map(Value::U64))?,
        Kind::Float => num(raw.parse().ok().map(Value::F32))?,
        Kind::Double => num(raw.parse().ok().map(Value::F64))?,
        Kind::Enum(e) => {
            if let Ok(n) = raw.parse::<i32>() {
                Value::EnumNumber(n)
            } else {
                let v = e
                    .values()
                    .find(|v| v.name() == raw)
                    .ok_or_else(|| TranscodeError::Http(format!("unknown enum value: {raw:?}")))?;
                Value::EnumNumber(v.number())
            }
        }
        Kind::Bytes => return Err(TranscodeError::Http("bytes fields cannot bind from path/query".into())),
        Kind::Message(_) => return Err(TranscodeError::Http("cannot bind a scalar to a message field".into())),
    })
}

/// Parse a query string into decoded key/value pairs.
fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (decode_query(k), decode_query(v))
        })
        .collect()
}

fn decode_query(s: &str) -> String {
    percent_decode(&s.replace('+', " "))
}

/// Minimal percent-decoding (`%XX`), lossy on invalid UTF-8.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trailing `:verb` is matched, not merely stripped from the template.
    /// Without that, `{id}:cancel` would also answer the bare resource URL and
    /// would capture `id = "5:cancel"` — and every custom verb on one resource
    /// would collide on a single template. The Go router is pinned by the same
    /// case (`go/webnext/httprule_test.go::TestMatchSegmentsCustomVerb`).
    #[test]
    fn custom_verb_is_part_of_the_match() {
        let (cancel, verb) = parse_template("/v1/things/{id}:cancel");
        assert_eq!(verb, "cancel");

        // The verb must be present, and it does not leak into the capture.
        let vars = match_segments(&cancel, &verb, "/v1/things/5:cancel").expect("should match");
        assert_eq!(vars, vec![(vec!["id".to_string()], "5".to_string())]);

        // The bare resource URL belongs to a different binding, and so does a
        // different verb on the same resource.
        assert!(match_segments(&cancel, &verb, "/v1/things/5").is_none());
        assert!(match_segments(&cancel, &verb, "/v1/things/5:archive").is_none());

        // Symmetrically, a verb-less binding does not swallow a custom-verb URL.
        let (plain, plain_verb) = parse_template("/v1/things/{id}");
        assert!(plain_verb.is_empty());
        assert!(match_segments(&plain, &plain_verb, "/v1/things/5:cancel").is_none());

        // A percent-encoded colon is data, not a verb separator, so it still binds.
        let vars = match_segments(&plain, &plain_verb, "/v1/things/urn%3Afoo").expect("should match");
        assert_eq!(vars, vec![(vec!["id".to_string()], "urn:foo".to_string())]);
    }

    /// A path pattern richer than a bare `*`/`**` — `{name=shelves/*}` — is split
    /// on `/` before braces are examined, so it compiles to segments that match
    /// nothing it was meant to. It fails closed, but silently; see
    /// `doc/HTTPRULE_GAPS.md`.
    #[test]
    fn nested_path_patterns_are_unsupported() {
        let (segments, _) = parse_template("/v1/{name=shelves/*/books/*}");
        assert!(match_segments(&segments, "", "/v1/shelves/1/books/2").is_none());
        assert!(match_segments(&segments, "", "/v1/anything").is_none());
        assert_eq!(segments.len(), 5);
        // The interior bare `*`s *are* wildcards — that much is supported — but the
        // braces have already been split into literals nothing will ever match.
        assert!(matches!(segments[1], Segment::Literal(ref l) if l == "{name=shelves"));
        assert!(matches!(segments[2], Segment::AnyOne));
    }

    /// Field names resolve by `.proto` name or by JSON (lowerCamelCase) name, since
    /// both turn up in hand-written URLs. The Go router is pinned by the same case
    /// (`httprule_test.go::TestFieldNamesResolveByProtoOrJSONName`).
    #[test]
    fn field_names_resolve_by_proto_or_json_name() {
        let pool = DescriptorPool::decode(testecho::FILE_DESCRIPTOR_SET).expect("descriptor set");
        // `HttpRule` itself is in the pool (echo.proto imports the annotations) and,
        // unlike the Echo messages, has multi-word fields — so the two spellings
        // genuinely differ here rather than coinciding.
        let desc = pool.get_message_by_name("google.api.HttpRule").expect("HttpRule");

        let by_proto = field_by_any_name(&desc, "response_body").expect("proto name");
        let by_json = field_by_any_name(&desc, "responseBody").expect("json name");
        assert_eq!(by_proto.number(), by_json.number(), "both spellings must reach one field");

        assert!(field_by_any_name(&desc, "additional_bindings").is_some());
        assert!(field_by_any_name(&desc, "additionalBindings").is_some());
        // A name that is neither still fails.
        assert!(field_by_any_name(&desc, "responseBodyy").is_none());
    }

    /// Bare `*` and `**` — the HttpRule grammar's unnamed wildcards — match without
    /// capturing anything.
    #[test]
    fn bare_wildcard_segments() {
        let (one, verb) = parse_template("/v1/*/things/{id}");
        assert!(matches!(one[1], Segment::AnyOne));
        let vars = match_segments(&one, &verb, "/v1/anything/things/7").expect("should match");
        assert_eq!(vars, vec![(vec!["id".to_string()], "7".to_string())]);
        // Exactly one segment: neither zero nor two.
        assert!(match_segments(&one, &verb, "/v1/things/7").is_none());
        assert!(match_segments(&one, &verb, "/v1/a/b/things/7").is_none());

        let (rest, verb) = parse_template("/v1/things/{id}/**");
        assert!(matches!(rest[3], Segment::AnyRest));
        let vars = match_segments(&rest, &verb, "/v1/things/7/a/b/c").expect("should match");
        assert_eq!(vars, vec![(vec!["id".to_string()], "7".to_string())]);
        // `**` also matches an empty remainder.
        assert!(match_segments(&rest, &verb, "/v1/things/7").is_some());
    }
}

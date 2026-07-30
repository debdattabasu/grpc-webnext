//! `google.api.http` transcoding: map REST-style `(HTTP method, path)` requests
//! onto gRPC methods, binding path segments, query params, and the request body
//! into the request message.
//!
//! This implements HttpRule; `doc/HTTPRULE_GAPS.md` is the authoritative statement
//! of the boundary, and as of 2026-07-28 there are no functional gaps in the method
//! option — only `HttpRule.selector` (service-config rules), declined.
//!
//! `go/webnext/httprule.go` is the deliberately parallel port of this file — same
//! segment/body model, same matching order, same coercion table — so the two
//! implementations can be compared side by side. **Change one, change the other in
//! the same commit.**
//!
//! Two ideas carry most of the weight:
//!
//!   (`Segment` and `set_from_json` below are private, so they are named here rather than
//!   linked — this is a tour of the file, not of the public API.)
//!
//!   * **Captures are patterns, not segments.** `Segment::Capture` holds its own
//!     sub-pattern, so `{f}`, `{f=**}` and `{f=shelves/*/books/*}` are one case
//!     rather than three. Splitting is brace-aware for exactly that reason.
//!   * **Conversions belong to the JSON decoder.** Anything without a scalar form —
//!     `bytes`, the well-known types, a `body:` naming a non-message field — is set
//!     by handing its protobuf-JSON text to the decoder rather than parsed here.
//!     See `set_from_json`.

use prost_reflect::prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage, Value,
};

use crate::transcode::TranscodeError;

/// The extension full name that carries an HttpRule on a method.
const HTTP_EXT: &str = "google.api.http";

/// A parsed path-template segment.
///
/// Note there is no separate "single capture" / "rest capture": both are a
/// [`Segment::Capture`] over a sub-pattern, which is what lets a capture span
/// several segments (`{name=shelves/*/books/*}`). `{f}` is sugar for `{f=*}`.
#[derive(Debug, Clone)]
enum Segment {
    /// A fixed path component that must match exactly.
    Literal(String),
    /// A bare `*` — matches exactly one component, capturing nothing.
    AnyOne,
    /// A bare `**` — matches the remaining components, capturing nothing.
    AnyRest,
    /// `{field=<sub-pattern>}` — binds whatever the sub-pattern consumed, joined
    /// by `/`, to a (dotted) field path. Sub-segments are never captures
    /// themselves; the grammar does not nest them.
    Capture(Vec<String>, Vec<Segment>),
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
            // `custom { kind: "*" }` is HttpRule's "leave the HTTP method
            // unspecified for this rule" — it matches any verb, not a literal `*`.
            if b.http_method != want && b.http_method != "*" {
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

/// Split `s` on `sep`, ignoring separators that appear inside `{...}`.
///
/// This is the whole reason multi-segment captures work: splitting the raw
/// template on `/` first would tear `{name=shelves/*}` into pieces that no longer
/// look like a capture, which is precisely how they used to compile into dead
/// literal routes.
fn split_outside_braces(s: &str, sep: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            _ if c == sep && depth == 0 => {
                out.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

/// Parse a path template into segments plus its trailing custom verb
/// (`/v1/things/{id}:cancel` -> the `cancel`), which the caller matches rather
/// than discards.
fn parse_template(template: &str) -> (Vec<Segment>, String) {
    let parts = split_outside_braces(template, ':');
    let path = parts[0];
    let custom_verb = if parts.len() > 1 { parts[1..].join(":") } else { String::new() };

    let segments = split_outside_braces(path.trim_matches('/'), '/')
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(parse_segment)
        .collect();
    (segments, custom_verb)
}

/// One template segment: a capture (with its own sub-pattern) or a plain one.
fn parse_segment(seg: &str) -> Segment {
    if let Some(inner) = seg.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        let (field, pattern) = inner.split_once('=').unwrap_or((inner, "*"));
        let field_path: Vec<String> = field.split('.').map(str::to_string).collect();
        let subs = pattern
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(plain_segment)
            .collect();
        return Segment::Capture(field_path, subs);
    }
    plain_segment(seg)
}

/// A non-capturing segment: a wildcard or a literal.
fn plain_segment(seg: &str) -> Segment {
    match seg {
        "*" => Segment::AnyOne,
        "**" => Segment::AnyRest,
        _ => Segment::Literal(seg.to_string()),
    }
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
    let end = match_list(segments, &parts, 0, &mut vars)?;
    (end == parts.len()).then_some(vars)
}

/// Match `segments` against `parts[i..]`, collecting captures. Returns the index
/// just past what was consumed, or `None` if the pattern does not fit.
fn match_list(segments: &[Segment], parts: &[&str], mut i: usize, vars: &mut Vec<PathVar>) -> Option<usize> {
    for seg in segments {
        match seg {
            Segment::Literal(lit) => {
                if parts.get(i) != Some(&lit.as_str()) {
                    return None;
                }
                i += 1;
            }
            Segment::AnyOne => {
                parts.get(i)?;
                i += 1;
            }
            Segment::AnyRest => i = parts.len(),
            Segment::Capture(field, subs) => {
                let start = i;
                i = match_list(subs, parts, i, vars)?;
                // The captured value is everything the sub-pattern consumed. Each
                // component is decoded separately, then joined — so an encoded `%2F`
                // inside a component never reads as a separator.
                let value =
                    parts[start..i].iter().map(|p| percent_decode(p)).collect::<Vec<_>>().join("/");
                vars.push((field.clone(), value));
            }
        }
    }
    Some(i)
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

/// Set one field by handing its protobuf-JSON text to the *decoder*, rather than
/// converting by hand.
///
/// This is the same move `response_body` makes on the way out, in reverse: the
/// library already knows how every field shape is spelled in JSON — base64 for
/// `bytes`, RFC 3339 for a `Timestamp`, `"a,b"` for a `FieldMask`, `"3.5s"` for a
/// `Duration` — and re-deriving those rules by hand in two languages is how the
/// implementations would drift. So build `{"<jsonName>": <text>}`, decode it into
/// a scratch message, and lift the field across.
fn set_from_json(msg: &mut DynamicMessage, field: &FieldDescriptor, json_text: &str) -> Result<(), TranscodeError> {
    let doc = format!("{{{}:{}}}", serde_json::to_string(field.json_name()).unwrap(), json_text);
    let scratch = deserialize_message(msg.descriptor(), doc.as_bytes())
        .map_err(|e| TranscodeError::Http(format!("invalid value for {}: {e}", field.name())))?;
    msg.set_field(field, scratch.get_field(field).into_owned());
    Ok(())
}

/// Whether a message field has a canonical protobuf-JSON **string** form, so a
/// bare URL value can be bound to it by quoting. These are the well-known types a
/// REST URL realistically carries — `?update_mask=a,b`, `?ttl=3.5s`,
/// `?since=2026-01-01T00:00:00Z`. Any other message is still refused: a query
/// parameter cannot carry an arbitrary submessage.
fn wkt_json_shape(md: &MessageDescriptor) -> Option<bool> {
    match md.full_name() {
        // Encoded as a JSON string — quote the raw value.
        "google.protobuf.Timestamp"
        | "google.protobuf.Duration"
        | "google.protobuf.FieldMask"
        | "google.protobuf.StringValue"
        | "google.protobuf.BytesValue"
        | "google.protobuf.Int64Value"
        | "google.protobuf.UInt64Value" => Some(true),
        // Encoded as a bare JSON number/bool — pass the raw value through.
        "google.protobuf.BoolValue"
        | "google.protobuf.Int32Value"
        | "google.protobuf.UInt32Value"
        | "google.protobuf.FloatValue"
        | "google.protobuf.DoubleValue" => Some(false),
        _ => None,
    }
}

/// Parse `json` into the top-level field `name` and set it.
fn set_message_field(msg: &mut DynamicMessage, name: &str, json: &[u8]) -> Result<(), TranscodeError> {
    let field = field_by_any_name(&msg.descriptor(), name)
        .ok_or_else(|| TranscodeError::Http(format!("unknown body field: {name}")))?;
    // Any top-level field, not just a message: `body: "content"` on a `string` takes
    // a JSON string, `body: "items"` on a repeated field takes an array. HttpRule
    // requires only that the field be top-level.
    let text = std::str::from_utf8(json)
        .map_err(|_| TranscodeError::Http(format!("body field {name}: not valid UTF-8")))?;
    set_from_json(msg, &field, text)
}

/// Set a (possibly nested, possibly repeated) scalar field from a string value.
fn set_by_path(msg: &mut DynamicMessage, path: &[String], raw: &str) -> Result<(), TranscodeError> {
    let field = field_by_any_name(&msg.descriptor(), &path[0])
        .ok_or_else(|| TranscodeError::Http(format!("unknown field: {}", path[0])))?;

    if path.len() == 1 {
        // Kinds with no scalar `Value` form bind through the JSON decoder instead:
        // `bytes` (base64) and the string-shaped well-known types.
        match field.kind() {
            Kind::Bytes => {
                return set_from_json(msg, &field, &serde_json::to_string(raw).unwrap());
            }
            Kind::Message(md) if !field.is_list() && !field.is_map() => {
                return match wkt_json_shape(&md) {
                    Some(true) => set_from_json(msg, &field, &serde_json::to_string(raw).unwrap()),
                    Some(false) => set_from_json(msg, &field, raw),
                    None => Err(TranscodeError::Http(format!(
                        "cannot bind a path/query value to message field {} ({})",
                        field.name(),
                        md.full_name()
                    ))),
                };
            }
            _ => {}
        }
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
        // Unreachable: `set_by_path` routes these to the JSON decoder above. Kept
        // so the match stays exhaustive over `Kind` rather than falling through.
        Kind::Bytes => return Err(TranscodeError::Http("bytes must bind through the JSON decoder".into())),
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

    /// A capture may span several segments — `{name=shelves/*/books/*}`, the
    /// canonical Google resource-name shape. The captured value is everything the
    /// sub-pattern consumed, joined by `/`. The Go router is pinned by the same
    /// case (`httprule_test.go::TestMultiSegmentCapture`).
    #[test]
    fn multi_segment_capture() {
        let (segments, verb) = parse_template("/v1/{name=shelves/*/books/*}");
        let vars = match_segments(&segments, &verb, "/v1/shelves/1/books/2").expect("should match");
        assert_eq!(vars, vec![(vec!["name".to_string()], "shelves/1/books/2".to_string())]);

        // The sub-pattern is a pattern, not a prefix: the shape has to line up.
        assert_eq!(segments.len(), 2);
        assert!(match_segments(&segments, &verb, "/v1/shelves/1/books").is_none());
        assert!(match_segments(&segments, &verb, "/v1/shelves/1/books/2/3").is_none());
        assert!(match_segments(&segments, &verb, "/v1/racks/1/books/2").is_none());

        // A trailing `**` inside a capture takes the remainder.
        let (segments, verb) = parse_template("/v1/{name=files/**}");
        let vars = match_segments(&segments, &verb, "/v1/files/a/b/c.txt").expect("should match");
        assert_eq!(vars, vec![(vec!["name".to_string()], "files/a/b/c.txt".to_string())]);

        // A custom verb still binds after a multi-segment capture, which is only
        // possible because the `:` split ignores colons inside braces.
        let (segments, verb) = parse_template("/v1/{name=shelves/*}:archive");
        assert_eq!(verb, "archive");
        let vars = match_segments(&segments, &verb, "/v1/shelves/7:archive").expect("should match");
        assert_eq!(vars, vec![(vec!["name".to_string()], "shelves/7".to_string())]);
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

    /// A synthetic message with the two kinds that bind through the JSON decoder
    /// rather than through `coerce`: `bytes`, and a message field.
    fn binder_fixture() -> MessageDescriptor {
        use prost_reflect::prost_types::{
            field_descriptor_proto::{Label, Type},
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };
        let field = |name: &str, number: i32, ty: Type, type_name: Option<&str>| FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            label: Some(Label::Optional as i32),
            r#type: Some(ty as i32),
            type_name: type_name.map(str::to_string),
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some("binder_fixture.proto".to_string()),
            package: Some("webnext.binder.test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Nested".to_string()),
                    field: vec![field("id", 1, Type::String, None)],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Req".to_string()),
                    field: vec![
                        field("blob", 1, Type::Bytes, None),
                        field("nested", 2, Type::Message, Some(".webnext.binder.test.Nested")),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
            .expect("build fixture pool")
            .get_message_by_name("webnext.binder.test.Req")
            .expect("Req")
    }

    /// `custom { kind: "*" }` means "leave the HTTP method unspecified for this
    /// rule", so the binding answers every verb rather than a literal `*`. Go is
    /// pinned by the same case (`TestCustomKindStarMatchesAnyVerb`).
    #[test]
    fn custom_kind_star_matches_any_verb() {
        let mut bindings = Vec::new();
        let input = binder_fixture();
        // `push_binding` takes the rule as a DynamicMessage, so build one: a
        // `custom` pattern whose kind is `*`.
        let pool = DescriptorPool::decode(testecho::FILE_DESCRIPTOR_SET).expect("descriptor set");
        let rule_desc = pool.get_message_by_name("google.api.HttpRule").expect("HttpRule");
        let custom_desc = pool.get_message_by_name("google.api.CustomHttpPattern").expect("custom");
        let mut custom = DynamicMessage::new(custom_desc);
        custom.set_field_by_name("kind", Value::String("*".into()));
        custom.set_field_by_name("path", Value::String("/v1/any".into()));
        let mut rule = DynamicMessage::new(rule_desc);
        rule.set_field_by_name("custom", Value::Message(custom));

        push_binding(&mut bindings, &rule, "/test.Svc/Any", &input);
        let router = HttpRouter { bindings };

        for verb in ["GET", "POST", "DELETE", "PATCH"] {
            assert!(router.match_request(verb, "/v1/any").is_some(), "{verb} should match");
        }
        // It is still a route, not a catch-all: the path must match.
        assert!(router.match_request("GET", "/v1/other").is_none());
    }

    /// A pool containing the well-known types a REST URL realistically carries.
    /// They are declared by hand (each is tiny) because prost-reflect keys its
    /// protobuf-JSON handling off the *full name*, so a faithful declaration is all
    /// it needs — and a synthetic pool keeps this test free of a codegen dependency.
    fn wkt_fixture() -> MessageDescriptor {
        use prost_reflect::prost_types::{
            field_descriptor_proto::{Label, Type},
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };
        let f = |name: &str, number: i32, ty: Type, label: Label, type_name: Option<&str>| {
            FieldDescriptorProto {
                name: Some(name.to_string()),
                number: Some(number),
                label: Some(label as i32),
                r#type: Some(ty as i32),
                type_name: type_name.map(str::to_string),
                ..Default::default()
            }
        };
        let wkt = |file: &str, msg: &str, fields: Vec<FieldDescriptorProto>| FileDescriptorProto {
            name: Some(file.to_string()),
            package: Some("google.protobuf".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some(msg.to_string()),
                field: fields,
                ..Default::default()
            }],
            ..Default::default()
        };

        let files = vec![
            wkt("google/protobuf/field_mask.proto", "FieldMask",
                vec![f("paths", 1, Type::String, Label::Repeated, None)]),
            wkt("google/protobuf/duration.proto", "Duration",
                vec![f("seconds", 1, Type::Int64, Label::Optional, None),
                     f("nanos", 2, Type::Int32, Label::Optional, None)]),
            wkt("google/protobuf/timestamp.proto", "Timestamp",
                vec![f("seconds", 1, Type::Int64, Label::Optional, None),
                     f("nanos", 2, Type::Int32, Label::Optional, None)]),
            wkt("google/protobuf/wrappers.proto", "StringValue",
                vec![f("value", 1, Type::String, Label::Optional, None)]),
            FileDescriptorProto {
                name: Some("wkt_fixture.proto".to_string()),
                package: Some("webnext.wkt.test".to_string()),
                syntax: Some("proto3".to_string()),
                dependency: vec![
                    "google/protobuf/field_mask.proto".to_string(),
                    "google/protobuf/duration.proto".to_string(),
                    "google/protobuf/timestamp.proto".to_string(),
                    "google/protobuf/wrappers.proto".to_string(),
                ],
                message_type: vec![DescriptorProto {
                    name: Some("Req".to_string()),
                    field: vec![
                        f("mask", 1, Type::Message, Label::Optional, Some(".google.protobuf.FieldMask")),
                        f("ttl", 2, Type::Message, Label::Optional, Some(".google.protobuf.Duration")),
                        f("at", 3, Type::Message, Label::Optional, Some(".google.protobuf.Timestamp")),
                        f("note", 4, Type::Message, Label::Optional, Some(".google.protobuf.StringValue")),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ];
        DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: files })
            .expect("build wkt pool")
            .get_message_by_name("webnext.wkt.test.Req")
            .expect("Req")
    }

    /// The well-known types a REST URL carries bind from a query param, spelled the
    /// way protobuf-JSON spells them — `?update_mask=a,b` being the canonical case.
    /// Go is pinned by `TestQueryBindsWellKnownTypes`. This lives in unit tests
    /// rather than the conformance matrix on purpose: see the note in
    /// `conformance/proto/conformance.proto`.
    #[test]
    fn well_known_types_bind_from_a_query() {
        let desc = wkt_fixture();
        for (field, raw) in [
            ("mask", "a,b.c"),
            ("ttl", "3.500s"),
            ("at", "2026-01-01T00:00:00Z"),
            ("note", "hi"),
        ] {
            let mut msg = DynamicMessage::new(desc.clone());
            set_by_path(&mut msg, &[field.to_string()], raw).unwrap_or_else(|e| panic!("{field}={raw}: {e}"));
            // Round-trip through the encoder: the value must come back as the same
            // canonical spelling it went in as.
            let json = serde_json::to_string(&msg.transcode_to_dynamic()).expect("encode");
            assert!(json.contains(raw), "{field}={raw} round-tripped as {json}");
        }

        // A malformed value is an error, not a silent default.
        let mut msg = DynamicMessage::new(desc);
        assert!(set_by_path(&mut msg, &["at".to_string()], "not-a-timestamp").is_err());
    }

    /// `bytes` binds from a URL through the *JSON decoder*, so it is spelled exactly
    /// as protobuf-JSON spells it — base64 — rather than guessed at. An arbitrary
    /// submessage is still refused. Go is pinned by the same cases
    /// (`TestQueryBindsWellKnownTypes{,ButNotArbitraryMessages}`).
    #[test]
    fn bytes_binds_from_a_url_and_arbitrary_messages_do_not() {
        let desc = binder_fixture();

        let mut msg = DynamicMessage::new(desc.clone());
        set_by_path(&mut msg, &["blob".to_string()], "AQID").expect("base64 binds");
        let field = desc.get_field_by_name("blob").unwrap();
        assert_eq!(msg.get_field(&field).as_bytes().unwrap().as_ref(), &[1u8, 2, 3]);

        let mut msg = DynamicMessage::new(desc.clone());
        assert!(set_by_path(&mut msg, &["blob".to_string()], "!!!").is_err(), "malformed base64");

        // An arbitrary submessage cannot come from a query param, and the error names
        // the type it refused.
        let mut msg = DynamicMessage::new(desc);
        let err = set_by_path(&mut msg, &["nested".to_string()], "{}").unwrap_err();
        assert!(format!("{err}").contains("Nested"), "{err}");
        // The supported spelling for the same intent:
        let mut msg = DynamicMessage::new(binder_fixture());
        set_by_path(&mut msg, &["nested".to_string(), "id".to_string()], "x").expect("dotted binds");
    }

    /// `body: "<field>"` may name ANY top-level field, not only a singular message —
    /// HttpRule requires only that it be top-level, which is also why a dotted body
    /// path stays refused.
    #[test]
    fn body_may_name_any_top_level_field() {
        let desc = binder_fixture();

        let mut msg = DynamicMessage::new(desc.clone());
        set_message_field(&mut msg, "blob", br#""AQID""#).expect("scalar body field");
        let field = desc.get_field_by_name("blob").unwrap();
        assert_eq!(msg.get_field(&field).as_bytes().unwrap().as_ref(), &[1u8, 2, 3]);

        let mut msg = DynamicMessage::new(desc.clone());
        set_message_field(&mut msg, "nested", br#"{"id":"inner"}"#).expect("message body field");

        // A dotted path is not a top-level field, so it stays refused — by the spec,
        // not by omission.
        let mut msg = DynamicMessage::new(desc);
        assert!(set_message_field(&mut msg, "nested.id", br#""x""#).is_err());
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

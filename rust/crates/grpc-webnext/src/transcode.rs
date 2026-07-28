//! JSON <-> protobuf transcoding for the `+json` codec.
//!
//! grpc-webnext carries opaque message bytes; the envelope (frames, trailers)
//! is always protobuf, but the *application message* may be JSON. Converting
//! JSON to the binary protobuf that a gRPC handler expects (and back) needs the
//! message descriptors, so a `Transcoder` is built from a compiled
//! `FileDescriptorSet` (e.g. `protoc --descriptor_set_out` /
//! `prost_build ... file_descriptor_set_path`).

use prost_reflect::prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use serde::Serialize;

use crate::httprule::HttpRouter;
pub use crate::httprule::{HttpCall, WsBinding};

#[derive(Debug, thiserror::Error)]
pub enum TranscodeError {
    #[error("failed to load descriptor set: {0}")]
    Descriptor(String),
    #[error("unknown method: {0}")]
    UnknownMethod(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("protobuf decode error: {0}")]
    Decode(String),
    #[error("http transcoding: {0}")]
    Http(String),
}

/// Transcodes application messages between JSON and binary protobuf, keyed by
/// gRPC method path (`/pkg.Service/Method`), and maps `google.api.http` REST
/// bindings onto gRPC methods.
#[derive(Clone)]
pub struct Transcoder {
    pool: DescriptorPool,
    router: HttpRouter,
}

impl Transcoder {
    /// Build from an encoded `FileDescriptorSet`.
    pub fn from_file_descriptor_set(bytes: &[u8]) -> Result<Self, TranscodeError> {
        let pool = DescriptorPool::decode(bytes)
            .map_err(|e| TranscodeError::Descriptor(e.to_string()))?;
        let router = HttpRouter::from_pool(&pool);
        Ok(Self { pool, router })
    }

    /// Whether any `google.api.http` REST bindings were found.
    pub fn has_http_rules(&self) -> bool {
        !self.router.is_empty()
    }

    /// Map a REST request `(method, path, query, body)` onto a gRPC call, or
    /// `Ok(None)` if no HTTP binding matches.
    pub fn transcode_http_request(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: &[u8],
    ) -> Result<Option<HttpCall>, TranscodeError> {
        self.router.transcode(method, path, query, body)
    }

    /// Resolve a WebSocket annotation route from its upgrade path, or `None` if the
    /// path matches no binding.
    pub fn match_ws(&self, path: &str, query: Option<&str>) -> Option<WsBinding> {
        self.router.match_ws(path, query)
    }

    /// Whether a gRPC method has any HTTP annotation (used to keep an annotated
    /// RPC's main route off the plain-HTTP surface).
    pub fn is_annotated_method(&self, grpc_method: &str) -> bool {
        self.router.is_annotated(grpc_method)
    }

    /// Resolve `(request_type, response_type)` for a method path.
    fn io_types(&self, path: &str) -> Result<(MessageDescriptor, MessageDescriptor), TranscodeError> {
        let (service, method) = path
            .trim_start_matches('/')
            .split_once('/')
            .ok_or_else(|| TranscodeError::UnknownMethod(path.to_string()))?;
        let svc = self
            .pool
            .get_service_by_name(service)
            .ok_or_else(|| TranscodeError::UnknownMethod(path.to_string()))?;
        let m = svc
            .methods()
            .find(|m| m.name() == method)
            .ok_or_else(|| TranscodeError::UnknownMethod(path.to_string()))?;
        Ok((m.input(), m.output()))
    }

    /// Whether `path` (`/pkg.Service/Method`) resolves to a known method — i.e. this
    /// transcoder can handle it. Callers use it to distinguish "unknown method"
    /// (UNIMPLEMENTED) from a genuine transcode/validation failure.
    pub fn has_method(&self, path: &str) -> bool {
        self.io_types(path).is_ok()
    }

    /// JSON request message -> binary protobuf. Empty input is the default message.
    pub fn request_json_to_proto(&self, path: &str, json: &[u8]) -> Result<Vec<u8>, TranscodeError> {
        let (input, _) = self.io_types(path)?;
        Ok(self.json_to_proto(input, json)?.encode_to_vec())
    }

    /// Binary protobuf response message -> JSON.
    pub fn response_proto_to_json(&self, path: &str, proto: &[u8]) -> Result<Vec<u8>, TranscodeError> {
        self.response_proto_to_json_body(path, proto, "")
    }

    /// Binary protobuf response message -> JSON, honoring a binding's
    /// `response_body`: when it names a (top-level) field, the JSON is **that
    /// field's value** rather than the whole message. An empty name is the
    /// ordinary whole-message encoding.
    pub fn response_proto_to_json_body(
        &self,
        path: &str,
        proto: &[u8],
        response_body: &str,
    ) -> Result<Vec<u8>, TranscodeError> {
        let (_, output) = self.io_types(path)?;
        if response_body.is_empty() {
            return self.proto_to_json(output, proto);
        }
        let field = crate::httprule::field_by_any_name(&output, response_body)
            .ok_or_else(|| TranscodeError::Http(format!("unknown response_body field: {response_body}")))?;

        // Encode the whole message with the library's own rules, then lift out the
        // one member. Doing it this way rather than serializing the field value
        // directly is deliberate: neither prost-reflect nor protojson can encode a
        // lone value, and re-deriving protobuf-JSON's scalar rules by hand (64-bit
        // as a string, bytes as base64, enums by name) in two languages is exactly
        // where the implementations would drift apart.
        let whole = self.proto_to_json(output, proto)?;
        let obj: serde_json::Value = serde_json::from_slice(&whole)?;
        let value = obj.get(field.json_name()).cloned().unwrap_or_else(|| json_zero(&field));
        Ok(serde_json::to_vec(&value)?)
    }

    fn json_to_proto(
        &self,
        desc: MessageDescriptor,
        json: &[u8],
    ) -> Result<DynamicMessage, TranscodeError> {
        if json.is_empty() {
            return Ok(DynamicMessage::new(desc));
        }
        let mut de = serde_json::Deserializer::from_slice(json);
        let msg = DynamicMessage::deserialize(desc, &mut de)?;
        de.end()?;
        Ok(msg)
    }

    fn proto_to_json(&self, desc: MessageDescriptor, proto: &[u8]) -> Result<Vec<u8>, TranscodeError> {
        let msg =
            DynamicMessage::decode(desc, proto).map_err(|e| TranscodeError::Decode(e.to_string()))?;
        let mut buf = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buf);
        msg.serialize(&mut ser)?;
        Ok(buf)
    }
}

/// The JSON a field carries when it is **absent** from the encoded message.
///
/// Whole-message encoding skips default values, so lifting a member out can come
/// up empty — but `response_body` promises a body, and a zero is not "no answer".
/// This is the one place protobuf-JSON's rules are restated by hand, so it is kept
/// to exactly the zero case, and mirrored field-for-field by the Go implementation
/// (`jsonZero` in `go/webnext/transcode.go`).
fn json_zero(field: &prost_reflect::FieldDescriptor) -> serde_json::Value {
    use prost_reflect::Kind;
    use serde_json::Value;

    if field.is_list() {
        return Value::Array(Vec::new());
    }
    if field.is_map() {
        return Value::Object(serde_json::Map::new());
    }
    match field.kind() {
        // An unset message field is null, not `{}` — it was never there.
        Kind::Message(_) => Value::Null,
        Kind::String => Value::String(String::new()),
        // Empty bytes are the empty base64 string.
        Kind::Bytes => Value::String(String::new()),
        Kind::Bool => Value::Bool(false),
        // 64-bit integers are JSON *strings* in protobuf-JSON, zero included.
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 | Kind::Uint64 | Kind::Fixed64 => {
            Value::String("0".to_string())
        }
        // An enum encodes as its value's name; number 0 is the default by
        // definition in proto3, but fall back to the number if it has none.
        Kind::Enum(e) => match e.get_value(0) {
            Some(v) => Value::String(v.name().to_string()),
            None => Value::from(0),
        },
        _ => Value::from(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
        FileDescriptorProto, FileDescriptorSet,
    };

    /// A synthetic message covering every field shape `json_zero` branches on —
    /// the Rust twin of the Go fixture in `go/webnext/httprule_test.go`.
    fn zero_fixture() -> MessageDescriptor {
        let field = |name: &str, number: i32, ty: Type| FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            label: Some(Label::Optional as i32),
            r#type: Some(ty as i32),
            ..Default::default()
        };
        let typed = |name: &str, number: i32, ty: Type, type_name: &str| FieldDescriptorProto {
            type_name: Some(type_name.to_string()),
            ..field(name, number, ty)
        };
        let mut tags = field("tags", 20, Type::String);
        tags.label = Some(Label::Repeated as i32);

        let file = FileDescriptorProto {
            name: Some("zero_fixture.proto".to_string()),
            package: Some("webnext.zero.test".to_string()),
            syntax: Some("proto3".to_string()),
            enum_type: vec![EnumDescriptorProto {
                name: Some("Color".to_string()),
                value: vec![
                    EnumValueDescriptorProto {
                        name: Some("COLOR_UNSPECIFIED".to_string()),
                        number: Some(0),
                        ..Default::default()
                    },
                    EnumValueDescriptorProto {
                        name: Some("RED".to_string()),
                        number: Some(1),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            message_type: vec![
                DescriptorProto {
                    name: Some("Nested".to_string()),
                    field: vec![field("id", 1, Type::String)],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Req".to_string()),
                    field: vec![
                        field("name", 1, Type::String),
                        field("count", 2, Type::Uint32),
                        field("big", 3, Type::Int64),
                        field("ratio", 4, Type::Double),
                        field("flag", 5, Type::Bool),
                        field("blob", 6, Type::Bytes),
                        typed("color", 7, Type::Enum, ".webnext.zero.test.Color"),
                        typed("nested", 8, Type::Message, ".webnext.zero.test.Nested"),
                        tags,
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
            .expect("build fixture pool");
        pool.get_message_by_name("webnext.zero.test.Req").expect("Req")
    }

    /// The zero-value table is the ONE place protobuf-JSON's rules are restated by
    /// hand, so it is the likeliest place for the two implementations to drift.
    /// Every kind is pinned here — with `TestJSONZeroPerKind` in
    /// `go/webnext/httprule_test.go` asserting the identical table.
    #[test]
    fn json_zero_per_kind() {
        let desc = zero_fixture();
        let cases = [
            ("name", "\"\""),                     // string
            ("count", "0"),                       // uint32
            ("big", "\"0\""),                     // int64 — a JSON *string*
            ("ratio", "0"),                       // double
            ("flag", "false"),                    // bool
            ("blob", "\"\""),                     // bytes — the empty base64 string
            ("color", "\"COLOR_UNSPECIFIED\""),   // enum — by name, not number
            ("nested", "null"),                   // an unset message was never there
            ("tags", "[]"),                       // repeated
        ];
        for (name, want) in cases {
            let field = desc.get_field_by_name(name).expect(name);
            let got = serde_json::to_string(&json_zero(&field)).expect("encode");
            assert_eq!(got, want, "json_zero({name})");
        }
    }

    /// Extraction end to end: `response_body` returns the named field's value,
    /// encoded by the library's own rules, and falls back to the zero when absent.
    #[test]
    fn response_body_extraction() {
        let tc = Transcoder::from_file_descriptor_set(testecho::FILE_DESCRIPTOR_SET).expect("transcoder");
        // echo.v1.Echo/Unary returns EchoResponse { message: string }.
        const METHOD: &str = "/echo.v1.Echo/Unary";
        let proto = tc.request_json_to_proto(METHOD, br#"{"message":"hi"}"#).expect("encode");

        // A populated field comes back as its bare JSON value, not wrapped.
        let body = tc.response_proto_to_json_body(METHOD, &proto, "message").expect("extract");
        assert_eq!(String::from_utf8(body).unwrap(), "\"hi\"");

        // The whole message is still the default.
        let whole = tc.response_proto_to_json(METHOD, &proto).expect("whole");
        assert_eq!(String::from_utf8(whole).unwrap(), r#"{"message":"hi"}"#);

        // An absent field falls back to its zero rather than vanishing.
        let empty = tc.request_json_to_proto(METHOD, b"{}").expect("encode empty");
        let body = tc.response_proto_to_json_body(METHOD, &empty, "message").expect("extract");
        assert_eq!(String::from_utf8(body).unwrap(), "\"\"");

        // A name that is no field at all is an error, not a silent whole-message answer.
        assert!(tc.response_proto_to_json_body(METHOD, &proto, "nope").is_err());
    }
}

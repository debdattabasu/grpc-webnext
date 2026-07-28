//! Native-JSON WebSocket frame format for the `+json` codec.
//!
//! JSON WebSocket messages are **text** frames carrying a flat object; you read
//! which field is present to know the frame kind. One WebSocket carries exactly one
//! stream, so the WS URL *is* the route — frames carry neither `method` nor a stream
//! id (human-readable):
//!
//! ```jsonc
//! { "metadata": {…}, "timeoutMillis": 5000 } // open (optional; metadata/deadline)
//! { "message": {…} }                         // data message
//! { "halfClose": true }                      // client done sending
//! { "status": { "code": 0, "message": "" } } // terminal (trailer / reset)
//! ```
//!
//! The application `message` is a *native* JSON value (not base64 bytes). Proto
//! mode uses binary frames (`crate::frame`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Metadata as a JSON object (single value per key).
pub type JsonMeta = BTreeMap<String, String>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonStatus {
    pub code: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// One WebSocket text frame. The frame kind is chosen by which of `message` /
/// `half_close` / `status` (or a bare `metadata` open) is present.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonFrame {
    /// Request metadata (on open) or initial response metadata (header) or trailing
    /// metadata (with `status`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonMeta>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_millis: Option<u32>,
    /// A data message (native JSON). On an open frame, the optional first message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<Value>,
    /// Set → the client is done sending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub half_close: Option<bool>,
    /// Set → terminal status (trailer, or a reset/cancel).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<JsonStatus>,
}

/// Parse a WebSocket text frame.
pub fn decode_json_frame(text: &str) -> Result<JsonFrame, serde_json::Error> {
    serde_json::from_str(text)
}

/// Serialize a `JsonFrame` to a WebSocket text payload.
pub fn encode_json_frame(frame: &JsonFrame) -> String {
    serde_json::to_string(frame).expect("JsonFrame serializes")
}

// --- Conversions to/from the internal protobuf `Frame` (server-side only) -----

use crate::pb::{frame::Kind, metadatum, Frame, HalfClose, Message, Metadatum, Reset, Subscribe};

fn meta_vec_to_json(items: &[Metadatum]) -> Option<JsonMeta> {
    let map: JsonMeta = items
        .iter()
        .filter_map(|m| match &m.value {
            Some(metadatum::Value::AsciiValue(s)) => Some((m.key.clone(), s.clone())),
            _ => None, // binary metadata is omitted from JSON frames
        })
        .collect();
    (!map.is_empty()).then_some(map)
}

fn json_to_meta_vec(meta: &Option<JsonMeta>) -> Vec<Metadatum> {
    meta.iter()
        .flatten()
        .map(|(k, v)| Metadatum {
            key: k.clone(),
            value: Some(metadatum::Value::AsciiValue(v.clone())),
        })
        .collect()
}

fn to_bytes(v: &Value) -> bytes::Bytes {
    serde_json::to_vec(v).unwrap_or_default().into()
}

/// Build a `Subscribe` from the open frame, taking the method from the WS route
/// (the frame itself carries no `method`).
pub fn json_open_to_subscribe(f: JsonFrame, method: String) -> Subscribe {
    Subscribe {
        method,
        headers: json_to_meta_vec(&f.metadata),
        timeout_millis: f.timeout_millis.unwrap_or(0),
        initial_payload: f.message.as_ref().map(to_bytes).unwrap_or_default(),
    }
}

/// Convert a post-open client `JsonFrame` (WS text) into the internal `Frame`. The
/// frame kind is chosen by which field is present.
pub fn json_frame_to_proto(f: JsonFrame) -> Frame {
    let kind = if let Some(status) = f.status {
        // A terminal status from the client is a cancel/reset.
        Kind::Reset(Reset { status_code: status.code, status_message: status.message })
    } else if f.half_close == Some(true) {
        Kind::HalfClose(HalfClose {})
    } else if let Some(message) = f.message {
        Kind::Message(Message { payload: to_bytes(&message) })
    } else {
        // Bare frame is treated as half-close.
        Kind::HalfClose(HalfClose {})
    };
    Frame { kind: Some(kind) }
}

/// Convert an internal server `Frame` into a `JsonFrame` for a WS text message.
/// Returns `None` for kinds with no JSON form (client-only kinds).
pub fn proto_frame_to_json(frame: &Frame) -> Option<JsonFrame> {
    let from_bytes = |b: &[u8]| serde_json::from_slice::<Value>(b).unwrap_or(Value::Null);
    Some(match frame.kind.as_ref()? {
        Kind::Message(m) => JsonFrame {
            message: Some(from_bytes(&m.payload)),
            ..Default::default()
        },
        Kind::Header(h) => JsonFrame {
            metadata: Some(meta_vec_to_json(&h.headers).unwrap_or_default()),
            ..Default::default()
        },
        Kind::Trailer(t) => JsonFrame {
            status: Some(JsonStatus { code: t.status_code, message: t.status_message.clone() }),
            metadata: meta_vec_to_json(&t.trailers),
            ..Default::default()
        },
        Kind::Reset(r) => JsonFrame {
            status: Some(JsonStatus { code: r.status_code, message: r.status_message.clone() }),
            ..Default::default()
        },
        _ => return None,
    })
}

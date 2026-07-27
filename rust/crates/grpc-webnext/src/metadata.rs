//! HTTP header <-> gRPC metadata conversion, protobuf metadata <-> gRPC
//! metadata conversion, and grpc-timeout parsing. Shared by the proxy and the
//! native server library.

use std::time::Duration;

use crate::pb::{metadatum, Metadatum};
use http::{HeaderMap, HeaderName, HeaderValue};
use tonic::metadata::{
    AsciiMetadataKey, BinaryMetadataKey, KeyAndValueRef, MetadataMap, MetadataValue,
};

/// Headers that must not be forwarded as gRPC metadata (hop-by-hop, framing, or
/// set by the gRPC stack itself).
const DENY: &[&str] = &[
    "host",
    "connection",
    "content-length",
    "content-type",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "te",
    "upgrade",
    "grpc-timeout",
    "grpc-status",
    "grpc-message",
    "grpc-status-details-bin",
    "grpc-encoding",
    "grpc-accept-encoding",
];

pub fn is_denied(name: &HeaderName) -> bool {
    DENY.contains(&name.as_str())
}

/// Copy request headers into a gRPC metadata map, minus the denylist.
pub fn request_headers_to_metadata(headers: &HeaderMap) -> MetadataMap {
    let mut filtered = HeaderMap::new();
    for (name, value) in headers.iter() {
        if !is_denied(name) {
            filtered.append(name.clone(), value.clone());
        }
    }
    MetadataMap::from_headers(filtered)
}

/// Merge gRPC metadata into HTTP headers, skipping the denylist.
pub fn merge_metadata_into_headers(meta: &MetadataMap, headers: &mut HeaderMap) {
    for (name, value) in meta.clone().into_headers().iter() {
        if is_denied(name) {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }
}

/// Convert a protobuf metadata list (from a frame) into a gRPC metadata map.
///
/// A `-bin` entry carries **raw bytes** in the frame but is **base64** on the HTTP wire, so
/// it must go through tonic's binary metadata API rather than straight into a `HeaderValue`
/// — raw bytes are not a legal header value at all (a control byte would simply be dropped).
pub fn metadata_vec_to_metadata(items: &[Metadatum]) -> MetadataMap {
    let mut md = MetadataMap::new();
    for m in items {
        match HeaderName::from_bytes(m.key.as_bytes()) {
            Ok(n) if !is_denied(&n) => {}
            _ => continue,
        }
        match &m.value {
            Some(metadatum::Value::AsciiValue(s)) => {
                // An ASCII value under a `-bin` key has no valid encoding; the key parse
                // rejects it, which is the behavior we want.
                if let (Ok(k), Ok(v)) = (
                    m.key.parse::<AsciiMetadataKey>(),
                    MetadataValue::try_from(s.as_str()),
                ) {
                    md.append(k, v);
                }
            }
            Some(metadatum::Value::BinValue(b)) => {
                if let Ok(k) = m.key.parse::<BinaryMetadataKey>() {
                    md.append_bin(k, MetadataValue::from_bytes(b));
                }
            }
            None => {}
        }
    }
    md
}

/// Convert a gRPC metadata map into a protobuf metadata list for a frame — the inverse of
/// [`metadata_vec_to_metadata`], so a `-bin` value is base64-**decoded** back to raw bytes.
pub fn metadata_to_vec(meta: &MetadataMap) -> Vec<Metadatum> {
    let mut out = Vec::new();
    for kv in meta.iter() {
        match kv {
            KeyAndValueRef::Ascii(name, value) => {
                let key = name.as_str().to_string();
                if is_denied_str(&key) {
                    continue;
                }
                if let Ok(s) = value.to_str() {
                    out.push(Metadatum {
                        key,
                        value: Some(metadatum::Value::AsciiValue(s.to_string())),
                    });
                }
            }
            KeyAndValueRef::Binary(name, value) => {
                let key = name.as_str().to_string();
                if is_denied_str(&key) {
                    continue;
                }
                if let Ok(bytes) = value.to_bytes() {
                    out.push(Metadatum {
                        key,
                        value: Some(metadatum::Value::BinValue(bytes)),
                    });
                }
            }
        }
    }
    out
}

fn is_denied_str(key: &str) -> bool {
    DENY.contains(&key)
}

/// Parse a gRPC `grpc-timeout` header (e.g. `100m`, `5S`) into a Duration.
pub fn parse_grpc_timeout(headers: &HeaderMap) -> Option<Duration> {
    let raw = headers.get("grpc-timeout")?.to_str().ok()?;
    let (value, unit) = raw.split_at(raw.len().checked_sub(1)?);
    let n: u64 = value.parse().ok()?;
    let d = match unit {
        "H" => Duration::from_secs(n.checked_mul(3600)?),
        "M" => Duration::from_secs(n.checked_mul(60)?),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    };
    Some(d)
}

/// Build a `grpc-timeout` header value from a Duration (millisecond unit).
pub fn format_grpc_timeout(d: Duration) -> HeaderValue {
    let millis = d.as_millis().min(u128::from(u64::MAX));
    HeaderValue::from_str(&format!("{millis}m")).expect("valid header value")
}

/// Convert a `Subscribe.timeout_millis` field (0 = no deadline) to a Duration.
pub fn grpc_timeout_from_millis(timeout_millis: u32) -> Option<Duration> {
    match timeout_millis {
        0 => None,
        m => Some(Duration::from_millis(u64::from(m))),
    }
}

/// Read the gRPC status `(code, message)` from a response's trailers, falling back to
/// its headers (a "trailers-only" response carries the status in the headers). The
/// `grpc-message` value is percent-decoded.
pub fn read_status(trailers: &HeaderMap, headers: &HeaderMap) -> (u32, String) {
    let get = |name: &str| trailers.get(name).or_else(|| headers.get(name));
    let code = get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    let message = get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .map(percent_decode)
        .unwrap_or_default();
    (code, message)
}

/// Minimal percent-encoding for a `grpc-message` header value.
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b' ' | b'-' | b'_' | b'.' | b'/' | b':') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Minimal gRPC `grpc-message` percent-decoding (`%XX`).
pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

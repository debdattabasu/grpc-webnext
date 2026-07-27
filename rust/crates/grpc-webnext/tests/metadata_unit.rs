//! Header/metadata mapping: grpc-timeout parsing, the request-header denylist, and the
//! frame <-> metadata conversion. (Moved out of `src/metadata.rs`'s inline `#[cfg(test)]`.)

use grpc_webnext::metadata::{
    metadata_to_vec, metadata_vec_to_metadata, parse_grpc_timeout, request_headers_to_metadata,
};
use grpc_webnext::pb::{metadatum, Metadatum};
use http::{HeaderMap, HeaderValue};
use std::time::Duration;

#[test]
fn parses_timeout_units() {
    let mut h = HeaderMap::new();
    h.insert("grpc-timeout", HeaderValue::from_static("100m"));
    assert_eq!(parse_grpc_timeout(&h), Some(Duration::from_millis(100)));
    h.insert("grpc-timeout", HeaderValue::from_static("5S"));
    assert_eq!(parse_grpc_timeout(&h), Some(Duration::from_secs(5)));
}

#[test]
fn drops_denied_headers() {
    let mut h = HeaderMap::new();
    h.insert("content-type", HeaderValue::from_static("x"));
    h.insert("x-custom", HeaderValue::from_static("v"));
    let back = request_headers_to_metadata(&h).into_headers();
    assert!(back.get("content-type").is_none());
    assert!(back.get("x-custom").is_some());
}

/// A frame's `-bin` metadata is **raw bytes**, but the HTTP wire carries `-bin` as base64.
/// Bytes that are not a legal header value (control bytes here) must therefore survive the
/// round trip through the header layer — a naive `HeaderValue::from_bytes` would drop them
/// entirely, which is what the `metadata-roundtrip-websocket` conformance case caught.
#[test]
fn binary_metadata_round_trips_through_headers() {
    let raw = vec![0x01, 0x02, 0x03, 0x04];
    let items = vec![
        Metadatum {
            key: "x-ascii".into(),
            value: Some(metadatum::Value::AsciiValue("plain".into())),
        },
        Metadatum {
            key: "x-blob-bin".into(),
            value: Some(metadatum::Value::BinValue(raw.clone().into())),
        },
    ];

    let md = metadata_vec_to_metadata(&items);
    // On the wire the binary value is base64, not the raw bytes. gRPC permits padded and
    // unpadded; tonic (like grpc-go) emits unpadded, and both are accepted on decode.
    assert_eq!(md.clone().into_headers().get("x-blob-bin").unwrap(), "AQIDBA");

    let back = metadata_to_vec(&md);
    let find = |key: &str| back.iter().find(|m| m.key == key).cloned();
    assert_eq!(
        find("x-ascii").and_then(|m| m.value),
        Some(metadatum::Value::AsciiValue("plain".into()))
    );
    assert_eq!(
        find("x-blob-bin").and_then(|m| m.value),
        Some(metadatum::Value::BinValue(raw.into()))
    );
}

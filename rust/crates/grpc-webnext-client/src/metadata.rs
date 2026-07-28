//! Call metadata: ASCII values, and `-bin` keys carrying raw bytes.
//!
//! On this path metadata is HTTP/2 headers and the gRPC spec applies verbatim — so
//! a `-bin` key is **base64** on the wire, unlike the custom `Frame` protocol where
//! `bin_value` holds the raw bytes. That seam has produced a real bug in this repo
//! before (see `/doc/GO_SERVER.md`), so the encode/decode lives here and nowhere else.

use std::collections::HashMap;

use base64::Engine as _;

/// Metadata for one call. Keys are lowercase, as HTTP/2 requires.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Metadata {
    entries: Vec<(String, MetadataValue)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataValue {
    Ascii(String),
    Binary(Vec<u8>),
}

impl Metadata {
    pub fn new() -> Metadata {
        Metadata::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Set an ASCII value, replacing any existing entries for the key.
    pub fn insert(&mut self, key: &str, value: impl Into<String>) -> &mut Self {
        self.remove(key);
        self.entries.push((key.to_ascii_lowercase(), MetadataValue::Ascii(value.into())));
        self
    }

    /// Set a binary value. The key must end in `-bin`, which is the gRPC signal
    /// that the value is base64 on the wire.
    pub fn insert_bin(&mut self, key: &str, value: impl Into<Vec<u8>>) -> &mut Self {
        debug_assert!(key.ends_with("-bin"), "binary metadata keys must end in -bin");
        self.remove(key);
        self.entries.push((key.to_ascii_lowercase(), MetadataValue::Binary(value.into())));
        self
    }

    /// Add a value without removing existing ones — gRPC metadata is multi-valued.
    pub fn append(&mut self, key: &str, value: impl Into<String>) -> &mut Self {
        self.entries.push((key.to_ascii_lowercase(), MetadataValue::Ascii(value.into())));
        self
    }

    pub fn remove(&mut self, key: &str) {
        let key = key.to_ascii_lowercase();
        self.entries.retain(|(k, _)| *k != key);
    }

    /// The first ASCII value for `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        let key = key.to_ascii_lowercase();
        self.entries.iter().find_map(|(k, v)| match v {
            MetadataValue::Ascii(s) if *k == key => Some(s.as_str()),
            _ => None,
        })
    }

    /// The first binary value for `key`.
    pub fn get_bin(&self, key: &str) -> Option<&[u8]> {
        let key = key.to_ascii_lowercase();
        self.entries.iter().find_map(|(k, v)| match v {
            MetadataValue::Binary(b) if *k == key => Some(b.as_slice()),
            _ => None,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &MetadataValue)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Render to wire headers, base64-encoding every `-bin` value.
    pub fn to_headers(&self) -> Vec<(String, String)> {
        self.entries
            .iter()
            .map(|(k, v)| {
                let value = match v {
                    MetadataValue::Ascii(s) => s.clone(),
                    MetadataValue::Binary(b) => base64::engine::general_purpose::STANDARD.encode(b),
                };
                (k.clone(), value)
            })
            .collect()
    }

    /// Read wire headers back, base64-decoding `-bin` values. Framing headers are
    /// dropped: they describe the transport, not the call.
    pub fn from_headers(headers: &HashMap<String, String>) -> Metadata {
        let mut md = Metadata::new();
        for (key, value) in headers {
            let key = key.to_ascii_lowercase();
            if key.starts_with(':') || is_framing_header(&key) {
                continue;
            }
            if key.ends_with("-bin") {
                // gRPC permits unpadded base64; a value we cannot decode is kept as
                // ASCII rather than dropped, so nothing silently disappears.
                match base64::engine::general_purpose::STANDARD
                    .decode(value)
                    .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(value))
                {
                    Ok(bytes) => md.entries.push((key, MetadataValue::Binary(bytes))),
                    Err(_) => md.entries.push((key, MetadataValue::Ascii(value.clone()))),
                }
            } else {
                md.entries.push((key, MetadataValue::Ascii(value.clone())));
            }
        }
        md.entries.sort_by(|a, b| a.0.cmp(&b.0)); // HashMap order is not stable
        md
    }
}

/// Headers that belong to the transport rather than the call. Mirrors the server's
/// own denylist so what a client sees matches what a server echoes.
fn is_framing_header(key: &str) -> bool {
    matches!(
        key,
        "content-type"
            | "content-length"
            | "te"
            | "grpc-encoding"
            | "grpc-accept-encoding"
            | "grpc-timeout"
            | "user-agent"
            | "date"
            | "trailer"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bin_values_are_base64_on_the_wire() {
        let mut md = Metadata::new();
        md.insert_bin("x-trace-bin", vec![0, 1, 2, 255]);
        assert_eq!(md.to_headers(), vec![("x-trace-bin".to_string(), "AAEC/w==".to_string())]);
    }

    #[test]
    fn bin_values_round_trip() {
        let mut md = Metadata::new();
        md.insert_bin("x-trace-bin", vec![0, 1, 2, 255]);
        md.insert("x-ascii", "plain");
        let wire: HashMap<String, String> = md.to_headers().into_iter().collect();
        let back = Metadata::from_headers(&wire);
        assert_eq!(back.get_bin("x-trace-bin"), Some(&[0u8, 1, 2, 255][..]));
        assert_eq!(back.get("x-ascii"), Some("plain"));
    }

    #[test]
    fn unpadded_base64_decodes_too() {
        // The gRPC spec explicitly allows senders to omit padding.
        let wire: HashMap<String, String> =
            [("x-t-bin".to_string(), "AAEC".to_string())].into_iter().collect();
        assert_eq!(Metadata::from_headers(&wire).get_bin("x-t-bin"), Some(&[0u8, 1, 2][..]));
    }

    #[test]
    fn undecodable_bin_is_kept_as_ascii_not_dropped() {
        let wire: HashMap<String, String> =
            [("x-t-bin".to_string(), "!!!not base64!!!".to_string())].into_iter().collect();
        let md = Metadata::from_headers(&wire);
        assert_eq!(md.get("x-t-bin"), Some("!!!not base64!!!"));
    }

    #[test]
    fn framing_and_pseudo_headers_are_not_call_metadata() {
        let wire: HashMap<String, String> = [
            (":status".to_string(), "200".to_string()),
            ("content-type".to_string(), "application/grpc".to_string()),
            ("x-real".to_string(), "yes".to_string()),
        ]
        .into_iter()
        .collect();
        let md = Metadata::from_headers(&wire);
        assert_eq!(md.len(), 1);
        assert_eq!(md.get("x-real"), Some("yes"));
    }

    #[test]
    fn keys_are_case_insensitive() {
        let mut md = Metadata::new();
        md.insert("X-Mixed-Case", "v");
        assert_eq!(md.get("x-mixed-case"), Some("v"));
        md.remove("X-MIXED-CASE");
        assert!(md.is_empty());
    }
}

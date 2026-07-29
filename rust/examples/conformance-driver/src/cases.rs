//! The declarative case model — the Rust reading of `conformance/schema/case.schema.json`.
//!
//! Deliberately a *second, independent* parse of the same contract rather than
//! anything shared with the TypeScript driver. Two drivers that agreed because they
//! read the cases through one implementation would prove less than one driver does;
//! the point of the matrix is that the only thing in common is the YAML on disk.

use base64::Engine as _;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SuiteFile {
    pub suite: String,
    pub cases: Vec<Case>,
}

// Several fields exist so that every case in the suite still *deserializes* — a
// driver that choked on a REST case could not report it as skipped — even though
// this driver never reads them. Dropping them would make the parse lossy and turn an
// unsupported case into a crash instead of a visible SKIP.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Case {
    pub name: String,
    pub rpc: Rpc,
    /// Present on a REST case. Only its presence matters here — this driver does not
    /// run REST (see `main.rs`), so the contents are never inspected.
    #[serde(default)]
    pub rest: Option<serde_yaml::Value>,
    #[serde(default)]
    pub codecs: Option<Vec<String>>,
    /// Unused: the transport profile is fixed by the client config, not the case, and
    /// this driver only has one. Accepted so the schema still parses.
    #[serde(default)]
    pub transports: Option<Vec<String>>,
    #[serde(default)]
    pub timeout_millis: Option<u64>,
    #[serde(default)]
    pub header_timeout_millis: Option<u64>,
    #[serde(default)]
    pub request_metadata: Option<Vec<Metadatum>>,
    #[serde(default)]
    pub requires: Option<Requires>,
    #[serde(default)]
    pub request: Option<Message>,
    #[serde(default)]
    pub requests: Option<Vec<Message>>,
    #[serde(default)]
    pub cancel_after_messages: Option<usize>,
    pub expect: Expect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum Rpc {
    Unary,
    ServerStream,
    ClientStream,
    BidiStream,
}

#[derive(Debug, Default, Deserialize)]
pub struct Requires {
    #[serde(default)]
    pub max_message_bytes: Option<u64>,
    #[serde(default)]
    pub transcoder: Option<bool>,
}

impl Case {
    /// The server config profile this case needs. Must agree exactly with the
    /// TypeScript harness's `requiresKey`, because that harness is what starts the
    /// servers and hands this driver a URL per profile.
    pub fn requires_key(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(r) = &self.requires {
            if let Some(max) = r.max_message_bytes {
                parts.push(format!("max:{max}"));
            }
            if r.transcoder == Some(false) {
                parts.push("notranscoder".to_string());
            }
        }
        if parts.is_empty() {
            "default".to_string()
        } else {
            parts.join(",")
        }
    }

    /// Whether the `proto` codec is in play. Mirrors the TS harness, which derives
    /// profiles from `codecs` alone — `transports` does not select a profile there.
    pub fn covers_proto(&self) -> bool {
        match &self.codecs {
            Some(c) => c.iter().any(|c| c == "proto"),
            None => true, // default is [proto, json]
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct Message {
    #[serde(default)]
    pub payload: Option<BytesSpec>,
    #[serde(default)]
    pub response_definition: Option<ResponseDefinitionSpec>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ResponseDefinitionSpec {
    #[serde(default)]
    pub status_code: Option<u32>,
    #[serde(default)]
    pub status_message: Option<String>,
    #[serde(default)]
    pub headers: Option<Vec<Metadatum>>,
    #[serde(default)]
    pub trailers: Option<Vec<Metadatum>>,
    #[serde(default)]
    pub payload: Option<BytesSpec>,
    #[serde(default)]
    pub stream_messages: Option<Vec<StreamMessageSpec>>,
    #[serde(default)]
    pub delay_ms: Option<u32>,
    #[serde(default)]
    pub oversize_response_bytes: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
pub struct StreamMessageSpec {
    #[serde(default)]
    pub payload: Option<BytesSpec>,
    #[serde(default)]
    pub delay_ms: Option<u32>,
}

/// A byte value: UTF-8 text, base64, or N zero bytes (for size-limit cases).
#[derive(Debug, Default, Deserialize)]
pub struct BytesSpec {
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub b64: Option<String>,
    #[serde(default)]
    pub zeros: Option<usize>,
}

impl BytesSpec {
    pub fn to_vec(&self) -> Vec<u8> {
        if let Some(t) = &self.text {
            return t.as_bytes().to_vec();
        }
        if let Some(b) = &self.b64 {
            return base64::engine::general_purpose::STANDARD
                .decode(b)
                .expect("case has invalid base64");
        }
        if let Some(n) = self.zeros {
            return vec![0u8; n];
        }
        Vec::new()
    }
}

#[derive(Debug, Deserialize)]
pub struct Metadatum {
    pub key: String,
    #[serde(default)]
    pub ascii: Option<String>,
    #[serde(default)]
    pub b64: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Default, Deserialize)]
pub struct Expect {
    #[serde(default)]
    pub http_status: Option<u16>,
    #[serde(default)]
    pub status: Option<StatusExpect>,
    #[serde(default)]
    pub response: Option<PayloadMatcher>,
    #[serde(default)]
    pub messages: Option<Vec<PayloadMatcher>>,
    #[serde(default)]
    pub message_count: Option<usize>,
    #[serde(default)]
    pub received_count: Option<u32>,
    #[serde(default)]
    pub headers_contain: Option<Vec<Metadatum>>,
    #[serde(default)]
    pub trailers_contain: Option<Vec<Metadatum>>,
    /// REST only; never asserted here, but parsed so a REST case still deserializes.
    #[serde(default)]
    pub raw_body: Option<String>,
    #[serde(default)]
    pub raw_messages: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
pub struct StatusExpect {
    #[serde(default)]
    pub code: Option<i32>,
    #[serde(default)]
    pub message_contains: Option<String>,
    #[serde(default)]
    pub not_ok: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PayloadMatcher {
    #[serde(default)]
    pub payload: Option<BytesSpec>,
    #[serde(default)]
    pub request_info: Option<RequestInfoMatcher>,
}

#[derive(Debug, Default, Deserialize)]
pub struct RequestInfoMatcher {
    #[serde(default)]
    pub request_headers_contain: Option<Vec<Metadatum>>,
    #[serde(default)]
    pub timeout_present: Option<bool>,
    #[serde(default)]
    pub json: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_values_decode_in_all_three_spellings() {
        assert_eq!(BytesSpec { text: Some("hi".into()), ..Default::default() }.to_vec(), b"hi");
        assert_eq!(
            BytesSpec { b64: Some("AAEC".into()), ..Default::default() }.to_vec(),
            vec![0u8, 1, 2]
        );
        assert_eq!(BytesSpec { zeros: Some(3), ..Default::default() }.to_vec(), vec![0u8; 3]);
        // An absent value is empty, not an error: `payload: {}` is a legal empty payload.
        assert_eq!(BytesSpec::default().to_vec(), Vec::<u8>::new());
    }

    #[test]
    fn requires_key_matches_the_typescript_harness() {
        // These strings are a cross-language contract: the TS harness starts one
        // server per key and hands this driver the URL for it. Drift here does not
        // fail loudly — it silently runs cases against the wrong server config.
        let case = |yaml: &str| -> Case { serde_yaml::from_str(yaml).unwrap() };
        let base = "name: n\nrpc: Unary\nexpect: {status: {code: 0}}\n";
        assert_eq!(case(base).requires_key(), "default");
        assert_eq!(
            case(&format!("{base}requires: {{max_message_bytes: 64}}\n")).requires_key(),
            "max:64"
        );
        assert_eq!(
            case(&format!("{base}requires: {{transcoder: false}}\n")).requires_key(),
            "notranscoder"
        );
        // `transcoder: true` is not part of the key — it is the default profile.
        assert_eq!(case(&format!("{base}requires: {{transcoder: true}}\n")).requires_key(), "default");
    }

    #[test]
    fn codec_selection_defaults_to_covering_proto() {
        let case = |yaml: &str| -> Case { serde_yaml::from_str(yaml).unwrap() };
        let base = "name: n\nrpc: Unary\nexpect: {status: {code: 0}}\n";
        assert!(case(base).covers_proto(), "the default is [proto, json]");
        assert!(case(&format!("{base}codecs: [proto]\n")).covers_proto());
        assert!(case(&format!("{base}codecs: [proto, json]\n")).covers_proto());
        assert!(!case(&format!("{base}codecs: [json]\n")).covers_proto());
    }
}

//! gRPC status: the canonical codes, and reading one off a response.

use std::collections::HashMap;
use std::fmt;

use crate::metadata::Metadata;

/// The canonical gRPC status codes (<https://grpc.io/docs/guides/status-codes/>).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u16)]
pub enum Code {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl Code {
    /// Map a wire value; anything outside 0..=16 is `Unknown`, per the gRPC spec's
    /// instruction to treat unrecognized codes as UNKNOWN rather than failing.
    pub fn from_i32(value: i32) -> Code {
        match value {
            0 => Code::Ok,
            1 => Code::Cancelled,
            2 => Code::Unknown,
            3 => Code::InvalidArgument,
            4 => Code::DeadlineExceeded,
            5 => Code::NotFound,
            6 => Code::AlreadyExists,
            7 => Code::PermissionDenied,
            8 => Code::ResourceExhausted,
            9 => Code::FailedPrecondition,
            10 => Code::Aborted,
            11 => Code::OutOfRange,
            12 => Code::Unimplemented,
            13 => Code::Internal,
            14 => Code::Unavailable,
            15 => Code::DataLoss,
            16 => Code::Unauthenticated,
            _ => Code::Unknown,
        }
    }
}

/// A terminal gRPC status. Trailing metadata rides along, because a server that
/// fails is often the one with the most to say about why.
#[derive(Debug, Clone)]
pub struct Status {
    pub code: Code,
    pub message: String,
    pub metadata: Metadata,
}

impl Status {
    pub fn new(code: Code, message: impl Into<String>) -> Status {
        Status { code, message: message.into(), metadata: Metadata::new() }
    }

    /// A transport-level failure: the call could not be carried. gRPC's status for
    /// that is UNAVAILABLE, never UNKNOWN — UNKNOWN means the *server* said
    /// something we could not interpret.
    pub fn unavailable(message: impl Into<String>) -> Status {
        Status::new(Code::Unavailable, message)
    }

    pub fn is_ok(&self) -> bool {
        self.code == Code::Ok
    }

    /// Read `grpc-status` / `grpc-message` out of a header or trailer block.
    /// Returns `None` when there is no `grpc-status` at all, which is how the
    /// caller tells "no trailers yet" from "trailers said OK".
    pub fn from_headers(headers: &HashMap<String, String>) -> Option<Status> {
        let raw = headers.get(GRPC_STATUS)?;
        let code = raw.trim().parse::<i32>().map(Code::from_i32).unwrap_or(Code::Unknown);
        let message = headers.get(GRPC_MESSAGE).map(|m| decode_grpc_message(m)).unwrap_or_default();
        let mut metadata = Metadata::from_headers(headers);
        // The status itself is not user metadata.
        for key in [GRPC_STATUS, GRPC_MESSAGE] {
            metadata.remove(key);
        }
        Some(Status { code, message, metadata })
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for Status {}

pub(crate) const GRPC_STATUS: &str = "grpc-status";
pub(crate) const GRPC_MESSAGE: &str = "grpc-message";

/// `grpc-message` is percent-encoded on the wire (gRPC's own restricted variant).
/// An undecodable value is passed through rather than lost — a mangled message is
/// still more useful than none.
fn decode_grpc_message(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn no_grpc_status_is_none() {
        assert!(Status::from_headers(&headers(&[("content-type", "application/grpc")])).is_none());
    }

    #[test]
    fn reads_code_and_percent_decoded_message() {
        let s = Status::from_headers(&headers(&[
            ("grpc-status", "9"),
            ("grpc-message", "not%20ready%3A%20try%20later"),
        ]))
        .unwrap();
        assert_eq!(s.code, Code::FailedPrecondition);
        assert_eq!(s.message, "not ready: try later");
    }

    #[test]
    fn keeps_trailing_metadata_but_not_the_status_itself() {
        let s = Status::from_headers(&headers(&[
            ("grpc-status", "0"),
            ("grpc-message", "ok"),
            ("x-trailer", "value"),
        ]))
        .unwrap();
        assert_eq!(s.metadata.get("x-trailer"), Some("value"));
        assert!(s.metadata.get("grpc-status").is_none());
        assert!(s.metadata.get("grpc-message").is_none());
    }

    #[test]
    fn unrecognized_codes_are_unknown_not_an_error() {
        // The spec says treat an unrecognized code as UNKNOWN; failing to parse the
        // response entirely would turn a live server into a dead one.
        for raw in ["99", "-1", "banana", ""] {
            let s = Status::from_headers(&headers(&[("grpc-status", raw)])).unwrap();
            assert_eq!(s.code, Code::Unknown, "grpc-status: {raw:?}");
        }
    }

    #[test]
    fn undecodable_message_is_passed_through() {
        let s = Status::from_headers(&headers(&[("grpc-status", "2"), ("grpc-message", "%ZZ")]))
            .unwrap();
        assert_eq!(s.message, "%ZZ");
    }
}

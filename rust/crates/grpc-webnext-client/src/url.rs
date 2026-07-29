//! Mapping an endpoint URL to the WebSocket URL and `:authority` to dial.
//!
//! Deliberately **not** behind `cfg(target_arch = "wasm32")` even though only the
//! browser entry point calls it. It is pure string handling with nothing
//! wasm-specific in it, and gating it on wasm32 meant its tests compiled on no
//! target anyone runs: `cargo test` skips them on the host, and there is no
//! browser test runner in CI. Two tests that can never fail are not coverage.

use crate::status::Status;

/// Map an http(s) endpoint to the ws(s) URL and the `:authority` to send.
///
/// Only the wasm32 `connect` entry point calls this, so on a host build nothing
/// does — but the tests below do, on every target, which is the whole point of it
/// living outside the wasm gate.
///
/// `base_url` is the ordinary endpoint URL — the same one native gRPC and the
/// TypeScript client use, because grpc-webnext serves every surface on one port.
/// The scheme maps to `ws`/`wss`: the tunnelled HTTP/2 is always cleartext h2c,
/// since the outer WebSocket already carries the transport security.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn websocket_url(base_url: &str) -> Result<(String, String), Status> {
    let trimmed = base_url.trim_end_matches('/');
    let (scheme, rest) = trimmed
        .split_once("://")
        .ok_or_else(|| Status::unavailable(format!("base URL has no scheme: {base_url}")))?;
    let ws_scheme = match scheme {
        "https" | "wss" => "wss",
        "http" | "ws" => "ws",
        other => {
            return Err(Status::unavailable(format!("unsupported URL scheme: {other}")));
        }
    };
    // The authority is host[:port] — everything before the first path segment.
    let authority = rest.split('/').next().unwrap_or(rest).to_string();
    Ok((format!("{ws_scheme}://{rest}"), authority))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_scheme_and_extracts_authority() {
        for (input, ws, authority) in [
            ("http://localhost:8080", "ws://localhost:8080", "localhost:8080"),
            ("https://api.example.com", "wss://api.example.com", "api.example.com"),
            ("https://api.example.com/grpc/", "wss://api.example.com/grpc", "api.example.com"),
            // A path must not leak into the authority: `:authority` is host[:port],
            // and sending "api.example.com/grpc" would be a malformed pseudo-header.
            ("http://127.0.0.1:8080/a/b", "ws://127.0.0.1:8080/a/b", "127.0.0.1:8080"),
            // Already-ws URLs pass through, so a caller who has one need not convert.
            ("ws://localhost:9090", "ws://localhost:9090", "localhost:9090"),
            ("wss://api.example.com", "wss://api.example.com", "api.example.com"),
        ] {
            let (got_ws, got_authority) = websocket_url(input).unwrap();
            assert_eq!((got_ws.as_str(), got_authority.as_str()), (ws, authority), "{input}");
        }
    }

    #[test]
    fn rejects_a_url_it_cannot_map() {
        // No scheme at all: this would otherwise dial a host named "localhost:8080"
        // under a scheme-less URL and fail somewhere far less obvious.
        assert!(websocket_url("localhost:8080").is_err());
        assert!(websocket_url("ftp://host").is_err());
        assert!(websocket_url("").is_err());
    }

    #[test]
    fn a_trailing_slash_does_not_become_an_empty_path_segment() {
        // "https://host/" must not map to "wss://host/", which some servers route
        // differently from "wss://host".
        let (ws, authority) = websocket_url("https://host/").unwrap();
        assert_eq!(ws, "wss://host");
        assert_eq!(authority, "host");
    }
}

//! **tonic's own generated client stubs, over a grpc-webnext tunnel.**
//!
//! The library half of the example: the generated stubs and the host dial helper,
//! shared by the demo binary and the end-to-end tests. See `main.rs` for the demo and
//! `tests/e2e.rs` for the tests, which serve a real Greeter in-process and drive it
//! through these stubs over a real WebSocket.

pub mod dial;

/// Stock `tonic-prost-build` output for `greeter.v1` — no grpc-webnext-specific
/// codegen anywhere. Clients for the demo and the tests; servers for the tests.
pub mod pb {
    tonic::include_proto!("greeter.v1");
}

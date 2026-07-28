//! # grpc-webnext-client
//!
//! A gRPC client for **Rust WASM frontends**, speaking real gRPC to a
//! [grpc-webnext](https://github.com/debdattabasu/grpc-webnext) endpoint over an
//! [h2ts](https://github.com/debdattabasu/h2ts) WebSocket tunnel.
//!
//! **No tonic, no hyper, no tokio.** The wire is real HTTP/2 — trailers,
//! multiplexing, flow control — so there is nothing to translate: framing plus a
//! status read off the trailers is the whole client. Everything is single-threaded
//! (`Rc`, `!Send`), which is what a browser actually is; nothing here asserts
//! `Send` to satisfy a runtime that does not exist on this target.
//!
//! ```no_run
//! # async fn demo(client: grpc_webnext_client::Client) -> Result<(), grpc_webnext_client::Status> {
//! use grpc_webnext_client::CallOptions;
//!
//! let reply = client
//!     .unary("/helloworld.Greeter/SayHello", encoded_request(), CallOptions::new())
//!     .await?;
//! let _ = reply.message; // your codec decodes these bytes
//! # Ok(()) }
//! # fn encoded_request() -> Vec<u8> { Vec::new() }
//! ```
//!
//! ## Codec
//!
//! The client deals in **message bytes**, so it is codec-agnostic: encode with
//! `prost`, or anything else. The `prost` feature adds typed helpers over
//! [`prost::Message`]; there is no generated service stub layer yet, so a service
//! is a handful of thin wrappers over [`Client::unary`] and friends.
//!
//! ## Streaming
//!
//! All four cardinalities. Backpressure on the response is real and costs nothing
//! here: `h2ts-client` replenishes the HTTP/2 receive window only as the body is
//! polled, so a consumer that stops reading stops the *server* rather than filling
//! memory — the same property the TypeScript client gets, for the same reason.
//!
//! The streaming cardinalities need `h2ts-client` **0.1.2**: at 0.1.1
//! `Response::into_body` took `self` while `trailers()` needed `&self`, so a caller
//! could stream the body or read the trailers, never both — and gRPC's terminal
//! status lives in the trailers. A streaming client built on 0.1.1 could not tell a
//! failed stream from a successfully empty one. `Response::into_parts` fixes that.
//!
//! ## Reconnect
//!
//! There is none: a dead tunnel stays dead and [`Client::is_closed`] reports it.
//! Deciding when to redial is the application's, and a frontend usually wants that
//! tied to its own lifecycle rather than hidden in a transport.

mod client;
mod codec;
mod metadata;
mod status;

pub use client::{CallOptions, Client, Streaming, UnaryResponse};
pub use codec::{encode_message, Deframer};
pub use metadata::{Metadata, MetadataValue};
pub use status::{Code, Status};

// Re-exported so callers can tune the tunnel without depending on h2ts-client
// directly.
pub use h2ts_client::{ConnectOptions, Transport};

/// The WebSocket subprotocol an h2ts client offers.
pub const H2TS_SUBPROTOCOL: &str = h2ts_client::DEFAULT_SUBPROTOCOL;

#[cfg(target_arch = "wasm32")]
mod web;
#[cfg(target_arch = "wasm32")]
pub use web::connect;

#[cfg(feature = "prost")]
mod typed;
#[cfg(feature = "prost")]
pub use typed::{TypedClient, TypedResponse};

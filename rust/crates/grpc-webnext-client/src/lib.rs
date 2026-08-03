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
//! [`prost::Message`], and a service is then a handful of thin wrappers over
//! [`Client::unary`] and friends.
//!
//! ## Generated stubs
//!
//! For `greeter.say_hello(request)` instead of hand-written wrappers, the `tonic`
//! feature lets **tonic's own generated stubs** run over the tunnel —
//! `GreeterClient::new(client.into_tonic())`, all four cardinalities, nothing
//! grpc-webnext-specific in the codegen. See [`tonic_service`], which also covers the
//! one build-script setting a browser target needs.
//!
//! ## Streaming
//!
//! All four cardinalities. Backpressure on the response is real and costs nothing
//! here: `h2ts-client` replenishes the HTTP/2 receive window only as the body is
//! polled, so a consumer that stops reading stops the *server* rather than filling
//! memory — the same property the TypeScript client gets, for the same reason.
//!
//! Dropping a [`Streaming`] cancels the RPC: the HTTP/2 stream is reset, so the
//! server stops work it has no reader for. That is also how a deadline stops a
//! stream, and why [`CallOptions::timeout`] covers a stream's whole lifetime rather
//! than only its opening.
//!
//! The streaming cardinalities need `h2ts-client` **0.1.2**: at 0.1.1
//! `Response::into_body` took `self` while `trailers()` needed `&self`, so a caller
//! could stream the body or read the trailers, never both — and gRPC's terminal
//! status lives in the trailers. A streaming client built on 0.1.1 could not tell a
//! failed stream from a successfully empty one. `Response::into_parts` fixes that.
//!
//! ## Reconnect
//!
//! [`Client`] is a gRPC **channel**, not a handle to a socket: the tunnel opens on
//! the first call and **reopens if it drops**, the way `tonic::transport::Channel`
//! does. The call that discovers a dead tunnel reports the failure and the next one
//! reconnects — the transport never silently replays a request the server may
//! already have seen, because that is a retry policy decision and not its to make.
//!
//! [`Client::state`] and [`Client::state_changes`] surface where the channel is
//! (gRPC's connectivity states, `WaitForStateChange` as a stream), so an app can
//! say something useful instead of guessing from a failed call.
//!
//! There is no reconnect **backoff**: like tonic, a redial happens when a call asks
//! for one, so the call rate bounds the dial rate. A client built with
//! [`Client::over_transport`] cannot redial at all — the transport is consumed —
//! and says so rather than pretending to be live.

mod client;
mod codec;
mod metadata;
mod state;
mod status;
mod url;

pub use client::{CallOptions, Client, Streaming, UnaryResponse};
pub use client::Connector;
pub use state::ConnectivityState;
pub use codec::{encode_message, Deframer};
pub use metadata::{Metadata, MetadataValue};
pub use status::{Code, Status};

// Re-exported so callers can tune the tunnel — or supply their own — without
// depending on h2ts-client directly. `TransportError` belongs here too: a caller
// building a `Transport` has to name the error type its sink produces, so leaving it
// out meant `Client::over_transport` could not be used without adding h2ts-client as
// a second dependency. `H2Connection` is here for the same reason one step up:
// [`Connector`] is a function returning one, so a *reconnecting* client could not be
// built without naming the type.
pub use h2ts_client::{ConnectOptions, H2Connection, Transport, TransportError};

/// Open an h2ts tunnel over a byte transport: the connection, plus the driver future
/// the caller must poll for anything to happen.
///
/// This is `h2ts_client::connect`, re-exported because a [`Connector`] has to produce
/// an [`H2Connection`] and there was otherwise no way to make one from here. In a
/// browser prefer [`connect`], which builds the whole reconnecting client.
pub use h2ts_client::connect as open_tunnel;

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

#[cfg(feature = "tonic")]
pub mod tonic_service;
#[cfg(feature = "tonic")]
pub use tonic_service::{ResponseBody, TonicService};

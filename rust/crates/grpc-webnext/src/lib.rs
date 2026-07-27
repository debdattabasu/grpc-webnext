//! grpc-webnext: serve the grpc-webnext wire protocol (unary over Fetch, streaming over
//! WebSocket, plus `+json` / REST transcoding) over any gRPC service.
//!
//! All the inbound protocol translation is shared; the only thing that varies is where the
//! translated gRPC call lands — a local in-process [`tonic::service::Routes`] you own
//! ([`serve_in_process`], the "native server" / wrap mode) or a remote upstream over an
//! HTTP/2 channel ([`serve_proxy`], the standalone binary proxy). Both are the same
//! [`Backend`]; the two entry points build one and run the identical handlers, so a client
//! can't tell a wrapped response from a proxied one.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::HeaderMap;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Incoming;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tonic::service::Routes;
use tonic::transport::Channel;

// Inbound protocol translation.
pub mod backend;
mod drain;
mod fetch;
mod h2ts;
mod reflect;
pub mod schema;
mod ws;

// Wire codec + gRPC-semantics core: generated types, frame codec, Fetch-response framing,
// metadata, HTTP-rule transcoding.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/grpc.webnext.v1.rs"));
}
pub mod codec;
pub mod frame;
mod framing;
pub mod grpc_framing;
pub mod httprule;
pub mod json_frame;
pub mod metadata;
pub mod transcode;

pub(crate) use drain::Drain;

pub use backend::Backend;
pub use schema::{Schema, SchemaSource};

pub use codec::BytesCodec;
pub use frame::{decode_frame, encode_frame, FrameError};
pub use framing::{
    decode_response_body, encode_request_body, encode_response_body, encode_trailer_block,
    FetchError, EMPTY_MESSAGE_BLOCK, LEN_PREFIX,
};
pub use grpc_framing::{deframe_all, frame as grpc_frame, Deframer};
pub use httprule::{HttpCall, HttpRouter, WsBinding};
pub use transcode::{TranscodeError, Transcoder};

// --- Wire constants ---------------------------------------------------------

pub const CT_PROTO: &str = "application/grpc-webnext+proto";
pub const CT_JSON: &str = "application/grpc-webnext+json";
pub(crate) const CT_GRPC: &str = "application/grpc";

/// Base subprotocol; a client offers it plus a codec/credential entry.
pub const WS_SUBPROTOCOL: &str = "grpc-webnext";
pub const WS_SUBPROTOCOL_JSON: &str = "grpc-webnext+json";
pub const WS_SUBPROTOCOL_PROTO: &str = "grpc-webnext+proto";

/// The proxy owns the client-facing deadline: it drops the call at the deadline
/// (surfacing DEADLINE_EXCEEDED) and forwards `grpc-timeout` downstream with this grace so
/// the callee's own enforcement is a later backstop rather than racing the local timer.
pub const DEADLINE_GRACE: Duration = Duration::from_millis(500);

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ResBody = UnsyncBoxBody<Bytes, BoxError>;

// Authorization is deliberately NOT a grpc-webnext hook — neither per-RPC nor
// per-connection. The request carries its metadata to the router on every transport, so
// auth belongs in a tonic interceptor (in-process) or the upstream server / mesh (proxy):
// one uniform, per-RPC check that also covers the native/h2ts path. Connection-level app
// auth is non-canonical (the ecosystem gates connections only on network identity, e.g.
// mTLS) and can't be uniform (Fetch has no connection). See `tests/inproc_auth.rs`.

// --- Public configs ---------------------------------------------------------

/// Configuration for [`serve_in_process`] — wrap an in-process tonic service.
#[derive(Clone)]
pub struct ServerConfig {
    pub max_message_bytes: usize,
    /// Descriptor-based JSON<->proto transcoder. When set, `+json`/REST requests are
    /// transcoded to the router's binary protobuf and back. `None` ⇒ `+json` is
    /// `UNIMPLEMENTED`.
    pub transcoder: Option<Arc<Transcoder>>,
    /// Allow plain `application/json` / blank content-type to reach *main* gRPC paths
    /// (and blank WS subprotocols to infer their codec). Off by default.
    pub allow_implicit_codec: bool,
    /// WebSocket keepalive ping interval (`None` disables).
    pub ws_keepalive: Option<Duration>,
    /// Dead-peer timeout after a keepalive ping (gRPC's `keepalive_timeout`, default 20s).
    pub ws_keepalive_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_message_bytes: 4 * 1024 * 1024,
            transcoder: None,
            allow_implicit_codec: false,
            ws_keepalive: None,
            ws_keepalive_timeout: Duration::from_secs(20),
        }
    }
}

/// Configuration for [`serve_proxy`] — front a remote upstream gRPC server.
#[derive(Clone)]
pub struct ProxyConfig {
    /// Upstream gRPC endpoint (e.g. `http://127.0.0.1:50051`).
    pub upstream: http::Uri,
    pub max_message_bytes: usize,
    pub ws_keepalive: Option<Duration>,
    pub ws_keepalive_timeout: Duration,
    /// Descriptor source for `+json` termination (`None` ⇒ binary-only).
    pub schema: SchemaSource,
    /// Reflection snapshot refresh interval (default 4h).
    pub reflection_ttl: Duration,
    /// Optional management endpoint: `POST` to this exact path forces a reflection reload.
    pub admin_reload_path: Option<String>,
    /// Accept plain `application/json` / blank on *main* gRPC paths (off by default).
    pub allow_implicit_codec: bool,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            upstream: http::Uri::default(),
            max_message_bytes: 4 * 1024 * 1024,
            ws_keepalive: None,
            ws_keepalive_timeout: Duration::from_secs(20),
            schema: SchemaSource::None,
            reflection_ttl: Duration::from_secs(4 * 60 * 60),
            admin_reload_path: None,
            allow_implicit_codec: false,
        }
    }
}

// --- Internal runtime -------------------------------------------------------

/// The per-connection knobs the shared handlers read, independent of which surface built
/// them.
pub(crate) struct RunConfig {
    pub max_message_bytes: usize,
    pub ws_keepalive: Option<Duration>,
    pub ws_keepalive_timeout: Duration,
    pub allow_implicit_codec: bool,
    pub admin_reload_path: Option<String>,
    /// Upstream `host:port` for the h2ts byte-pump path (proxy only; `None` in-process).
    pub upstream_authority: Option<String>,
}

/// Everything a connection needs: where to dispatch, how to transcode, and the policy.
/// Cheap to clone.
///
/// Holding one also makes a task a *live connection* for shutdown purposes — the
/// [`Drain`] inside carries the liveness token a drain waits on — so a task that
/// outlives its connection must not keep a clone.
#[derive(Clone)]
pub(crate) struct Runtime {
    pub backend: Backend,
    pub schema: Schema,
    pub cfg: Arc<RunConfig>,
    pub drain: Drain,
}

// --- Entry points -----------------------------------------------------------

/// Serve grpc-webnext + native gRPC for an in-process `routes` on `listener`, until the
/// listener errors.
pub async fn serve_in_process(
    listener: TcpListener,
    routes: Routes,
    config: ServerConfig,
) -> std::io::Result<()> {
    serve_in_process_with_shutdown(listener, routes, config, std::future::pending()).await
}

/// [`serve_in_process`], but draining once `shutdown` resolves.
///
/// Draining stops accepting connections, tells every open one to finish (an HTTP/2
/// `GOAWAY` — including down an in-process h2ts tunnel — and a close on any WebSocket
/// that has not opened its stream yet), lets in-flight RPCs complete, and resolves when
/// the last connection is gone.
///
/// There is deliberately no drain-timeout knob: the returned future *is* the drain, so
/// bounding it is [`tokio::time::timeout`], and dropping it force-closes whatever is
/// still open. A long-lived stream will hold the drain open until then, which is what
/// "graceful" means.
///
/// ```no_run
/// # async fn f(listener: tokio::net::TcpListener, routes: tonic::service::Routes) {
/// use std::time::Duration;
/// let server = grpc_webnext::serve_in_process_with_shutdown(
///     listener,
///     routes,
///     Default::default(),
///     async { tokio::signal::ctrl_c().await.ok(); },
/// );
/// // Drain, but never take longer than 30s to exit.
/// let _ = tokio::time::timeout(Duration::from_secs(30), server).await;
/// # }
/// ```
pub async fn serve_in_process_with_shutdown(
    listener: TcpListener,
    routes: Routes,
    config: ServerConfig,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    let schema = Schema::from_transcoder(config.transcoder.clone());
    let cfg = Arc::new(RunConfig {
        max_message_bytes: config.max_message_bytes,
        ws_keepalive: config.ws_keepalive,
        ws_keepalive_timeout: config.ws_keepalive_timeout,
        allow_implicit_codec: config.allow_implicit_codec,
        admin_reload_path: None,
        upstream_authority: None,
    });
    let (controller, drain) = drain::channel();
    let rt = Runtime { backend: Backend::InProcess(routes), schema, cfg, drain };
    run(listener, rt, controller, shutdown).await
}

/// Serve grpc-webnext + native gRPC passthrough in front of an upstream gRPC server.
pub async fn serve_proxy(listener: TcpListener, config: ProxyConfig) -> std::io::Result<()> {
    serve_proxy_with_shutdown(listener, config, std::future::pending()).await
}

/// [`serve_proxy`], but draining once `shutdown` resolves — see
/// [`serve_in_process_with_shutdown`] for the semantics.
///
/// One surface drains differently here: the proxy's h2ts tunnels are a **byte-transparent**
/// pump to the upstream, so there is no `GOAWAY` to inject without parsing the very traffic
/// the proxy exists not to parse. Those tunnels run until the caller's deadline; every other
/// surface drains as it does in-process.
pub async fn serve_proxy_with_shutdown(
    listener: TcpListener,
    config: ProxyConfig,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    // Lazy connect: the upstream need not be up when we start.
    let channel = Channel::builder(config.upstream.clone()).connect_lazy();
    // Raw host:port for the h2ts byte-pump path (which bypasses the gRPC `Channel`).
    let upstream_authority = config.upstream.authority().map(|a| match a.port_u16() {
        Some(_) => a.to_string(),
        None => {
            let port = if config.upstream.scheme_str() == Some("https") { 443 } else { 80 };
            format!("{}:{}", a.host(), port)
        }
    });
    // A bad bundled descriptor set is a config error — surface it at startup.
    let schema = Schema::build(config.schema.clone(), channel.clone(), config.reflection_ttl)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
    // Eager reflection load + TTL refresh (no-op for None/Bundled).
    schema.start();
    let cfg = Arc::new(RunConfig {
        max_message_bytes: config.max_message_bytes,
        ws_keepalive: config.ws_keepalive,
        ws_keepalive_timeout: config.ws_keepalive_timeout,
        allow_implicit_codec: config.allow_implicit_codec,
        admin_reload_path: config.admin_reload_path,
        upstream_authority,
    });
    let (controller, drain) = drain::channel();
    let rt = Runtime { backend: Backend::Upstream(channel), schema, cfg, drain };
    run(listener, rt, controller, shutdown).await
}

/// The connection accept loop, shared by both surfaces.
async fn run(
    listener: TcpListener,
    rt: Runtime,
    controller: drain::DrainController,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    let mut shutdown = std::pin::pin!(shutdown);
    let result = loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = shutdown.as_mut() => break Ok(()),
        };
        let (stream, _peer) = match accepted {
            Ok(pair) => pair,
            Err(e) => break Err(e),
        };
        let io = TokioIo::new(stream);
        let rt = rt.clone();
        tokio::spawn(async move {
            let drain = rt.drain.clone();
            let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                let rt = rt.clone();
                async move { fetch::handle(&rt, req).await }
            });
            // The connection borrows the builder, so the builder has to outlive it.
            let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
            let conn = builder.serve_connection_with_upgrades(io, service);
            // Once draining starts this asks the connection to finish: `GOAWAY` on
            // HTTP/2, no-more-requests on HTTP/1. In-flight requests still complete.
            if let Err(e) = drain::serve_graceful!(&drain, conn) {
                tracing::debug!("connection error: {e}");
            }
        });
    };

    // Dropping the listener stops accepting; dropping `rt` releases this task's own
    // liveness token, without which the drain would wait on itself forever.
    drop(listener);
    drop(rt);
    controller.finish().await;
    result
}

/// Bind an ephemeral local address, serve an in-process `routes`, and return the bound
/// address + task handle. For tests and simple mains.
pub async fn bind_and_serve_in_process(
    routes: Routes,
    config: ServerConfig,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(serve_in_process(listener, routes, config));
    Ok((addr, handle))
}

/// Bind an ephemeral local address, serve a proxy, and return the bound address + handle.
pub async fn bind_and_serve_proxy(
    config: ProxyConfig,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(serve_proxy(listener, config));
    Ok((addr, handle))
}

// --- WebSocket handshake helpers --------------------------------------------

/// Parse the `Sec-WebSocket-Protocol` request header into its comma-separated tokens.
/// Used to negotiate the codec / detect the `h2ts` subprotocol at handshake time.
pub fn ws_subprotocols(headers: &HeaderMap) -> Vec<String> {
    headers
        .get(http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect())
        .unwrap_or_default()
}

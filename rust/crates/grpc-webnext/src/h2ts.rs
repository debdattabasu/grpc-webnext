//! Binary path over **real HTTP/2**, tunneled through a WebSocket by
//! [h2ts](https://github.com/debdattabasu/h2ts).
//!
//! When a WebSocket handshake offers the `h2ts` subprotocol the client is speaking real
//! gRPC (HTTP/2 with trailers) over the tunnel — not the custom `Frame` protocol. So there
//! is nothing to translate:
//!
//! * **in-process** — hand the tunnel straight to the tonic [`Routes`](tonic::service::Routes)
//!   via [`serve_h2`](h2ts_server::serve_h2); tonic serves real gRPC over it.
//! * **proxy** — [`bridge`](h2ts_server::bridge) the tunnel's bytes to the h2c upstream;
//!   the proxy never parses gRPC (schema-agnostic, incremental — wslay streams sub-frame).
//!
//! This is the `{ encoding: proto, unary: h2ts, streaming: h2ts }` default. The `+json`
//! and Fetch/custom-`Frame` paths are untouched and live elsewhere.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Empty};
use hyper::body::{Body, Frame as HttpBodyFrame, Incoming, SizeHint};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpStream;
use tonic::body::Body as TonicBody;
use tonic::Status;
use tower::ServiceExt as _;

use crate::{Backend, BoxError, ResBody, Runtime};

/// Complete the h2ts handshake and, once upgraded, serve real gRPC over the tunnel
/// (in-process) or byte-pump it to the upstream (proxy). Returns the `101` immediately;
/// the tunnel runs on a spawned task. Only called when the client offered `h2ts`.
pub(crate) fn serve(rt: &Runtime, req: &mut Request<Incoming>) -> Response<ResBody> {
    let (response, ws_fut) = match h2ts_server::accept(req) {
        Ok(pair) => pair,
        Err(e) => return e.rejection_response().map(box_empty),
    };

    let rt = rt.clone();
    tokio::spawn(async move {
        let ws = match ws_fut.await {
            Ok(ws) => ws,
            Err(e) => {
                tracing::debug!("h2ts upgrade failed: {e}");
                return;
            }
        };
        match &rt.backend {
            // Real gRPC, served directly by the in-process router — no translation. The
            // request body is wrapped so an oversized gRPC message is rejected with
            // RESOURCE_EXHAUSTED, honoring `max_message_bytes` like the custom paths do.
            Backend::InProcess(routes) => {
                let routes = routes.clone();
                let max = rt.cfg.max_message_bytes;
                let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                    let routes = routes.clone();
                    async move {
                        let req = req.map(|body| TonicBody::new(GrpcSizeLimit::new(body, max)));
                        routes.oneshot(req).await
                    }
                });
                // Own the inner HTTP/2 connection rather than calling
                // `h2ts_server::serve_h2`, which is these same two lines without handing
                // back the connection: holding it is what lets a drain send a real
                // `GOAWAY` down the tunnel. `WsByteStream` is h2ts's supported handle for
                // exactly this — the tunnel as a byte stream, bridged incrementally.
                let io = TokioIo::new(h2ts_server::WsByteStream::new(ws));
                let builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
                let conn = builder.serve_connection(io, service);
                if let Err(e) = crate::drain::serve_graceful!(&rt.drain, conn) {
                    tracing::debug!("h2ts connection ended: {e}");
                }
            }
            // Byte-transparent tunnel to the h2c upstream — no gRPC parsing.
            Backend::Upstream(_) => {
                let Some(authority) = rt.cfg.upstream_authority.clone() else {
                    tracing::debug!("h2ts proxy: no upstream authority configured");
                    return;
                };
                match TcpStream::connect(&authority).await {
                    Ok(tcp) => {
                        let _ = h2ts_server::bridge(ws, tcp).await;
                    }
                    Err(e) => tracing::debug!("h2ts proxy connect {authority} failed: {e}"),
                }
            }
        }
    });

    response.map(box_empty)
}

/// The `101` (and any rejection) response body is `Empty`; box it into the crate's `ResBody`.
fn box_empty(body: Empty<Bytes>) -> ResBody {
    body.map_err(|e: Infallible| -> BoxError { match e {} }).boxed_unsync()
}

/// A request body that passes bytes through unchanged but fails the stream with
/// `RESOURCE_EXHAUSTED` if any length-prefixed gRPC message declares a size over `max` —
/// giving the real-gRPC h2ts path the same `max_message_bytes` request limit the custom
/// Fetch/WebSocket paths enforce. tonic surfaces the boxed `Status` to the client verbatim.
struct GrpcSizeLimit<B> {
    inner: B,
    max: usize,
    // gRPC frame parse state: a 5-byte prefix (1 compression flag + u32 big-endian length),
    // then that many message bytes.
    header_seen: usize,
    len_buf: [u8; 4],
    body_remaining: usize,
}

impl<B> GrpcSizeLimit<B> {
    fn new(inner: B, max: usize) -> Self {
        Self { inner, max, header_seen: 0, len_buf: [0; 4], body_remaining: 0 }
    }

    /// Walk `data`, tracking frame boundaries; error if a declared message length exceeds `max`.
    fn inspect(&mut self, data: &[u8]) -> Result<(), BoxError> {
        let mut i = 0;
        while i < data.len() {
            if self.header_seen < 5 {
                if self.header_seen >= 1 {
                    self.len_buf[self.header_seen - 1] = data[i];
                }
                self.header_seen += 1;
                i += 1;
                if self.header_seen == 5 {
                    let len = u32::from_be_bytes(self.len_buf) as usize;
                    if len > self.max {
                        return Err(Box::new(Status::resource_exhausted(format!(
                            "request message exceeds size limit ({} bytes)",
                            self.max
                        ))) as BoxError);
                    }
                    self.body_remaining = len;
                }
            } else {
                let take = self.body_remaining.min(data.len() - i);
                self.body_remaining -= take;
                i += take;
                if self.body_remaining == 0 {
                    self.header_seen = 0;
                }
            }
        }
        Ok(())
    }
}

impl<B> Body for GrpcSizeLimit<B>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: Into<BoxError>,
{
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<HttpBodyFrame<Bytes>, BoxError>>> {
        let this = self.as_mut().get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    if let Err(e) = this.inspect(data) {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

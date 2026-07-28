//! The client: all four RPC cardinalities over one h2ts tunnel.
//!
//! Everything here is single-threaded (`Rc`, `!Send`), which is what a wasm
//! frontend actually is — nothing asserts `Send` to satisfy a runtime this target
//! does not have.

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use futures::future::{FutureExt, LocalBoxFuture, Shared};
use futures::stream::{LocalBoxStream, Stream, StreamExt};
use h2ts_client::{
    ConnectOptions, H2Connection, RequestBody, RequestInit, Response, Trailers, Transport,
};

use crate::codec::{encode_message, Deframer};
use crate::metadata::Metadata;
use crate::state::{ConnectivityState, StateWatch};
use crate::status::{Code, Status};

/// Opens a tunnel on demand. The client holds one so it can **redial**, which is
/// what makes it a gRPC channel rather than a handle to a socket.
pub type Connector =
    Rc<dyn Fn() -> LocalBoxFuture<'static, Result<H2Connection, Status>>>;

type SharedDial = Shared<LocalBoxFuture<'static, Result<Rc<H2Connection>, Status>>>;

const CONTENT_TYPE: &str = "application/grpc+proto";

/// Per-call options.
#[derive(Debug, Clone, Default)]
pub struct CallOptions {
    pub metadata: Metadata,
    /// Deadline for the call.
    ///
    /// Sent as `grpc-timeout` **and** enforced locally, because on this path the
    /// header alone guarantees nothing: the h2ts tunnel hands requests straight to
    /// a tonic `Routes`, and the piece of tonic that honors `grpc-timeout` lives in
    /// its own hyper server, which is not in this path. A client that only sent the
    /// header would wait forever on a server that ignores it. The header still goes
    /// out so anything that *does* enforce it — a proxy, an upstream — can.
    pub timeout: Option<std::time::Duration>,
    /// Reject an inbound message larger than this (RESOURCE_EXHAUSTED).
    pub max_message_bytes: Option<usize>,
}

impl CallOptions {
    pub fn new() -> CallOptions {
        CallOptions::default()
    }
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    pub fn with_max_message_bytes(mut self, max: usize) -> Self {
        self.max_message_bytes = Some(max);
        self
    }
}

/// A grpc-webnext client: a gRPC **channel**, not a handle to a socket.
///
/// Cheap to clone — clones share one tunnel, which is the point, since calls
/// multiplex over it. When that tunnel dies the channel redials on the next call,
/// the way `tonic::transport::Channel` does: the call that discovers the failure
/// reports it, and the one after reconnects. Watch [`Client::state_changes`] if the
/// application wants to say something about it.
#[derive(Clone)]
pub struct Client {
    inner: Rc<Inner>,
}

struct Inner {
    /// `None` for a client built over a caller-supplied transport: that transport
    /// is consumed, so there is nothing to redial with.
    connector: Option<Connector>,
    tunnel: RefCell<Option<Rc<H2Connection>>>,
    /// The dial in flight, shared so concurrent calls open **one** socket rather
    /// than one each.
    dialing: RefCell<Option<SharedDial>>,
    state: StateWatch,
    authority: String,
}

impl Client {
    /// Build a client over an existing h2ts connection. Single-shot: there is no
    /// way to redial, so a dropped tunnel stays dropped. Prefer
    /// [`connect`](crate::connect), which reconnects.
    pub fn from_connection(conn: H2Connection, authority: impl Into<String>) -> Client {
        let state = StateWatch::default();
        state.set(ConnectivityState::Ready);
        Client {
            inner: Rc::new(Inner {
                connector: None,
                tunnel: RefCell::new(Some(Rc::new(conn))),
                dialing: RefCell::new(None),
                state,
                authority: authority.into(),
            }),
        }
    }

    /// Build a **reconnecting** client from something that can open a tunnel on
    /// demand. Each call to `connector` must yield a fresh connection, with its
    /// driver already spawned.
    pub fn with_connector(connector: Connector, authority: impl Into<String>) -> Client {
        Client {
            inner: Rc::new(Inner {
                connector: Some(connector),
                tunnel: RefCell::new(None),
                dialing: RefCell::new(None),
                state: StateWatch::default(),
                authority: authority.into(),
            }),
        }
    }

    /// Build a client over any byte transport, returning the driver future the
    /// caller must poll. Spawn it: `wasm_bindgen_futures::spawn_local` in a
    /// browser, `tokio::task::spawn_local` on a `LocalSet` natively.
    ///
    /// Single-shot, since the transport is consumed — see
    /// [`from_connection`](Self::from_connection).
    pub fn over_transport(
        transport: Transport,
        authority: impl Into<String>,
        options: ConnectOptions,
    ) -> (Client, impl Future<Output = ()>) {
        let (conn, driver) = h2ts_client::connect(transport, options);
        (Client::from_connection(conn, authority), driver)
    }

    /// The channel's connectivity state.
    pub fn state(&self) -> ConnectivityState {
        // A tunnel that died since the last call has not been noticed yet.
        if matches!(self.inner.state.get(), ConnectivityState::Ready)
            && self.inner.tunnel.borrow().as_ref().is_none_or(|c| c.is_closed())
        {
            self.inner.state.set(ConnectivityState::Idle);
        }
        self.inner.state.get()
    }

    /// Watch connectivity transitions — gRPC's `WaitForStateChange`, as a stream.
    /// Repeats are collapsed, so every item is a real change.
    pub fn state_changes(&self) -> impl Stream<Item = ConnectivityState> + 'static {
        self.inner.state.watch()
    }

    /// True if there is no usable tunnel right now. A reconnecting client will open
    /// one on the next call; a single-shot one will not.
    pub fn is_closed(&self) -> bool {
        self.inner.tunnel.borrow().as_ref().is_none_or(|c| c.is_closed())
    }

    /// The live tunnel, dialing one if needed.
    async fn tunnel(&self) -> Result<Rc<H2Connection>, Status> {
        // Fast path: a live tunnel. Never hold the borrow across an await.
        let cached_is_dead = {
            let tunnel = self.inner.tunnel.borrow();
            match tunnel.as_ref() {
                Some(conn) if !conn.is_closed() => return Ok(conn.clone()),
                Some(_) => true,
                None => false,
            }
        };
        if cached_is_dead {
            // Report the disconnect before the redial. A watcher that only ever saw
            // Connecting -> Ready could not tell a reconnect from a first connect,
            // which is the difference between "still fine" and "we dropped and
            // recovered".
            self.inner.tunnel.borrow_mut().take();
            self.inner.state.set(ConnectivityState::Idle);
        }

        let Some(connector) = self.inner.connector.clone() else {
            return Err(Status::unavailable(
                "the tunnel is closed and this client cannot redial \
                 (built over a caller-supplied transport)",
            ));
        };

        // Join a dial already in flight rather than opening a second socket.
        let dial = {
            let mut dialing = self.inner.dialing.borrow_mut();
            match dialing.as_ref() {
                Some(shared) => shared.clone(),
                None => {
                    self.inner.state.set(ConnectivityState::Connecting);
                    let shared: SharedDial =
                        async move { connector().await.map(Rc::new) }.boxed_local().shared();
                    *dialing = Some(shared.clone());
                    shared
                }
            }
        };

        let result = dial.await;
        // Clear the slot so a later failure redials rather than replaying this one.
        self.inner.dialing.borrow_mut().take();
        match result {
            Ok(conn) => {
                self.inner.state.set(ConnectivityState::Ready);
                *self.inner.tunnel.borrow_mut() = Some(conn.clone());
                Ok(conn)
            }
            Err(e) => {
                self.inner.state.set(ConnectivityState::TransientFailure);
                Err(e)
            }
        }
    }

    /// Note that the tunnel we just used is gone, so the next call redials.
    fn forget(&self, dead: &Rc<H2Connection>) {
        let mut tunnel = self.inner.tunnel.borrow_mut();
        if tunnel.as_ref().is_some_and(|c| Rc::ptr_eq(c, dead)) {
            tunnel.take();
            self.inner.state.set(ConnectivityState::Idle);
        }
    }

    /// Unary: one message out, one message back.
    pub async fn unary(
        &self,
        path: &str,
        request: Vec<u8>,
        options: CallOptions,
    ) -> Result<UnaryResponse, Status> {
        self.single_response(path, RequestBody::Bytes(encode_message(&request)), &options).await
    }

    /// Client streaming: a stream of messages out, one message back.
    ///
    /// The request body is uploaded as the stream yields, under HTTP/2 flow
    /// control — a slow server pushes back rather than the messages piling up here.
    pub async fn client_streaming<S>(
        &self,
        path: &str,
        requests: S,
        options: CallOptions,
    ) -> Result<UnaryResponse, Status>
    where
        S: Stream<Item = Vec<u8>> + 'static,
    {
        let body = RequestBody::stream(requests.map(|m| encode_message(&m)));
        self.single_response(path, body, &options).await
    }

    /// Server streaming: one message out, a stream of messages back.
    pub async fn server_streaming(
        &self,
        path: &str,
        request: Vec<u8>,
        options: CallOptions,
    ) -> Result<Streaming, Status> {
        let body = RequestBody::Bytes(encode_message(&request));
        Ok(Streaming::new(self.request(path, body, &options).await?, &options))
    }

    /// Bidirectional streaming: a stream out, a stream back, concurrently.
    pub async fn bidi_streaming<S>(
        &self,
        path: &str,
        requests: S,
        options: CallOptions,
    ) -> Result<Streaming, Status>
    where
        S: Stream<Item = Vec<u8>> + 'static,
    {
        let body = RequestBody::stream(requests.map(|m| encode_message(&m)));
        Ok(Streaming::new(self.request(path, body, &options).await?, &options))
    }

    /// Issue a request whose response is exactly one message, and read it to the
    /// terminal status.
    async fn single_response(
        &self,
        path: &str,
        body: RequestBody,
        options: &CallOptions,
    ) -> Result<UnaryResponse, Status> {
        match options.timeout {
            Some(timeout) => {
                deadline(timeout, self.single_response_inner(path, body, options)).await
            }
            None => self.single_response_inner(path, body, options).await,
        }
    }

    async fn single_response_inner(
        &self,
        path: &str,
        body: RequestBody,
        options: &CallOptions,
    ) -> Result<UnaryResponse, Status> {
        let mut response = self.request(path, body, options).await?;

        // The status is in the trailers, which only exist once the body is done.
        let bytes = response
            .bytes()
            .await
            .map_err(|e| Status::unavailable(format!("response body failed: {e}")))?;

        let mut deframer = Deframer::new(options.max_message_bytes);
        let messages = deframer.push(&bytes)?;
        if deframer.pending() > 0 {
            return Err(Status::new(Code::Internal, "response body ended mid-message (truncated)"));
        }

        // Trailers first: a non-OK status explains a missing message, so reporting
        // "no message" over the server's own reason would bury the cause.
        let status = match response.trailers() {
            Some(trailers) => Status::from_headers(&trailers).unwrap_or_else(|| {
                Status::new(Code::Internal, "response trailers carried no grpc-status")
            }),
            None => Status::new(Code::Internal, "response ended without trailers"),
        };
        if !status.is_ok() {
            return Err(status);
        }

        let message = messages
            .into_iter()
            .next()
            .ok_or_else(|| Status::new(Code::Internal, "response carried no message"))?;
        Ok(UnaryResponse {
            message,
            headers: Metadata::from_headers(&response.headers),
            trailers: status.metadata,
        })
    }

    /// Issue the request and wait for response headers. A trailers-only response —
    /// how gRPC reports a call that failed before producing anything — becomes the
    /// error here, so no caller has to special-case it.
    async fn request(
        &self,
        path: &str,
        body: RequestBody,
        options: &CallOptions,
    ) -> Result<Response, Status> {
        let conn = self.tunnel().await?;
        let mut headers = vec![
            ("content-type".to_string(), CONTENT_TYPE.to_string()),
            ("te".to_string(), "trailers".to_string()),
        ];
        if let Some(timeout) = options.timeout {
            // `m` is milliseconds; never send 0, which would mean "already expired".
            headers.push(("grpc-timeout".to_string(), format!("{}m", options_millis(timeout))));
        }
        headers.extend(options.metadata.to_headers());

        let response = match conn
            .request(RequestInit {
                method: Some("POST".to_string()),
                path: Some(path.to_string()),
                authority: Some(self.inner.authority.clone()),
                scheme: Some("http".to_string()),
                headers,
                body,
            })
            .await
        {
            Ok(response) => response,
            Err(e) => {
                // The call that discovers a dead tunnel reports the failure; dropping it
                // here is what makes the *next* call redial. Same shape as tonic's
                // `Reconnect`, which returns to Idle on error rather than retrying in
                // place — replaying a request the server may already have seen is a
                // policy decision, not the transport's to make.
                if conn.is_closed() {
                    self.forget(&conn);
                }
                return Err(Status::unavailable(format!("request failed: {e}")));
            }
        };

        if response.status != 200 {
            return Err(Status::unavailable(format!("HTTP {}", response.status)));
        }
        // A status in the *headers* means trailers-only: the call is already over.
        if let Some(status) = Status::from_headers(&response.headers) {
            return Err(if status.is_ok() {
                Status::new(Code::Internal, "trailers-only response reported OK with no message")
            } else {
                status
            });
        }
        Ok(response)
    }
}

fn options_millis(timeout: std::time::Duration) -> u128 {
    timeout.as_millis().max(1)
}

/// Race a call against its deadline. Losing the race is DEADLINE_EXCEEDED, and
/// dropping the future is what actually stops the work — the h2ts stream is reset
/// on drop, so the server learns about it.
async fn deadline<T>(
    timeout: std::time::Duration,
    work: impl std::future::Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    use futures::future::{select, Either};
    let timer = futures_timer::Delay::new(timeout);
    futures::pin_mut!(work);
    futures::pin_mut!(timer);
    match select(work, timer).await {
        Either::Left((result, _)) => result,
        Either::Right(((), _)) => {
            Err(Status::new(Code::DeadlineExceeded, "deadline exceeded"))
        }
    }
}

/// An in-flight response stream.
///
/// Backpressure is real and needs no code here: the body replenishes the HTTP/2
/// receive window only as it is polled, so a consumer that stops reading stops the
/// server rather than filling memory. That is the same guarantee the TypeScript
/// client gets, and for the same reason — it is HTTP/2 doing its job.
pub struct Streaming {
    /// Initial metadata, available as soon as the stream opens.
    pub headers: Metadata,
    // (the body is a boxed stream, so this is hand-written rather than derived)
    body: LocalBoxStream<'static, Result<Vec<u8>, h2ts_client::H2Error>>,
    trailers: Trailers,
    deframer: Deframer,
    ready: std::collections::VecDeque<Vec<u8>>,
    ended: bool,
    failed: Option<Status>,
}

impl std::fmt::Debug for Streaming {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streaming")
            .field("headers", &self.headers)
            .field("ended", &self.ended)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl Streaming {
    fn new(response: Response, options: &CallOptions) -> Streaming {
        let headers = Metadata::from_headers(&response.headers);
        // `into_parts` is what makes a gRPC stream expressible at all: the body and
        // the trailers carrying the terminal status must both outlive each other.
        let (body, trailers) = response.into_parts();
        Streaming {
            headers,
            body: body.boxed_local(),
            trailers,
            deframer: Deframer::new(options.max_message_bytes),
            ready: Default::default(),
            ended: false,
            failed: None,
        }
    }

    /// The next response message, or `None` once the stream ends.
    ///
    /// After `None`, call [`Streaming::status`] for how it ended — a stream that
    /// finishes cleanly still has a status, and it is not always OK.
    pub async fn message(&mut self) -> Result<Option<Vec<u8>>, Status> {
        loop {
            if let Some(message) = self.ready.pop_front() {
                return Ok(Some(message));
            }
            if let Some(status) = self.failed.clone() {
                return Err(status);
            }
            if self.ended {
                return Ok(None);
            }
            match self.body.next().await {
                Some(Ok(chunk)) => match self.deframer.push(&chunk) {
                    Ok(messages) => self.ready.extend(messages),
                    Err(status) => return Err(self.fail(status)),
                },
                Some(Err(e)) => {
                    return Err(self.fail(Status::unavailable(format!("stream failed: {e}"))))
                }
                None => {
                    self.ended = true;
                    // A body that stops mid-message is truncated; handing that back as a
                    // clean end of stream would lose data silently.
                    if self.deframer.pending() > 0 {
                        return Err(self.fail(Status::new(
                            Code::Internal,
                            "response body ended mid-message (truncated)",
                        )));
                    }
                }
            }
        }
    }

    /// The terminal status. Only meaningful once [`Streaming::message`] has
    /// returned `None`; before that the trailers have not arrived.
    pub fn status(&self) -> Status {
        if let Some(status) = &self.failed {
            return status.clone();
        }
        match self.trailers.get() {
            Some(trailers) => Status::from_headers(&trailers).unwrap_or_else(|| {
                Status::new(Code::Internal, "response trailers carried no grpc-status")
            }),
            // A stream that ends without trailers never delivered a status, which is
            // the server violating the protocol — not a silent success.
            None => Status::new(Code::Internal, "response ended without trailers"),
        }
    }

    /// Drain to the end and return the terminal status.
    pub async fn finish(&mut self) -> Status {
        loop {
            match self.message().await {
                Ok(Some(_)) => continue,
                Ok(None) => return self.status(),
                Err(status) => return status,
            }
        }
    }

    fn fail(&mut self, status: Status) -> Status {
        self.ended = true;
        self.failed = Some(status.clone());
        status
    }
}

/// A completed single-response call.
#[derive(Debug, Clone)]
pub struct UnaryResponse {
    pub message: Vec<u8>,
    /// Initial metadata (response headers).
    pub headers: Metadata,
    /// Trailing metadata — not the same thing, and often where the detail is.
    pub trailers: Metadata,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sub_millisecond_timeout_never_becomes_zero() {
        // `grpc-timeout: 0m` means "already expired", so rounding down would turn a
        // very short deadline into an instant failure.
        assert_eq!(options_millis(std::time::Duration::from_micros(1)), 1);
        assert_eq!(options_millis(std::time::Duration::from_millis(250)), 250);
    }
}

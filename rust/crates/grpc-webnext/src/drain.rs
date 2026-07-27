//! Graceful shutdown coordination.
//!
//! Draining a grpc-webnext server means four different things on four surfaces, so rather
//! than a per-surface mechanism there is one primitive here that all of them share: a
//! broadcast *"start draining"* signal, plus a liveness token every live connection holds
//! until it is finished.
//!
//! * **Fetch / native gRPC** — hyper's own `graceful_shutdown`: HTTP/2 sends `GOAWAY`,
//!   HTTP/1 stops reading new requests; in-flight requests still complete.
//! * **h2ts tunnels (in-process)** — the same thing, because the server owns the inner
//!   HTTP/2 connection: a real `GOAWAY` down the tunnel.
//! * **Custom-`Frame` WebSockets** — a socket carries exactly one stream, so "stop
//!   accepting new streams" is "close the ones that have not opened one yet"; a socket with
//!   a live stream runs to its terminal frame, which closes the socket anyway.
//! * **h2ts tunnels (proxy)** — byte-transparent, so there is no `GOAWAY` to inject without
//!   parsing the traffic the proxy exists not to parse. Those run until the caller's
//!   deadline.
//!
//! The token is what makes "wait for the last one" work without a registry: every task that
//! matters holds a [`crate::Runtime`], which holds a [`Drain`], which holds an
//! `mpsc::Sender`. When the last one drops, the controller's `recv()` resolves.

use tokio::sync::{mpsc, watch};

/// The drain handle carried by every connection (inside [`crate::Runtime`]).
#[derive(Clone)]
pub(crate) struct Drain {
    started: watch::Receiver<bool>,
    /// Liveness token; see the module docs. Never read — its `Drop` is the signal.
    _token: mpsc::Sender<()>,
}

/// The server side of the drain: starts it, then waits for the last connection.
pub(crate) struct DrainController {
    start: watch::Sender<bool>,
    tokens: mpsc::Receiver<()>,
}

/// Build a linked (controller, handle) pair.
pub(crate) fn channel() -> (DrainController, Drain) {
    let (start, started) = watch::channel(false);
    let (token, tokens) = mpsc::channel(1);
    (
        DrainController { start, tokens },
        Drain { started, _token: token },
    )
}

impl Drain {
    /// Whether draining has begun.
    pub(crate) fn started(&self) -> bool {
        *self.started.borrow()
    }

    /// A receiver that fires on the *transition* into draining. Pair it with
    /// [`Drain::started`]: a fresh receiver counts the current value as already seen, so on
    /// its own it would never fire for a drain that had already begun.
    pub(crate) fn subscribe(&self) -> watch::Receiver<bool> {
        self.started.clone()
    }

    /// Resolves as soon as draining begins — immediately if it already has.
    pub(crate) async fn wait(&self) {
        let mut rx = self.subscribe();
        if *rx.borrow() {
            return;
        }
        let _ = rx.changed().await;
    }
}

impl DrainController {
    /// Start draining, then resolve once every connection has finished.
    ///
    /// The caller must already have dropped its own [`crate::Runtime`] — its token counts
    /// like any other, so holding it would wait forever.
    pub(crate) async fn finish(mut self) {
        let _ = self.start.send(true);
        // Every remaining sender is a live connection; `recv` resolves to `None` when the
        // last one drops. Nothing ever sends, so that is the only way this ends.
        let _ = self.tokens.recv().await;
    }
}

/// Drive a hyper connection to completion, asking it to shut down gracefully — `GOAWAY` on
/// HTTP/2, no-more-requests on HTTP/1 — as soon as draining starts. In-flight requests still
/// finish; only *new* ones are refused. Evaluates to the connection's `Result`.
///
/// This is a macro rather than a generic `fn` on purpose. Written as
/// `fn serve<C: GracefulConnection>(…) -> impl Future`, the body's `async` block makes the
/// compiler try to prove `From<Box<dyn Error + Send + Sync + '_>>` for *every* lifetime,
/// which does not hold — so it does not compile. (hyper-util dodges this by hand-writing a
/// `Future` impl rather than using an `async` block.) Expanding at the call site keeps the
/// connection type concrete and sidesteps the inference entirely.
macro_rules! serve_graceful {
    ($drain:expr, $conn:expr) => {{
        let drain = $drain;
        let mut conn = std::pin::pin!($conn);
        let mut completed = None;
        if drain.started() {
            conn.as_mut().graceful_shutdown();
        } else {
            let mut started = drain.subscribe();
            tokio::select! {
                result = conn.as_mut() => completed = Some(result),
                _ = started.changed() => conn.as_mut().graceful_shutdown(),
            }
        }
        match completed {
            Some(result) => result,
            None => conn.await,
        }
    }};
}

pub(crate) use serve_graceful;

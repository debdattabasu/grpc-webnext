//! End-to-end: **tonic's generated client stubs** against a **real grpc-webnext
//! server**, over a real WebSocket, on the host.
//!
//! Both ends are stock codegen — `GreeterServer` behind grpc-webnext, `GreeterClient`
//! over the tunnel — so what these tests pin is the seam between them: the
//! `tower::Service` adapter in `grpc_webnext_client::tonic_service`. Everything above
//! it (codec, framing, status, metadata) is tonic's, everything below it is h2ts's,
//! and a bug in the middle looks like one of the two lying about the other.
//!
//! The wasm target cannot be tested without a browser, but almost nothing here is
//! wasm-specific: swapping `web_sys::WebSocket` for `tokio-tungstenite` exercises the
//! identical code path. Everything is `!Send` by design, so it all runs on a
//! `LocalSet`.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use example_tonic_stub_client::dial;
use example_tonic_stub_client::pb::greeter_client::GreeterClient;
use example_tonic_stub_client::pb::greeter_server::{Greeter, GreeterServer};
use example_tonic_stub_client::pb::{
    ChatMessage, CountdownRequest, HelloReply, HelloRequest, SleepRequest, Tick,
};
use futures::{Stream, StreamExt};
use grpc_webnext::{bind_and_serve_in_process, ServerConfig};
use grpc_webnext_client::{Client, TonicService};
use tonic::service::Routes;
use tonic::{Request, Response, Status, Streaming};

/// The service under test. Purpose-built rather than borrowed from the example
/// server: every RPC here has a behavior the adapter needs stressed — a stalling
/// stream for deadlines, a lazy producer for backpressure, a failure that carries
/// trailing metadata, and a handler that reports the metadata it was sent.
#[derive(Default)]
struct ProbeSvc {
    /// Messages `Countdown` has actually *produced*, which is how backpressure is
    /// observable at all: the proof is that the server stops generating, not merely
    /// that the client stops receiving.
    produced: Arc<AtomicUsize>,
    /// The metadata the last request arrived with. The service does not reflect
    /// headers, so this is how a request's metadata is observed.
    seen: Arc<Mutex<Option<tonic::metadata::MetadataMap>>>,
    /// Signalled when the stalling `Countdown` stream is dropped — i.e. when the
    /// server's own handler is torn down, which is the only trustworthy evidence
    /// that a cancellation actually arrived.
    cancelled: Option<tokio::sync::mpsc::UnboundedSender<()>>,
}

/// Fires when the handler's stream is dropped.
struct CancelGuard(Option<tokio::sync::mpsc::UnboundedSender<()>>);

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if let Some(tx) = &self.0 {
            let _ = tx.send(());
        }
    }
}

#[tonic::async_trait]
impl Greeter for ProbeSvc {
    /// `name == "fail"` fails with trailing metadata attached — a trailers-only
    /// response carrying more than a status. Anything else succeeds, with response
    /// metadata (initial headers) set.
    async fn say_hello(&self, request: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        *self.seen.lock().unwrap() = Some(request.metadata().clone());
        let name = request.into_inner().name;
        if name == "fail" {
            let mut trailers = tonic::metadata::MetadataMap::new();
            trailers.insert("x-detail", "quota-exhausted".parse().unwrap());
            trailers.insert_bin(
                "x-detail-bin",
                tonic::metadata::MetadataValue::from_bytes(&[0, 1, 250]),
            );
            return Err(Status::with_metadata(
                tonic::Code::FailedPrecondition,
                "no",
                trailers,
            ));
        }
        let mut response = Response::new(HelloReply { message: format!("Hello, {name}!") });
        response.metadata_mut().insert("x-served-by", "probe".parse().unwrap());
        response
            .metadata_mut()
            .insert_bin("x-served-bin", tonic::metadata::MetadataValue::from_bytes(&[7, 250]));
        Ok(response)
    }

    async fn sleep(&self, request: Request<SleepRequest>) -> Result<Response<HelloReply>, Status> {
        tokio::time::sleep(Duration::from_millis(u64::from(request.into_inner().millis))).await;
        Ok(Response::new(HelloReply { message: "awake".into() }))
    }

    type CountdownStream = Pin<Box<dyn Stream<Item = Result<Tick, Status>> + Send>>;

    /// `from == 0` emits one tick and then stalls forever — the shape a deadline
    /// exists for. Otherwise it emits `from` large ticks, counting as it *produces*
    /// them so a stalled consumer is visible from the server's side.
    async fn countdown(
        &self,
        request: Request<CountdownRequest>,
    ) -> Result<Response<Self::CountdownStream>, Status> {
        let from = request.into_inner().from;
        if from == 0 {
            // One tick, then silence forever. The guard lives in the stream's own
            // state, so it is dropped exactly when the handler's stream is — which is
            // what makes cancellation observable from the server's side.
            let guard = CancelGuard(self.cancelled.clone());
            let output = futures::stream::unfold((false, guard), |(sent, guard)| async move {
                if sent {
                    futures::future::pending::<()>().await;
                    unreachable!("pending never completes");
                }
                Some((Ok(Tick { value: 0 }), (true, guard)))
            });
            return Ok(Response::new(Box::pin(output)));
        }
        let produced = self.produced.clone();
        // `iter().map()` is lazy: the counter advances as the transport *pulls*,
        // which is exactly the signal a backpressure test needs.
        let output = futures::stream::iter(0..from).map(move |value| {
            produced.fetch_add(1, Ordering::SeqCst);
            Ok(Tick { value })
        });
        Ok(Response::new(Box::pin(output)))
    }

    async fn concat(&self, request: Request<Streaming<ChatMessage>>) -> Result<Response<HelloReply>, Status> {
        let mut inbound = request.into_inner();
        let mut parts = Vec::new();
        while let Some(message) = inbound.next().await {
            parts.push(message?.text);
        }
        Ok(Response::new(HelloReply { message: parts.join(" ") }))
    }

    type ChatStream = Pin<Box<dyn Stream<Item = Result<ChatMessage, Status>> + Send>>;

    async fn chat(
        &self,
        request: Request<Streaming<ChatMessage>>,
    ) -> Result<Response<Self::ChatStream>, Status> {
        let inbound = request.into_inner();
        let output = inbound.map(|message| {
            message.map(|m| ChatMessage { text: format!("echo: {}", m.text) })
        });
        Ok(Response::new(Box::pin(output)))
    }
}

/// Start the real server and return its URL plus the two probes.
async fn start_server() -> (String, Arc<AtomicUsize>, Arc<Mutex<Option<tonic::metadata::MetadataMap>>>) {
    let service = ProbeSvc::default();
    let produced = service.produced.clone();
    let seen = service.seen.clone();
    (serve(service).await, produced, seen)
}

/// Start a server whose stalling stream reports when its handler is torn down.
async fn start_server_with_cancel() -> (String, tokio::sync::mpsc::UnboundedReceiver<()>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (serve(ProbeSvc { cancelled: Some(tx), ..Default::default() }).await, rx)
}

async fn serve(service: ProbeSvc) -> String {
    let (addr, handle) =
        bind_and_serve_in_process(Routes::new(GreeterServer::new(service)), ServerConfig::default())
            .await
            .unwrap();
    // The server lives as long as the test.
    std::mem::forget(handle);
    format!("http://{addr}")
}

/// The whole point, in one line: a stock generated stub over the tunnel.
async fn connect() -> (GreeterClient<TonicService>, Arc<AtomicUsize>, Arc<Mutex<Option<tonic::metadata::MetadataMap>>>) {
    let (base_url, produced, seen) = start_server().await;
    (GreeterClient::new(dial::client(&base_url).into_tonic()), produced, seen)
}

/// Run `body` on a LocalSet — everything in this client is single-threaded.
fn run<F: std::future::Future<Output = ()> + 'static>(body: F) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    tokio::task::LocalSet::new().block_on(&runtime, body);
}

#[test]
fn unary_round_trips_through_a_generated_stub() {
    run(async {
        let (mut greeter, _, _) = connect().await;
        let reply = greeter
            .say_hello(HelloRequest { name: "world".into() })
            .await
            .expect("unary should succeed");
        assert_eq!(reply.into_inner().message, "Hello, world!");
    });
}

#[test]
fn server_streaming_yields_every_message_then_completes() {
    run(async {
        let (mut greeter, _, _) = connect().await;
        let mut ticks = greeter
            .countdown(CountdownRequest { from: 3 })
            .await
            .expect("the stream should open")
            .into_inner();

        let mut seen = Vec::new();
        while let Some(tick) = ticks.next().await {
            seen.push(tick.expect("no stream error").value);
        }
        assert_eq!(seen, vec![0, 1, 2]);
    });
}

/// Client streaming — a cardinality grpc-web cannot express at all, and the one
/// tonic path (`Grpc::client_streaming` with a real multi-message body) that no
/// other test here reaches.
#[test]
fn client_streaming_uploads_every_message() {
    run(async {
        let (mut greeter, _, _) = connect().await;
        let words = futures::stream::iter(
            ["a", "b", "c"].map(|text| ChatMessage { text: text.into() }),
        );
        let reply = greeter.concat(words).await.expect("client streaming should succeed");
        assert_eq!(reply.into_inner().message, "a b c");
    });
}

#[test]
fn bidi_streams_both_directions_in_order() {
    run(async {
        let (mut greeter, _, _) = connect().await;
        let outbound = futures::stream::iter(
            ["one", "two", "three"].map(|text| ChatMessage { text: text.into() }),
        );
        let mut chat = greeter
            .chat(outbound)
            .await
            .expect("the stream should open")
            .into_inner();

        let mut seen = Vec::new();
        while let Some(message) = chat.next().await {
            seen.push(message.expect("no stream error").text);
        }
        assert_eq!(seen, vec!["echo: one", "echo: two", "echo: three"]);
    });
}

/// Request metadata reaches the server, including the `-bin` leg.
///
/// That leg is the one that has produced a real bug in this repo: on this path it
/// must be base64 on the wire, and tonic base64s it *before* the adapter sees it — so
/// encoding it a second time would yield mojibake rather than an error.
#[test]
fn request_metadata_reaches_the_server_including_the_binary_leg() {
    run(async {
        let (mut greeter, _, seen) = connect().await;

        let mut request = Request::new(HelloRequest { name: "metadata".into() });
        request.metadata_mut().insert("x-request-id", "abc-123".parse().unwrap());
        request.metadata_mut().insert_bin(
            "x-trace-bin",
            tonic::metadata::MetadataValue::from_bytes(&[1, 2, 3, 250]),
        );
        greeter.say_hello(request).await.expect("unary with metadata should succeed");

        let received = seen.lock().unwrap().clone().expect("the server saw a request");
        assert_eq!(received.get("x-request-id").unwrap(), "abc-123");
        assert_eq!(
            received.get_bin("x-trace-bin").unwrap().to_bytes().unwrap().as_ref(),
            &[1, 2, 3, 250]
        );
    });
}

/// Response metadata reaches the caller, both legs.
///
/// For a unary call tonic merges the trailers into the response metadata, so this
/// also proves the adapter emits a trailers frame on the *success* path — the frame
/// that carries `grpc-status`, and the one thing tonic cannot do without.
#[test]
fn response_metadata_reaches_the_caller_including_the_binary_leg() {
    run(async {
        let (mut greeter, _, _) = connect().await;
        let reply = greeter
            .say_hello(HelloRequest { name: "world".into() })
            .await
            .expect("unary should succeed");

        assert_eq!(reply.metadata().get("x-served-by").unwrap(), "probe");
        assert_eq!(
            reply.metadata().get_bin("x-served-bin").unwrap().to_bytes().unwrap().as_ref(),
            &[7, 250]
        );
    });
}

/// A trailers-only error keeps its trailing metadata.
///
/// This is the neighborhood a real bug has already been found in — trailing metadata
/// dropped on exactly this response shape, on two implementations at once. A failing
/// call is also when metadata matters most, since it is where a server puts the detail
/// the status code has no room for.
#[test]
fn a_trailers_only_error_keeps_its_status_and_trailing_metadata() {
    run(async {
        let (mut greeter, _, _) = connect().await;
        let status = greeter
            .say_hello(HelloRequest { name: "fail".into() })
            .await
            .expect_err("the server always fails this one");

        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "no");
        assert_eq!(
            status.metadata().get("x-detail").unwrap(),
            "quota-exhausted",
            "the status arrived without its trailing metadata"
        );
        assert_eq!(
            status.metadata().get_bin("x-detail-bin").unwrap().to_bytes().unwrap().as_ref(),
            &[0, 1, 250]
        );
    });
}

/// `Request::set_timeout` ends the call, and arrives as DEADLINE_EXCEEDED rather than
/// decaying to UNKNOWN on the way through the boxed-error boundary.
#[test]
fn a_deadline_ends_a_call_that_would_otherwise_run_on() {
    run(async {
        let (mut greeter, _, _) = connect().await;
        let mut request = Request::new(SleepRequest { millis: 5_000 });
        request.set_timeout(Duration::from_millis(150));

        let started = std::time::Instant::now();
        let status = greeter.sleep(request).await.expect_err("the call should time out");

        assert_eq!(status.code(), tonic::Code::DeadlineExceeded, "got {status}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the call ran for {:?} — nothing enforced the deadline",
            started.elapsed()
        );
    });
}

/// The deadline must bound a **stream**, not just its opening.
///
/// `Countdown(0)` sends one tick and then never sends another, which is the shape a
/// deadline exists for: headers arrive promptly, so anything that only bounded the
/// open would call this a success and hand the caller a stream that never ends. This
/// is also where the adapter deliberately diverges from tonic-over-`Channel`, where
/// `set_timeout` sends the header and nothing local enforces it.
#[test]
fn a_deadline_bounds_a_stream_that_stalls_after_it_opens() {
    run(async {
        let (mut greeter, _, _) = connect().await;
        let mut request = Request::new(CountdownRequest { from: 0 });
        request.set_timeout(Duration::from_millis(300));

        let mut ticks = greeter
            .countdown(request)
            .await
            .expect("the stream opens promptly — that is the point")
            .into_inner();

        let first = ticks.next().await.expect("one tick arrives").expect("no error");
        assert_eq!(first.value, 0);

        // Now the server goes quiet forever. The deadline is the only way out.
        let status = ticks
            .next()
            .await
            .expect("the stream must end, not hang")
            .expect_err("the deadline must end it");
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded, "got {status}");
    });
}

/// Backpressure survives the adapter.
///
/// The crate documents it and implements none of it: `h2ts-client` replenishes the
/// HTTP/2 receive window only as the body is polled, so a consumer that stops reading
/// stops the *server*. Routing that body through tonic is exactly where it could be
/// quietly lost to a buffer, so it is pinned from the producing side — the server's
/// own message count — rather than from what the client received.
#[test]
fn a_consumer_that_stops_reading_stops_the_server() {
    run(async {
        let (mut greeter, produced, _) = connect().await;

        // A `Tick` is tiny — a varint plus gRPC's 5-byte frame header, ~9 bytes here —
        // so the volume has to come from the count: ~1.8 MiB against h2ts's 1 MiB
        // default per-stream window, which an unthrottled server would run through
        // well inside the wait below. Sized deliberately: at 40k the whole stream fits
        // *inside* the window, and the test would pass whether or not backpressure
        // existed.
        const COUNT: u32 = 200_000;
        let mut ticks = greeter
            .countdown(CountdownRequest { from: COUNT })
            .await
            .expect("the stream opens")
            .into_inner();

        ticks.next().await.expect("one message").expect("no error");

        // Now stop reading. Nothing polls the body, so no WINDOW_UPDATE goes out.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let stalled = produced.load(Ordering::SeqCst);
        assert!(
            (stalled as u32) < COUNT,
            "the server produced all {COUNT} messages while the consumer was away — \
             there is no backpressure through the adapter"
        );

        // Reading again releases it, so this is throttling and not a stall.
        let mut seen = 1;
        while ticks.next().await.transpose().expect("no error").is_some() {
            seen += 1;
        }
        assert_eq!(seen, COUNT as usize, "every message must still arrive, in the end");
        assert_eq!(produced.load(Ordering::SeqCst), COUNT as usize);
    });
}

/// Dropping a stream cancels it **at the server**, rather than leaving a handler
/// running for a caller that has gone away.
///
/// This is load-bearing beyond tidiness, and it is the adapter's to get right: the
/// response body it hands tonic owns the h2ts body, so if dropping it did not reset
/// the HTTP/2 stream, every abandoned or timed-out RPC would leave the server working
/// on a result nobody will read. The signal comes from the server's own handler being
/// torn down, not from anything the client can see.
#[test]
fn dropping_a_stream_cancels_it_at_the_server() {
    run(async {
        let (base_url, mut cancelled) = start_server_with_cancel().await;
        let mut greeter = GreeterClient::new(dial::client(&base_url).into_tonic());

        let mut ticks = greeter
            .countdown(CountdownRequest { from: 0 })
            .await
            .expect("the stream opens")
            .into_inner();
        let first = ticks.next().await.expect("one tick").expect("no error");
        assert_eq!(first.value, 0);

        assert!(cancelled.try_recv().is_err(), "not cancelled while the caller still holds it");
        drop(ticks);

        tokio::time::timeout(Duration::from_secs(5), cancelled.recv())
            .await
            .expect("the server was never told; the handler is still running")
            .expect("cancel signal");
    });
}

/// The channel contract survives the adapter: a dropped tunnel is redialed by the
/// next call, so an app never owns socket lifecycle to make an RPC. This is what
/// `tonic::transport::Channel` does, and a stub cannot tell the difference.
#[test]
fn a_dropped_tunnel_is_redialed_on_the_next_call() {
    run(async {
        let (base_url, _, _) = start_server().await;
        let relay = Relay::start(port_of(&base_url)).await;
        let client = dial::client(&relay.url());
        let mut greeter = GreeterClient::new(client.clone().into_tonic());

        let reply = greeter
            .say_hello(HelloRequest { name: "before".into() })
            .await
            .expect("first call");
        assert_eq!(reply.into_inner().message, "Hello, before!");

        // Kill the tunnel out from under the client, as a network blip would.
        kill_tunnel(&relay, &client).await;

        let reply = greeter
            .say_hello(HelloRequest { name: "after".into() })
            .await
            .expect("the next call must redial");
        assert_eq!(reply.into_inner().message, "Hello, after!");
    });
}

// --- a severable connection, for the reconnect test ------------------------------------

/// A TCP forwarder whose live connections can be severed without it stopping
/// listening — a network blip, with the server left standing so any failure to
/// recover is unambiguously the client's.
struct Relay {
    port: u16,
    live: std::rc::Rc<std::cell::RefCell<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Relay {
    async fn start(target_port: u16) -> Relay {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let live: std::rc::Rc<std::cell::RefCell<Vec<tokio::task::JoinHandle<()>>>> =
            Default::default();
        let accepted = live.clone();
        tokio::task::spawn_local(async move {
            while let Ok((mut client, _)) = listener.accept().await {
                let Ok(mut upstream) =
                    tokio::net::TcpStream::connect(("127.0.0.1", target_port)).await
                else {
                    continue;
                };
                // Aborting the pump drops both sockets, which is the severing.
                accepted.borrow_mut().push(tokio::task::spawn_local(async move {
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                }));
            }
        });
        Relay { port, live }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Sever every live connection; new ones still succeed.
    fn cut(&self) {
        for task in self.live.borrow_mut().drain(..) {
            task.abort();
        }
    }
}

fn port_of(base_url: &str) -> u16 {
    base_url.rsplit(':').next().unwrap().parse().unwrap()
}

/// Cut the relay and let the close propagate so `is_closed()` reflects it.
async fn kill_tunnel(relay: &Relay, client: &Client) {
    relay.cut();
    for _ in 0..200 {
        if client.is_closed() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the tunnel did not close");
}

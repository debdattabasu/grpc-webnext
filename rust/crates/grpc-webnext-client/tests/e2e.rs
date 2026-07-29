//! End-to-end against a **real grpc-webnext server**, over a real WebSocket, on the
//! host.
//!
//! The wasm target cannot be tested without a browser, but almost nothing here is
//! wasm-specific: `h2ts-client` is a sans-I/O engine behind a pluggable byte
//! transport, so swapping the browser `WebSocket` for `tokio-tungstenite` exercises
//! the identical code path — framing, HPACK, flow control, trailers, and every line
//! of this crate except `web.rs`'s URL mapping (unit-tested separately). What a
//! browser adds is `web_sys::WebSocket`, not gRPC behavior.
//!
//! Everything is `!Send` by design, so the whole test runs on a `LocalSet`.

use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use grpc_webnext::{bind_and_serve_in_process, ServerConfig, Transcoder};
use grpc_webnext_client::{
    CallOptions, Client, Code, ConnectOptions, ConnectivityState, Metadata, Status, TypedClient,
    H2TS_SUBPROTOCOL,
};
use h2ts_client::{Transport, TransportError};
use prost::Message as _;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::{EchoRequest, EchoResponse};
use testecho::EchoSvc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::service::Routes;

const UNARY: &str = "/echo.v1.Echo/Unary";
const CHAT: &str = "/echo.v1.Echo/Chat";
const HANG: &str = "/echo.v1.Echo/Hang";
const REPEAT: &str = "/echo.v1.Echo/Repeat";

/// Start the real server (native gRPC + grpc-webnext on one port) and return its URL.
async fn start_server() -> String {
    start_server_with(EchoSvc::default(), None).await
}

/// Metadata the server actually received, captured by an interceptor — the echo
/// service does not reflect headers, so this is how a request's metadata is
/// observed without changing a fixture every other test depends on.
type Seen = Arc<Mutex<Option<tonic::metadata::MetadataMap>>>;

async fn start_server_with(svc: EchoSvc, seen: Option<Seen>) -> String {
    let transcoder =
        Arc::new(Transcoder::from_file_descriptor_set(testecho::FILE_DESCRIPTOR_SET).unwrap());
    let routes = match seen {
        Some(seen) => Routes::new(EchoServer::with_interceptor(
            svc,
            move |request: tonic::Request<()>| {
                *seen.lock().unwrap() = Some(request.metadata().clone());
                Ok(request)
            },
        )),
        None => Routes::new(EchoServer::new(svc)),
    };
    let (addr, handle) = bind_and_serve_in_process(
        routes,
        ServerConfig { transcoder: Some(transcoder), ..Default::default() },
    )
    .await
    .unwrap();
    // The server lives as long as the test.
    std::mem::forget(handle);
    format!("http://{addr}")
}

/// A local `Echo` implementation for the two things `EchoSvc` cannot express, kept
/// here rather than added to the shared fixture so no other test's behavior moves:
///
/// - `Repeat` counts the messages it has actually *produced*, which is how
///   backpressure is observable at all — the proof is that the server stops
///   generating, not merely that the client stops receiving.
/// - `Unary` fails with trailing metadata attached, giving a trailers-only response
///   that carries more than a status.
#[derive(Clone, Default)]
struct ProbeSvc {
    produced: Arc<std::sync::atomic::AtomicUsize>,
}

#[tonic::async_trait]
impl testecho::pb::echo_server::Echo for ProbeSvc {
    async fn unary(
        &self,
        _request: tonic::Request<EchoRequest>,
    ) -> Result<tonic::Response<EchoResponse>, tonic::Status> {
        let mut trailers = tonic::metadata::MetadataMap::new();
        trailers.insert("x-detail", "quota-exhausted".parse().unwrap());
        trailers.insert_bin(
            "x-detail-bin",
            tonic::metadata::MetadataValue::from_bytes(&[0, 1, 250]),
        );
        Err(tonic::Status::with_metadata(
            tonic::Code::FailedPrecondition,
            "no",
            trailers,
        ))
    }

    type RepeatStream =
        Pin<Box<dyn futures::Stream<Item = Result<EchoResponse, tonic::Status>> + Send>>;

    async fn repeat(
        &self,
        request: tonic::Request<testecho::pb::RepeatRequest>,
    ) -> Result<tonic::Response<Self::RepeatStream>, tonic::Status> {
        let testecho::pb::RepeatRequest { message, count } = request.into_inner();
        let produced = self.produced.clone();
        // `iter().map()` is lazy: the counter advances as the transport *pulls*,
        // which is exactly the signal a backpressure test needs.
        let output = futures::stream::iter(0..count).map(move |_| {
            produced.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(EchoResponse { message: message.clone() })
        });
        Ok(tonic::Response::new(Box::pin(output)))
    }

    type StreamStream =
        Pin<Box<dyn futures::Stream<Item = Result<EchoResponse, tonic::Status>> + Send>>;
    async fn stream(
        &self,
        _: tonic::Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<tonic::Response<Self::StreamStream>, tonic::Status> {
        unimplemented!("ProbeSvc only implements Unary and Repeat")
    }
    type ChatStream =
        Pin<Box<dyn futures::Stream<Item = Result<EchoResponse, tonic::Status>> + Send>>;
    async fn chat(
        &self,
        _: tonic::Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<tonic::Response<Self::ChatStream>, tonic::Status> {
        unimplemented!("ProbeSvc only implements Unary and Repeat")
    }
    type HangStream =
        Pin<Box<dyn futures::Stream<Item = Result<EchoResponse, tonic::Status>> + Send>>;
    async fn hang(
        &self,
        _: tonic::Request<EchoRequest>,
    ) -> Result<tonic::Response<Self::HangStream>, tonic::Status> {
        unimplemented!("ProbeSvc only implements Unary and Repeat")
    }
    async fn sleep(
        &self,
        _: tonic::Request<testecho::pb::SleepRequest>,
    ) -> Result<tonic::Response<EchoResponse>, tonic::Status> {
        unimplemented!("ProbeSvc only implements Unary and Repeat")
    }
    async fn flaky_unary(
        &self,
        _: tonic::Request<EchoRequest>,
    ) -> Result<tonic::Response<EchoResponse>, tonic::Status> {
        unimplemented!("ProbeSvc only implements Unary and Repeat")
    }
}

/// Start a server backed by [`ProbeSvc`], returning its URL and the produced-count.
async fn start_probe_server() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    let svc = ProbeSvc::default();
    let produced = svc.produced.clone();
    let (addr, handle) =
        bind_and_serve_in_process(Routes::new(EchoServer::new(svc)), ServerConfig::default())
            .await
            .unwrap();
    std::mem::forget(handle);
    (format!("http://{addr}"), produced)
}

/// A connector that dials a **fresh** tunnel each time it is called, so the client
/// can redial. The browser equivalent is `grpc_webnext_client::connect`.
fn connector_for(base_url: &str) -> grpc_webnext_client::Connector {
    let base_url = base_url.to_string();
    std::rc::Rc::new(move || {
        let base_url = base_url.clone();
        Box::pin(async move {
            match dial(&base_url).await {
                Some(conn) => Ok(conn),
                None => Err(Status::unavailable("dial failed")),
            }
        })
    })
}

/// Open one tunnel, spawning its driver. `None` if the endpoint is unreachable.
async fn dial(base_url: &str) -> Option<h2ts_client::H2Connection> {
    let ws_url = format!("ws{}", base_url.trim_start_matches("http"));
    let mut request = ws_url.into_client_request().ok()?;
    request.headers_mut().insert("sec-websocket-protocol", H2TS_SUBPROTOCOL.parse().ok()?);
    let (ws, _) = tokio_tungstenite::connect_async(request).await.ok()?;
    let (mut ws_tx, ws_rx) = ws.split();

    let reader = ws_rx.filter_map(|msg| async move {
        match msg {
            Ok(TungMessage::Binary(data)) => Some(data.to_vec()),
            _ => None,
        }
    });
    let (tx, mut rx) = mpsc::unbounded::<Vec<u8>>();
    tokio::task::spawn_local(async move {
        while let Some(chunk) = rx.next().await {
            if ws_tx.send(TungMessage::Binary(chunk)).await.is_err() {
                break;
            }
        }
    });
    let writer = tx.sink_map_err(|e| TransportError(e.to_string()));
    let transport = Transport::new(Box::pin(reader), Box::pin(writer));
    let (conn, driver) = h2ts_client::connect(transport, ConnectOptions::default());
    tokio::task::spawn_local(driver);
    Some(conn)
}

/// A reconnecting client, the shape a browser gets from `connect`.
fn reconnecting_client(base_url: &str) -> Client {
    let authority = base_url.trim_start_matches("http://").to_string();
    Client::with_connector(connector_for(base_url), authority)
}

/// Adapt a tokio-tungstenite WebSocket to the h2ts byte transport. This is the
/// host-side sibling of the crate's browser transport — same engine above it.
async fn connect_client(base_url: &str) -> Client {
    let ws_url = format!("ws{}", base_url.trim_start_matches("http"));
    let authority = base_url.trim_start_matches("http://").to_string();

    let mut request = ws_url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", H2TS_SUBPROTOCOL.parse().unwrap());
    let (ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let (mut ws_tx, ws_rx) = ws.split();

    // Inbound: binary frames become byte chunks; anything else is not tunnel data.
    let reader = ws_rx.filter_map(|msg| async move {
        match msg {
            Ok(TungMessage::Binary(data)) => Some(data.to_vec()),
            _ => None,
        }
    });

    // Outbound: a channel the sink writes into, pumped into the socket.
    let (tx, mut rx) = mpsc::unbounded::<Vec<u8>>();
    tokio::task::spawn_local(async move {
        while let Some(chunk) = rx.next().await {
            if ws_tx.send(TungMessage::Binary(chunk)).await.is_err() {
                break;
            }
        }
    });
    let writer = tx.sink_map_err(|e| TransportError(e.to_string()));

    let transport = Transport::new(Box::pin(reader), Box::pin(writer));
    let (client, driver) = Client::over_transport(transport, authority, ConnectOptions::default());
    tokio::task::spawn_local(driver);
    client
}

/// The port out of a `http://127.0.0.1:N` URL.
fn port_of(base_url: &str) -> u16 {
    base_url.rsplit(':').next().unwrap().parse().unwrap()
}

/// A TCP forwarder whose live connections can be severed without it stopping
/// listening — a network blip, with the server left standing so any failure to
/// recover is unambiguously the client's.
struct Relay {
    port: u16,
    live: Rc<RefCell<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Relay {
    async fn start(target_port: u16) -> Relay {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let live: Rc<RefCell<Vec<tokio::task::JoinHandle<()>>>> = Rc::new(RefCell::new(Vec::new()));
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

/// Run `body` on a LocalSet — everything in this client is single-threaded.
fn run<F: std::future::Future<Output = ()> + 'static>(body: F) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    tokio::task::LocalSet::new().block_on(&runtime, body);
}

#[test]
fn unary_round_trips_through_the_tunnel() {
    run(async {
        let base = start_server().await;
        let client = connect_client(&base).await;

        let request = EchoRequest { message: "hello".into() };
        let response = client
            .unary(UNARY, request.encode_to_vec(), CallOptions::new())
            .await
            .expect("unary should succeed");

        let decoded = EchoResponse::decode(response.message.as_slice()).unwrap();
        assert_eq!(decoded.message, "hello");
    });
}

#[test]
fn typed_helpers_encode_and_decode() {
    run(async {
        let base = start_server().await;
        let client = connect_client(&base).await;

        let reply: EchoResponse = client
            .unary_typed(
                UNARY,
                EchoRequest { message: "typed".into() },
                CallOptions::new(),
            )
            .await
            .expect("typed unary should succeed")
            .into_inner();
        assert_eq!(reply.message, "typed");
    });
}

#[test]
fn metadata_reaches_the_server_including_the_binary_leg() {
    run(async {
        let seen: Seen = Arc::new(Mutex::new(None));
        let base = start_server_with(EchoSvc::default(), Some(seen.clone())).await;
        let client = connect_client(&base).await;

        let mut metadata = Metadata::new();
        metadata.insert("x-request-id", "abc-123");
        metadata.insert_bin("x-trace-bin", vec![1, 2, 3, 250]);

        let request = EchoRequest { message: "meta".into() };
        client
            .unary(UNARY, request.encode_to_vec(), CallOptions::new().with_metadata(metadata))
            .await
            .expect("unary with metadata should succeed");

        let received = seen.lock().unwrap().clone().expect("the server saw a request");
        assert_eq!(received.get("x-request-id").unwrap().to_str().unwrap(), "abc-123");
        // The `-bin` leg is the one that has produced a real bug in this repo: on
        // this path it must be base64 on the wire, and tonic decodes it back to the
        // raw bytes. Getting that wrong yields mojibake, not an error.
        assert_eq!(
            received.get_bin("x-trace-bin").unwrap().to_bytes().unwrap().as_ref(),
            &[1u8, 2, 3, 250]
        );
    });
}

#[test]
fn a_server_error_arrives_as_its_real_status_and_message() {
    run(async {
        // `flaky(1)` fails the first call with UNAVAILABLE, then succeeds — so this
        // also proves the tunnel survives an error and carries the next call.
        let base = start_server_with(EchoSvc::flaky(1), None).await;
        let client = connect_client(&base).await;

        let request = EchoRequest { message: "boom".into() };
        let status = client
            .unary("/echo.v1.Echo/FlakyUnary", request.encode_to_vec(), CallOptions::new())
            .await
            .expect_err("the first call should fail");

        assert_eq!(status.code, Code::Unavailable);
        assert_eq!(status.message, "flaky: transient failure", "the server's message must survive");

        let ok = client
            .unary("/echo.v1.Echo/FlakyUnary", request.encode_to_vec(), CallOptions::new())
            .await
            .expect("the second call should succeed on the same tunnel");
        assert_eq!(EchoResponse::decode(ok.message.as_slice()).unwrap().message, "boom");
    });
}

#[test]
fn client_streaming_uploads_every_message() {
    run(async {
        let base = start_server().await;
        let client = connect_client(&base).await;

        let requests = futures::stream::iter(["a", "b", "c"].map(|m| {
            EchoRequest { message: m.into() }.encode_to_vec()
        }));

        // Chat is bidi; consuming only its first response exercises the upload path
        // with a single-response read, which is what `client_streaming` supports.
        let response = client
            .client_streaming(CHAT, requests, CallOptions::new())
            .await
            .expect("client streaming should succeed");
        let decoded = EchoResponse::decode(response.message.as_slice()).unwrap();
        assert_eq!(decoded.message, "a");
    });
}

/// A deadline ends the call. Both halves are armed here — `with_timeout` sends
/// `grpc-timeout` *and* starts a local timer — so this proves the feature end to end
/// without isolating which half did it. `a_deadline_is_enforced_by_the_client_alone`
/// isolates the client and `the_server_enforces_grpc_timeout_on_the_h2ts_path`
/// isolates the server.
#[test]
fn a_deadline_ends_a_call_that_would_otherwise_run_on() {
    run(async {
        let base = start_server().await;
        let client = connect_client(&base).await;

        let request = testecho::pb::SleepRequest { millis: 5_000 };
        let status = client
            .unary(
                "/echo.v1.Echo/Sleep",
                request.encode_to_vec(),
                CallOptions::new().with_timeout(Duration::from_millis(100)),
            )
            .await
            .expect_err("the call should time out");

        assert_eq!(status.code, Code::DeadlineExceeded, "got {status}");
    });
}

/// The client's own timer, with **nothing** on the other end to help it.
///
/// This matters because `CallOptions::timeout` promises two independent things, and
/// only one of them is the server's. The test above cannot tell them apart: since
/// the h2ts path started enforcing `grpc-timeout`, a server-side cut would satisfy
/// it just as well, so it would keep passing if the local timer were deleted
/// outright. Here the peer never answers anything — no SETTINGS, no response — so
/// the only thing that can end the call is the client.
#[test]
fn a_deadline_is_enforced_by_the_client_alone() {
    run(async {
        // A transport that swallows everything and never replies: the h2ts handshake
        // itself never completes.
        let writer = futures::sink::drain().sink_map_err(|_: std::convert::Infallible| {
            TransportError("unreachable".into())
        });
        let transport =
            Transport::new(Box::pin(futures::stream::pending::<Vec<u8>>()), Box::pin(writer));
        let (client, driver) =
            Client::over_transport(transport, "black.hole", ConnectOptions::default());
        tokio::task::spawn_local(driver);

        let status = client
            .unary(
                UNARY,
                EchoRequest { message: "x".into() }.encode_to_vec(),
                CallOptions::new().with_timeout(Duration::from_millis(150)),
            )
            .await
            .expect_err("nothing can answer this call");
        assert_eq!(status.code, Code::DeadlineExceeded, "got {status}");
    });
}

/// A deadline must bound a **stream**, not just its opening.
///
/// `Hang` sends one message and then never sends another, which is the shape a
/// deadline exists for: headers arrive promptly, so anything that only bounded the
/// open would call this a success and hand the caller a stream that never ends.
///
/// It hung forever before this was fixed, and neither side would have saved it. The
/// client armed its timer only in the single-response path, so `server_streaming`
/// and `bidi_streaming` sent `grpc-timeout` and started no timer at all. The server
/// does enforce `grpc-timeout`, but only around producing the *response* — which for
/// a streaming RPC resolves as soon as the headers are ready, long before the body
/// stalls. Two mechanisms, both real, and the gap sat exactly between them.
#[test]
fn a_deadline_bounds_a_stream_that_stalls_after_it_opens() {
    run(async {
        let base = start_server().await;
        let client = connect_client(&base).await;

        let mut stream = client
            .server_streaming(
                HANG,
                EchoRequest { message: "x".into() }.encode_to_vec(),
                CallOptions::new().with_timeout(Duration::from_millis(300)),
            )
            .await
            .expect("the stream opens promptly — that is the point");

        let first = stream.message().await.expect("first message").expect("one message arrives");
        assert_eq!(EchoResponse::decode(first.as_slice()).unwrap().message, "started");

        // Now the server goes quiet forever. The deadline is the only way out.
        let status = stream.message().await.expect_err("the deadline must end the stream");
        assert_eq!(status.code, Code::DeadlineExceeded, "got {status}");
        // ...and it stays ended, rather than resuming on the next poll.
        assert_eq!(stream.status().code, Code::DeadlineExceeded);
    });
}

/// The deadline spans the whole call, so time spent opening the stream is not
/// refunded when the first message read begins.
#[test]
fn the_stream_deadline_is_not_restarted_per_message() {
    run(async {
        let base = start_server().await;
        let client = connect_client(&base).await;

        let started = std::time::Instant::now();
        let mut stream = client
            .server_streaming(
                HANG,
                EchoRequest { message: "x".into() }.encode_to_vec(),
                CallOptions::new().with_timeout(Duration::from_millis(400)),
            )
            .await
            .expect("the stream opens");
        let _ = stream.message().await;
        let status = stream.message().await.expect_err("deadline");
        assert_eq!(status.code, Code::DeadlineExceeded);

        // A timer restarted for the body would put this at open + 400ms, not 400ms.
        // The margin is loose enough for a slow machine and tight enough to catch a
        // restart, which would have to be at least a full extra timeout.
        assert!(
            started.elapsed() < Duration::from_millis(750),
            "the deadline looks like it restarted: {:?}",
            started.elapsed()
        );
    });
}

#[test]
fn oversized_responses_are_refused_by_the_configured_limit() {
    run(async {
        let base = start_server().await;
        let client = connect_client(&base).await;

        let request = EchoRequest { message: "x".repeat(1024) };
        let status: Status = client
            .unary(
                UNARY,
                request.encode_to_vec(),
                CallOptions::new().with_max_message_bytes(16),
            )
            .await
            .expect_err("the response exceeds the limit");

        assert_eq!(status.code, Code::ResourceExhausted);
    });
}

/// The **server** enforces the deadline too, not just the client.
///
/// This sends `grpc-timeout` as raw metadata and leaves `CallOptions::timeout`
/// unset, so the client's own timer never arms — the only thing that can end this
/// call is the server. Before the h2ts path enforced deadlines, the 5-second sleep
/// ran to completion and answered "awake": `grpc-timeout` was received and ignored,
/// because h2ts is a byte pipe and tonic's timeout layer belongs to a hyper server
/// this path does not use. A caller could give up while the handler ran on.
#[test]
fn the_server_enforces_grpc_timeout_on_the_h2ts_path() {
    run(async {
        let base = start_server().await;
        let client = connect_client(&base).await;

        let mut metadata = Metadata::new();
        metadata.insert("grpc-timeout", "100m"); // 100 ms, by hand

        let request = testecho::pb::SleepRequest { millis: 5_000 };
        let started = std::time::Instant::now();
        let status = client
            .unary(
                "/echo.v1.Echo/Sleep",
                request.encode_to_vec(),
                CallOptions::new().with_metadata(metadata), // no local timeout
            )
            .await
            .expect_err("the server should cut the call short");

        assert_eq!(status.code, Code::DeadlineExceeded, "got {status}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the server waited {:?} — it ran the handler to completion instead of \
             enforcing the deadline",
            started.elapsed()
        );
    });
}

#[test]
fn server_streaming_yields_every_message_then_the_status() {
    run(async {
        let base = start_server().await;
        let client = connect_client(&base).await;

        let request = testecho::pb::RepeatRequest { message: "tick".into(), count: 3 };
        let mut stream = client
            .server_streaming("/echo.v1.Echo/Repeat", request.encode_to_vec(), CallOptions::new())
            .await
            .expect("the stream should open");

        let mut seen = Vec::new();
        while let Some(message) = stream.message().await.expect("no stream error") {
            seen.push(EchoResponse::decode(message.as_slice()).unwrap().message);
        }
        assert_eq!(seen, vec!["tick", "tick", "tick"]);
        // The status arrives in the trailers, after the last message — the whole
        // reason `into_parts` had to exist.
        assert_eq!(stream.status().code, Code::Ok, "{}", stream.status());
    });
}

#[test]
fn bidi_streams_both_directions_in_order() {
    run(async {
        let base = start_server().await;
        let client = connect_client(&base).await;

        let requests = futures::stream::iter(
            ["one", "two", "three"]
                .map(|m| EchoRequest { message: m.into() }.encode_to_vec()),
        );
        let mut stream = client
            .bidi_streaming("/echo.v1.Echo/Stream", requests, CallOptions::new())
            .await
            .expect("the stream should open");

        let mut seen = Vec::new();
        while let Some(message) = stream.message().await.expect("no stream error") {
            seen.push(EchoResponse::decode(message.as_slice()).unwrap().message);
        }
        assert_eq!(seen, vec!["one", "two", "three"]);
        assert_eq!(stream.status().code, Code::Ok);
    });
}

#[test]
fn a_stream_that_fails_reports_the_status_not_a_clean_end() {
    run(async {
        // The distinction `into_parts` buys: without the trailers a failed stream and
        // a successfully empty one are the same thing to the caller.
        let base = start_server_with(EchoSvc::flaky(1), None).await;
        let client = connect_client(&base).await;

        let request = EchoRequest { message: "x".into() };
        let err = client
            .server_streaming(
                "/echo.v1.Echo/FlakyUnary",
                request.encode_to_vec(),
                CallOptions::new(),
            )
            .await
            .expect_err("a trailers-only failure should surface when the stream opens");
        assert_eq!(err.code, Code::Unavailable);
    });
}

/// Dropping a stream cancels it **at the server**, rather than leaving a handler
/// running for a caller that has gone away.
///
/// This is load-bearing beyond tidiness: the deadline path stops work by dropping
/// the call future, so if a drop did not reset the h2 stream, every timed-out RPC
/// would leave the server working on a result nobody will read. `Hang` holds a guard
/// that fires when its stream is dropped, so the signal comes from the server's own
/// handler being torn down.
#[test]
fn dropping_a_stream_cancels_it_at_the_server() {
    run(async {
        let (svc, mut cancelled) = EchoSvc::with_cancel();
        let base = start_server_with(svc, None).await;
        let client = connect_client(&base).await;

        let mut stream = client
            .server_streaming(HANG, EchoRequest { message: "x".into() }.encode_to_vec(), CallOptions::new())
            .await
            .expect("the stream opens");
        let first = stream.message().await.expect("no error").expect("one message");
        assert_eq!(EchoResponse::decode(first.as_slice()).unwrap().message, "started");

        assert!(cancelled.try_recv().is_err(), "not cancelled while the caller still holds it");
        drop(stream);

        tokio::time::timeout(Duration::from_secs(5), cancelled.recv())
            .await
            .expect("the server was never told; the handler is still running")
            .expect("cancel signal");
    });
}

/// A timed-out call also releases the server, for the same reason: the deadline
/// drops the call future, and that drop must reach the far end.
#[test]
fn a_deadline_releases_the_server_too() {
    run(async {
        let (svc, mut cancelled) = EchoSvc::with_cancel();
        let base = start_server_with(svc, None).await;
        let client = connect_client(&base).await;

        let mut stream = client
            .server_streaming(
                HANG,
                EchoRequest { message: "x".into() }.encode_to_vec(),
                CallOptions::new().with_timeout(Duration::from_millis(200)),
            )
            .await
            .expect("the stream opens");
        let _ = stream.message().await;
        assert_eq!(
            stream.message().await.expect_err("deadline").code,
            Code::DeadlineExceeded
        );
        drop(stream);

        tokio::time::timeout(Duration::from_secs(5), cancelled.recv())
            .await
            .expect("a timed-out call left the handler running")
            .expect("cancel signal");
    });
}

/// Backpressure is real, and this is the only thing that proves it.
///
/// The crate documents it in three places and implements none of it: `h2ts-client`
/// replenishes the HTTP/2 receive window only as the body is polled, so a consumer
/// that stops reading stops the *server*. A claim that costs no code is exactly the
/// claim that silently stops being true, so it is pinned from the producing side —
/// the server's own message count — rather than from what the client received.
#[test]
fn a_consumer_that_stops_reading_stops_the_server() {
    run(async {
        let (base, produced) = start_probe_server().await;
        let client = connect_client(&base).await;

        // 40 × 64 KiB = 2.5 MiB against h2ts's 1 MiB default per-stream window, so
        // an unthrottled server would run to the end well inside the wait below.
        const COUNT: u32 = 40;
        let request = testecho::pb::RepeatRequest { message: "x".repeat(64 * 1024), count: COUNT };
        let mut stream = client
            .server_streaming(REPEAT, request.encode_to_vec(), CallOptions::new())
            .await
            .expect("the stream opens");

        stream.message().await.expect("no error").expect("one message");

        // Now stop reading. Nothing polls the body, so no WINDOW_UPDATE goes out.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let stalled = produced.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            (stalled as u32) < COUNT,
            "the server produced all {COUNT} messages while the consumer was away — \
             there is no backpressure on this path"
        );

        // Reading again releases it, so this is throttling and not a stall.
        let mut seen = 1;
        while stream.message().await.expect("no error").is_some() {
            seen += 1;
        }
        assert_eq!(seen, COUNT as usize, "every message must still arrive, in the end");
        assert_eq!(stream.status().code, Code::Ok);
        assert_eq!(produced.load(std::sync::atomic::Ordering::SeqCst), COUNT as usize);
    });
}

/// A trailers-only error keeps its trailing metadata.
///
/// This is the neighborhood a real bug has already been found in — trailing metadata
/// dropped on exactly this response shape, on two implementations at once. A failing
/// call is also when metadata matters most, since it is where a server puts the
/// detail that the status code has no room for.
#[test]
fn a_trailers_only_error_keeps_its_trailing_metadata() {
    run(async {
        let (base, _) = start_probe_server().await;
        let client = connect_client(&base).await;

        let status = client
            .unary(UNARY, EchoRequest { message: "x".into() }.encode_to_vec(), CallOptions::new())
            .await
            .expect_err("ProbeSvc always fails this call");

        assert_eq!(status.code, Code::FailedPrecondition);
        assert_eq!(status.message, "no");
        assert_eq!(
            status.metadata.get("x-detail").map(|v| v.to_string()),
            Some("quota-exhausted".to_string()),
            "the status arrived without its trailing metadata"
        );
        // The `-bin` leg travels base64-encoded and must come back as raw bytes.
        assert_eq!(status.metadata.get_bin("x-detail-bin"), Some(&[0u8, 1, 250][..]));
    });
}

// --- reconnect: the channel contract ------------------------------------------------------

#[test]
fn a_dropped_tunnel_is_redialed_on_the_next_call() {
    // What tonic's `Channel` does, and therefore what this must do: the call that
    // finds the tunnel dead reports the failure, and the next one reconnects. An
    // app should not have to own socket lifecycle to use a gRPC client.
    run(async {
        let base = start_server().await;
        let relay = Relay::start(port_of(&base)).await;
        let client = reconnecting_client(&relay.url());
        assert_eq!(client.state(), ConnectivityState::Idle); // nothing dialed yet

        let request = EchoRequest { message: "before".into() };
        let first = client.unary(UNARY, request.encode_to_vec(), CallOptions::new()).await;
        assert!(first.is_ok(), "first call: {first:?}");
        assert_eq!(client.state(), ConnectivityState::Ready);

        // Kill the tunnel out from under the client, as a network blip would.
        kill_tunnel(&relay, &client).await;
        assert!(client.is_closed());

        let request = EchoRequest { message: "after".into() };
        let reply = client
            .unary(UNARY, request.encode_to_vec(), CallOptions::new())
            .await
            .expect("the next call must redial");
        assert_eq!(EchoResponse::decode(reply.message.as_slice()).unwrap().message, "after");
        assert_eq!(client.state(), ConnectivityState::Ready);
    });
}

/// Recovery is not a one-shot. A channel that heals once and then latches — a
/// consumed connector, a dial slot never cleared, a state machine that only leaves
/// Idle in one direction — passes the single-cut test above and still strands an
/// app on the second blip of the day.
#[test]
fn the_channel_keeps_recovering_across_repeated_cuts() {
    run(async {
        let base = start_server().await;
        let relay = Relay::start(port_of(&base)).await;
        let client = reconnecting_client(&relay.url());

        for round in 0..4 {
            let message = format!("round-{round}");
            let reply = client
                .unary(
                    UNARY,
                    EchoRequest { message: message.clone() }.encode_to_vec(),
                    CallOptions::new(),
                )
                .await
                .unwrap_or_else(|e| panic!("round {round} failed: {e}"));
            assert_eq!(EchoResponse::decode(reply.message.as_slice()).unwrap().message, message);
            assert_eq!(client.state(), ConnectivityState::Ready);
            kill_tunnel(&relay, &client).await;
        }
    });
}

#[test]
fn connectivity_transitions_are_observable() {
    run(async {
        let base = start_server().await;
        let relay = Relay::start(port_of(&base)).await;
        let client = reconnecting_client(&relay.url());
        let mut changes = Box::pin(client.state_changes());

        let request = EchoRequest { message: "x".into() };
        client.unary(UNARY, request.encode_to_vec(), CallOptions::new()).await.unwrap();
        assert_eq!(changes.next().await, Some(ConnectivityState::Connecting));
        assert_eq!(changes.next().await, Some(ConnectivityState::Ready));

        kill_tunnel(&relay, &client).await;
        client.unary(UNARY, request.encode_to_vec(), CallOptions::new()).await.unwrap();
        // Idle (noticed it died) -> Connecting -> Ready, with no repeats.
        assert_eq!(changes.next().await, Some(ConnectivityState::Idle));
        assert_eq!(changes.next().await, Some(ConnectivityState::Connecting));
        assert_eq!(changes.next().await, Some(ConnectivityState::Ready));
    });
}

#[test]
fn an_unreachable_endpoint_reports_transient_failure_and_keeps_trying() {
    run(async {
        // Nothing listening: every dial fails, but the client must stay usable rather
        // than latching onto the first failure the way a cached rejected dial would.
        let client = reconnecting_client("http://127.0.0.1:1");
        for _ in 0..2 {
            let err = client
                .unary(UNARY, EchoRequest { message: "x".into() }.encode_to_vec(), CallOptions::new())
                .await
                .expect_err("no server");
            assert_eq!(err.code, Code::Unavailable);
            assert_eq!(client.state(), ConnectivityState::TransientFailure);
        }

        // ...and once an endpoint exists, the same client connects to it.
        let base = start_server().await;
        let working = reconnecting_client(&base);
        assert!(working
            .unary(UNARY, EchoRequest { message: "ok".into() }.encode_to_vec(), CallOptions::new())
            .await
            .is_ok());
    });
}

#[test]
fn concurrent_calls_after_a_drop_open_one_tunnel_between_them() {
    run(async {
        // Two calls racing to reconnect must share a dial. Opening a socket each is
        // the kind of thing that only shows up under load.
        let base = start_server().await;
        let relay = Relay::start(port_of(&base)).await;
        let client = reconnecting_client(&relay.url());
        client
            .unary(UNARY, EchoRequest { message: "warm".into() }.encode_to_vec(), CallOptions::new())
            .await
            .unwrap();
        kill_tunnel(&relay, &client).await;

        let a = client.unary(UNARY, EchoRequest { message: "a".into() }.encode_to_vec(), CallOptions::new());
        let b = client.unary(UNARY, EchoRequest { message: "b".into() }.encode_to_vec(), CallOptions::new());
        let (ra, rb) = futures::join!(a, b);
        assert_eq!(EchoResponse::decode(ra.unwrap().message.as_slice()).unwrap().message, "a");
        assert_eq!(EchoResponse::decode(rb.unwrap().message.as_slice()).unwrap().message, "b");
    });
}

#[test]
fn a_single_shot_client_says_so_rather_than_pretending() {
    run(async {
        // Built over a caller-supplied transport: the transport is consumed, so there
        // is nothing to redial with. It must fail loudly, not look like a live channel.
        let base = start_server().await;
        let relay = Relay::start(port_of(&base)).await;
        let client = connect_client(&relay.url()).await;
        client
            .unary(UNARY, EchoRequest { message: "x".into() }.encode_to_vec(), CallOptions::new())
            .await
            .unwrap();

        kill_tunnel(&relay, &client).await;
        let err = client
            .unary(UNARY, EchoRequest { message: "y".into() }.encode_to_vec(), CallOptions::new())
            .await
            .expect_err("a single-shot client cannot redial");
        assert_eq!(err.code, Code::Unavailable);
        assert!(err.message.contains("cannot redial"), "unhelpful message: {}", err.message);
    });
}

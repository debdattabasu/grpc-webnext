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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use grpc_webnext::{bind_and_serve_in_process, ServerConfig, Transcoder};
use grpc_webnext_client::{
    CallOptions, Client, Code, ConnectOptions, Metadata, Status, TypedClient, H2TS_SUBPROTOCOL,
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

/// The deadline is enforced *by the client*, and that is not a shortcut.
///
/// On the h2ts path the tunnel hands requests straight to a tonic `Routes`, and the
/// piece of tonic that honors `grpc-timeout` lives in its own hyper server, which is
/// not in this path — so nothing server-side stops the work. This test originally
/// asserted the server would time out and caught that assumption: the 5-second sleep
/// ran to completion and returned "awake".
#[test]
fn a_deadline_is_enforced_locally_because_this_path_has_no_server_timer() {
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

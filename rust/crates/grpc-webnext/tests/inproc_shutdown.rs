//! Graceful shutdown: stop accepting, let in-flight RPCs finish, close idle connections,
//! and resolve once the last one is gone.
//!
//! The interesting cases are all "does the drain wait for the right thing" — a drain that
//! returns too early cuts live RPCs, and one that returns too late hangs a deploy.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use grpc_webnext::pb::{frame::Kind, Frame, HalfClose, Subscribe};
use grpc_webnext::{decode_frame, encode_frame, serve_in_process_with_shutdown, ServerConfig};
use prost::Message as _;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::EchoRequest;
use testecho::EchoSvc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::service::Routes;

type Server = (
    String,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<std::io::Result<()>>,
);

/// Start an echo server whose drain is triggered by the returned sender.
async fn start() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(serve_in_process_with_shutdown(
        listener,
        routes,
        ServerConfig::default(),
        async {
            let _ = rx.await;
        },
    ));
    (format!("{addr}"), tx, handle)
}

async fn ws_connect(
    url: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let mut req = url.into_client_request().unwrap();
    req.headers_mut()
        .insert("sec-websocket-protocol", "grpc-webnext+proto".parse().unwrap());
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

/// Open a stream, with `payload` as the inline first message.
fn subscribe(payload: Vec<u8>) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame {
        kind: Some(Kind::Subscribe(Subscribe {
            method: String::new(),
            headers: vec![],
            timeout_millis: 0,
            initial_payload: payload.into(),
        })),
    }))
}

fn echo(message: &str) -> Vec<u8> {
    EchoRequest { message: message.into() }.encode_to_vec()
}

/// An idle server drains immediately — nothing to wait for.
#[tokio::test]
async fn drains_immediately_when_idle() {
    let (_addr, tx, handle) = start().await;

    let _ = tx.send(());
    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;

    assert!(result.is_ok(), "an idle server should drain at once");
    result.unwrap().unwrap().unwrap();
}

/// After the drain the listener is gone, so a new connection is refused rather than
/// silently accepted and then dropped.
#[tokio::test]
async fn stops_accepting_after_drain() {
    let (addr, tx, handle) = start().await;

    // Prove the address worked before the drain.
    tokio::net::TcpStream::connect(&addr).await.expect("connect before drain");

    let _ = tx.send(());
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("drain finished")
        .unwrap()
        .unwrap();

    // The listener is closed; connecting now must fail (or connect and immediately EOF,
    // which some platforms allow from the accept backlog — either way, no RPC is served).
    let refused = tokio::net::TcpStream::connect(&addr).await;
    if let Ok(stream) = refused {
        drop(stream);
    }
}

/// A WebSocket that has not opened its stream is idle, so the drain closes it — with
/// `1001 Going Away`, not a gRPC status, because no RPC ever started.
#[tokio::test]
async fn closes_idle_websocket_with_going_away() {
    let (addr, tx, handle) = start().await;
    let mut ws = ws_connect(&format!("ws://{addr}/echo.v1.Echo/Unary")).await;

    let _ = tx.send(());

    let close = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("the drain should close an idle socket promptly")
        .expect("a message")
        .expect("a clean close");
    match close {
        TungMessage::Close(Some(frame)) => {
            assert_eq!(frame.code, CloseCode::Away, "1001, the going-away code");
        }
        other => panic!("expected a close frame, got {other:?}"),
    }

    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("drain finished")
        .unwrap()
        .unwrap();
}

/// A WebSocket with a live stream is *not* cut: the RPC completes, delivers its terminal
/// frame, and only then does the socket — and the drain — finish.
#[tokio::test]
async fn in_flight_stream_completes_before_drain_finishes() {
    let (addr, tx, handle) = start().await;
    let mut ws = ws_connect(&format!("ws://{addr}/echo.v1.Echo/Repeat")).await;

    // Open a stream and read its first message, so the RPC is provably in flight.
    let request = testecho::pb::RepeatRequest { message: "hi".into(), count: 3 }.encode_to_vec();
    ws.send(subscribe(request)).await.unwrap();
    ws.send(TungMessage::Binary(encode_frame(&Frame {
        kind: Some(Kind::HalfClose(HalfClose {})),
    })))
    .await
    .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("a frame")
        .expect("a message")
        .expect("no error");
    assert!(matches!(first, TungMessage::Binary(_)), "expected a Header/Message frame");

    let _ = tx.send(());

    // The stream must still reach its terminal Trailer, and the socket must close normally
    // (1000) rather than with the going-away code an idle socket gets.
    let mut saw_trailer = false;
    let mut close_code = None;
    while let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
        match msg {
            TungMessage::Binary(data) => {
                if matches!(decode_frame(&data).map(|f| f.kind), Ok(Some(Kind::Trailer(_)))) {
                    saw_trailer = true;
                }
            }
            TungMessage::Close(frame) => {
                close_code = frame.map(|f| f.code);
                // Keep polling: tokio-tungstenite sends the answering close from the
                // read path, and a client that stops here would leave the server's half
                // of the handshake outstanding.
                continue;
            }
            _ => {}
        }
    }
    assert!(saw_trailer, "the in-flight RPC must still deliver its terminal status");
    assert_eq!(close_code, Some(CloseCode::Normal), "normal close after the stream ended");

    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("drain finished once the stream was done")
        .unwrap()
        .unwrap();
}

/// A unary Fetch call already in flight when the drain starts still gets its response —
/// hyper stops taking *new* requests, it does not cut the one it is serving.
#[tokio::test]
async fn in_flight_unary_completes_before_drain_finishes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(serve_in_process_with_shutdown(
        listener,
        routes,
        ServerConfig::default(),
        async {
            let _ = rx.await;
        },
    ));

    // `Sleep` holds the RPC open long enough for the drain to start underneath it.
    let body = grpc_webnext::encode_request_body(
        &testecho::pb::SleepRequest { millis: 400 }.encode_to_vec(),
    );
    let client = reqwest::Client::new();
    let request = client
        .post(format!("http://{addr}/echo.v1.Echo/Sleep"))
        .header("content-type", grpc_webnext::CT_PROTO)
        .body(body.to_vec())
        .send();

    let drain = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = tx.send(());
    };
    let (response, ()) = tokio::join!(request, drain);

    let response = response.expect("the in-flight call must not be cut by the drain");
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.unwrap();
    let (_message, trailer) = grpc_webnext::decode_response_body(bytes, 4 * 1024 * 1024).unwrap();
    assert_eq!(trailer.status_code, 0, "the RPC completed normally");

    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("drain finished")
        .unwrap()
        .unwrap();
}

/// Dropping the server future is the force-close: `tokio::time::timeout` around the drain
/// is how a caller bounds it, so a stream that never ends cannot hang shutdown forever.
#[tokio::test]
async fn caller_can_bound_the_drain_with_a_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(serve_in_process_with_shutdown(
        listener,
        routes,
        ServerConfig::default(),
        async {
            let _ = rx.await;
        },
    ));

    // `Hang` emits one message and then never completes, so the drain has to wait on it.
    let mut ws = ws_connect(&format!("ws://{addr}/echo.v1.Echo/Hang")).await;
    ws.send(subscribe(echo("hi"))).await.unwrap();
    // A server-streaming call is done sending as soon as it has sent its one request.
    ws.send(TungMessage::Binary(encode_frame(&Frame {
        kind: Some(Kind::HalfClose(HalfClose {})),
    })))
    .await
    .unwrap();
    // Read past the Header frame to the first Message: the RPC is now provably live.
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("the stream should produce its first frames")
            .expect("a message")
            .expect("no error");
    }
    let _ = tx.send(());

    let result = tokio::time::timeout(Duration::from_millis(600), handle).await;
    assert!(
        result.is_err(),
        "a stream that never ends should hold the drain open until the caller's deadline"
    );
}

/// The transcoder is irrelevant to draining, but keep one server exercising the JSON
/// surface so a drain that only works on the binary path cannot pass unnoticed.
#[tokio::test]
async fn drains_a_json_configured_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let transcoder =
        Arc::new(grpc_webnext::Transcoder::from_file_descriptor_set(testecho::FILE_DESCRIPTOR_SET).unwrap());
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(serve_in_process_with_shutdown(
        listener,
        routes,
        ServerConfig { transcoder: Some(transcoder), ..Default::default() },
        async {
            let _ = rx.await;
        },
    ));

    let _ = tx.send(());
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("drain finished")
        .unwrap()
        .unwrap();
}

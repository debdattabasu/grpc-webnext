//! Single-stream teardown: one WebSocket carries exactly one stream, so once that stream
//! reaches a terminal status the server closes the socket — for **proto and json alike**,
//! and on **any** terminal (success or error). (The h2ts binary path is a separate,
//! multiplexed connection the server never owns, so it is untouched by this.)

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use grpc_webnext::pb::{frame::Kind, Frame, HalfClose, Subscribe};
use grpc_webnext::{decode_frame, encode_frame, Transcoder};
use grpc_webnext::{bind_and_serve_in_process, ServerConfig};
use prost::Message as _;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::EchoRequest;
use testecho::EchoSvc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::service::Routes;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(url: &str, subprotocol: &str) -> Ws {
    let mut req = url.into_client_request().unwrap();
    req.headers_mut().insert("sec-websocket-protocol", subprotocol.parse().unwrap());
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

async fn start_echo() -> String {
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (addr, _handle) = bind_and_serve_in_process(routes, ServerConfig::default()).await.unwrap();
    format!("ws://{addr}")
}

async fn start_echo_json() -> String {
    let transcoder = Arc::new(Transcoder::from_file_descriptor_set(testecho::FILE_DESCRIPTOR_SET).unwrap());
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (addr, _handle) = bind_and_serve_in_process(
        routes,
        ServerConfig { transcoder: Some(transcoder), ..Default::default() },
    )
    .await
    .unwrap();
    format!("ws://{addr}")
}

fn subscribe_proto() -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame {
        kind: Some(Kind::Subscribe(Subscribe {
            method: String::new(),
            headers: vec![],
            timeout_millis: 0,
            initial_payload: EchoRequest { message: "hi".into() }.encode_to_vec().into(),
        })),
    }))
}

fn half_close_proto() -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame { kind: Some(Kind::HalfClose(HalfClose {})) }))
}

fn text(v: serde_json::Value) -> TungMessage {
    TungMessage::Text(v.to_string().into())
}

/// Drain to the `Close`, asserting a terminal status frame arrives first, and return the
/// close code. `terminal` reports whether a decoded frame was a terminal status.
async fn drain_to_close(ws: &mut Ws, mut terminal: impl FnMut(&TungMessage) -> bool) -> CloseCode {
    let mut saw_terminal = false;
    loop {
        match ws.next().await {
            Some(Ok(TungMessage::Close(cf))) => {
                assert!(saw_terminal, "a terminal status frame must arrive before the close");
                return cf.map(|c| c.code).expect("close frame present");
            }
            Some(Ok(msg)) => {
                if terminal(&msg) {
                    saw_terminal = true;
                }
            }
            Some(Err(e)) => panic!("ws errored before a clean close: {e}"),
            None => panic!("stream ended without a Close frame"),
        }
    }
}

#[tokio::test]
async fn proto_ws_closes_after_ok_terminal() {
    let base = start_echo().await;
    let mut ws = ws_connect(&format!("{base}/echo.v1.Echo/Unary"), "grpc-webnext+proto").await;
    ws.send(subscribe_proto()).await.unwrap();
    ws.send(half_close_proto()).await.unwrap();

    let code = drain_to_close(&mut ws, |msg| {
        let TungMessage::Binary(data) = msg else { return false };
        matches!(decode_frame(data).map(|f| f.kind), Ok(Some(Kind::Trailer(_))))
    })
    .await;
    assert_eq!(code, CloseCode::Normal, "normal (1000) close after the RPC terminates");
}

#[tokio::test]
async fn json_ws_closes_after_ok_terminal() {
    let base = start_echo_json().await;
    let mut ws = ws_connect(&format!("{base}/echo.v1.Echo/Unary"), "grpc-webnext+json").await;
    ws.send(text(serde_json::json!({ "message": {"message": "hi"} }))).await.unwrap();
    ws.send(text(serde_json::json!({ "halfClose": true }))).await.unwrap();

    let code = drain_to_close(&mut ws, |msg| {
        let TungMessage::Text(t) = msg else { return false };
        serde_json::from_str::<serde_json::Value>(t).ok().and_then(|v| v.get("status").cloned()).is_some()
    })
    .await;
    assert_eq!(code, CloseCode::Normal, "normal (1000) close after the json RPC terminates");
}

#[tokio::test]
async fn proto_ws_closes_after_error_terminal() {
    // Any terminal closes the socket — including an error status (here an interceptor
    // rejection, the standard place auth now lives).
    fn deny(_req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        Err(tonic::Status::unauthenticated("denied"))
    }
    let routes = Routes::new(EchoServer::with_interceptor(EchoSvc::default(), deny));
    let (addr, _handle) = bind_and_serve_in_process(routes, ServerConfig::default()).await.unwrap();
    let base = format!("ws://{addr}");

    let mut ws = ws_connect(&format!("{base}/echo.v1.Echo/Unary"), "grpc-webnext+proto").await;
    ws.send(subscribe_proto()).await.unwrap();
    ws.send(half_close_proto()).await.unwrap();

    let code = drain_to_close(&mut ws, |msg| {
        let TungMessage::Binary(data) = msg else { return false };
        match decode_frame(data).map(|f| f.kind) {
            Ok(Some(Kind::Trailer(t))) => t.status_code == tonic::Code::Unauthenticated as u32,
            Ok(Some(Kind::Reset(r))) => r.status_code == tonic::Code::Unauthenticated as u32,
            _ => false,
        }
    })
    .await;
    assert_eq!(code, CloseCode::Normal, "the socket closes after an error terminal too");
}

#[tokio::test]
async fn proto_ws_closes_after_size_limit_reset() {
    // A `Reset` is a terminal frame too, so the size-limit rejection tears the socket down
    // exactly like a `Trailer` does. Without this, an oversized message left the connection
    // open with no stream on it — a leak the client can only resolve by timing out.
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (addr, _handle) =
        bind_and_serve_in_process(routes, ServerConfig { max_message_bytes: 1024, ..Default::default() })
            .await
            .unwrap();
    let base = format!("ws://{addr}");

    let mut ws = ws_connect(&format!("{base}/echo.v1.Echo/Unary"), "grpc-webnext+proto").await;
    // Open the stream, then push a message past the limit.
    ws.send(subscribe_proto()).await.unwrap();
    let oversized = EchoRequest { message: "z".repeat(4096) }.encode_to_vec();
    ws.send(TungMessage::Binary(encode_frame(&Frame {
        kind: Some(Kind::Message(grpc_webnext::pb::Message { payload: oversized.into() })),
    })))
    .await
    .unwrap();

    let code = drain_to_close(&mut ws, |msg| {
        let TungMessage::Binary(data) = msg else { return false };
        match decode_frame(data).map(|f| f.kind) {
            Ok(Some(Kind::Reset(r))) => r.status_code == tonic::Code::ResourceExhausted as u32,
            _ => false,
        }
    })
    .await;
    assert_eq!(code, CloseCode::Normal, "the socket closes after a size-limit Reset");
}

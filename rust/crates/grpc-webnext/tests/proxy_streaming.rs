//! End-to-end: grpc-webnext WebSocket streaming -> proxy -> tonic echo (bidi).

use futures::{SinkExt, StreamExt};
use grpc_webnext::pb::{frame::Kind, Frame, HalfClose, Message as WsMessage, Subscribe};
use grpc_webnext::{decode_frame, encode_frame};
use grpc_webnext::{bind_and_serve_proxy, ProxyConfig};
use prost::Message;
use testecho::pb::{EchoRequest, EchoResponse};
use tokio_tungstenite::tungstenite::Message as TungMessage;

fn frame(kind: Kind) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame { kind: Some(kind) }))
}

#[tokio::test]
async fn streaming_round_trip() {
    let upstream_addr = testecho::spawn().await;
    let (proxy_addr, _handle) = bind_and_serve_proxy(ProxyConfig {
        upstream: format!("http://{upstream_addr}").parse().unwrap(),
        max_message_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })
    .await
    .unwrap();

    // Single-stream: the method is the WS URL path.
    let mut ws = connect_proto(&format!("ws://{proxy_addr}/echo.v1.Echo/Stream")).await;

    // Open the bidi stream, sending the first request as initial_payload.
    ws.send(frame(Kind::Subscribe(Subscribe {
        method: "/echo.v1.Echo/Stream".into(),
        headers: vec![],
        timeout_millis: 0,
        initial_payload: EchoRequest { message: "a".into() }.encode_to_vec().into(),
    })))
    .await
    .unwrap();

    // Second request message, then half-close.
    ws.send(frame(Kind::Message(WsMessage {
        payload: EchoRequest { message: "b".into() }.encode_to_vec().into(),
    })))
    .await
    .unwrap();
    ws.send(frame(Kind::HalfClose(HalfClose {})))
        .await
        .unwrap();

    // Collect frames until the Trailer.
    let mut echoed = Vec::new();
    let mut got_header = false;
    let mut status_code = None;

    while let Some(msg) = ws.next().await {
        let TungMessage::Binary(data) = msg.unwrap() else { continue };
        let f = decode_frame(&data).unwrap();
        match f.kind.unwrap() {
            Kind::Header(_) => {
                got_header = true;
            }
            Kind::Message(m) => {
                echoed.push(EchoResponse::decode(&m.payload[..]).unwrap().message);
            }
            Kind::Trailer(t) => {
                status_code = Some(t.status_code);
                break;
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    assert!(got_header, "expected a Header frame before messages");
    assert_eq!(echoed, vec!["a", "b"]);
    assert_eq!(status_code, Some(0));
}

/// Connect a single-stream binary WebSocket (offers the `grpc-webnext+proto` subprotocol,
/// as a real SDK client does).
async fn connect_proto(
    url: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request().unwrap();
    req.headers_mut()
        .insert("sec-websocket-protocol", "grpc-webnext+proto".parse().unwrap());
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

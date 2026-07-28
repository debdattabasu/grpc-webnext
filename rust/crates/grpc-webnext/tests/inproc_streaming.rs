//! Native server: grpc-webnext WebSocket streaming (bidi) into the inner Routes.

use futures::{SinkExt, StreamExt};
use grpc_webnext::pb::{frame::Kind, Frame, HalfClose, Message as WsMessage, Subscribe};
use grpc_webnext::{decode_frame, encode_frame};
use grpc_webnext::{bind_and_serve_in_process, ServerConfig};
use prost::Message;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::{EchoRequest, EchoResponse};
use testecho::EchoSvc;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::service::Routes;

fn frame(kind: Kind) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame { kind: Some(kind) }))
}

#[tokio::test]
async fn streaming_round_trip() {
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    // No codec subprotocol on the test connection -> allow first-frame inference.
    // Single-stream mode takes the method from the URL path.
    let config = ServerConfig { allow_implicit_codec: true, ..Default::default() };
    let (addr, _handle) = bind_and_serve_in_process(routes, config).await.unwrap();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/echo.v1.Echo/Stream"))
        .await
        .unwrap();

    ws.send(frame(Kind::Subscribe(Subscribe {
        method: "/echo.v1.Echo/Stream".into(),
        headers: vec![],
        timeout_millis: 0,
        initial_payload: EchoRequest { message: "a".into() }.encode_to_vec().into(),
    })))
    .await
    .unwrap();
    ws.send(frame(Kind::Message(WsMessage {
        payload: EchoRequest { message: "b".into() }.encode_to_vec().into(),
    })))
    .await
    .unwrap();
    ws.send(frame(Kind::HalfClose(HalfClose {})))
        .await
        .unwrap();

    let mut echoed = Vec::new();
    let mut status = None;
    while let Some(msg) = ws.next().await {
        let TungMessage::Binary(data) = msg.unwrap() else { continue };
        match decode_frame(&data).unwrap().kind.unwrap() {
            Kind::Header(_) => {}
            Kind::Message(m) => echoed.push(EchoResponse::decode(&m.payload[..]).unwrap().message),
            Kind::Trailer(t) => {
                status = Some(t.status_code);
                break;
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    assert_eq!(echoed, vec!["a", "b"]);
    assert_eq!(status, Some(0));
}

//! Client cancellation over WebSocket must cancel the in-process gRPC handler.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use grpc_webnext::pb::{frame::Kind, Frame, HalfClose, Reset, Subscribe};
use grpc_webnext::{decode_frame, encode_frame};
use grpc_webnext::{bind_and_serve_in_process, ServerConfig};
use prost::Message;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::EchoRequest;
use testecho::EchoSvc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::service::Routes;

fn frame(kind: Kind) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame { kind: Some(kind) }))
}

#[tokio::test]
async fn reset_cancels_in_process_handler() {
    let (svc, mut cancel_rx) = EchoSvc::with_cancel();
    let routes = Routes::new(EchoServer::new(svc));
    // Single-stream mode takes the method from the URL path.
    let config = ServerConfig { allow_implicit_codec: true, ..Default::default() };
    let (addr, _handle) = bind_and_serve_in_process(routes, config).await.unwrap();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/echo.v1.Echo/Hang"))
        .await
        .unwrap();

    ws.send(frame(Kind::Subscribe(Subscribe {
        method: "/echo.v1.Echo/Hang".into(),
        headers: vec![],
        timeout_millis: 0,
        initial_payload: EchoRequest { message: "go".into() }.encode_to_vec().into(),
    })))
    .await
    .unwrap();
    ws.send(frame(Kind::HalfClose(HalfClose {})))
        .await
        .unwrap();

    // Wait until the handler has started (first Message frame).
    timeout(Duration::from_secs(10), async {
        loop {
            let msg = ws.next().await.expect("frame").unwrap();
            if let TungMessage::Binary(data) = msg {
                if matches!(decode_frame(&data).unwrap().kind, Some(Kind::Message(_))) {
                    return;
                }
            }
        }
    })
    .await
    .expect("handler never started");

    assert!(cancel_rx.try_recv().is_err(), "cancelled before reset");

    ws.send(frame(Kind::Reset(Reset {
        status_code: 1,
        status_message: "cancelled".into(),
    })))
    .await
    .unwrap();

    assert!(
        timeout(Duration::from_secs(5), cancel_rx.recv()).await.is_ok(),
        "in-process handler was not cancelled after Reset",
    );
}

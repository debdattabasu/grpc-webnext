//! The proxy enforces deadlines locally (dropping the upstream call) while also
//! forwarding grpc-timeout downstream.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use grpc_webnext::pb::{frame::Kind, Frame, HalfClose, Subscribe};
use grpc_webnext::{decode_frame, decode_response_body, encode_frame, encode_request_body};
use grpc_webnext::{bind_and_serve_proxy, ProxyConfig, CT_PROTO};
use prost::Message;
use testecho::pb::{EchoRequest, SleepRequest};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as TungMessage;

const DEADLINE_EXCEEDED: u32 = 4;

fn frame(kind: Kind) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame { kind: Some(kind) }))
}

async fn proxy_over(upstream: std::net::SocketAddr) -> String {
    let (proxy_addr, _handle) = bind_and_serve_proxy(ProxyConfig {
        upstream: format!("http://{upstream}").parse().unwrap(),
        max_message_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })
    .await
    .unwrap();
    format!("http://{proxy_addr}")
}

#[tokio::test]
async fn unary_deadline_returns_deadline_exceeded() {
    let upstream = testecho::spawn().await;
    let base = proxy_over(upstream).await;

    // Upstream sleeps 5s; the client deadline is 200ms -> proxy must give up.
    let body = encode_request_body(&SleepRequest { millis: 5000 }.encode_to_vec()).to_vec();
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Sleep"))
        .header("content-type", CT_PROTO)
        .header("grpc-timeout", "200m")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let raw = bytes::Bytes::from(resp.bytes().await.unwrap().to_vec());
    let (_msg, trailer) = decode_response_body(raw, 4 * 1024 * 1024).unwrap();
    assert_eq!(trailer.status_code, DEADLINE_EXCEEDED);
}

#[tokio::test]
async fn streaming_deadline_trailer_and_upstream_cancel() {
    // Observe that the deadline also cancels the upstream call.
    let (upstream, mut cancel_rx) = testecho::spawn_with_cancel().await;
    let base = proxy_over(upstream).await;
    // Single-stream: the method is the WS URL path.
    let url = format!("{}/echo.v1.Echo/Hang", base.replacen("http", "ws", 1));

    let mut ws = connect_proto(&url).await;
    ws.send(frame(Kind::Subscribe(Subscribe {
        method: "/echo.v1.Echo/Hang".into(),
        headers: vec![],
        timeout_millis: 200,
        initial_payload: EchoRequest { message: "go".into() }.encode_to_vec().into(),
    })))
    .await
    .unwrap();
    ws.send(frame(Kind::HalfClose(HalfClose {})))
        .await
        .unwrap();

    // Expect a Trailer with DEADLINE_EXCEEDED within a few seconds.
    let status = timeout(Duration::from_secs(5), async {
        while let Some(msg) = ws.next().await {
            if let Ok(TungMessage::Binary(data)) = msg {
                if let Some(Kind::Trailer(t)) = decode_frame(&data).unwrap().kind {
                    return t.status_code;
                }
            }
        }
        panic!("stream ended without a Trailer");
    })
    .await
    .expect("no trailer before timeout");
    assert_eq!(status, DEADLINE_EXCEEDED);

    // The upstream Hang call must have been cancelled by the deadline.
    assert!(
        timeout(Duration::from_secs(5), cancel_rx.recv()).await.is_ok(),
        "deadline did not cancel the upstream call",
    );
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

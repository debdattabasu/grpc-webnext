//! Authorization is not a grpc-webnext concern. The request carries its metadata into the
//! router on every transport, so per-RPC auth is a standard tonic interceptor (in-process) /
//! the upstream server or mesh (proxy) — there are **no** grpc-webnext auth hooks (neither
//! per-RPC nor per-connection). This test pins that an interceptor fires on the grpc-webnext
//! Fetch and WebSocket surfaces, exactly as it does on the native/h2ts path.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use grpc_webnext::pb::{frame::Kind, Frame, Subscribe};
use grpc_webnext::{decode_frame, decode_response_body, encode_frame, encode_request_body};
use grpc_webnext::{bind_and_serve_in_process, ServerConfig, CT_PROTO};
use prost::Message as _;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::{EchoRequest, EchoResponse};
use testecho::EchoSvc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::{Code, Status};

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type Resp = tokio_tungstenite::tungstenite::handshake::client::Response;

async fn connect(url: &str, subprotocol: Option<&str>) -> (Ws, Resp) {
    let mut req = url.into_client_request().unwrap();
    if let Some(sp) = subprotocol {
        req.headers_mut().insert("sec-websocket-protocol", sp.parse().unwrap());
    }
    tokio_tungstenite::connect_async(req).await.unwrap()
}

fn subscribe(metadata: &[(&str, &str)]) -> TungMessage {
    let headers = metadata
        .iter()
        .map(|(k, v)| grpc_webnext::pb::Metadatum {
            key: k.to_string(),
            value: Some(grpc_webnext::pb::metadatum::Value::AsciiValue(v.to_string())),
        })
        .collect();
    TungMessage::Binary(encode_frame(&Frame {
        kind: Some(Kind::Subscribe(Subscribe {
            method: "/echo.v1.Echo/Unary".into(),
            headers,
            timeout_millis: 0,
            initial_payload: EchoRequest { message: "hi".into() }.encode_to_vec().into(),
        })),
    }))
}

fn half_close() -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame {
        kind: Some(Kind::HalfClose(grpc_webnext::pb::HalfClose {})),
    }))
}

/// POST a grpc-webnext `+proto` unary request; decode the framed `(message, trailer)`.
async fn fetch_unary(base: &str, authorization: Option<&str>) -> (Bytes, u32, String) {
    let mut req = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", CT_PROTO)
        .body(encode_request_body(&EchoRequest { message: "hi".into() }.encode_to_vec()).to_vec());
    if let Some(a) = authorization {
        req = req.header("authorization", a);
    }
    let resp = req.send().await.unwrap();
    // grpc-webnext always carries the gRPC status in the trailer block, so the HTTP
    // status is 200 even on an auth failure.
    assert_eq!(resp.status(), 200);
    let raw = Bytes::from(resp.bytes().await.unwrap().to_vec());
    let (message, trailer) = decode_response_body(raw, 4 * 1024 * 1024).unwrap();
    (message, trailer.status_code, trailer.status_message)
}

#[tokio::test]
async fn tonic_interceptor_guards_both_grpc_webnext_surfaces() {
    // The standard (and only) per-RPC auth story: a tonic interceptor on the router. The
    // request is rebuilt and dispatched to the router on every transport, so the interceptor
    // sees the `authorization` metadata and its rejection propagates as a normal gRPC status
    // (Fetch trailer / WS `Trailer`) — no grpc-webnext-specific hook. (The h2ts path is real
    // gRPC straight into the router, so it's covered by tonic itself.)
    fn auth(req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        match req.metadata().get("authorization").and_then(|v| v.to_str().ok()) {
            Some("Bearer good") => Ok(req),
            _ => Err(Status::unauthenticated("denied")),
        }
    }
    let routes = tonic::service::Routes::new(EchoServer::with_interceptor(EchoSvc::default(), auth));
    let (addr, _handle) = bind_and_serve_in_process(
        routes,
        ServerConfig { allow_implicit_codec: true, ..Default::default() },
    )
    .await
    .unwrap();
    let http = format!("http://{addr}");
    let ws = format!("ws://{addr}");

    // Fetch: denied without the token, admitted (and echoing) with it.
    let (_m, code, _d) = fetch_unary(&http, None).await;
    assert_eq!(code, Code::Unauthenticated as u32, "fetch denied without token");
    let (msg, code, detail) = fetch_unary(&http, Some("Bearer good")).await;
    assert_eq!(code, 0, "fetch admitted with token: {detail}");
    assert_eq!(EchoResponse::decode(msg).unwrap().message, "hi");

    // WebSocket (custom Frame): the interceptor's rejection arrives as a stream status and
    // the connection stays open (stream-level, per standard HTTP/2 semantics).
    let (mut s, _) = connect(&format!("{ws}/echo.v1.Echo/Unary"), Some("grpc-webnext+proto")).await;
    s.send(subscribe(&[])).await.unwrap();
    s.send(half_close()).await.unwrap();
    let (code, _) = read_until_status(&mut s).await.expect("status");
    assert_eq!(code, Code::Unauthenticated as u32, "ws denied without token");

    let (mut s, _) = connect(&format!("{ws}/echo.v1.Echo/Unary"), Some("grpc-webnext+proto")).await;
    s.send(subscribe(&[("authorization", "Bearer good")])).await.unwrap();
    s.send(half_close()).await.unwrap();
    let (code, _) = read_until_status(&mut s).await.expect("status");
    assert_eq!(code, 0, "ws admitted with token");
}

/// Drain frames until a terminal Trailer/Reset; returns (status_code, message).
async fn read_until_status<S>(ws: &mut S) -> Option<(u32, String)>
where
    S: StreamExt<Item = Result<TungMessage, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(msg) = ws.next().await {
        let TungMessage::Binary(data) = msg.ok()? else { continue };
        match decode_frame(&data).ok()?.kind? {
            Kind::Trailer(t) => return Some((t.status_code, t.status_message)),
            Kind::Reset(r) => return Some((r.status_code, r.status_message)),
            _ => {}
        }
    }
    None
}

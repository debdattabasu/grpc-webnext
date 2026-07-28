//! Native server +json: descriptor-driven JSON<->protobuf transcoding.

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use grpc_webnext::pb::{frame::Kind, Frame, HalfClose, Message as WsMessage, Subscribe};
use grpc_webnext::{decode_frame, encode_frame, Transcoder};
use grpc_webnext::{bind_and_serve_in_process, ServerConfig, CT_JSON};
use prost::Message as _;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::{EchoRequest, EchoResponse};
use testecho::EchoSvc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::service::Routes;

const CT_JSON_STR: &str = CT_JSON;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Connect a WebSocket with an optional `Sec-WebSocket-Protocol` value.
async fn ws_connect(url: &str, subprotocol: Option<&str>) -> Ws {
    let mut req = url.into_client_request().unwrap();
    if let Some(sp) = subprotocol {
        req.headers_mut().insert("sec-websocket-protocol", sp.parse().unwrap());
    }
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

fn bin(kind: Kind) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame { kind: Some(kind) }))
}

async fn start_json_server() -> String {
    start_json_server_impl(false).await
}

/// A server that also allows implicit-codec access to main endpoints (no
/// content-type / blank WS subprotocol / `application/json`).
async fn start_json_server_lax() -> String {
    start_json_server_impl(true).await
}

async fn start_json_server_impl(allow_implicit_codec: bool) -> String {
    let transcoder = Arc::new(Transcoder::from_file_descriptor_set(testecho::FILE_DESCRIPTOR_SET).unwrap());
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (addr, _handle) = bind_and_serve_in_process(
        routes,
        ServerConfig { transcoder: Some(transcoder), allow_implicit_codec, ..Default::default() },
    )
    .await
    .unwrap();
    format!("http://{addr}")
}

/// A `+json` server with a small message-size limit, to exercise the size guard.
async fn start_json_server_small_limit(max_message_bytes: usize) -> String {
    let transcoder = Arc::new(Transcoder::from_file_descriptor_set(testecho::FILE_DESCRIPTOR_SET).unwrap());
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (addr, _handle) = bind_and_serve_in_process(
        routes,
        ServerConfig { transcoder: Some(transcoder), max_message_bytes, ..Default::default() },
    )
    .await
    .unwrap();
    format!("http://{addr}")
}

#[tokio::test]
async fn unary_json_round_trip() {
    let base = start_json_server().await;

    // Native JSON: bare JSON body, gRPC status in HTTP headers.
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", CT_JSON_STR)
        .body(r#"{"message":"hello json"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-type").unwrap(), CT_JSON_STR);
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "0");

    let body = resp.bytes().await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "hello json");
}

#[tokio::test]
async fn unary_json_bad_input_is_invalid_argument() {
    let base = start_json_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", CT_JSON_STR)
        .body(r#"{"message": 12345}"#) // wrong type: message is a string
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers().get("grpc-status").unwrap().to_str().unwrap(),
        (tonic::Code::InvalidArgument as u32).to_string(),
    );
}

fn text(json: serde_json::Value) -> tokio_tungstenite::tungstenite::Message {
    tokio_tungstenite::tungstenite::Message::Text(json.to_string().into())
}

#[tokio::test]
async fn streaming_json_round_trip() {
    // Single-stream JSON (default): the WS URL is the route, so frames carry no
    // `streamId` and no `method`. First inbound frame opens the stream.
    let base = start_json_server().await;
    let url = format!("{}/echo.v1.Echo/Stream", base.replacen("http", "ws", 1));
    let mut ws = ws_connect(&url, Some("grpc-webnext+json")).await;

    ws.send(text(serde_json::json!({ "message": {"message": "a"} }))).await.unwrap();
    ws.send(text(serde_json::json!({ "message": {"message": "b"} }))).await.unwrap();
    ws.send(text(serde_json::json!({ "halfClose": true }))).await.unwrap();

    let mut echoed = Vec::new();
    while let Some(msg) = ws.next().await {
        let TungMessage::Text(t) = msg.unwrap() else { continue };
        let jf: serde_json::Value = serde_json::from_str(&t).unwrap();
        assert!(jf.get("streamId").is_none(), "single-stream frames omit streamId");
        if jf.get("status").is_some() {
            assert_eq!(jf["status"]["code"], 0);
            break;
        } else if let Some(m) = jf.get("message") {
            echoed.push(m["message"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(echoed, vec!["a", "b"]);
}

#[tokio::test]
async fn main_endpoint_rejects_plain_content_types_by_default() {
    // On a main endpoint, plain `application/json` and no content-type are rejected
    // (415) by default — main endpoints need an explicit grpc-webnext content-type.
    let base = start_json_server().await;
    for ct in [Some("application/json"), None] {
        let mut req = reqwest::Client::new()
            .post(format!("{base}/echo.v1.Echo/Unary"))
            .body(r#"{"message":"plain"}"#);
        if let Some(ct) = ct {
            req = req.header("content-type", ct);
        }
        let resp = req.send().await.unwrap();
        assert_eq!(resp.status(), 415, "content-type {ct:?} should be rejected on a main endpoint");
    }
}

#[tokio::test]
async fn implicit_codec_flag_allows_plain_json_on_main_endpoint() {
    // With `allow_implicit_codec`, plain `application/json` / no content-type reach
    // main endpoints and default to JSON (response is `application/json`).
    // With the flag on, plain JSON reaches a main gRPC method path.
    let base = start_json_server_lax().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/FlakyUnary"))
        .header("content-type", "application/json")
        .body(r#"{"message":"plain"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-type").unwrap(), "application/json");
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "0");
    let json: serde_json::Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(json["message"], "plain");
}

#[tokio::test]
async fn implicit_codec_flag_allows_plain_json_on_annotated_rpc_main_path() {
    // With the flag on, plain JSON reaches ANY main path — the annotation just adds a
    // REST alias, it doesn't lock the gRPC method path.
    let base = start_json_server_lax().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Unary")) // Unary is annotated
        .header("content-type", "application/json")
        .body(r#"{"message":"x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(json["message"], "x");
}

#[tokio::test]
async fn fetch_proto_on_rest_url_is_rejected() {
    // Binary on a REST-annotated URL is the wrong surface -> explicit 415.
    let base = start_json_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/echo"))
        .header("content-type", "application/grpc-webnext+proto")
        .body("")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 415);
}

#[tokio::test]
async fn fetch_grpc_webnext_json_transcodes_on_rest_url() {
    // grpc-webnext+json is JSON, so it also works on a REST URL -> transcode.
    let base = start_json_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/echo"))
        .header("content-type", CT_JSON_STR)
        .body(r#"{"message":"sdkjson"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "0");
    let json: serde_json::Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(json["message"], "sdkjson");
}

#[tokio::test]
async fn fetch_unknown_content_type_is_rejected() {
    let base = start_json_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", "text/plain")
        .body("hi")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 415);
}

#[tokio::test]
async fn fetch_json_unknown_method_is_unimplemented() {
    // A JSON call to a nonexistent method fails explicitly (UNIMPLEMENTED), not via a
    // vague transcode error.
    let base = start_json_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Nope"))
        .header("content-type", CT_JSON_STR)
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "12");
}

#[tokio::test]
async fn fetch_json_without_transcoder_is_unimplemented() {
    // No transcoder configured: `+json` returns UNIMPLEMENTED in the grpc-status header
    // (HTTP 200), not an HTTP 501 — the JSON codec always carries status in a header,
    // matching the proxy so the two `+json` surfaces are indistinguishable.
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (addr, _handle) = bind_and_serve_in_process(routes, ServerConfig::default()).await.unwrap();
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/echo.v1.Echo/Unary"))
        .header("content-type", CT_JSON_STR)
        .body(r#"{"message":"x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "12");
}

#[tokio::test]
async fn fetch_json_over_size_is_resource_exhausted() {
    // An oversized `+json` body returns RESOURCE_EXHAUSTED in the header (HTTP 200),
    // not an HTTP 413 — mirroring the proxy.
    let base = start_json_server_small_limit(64).await;
    let big = format!(r#"{{"message":"{}"}}"#, "a".repeat(4096));
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", CT_JSON_STR)
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("grpc-status").unwrap().to_str().unwrap(),
        (tonic::Code::ResourceExhausted as u32).to_string(),
    );
}

#[tokio::test]
async fn ws_json_over_size_is_reset() {
    // A WebSocket message larger than max_message_bytes resets the stream with
    // RESOURCE_EXHAUSTED (the WS analogue of the Fetch size limit; WS previously
    // enforced no limit at all).
    let base = start_json_server_small_limit(128).await;
    let url = format!("{}/echo.v1.Echo/Stream", base.replacen("http", "ws", 1));
    let mut ws = ws_connect(&url, Some("grpc-webnext+json")).await;
    // Open the stream with a small message, then send an oversized one.
    ws.send(text(serde_json::json!({ "message": {"message": "ok"} }))).await.unwrap();
    ws.send(text(serde_json::json!({ "message": {"message": "a".repeat(4096)} }))).await.unwrap();
    let mut code = None;
    while let Some(msg) = ws.next().await {
        let TungMessage::Text(t) = msg.unwrap() else { continue };
        let jf: serde_json::Value = serde_json::from_str(&t).unwrap();
        if let Some(s) = jf.get("status") {
            code = s["code"].as_u64();
            break;
        }
    }
    assert_eq!(code, Some(tonic::Code::ResourceExhausted as u64));
}

#[tokio::test]
async fn implicit_codec_flag_allows_no_content_type_on_main_endpoint() {
    // With the flag, a plain `curl` (no content-type) to a main endpoint works.
    let base = start_json_server_lax().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/FlakyUnary")) // un-annotated
        .body(r#"{"message":"bare"}"#) // note: no content-type header
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-type").unwrap(), "application/json");
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "0");
    let json: serde_json::Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(json["message"], "bare");
}

#[tokio::test]
async fn ws_rejects_missing_codec_subprotocol_by_default() {
    // A WebSocket with no codec subprotocol is rejected (UNIMPLEMENTED -> close 4012).
    let base = start_json_server().await;
    let url = base.replacen("http", "ws", 1);
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let mut closed = None;
    while let Some(msg) = ws.next().await {
        if let TungMessage::Close(cf) = msg.unwrap() {
            closed = cf;
            break;
        }
    }
    let cf = closed.expect("expected a close frame");
    assert_eq!(u16::from(cf.code), 4000 + tonic::Code::Unimplemented as u16);
}

#[tokio::test]
async fn ws_rejection_drains_before_closing() {
    // A rejection must not close the socket while the peer may still be writing.
    //
    // Every client sends the moment the handshake completes — a browser's `onopen` fires as
    // soon as the 101 lands — so when the server rejects, a frame is usually already in
    // flight. Closing a socket that still has unread bytes queued makes the OS answer with an
    // RST, and an RST tells the peer's kernel to discard its receive buffer *including the
    // close frame it had not yet parsed*: the client then sees a bare 1006 and reports
    // UNAVAILABLE instead of the status the close was carrying, which defeats the entire
    // point of the private close code. So the server reads the peer out first, exactly as
    // normal teardown does.
    //
    // Asserting that directly (write, then race the reset) would only reproduce about half
    // the time. This asserts the property underneath it: once the rejection close has been
    // read, the TCP connection is still open. Go: TestWSRejectionDoesNotCloseWhilePeerMayWrite.
    use tokio::io::AsyncReadExt;

    let base = start_json_server().await;
    let url = base.replacen("http", "ws", 1);
    let mut ws = ws_connect(&url, None).await;
    while let Some(msg) = ws.next().await {
        if matches!(msg.unwrap(), TungMessage::Close(_)) {
            break;
        }
    }

    // Stop polling here, so tungstenite never flushes its answering close: with no echo
    // sent, the only thing that can end this read is the server closing the socket — which
    // is the bug. Watching the socket rather than the WebSocket also sidesteps tungstenite
    // reporting a completed close as a clean end-of-stream either way.
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(std::time::Duration::from_millis(300), ws.get_mut().read(&mut byte)).await;
    assert!(
        read.is_err(),
        "after the rejection close the server closed the socket ({read:?}); it must drain the \
         peer first, or an in-flight client frame turns the close into an RST"
    );
}

#[tokio::test]
async fn transcode_get_binds_path_param() {
    // GET /v1/echo/{message} -> Unary, binding `message` from the path.
    let base = start_json_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/v1/echo/hello%20world"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-type").unwrap(), "application/json");
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "0");
    let json: serde_json::Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(json["message"], "hello world");
}

#[tokio::test]
async fn transcode_post_body_wildcard() {
    // POST /v1/echo with `body: "*"` -> the whole JSON body is the request message.
    let base = start_json_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/echo"))
        .header("content-type", "application/json")
        .body(r#"{"message":"posted"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "0");
    let json: serde_json::Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(json["message"], "posted");
}

#[tokio::test]
async fn transcode_get_binds_query_number() {
    // GET /v1/sleep?millis=0 -> Sleep, coercing the uint32 field from a query param.
    let base = start_json_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/v1/sleep?millis=0"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "0");
    let json: serde_json::Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(json["message"], "awake");
}

#[tokio::test]
async fn transcode_unmatched_path_falls_back_to_direct_when_implicit() {
    // With the flag, a plain-JSON path with no REST binding falls back to a direct
    // /pkg.Service/Method call.
    let base = start_json_server_lax().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/FlakyUnary")) // un-annotated -> direct fallback
        .header("content-type", "application/json")
        .body(r#"{"message":"direct"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "0");
    let json: serde_json::Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(json["message"], "direct");
}

#[tokio::test]
async fn ws_subprotocol_pins_codec_to_json() {
    let base = start_json_server().await;
    let url = format!("{}/echo.v1.Echo/Stream", base.replacen("http", "ws", 1));
    let mut ws = ws_connect(&url, Some("grpc-webnext+json")).await;

    // A binary frame first would normally lock to proto, but the subprotocol pinned
    // JSON up front, so it is dropped.
    ws.send(bin(Kind::Message(WsMessage { payload: b"junk".as_slice().into() }))).await.unwrap();
    // Text opens the stream and echoes.
    ws.send(text(serde_json::json!({ "message": {"message": "a"} }))).await.unwrap();
    ws.send(text(serde_json::json!({ "halfClose": true }))).await.unwrap();

    let mut echoed = Vec::new();
    while let Some(msg) = ws.next().await {
        match msg.unwrap() {
            TungMessage::Binary(_) => panic!("binary frame on a json-pinned connection"),
            TungMessage::Text(t) => {
                let jf: serde_json::Value = serde_json::from_str(&t).unwrap();
                if jf.get("status").is_some() {
                    break;
                } else if let Some(m) = jf.get("message") {
                    echoed.push(m["message"].as_str().unwrap().to_string());
                }
            }
            _ => {}
        }
    }
    assert_eq!(echoed, vec!["a"]);
}

#[tokio::test]
async fn ws_locks_to_text_on_first_text_frame() {
    // Blank subprotocol -> codec inferred from the first frame (implicit/lax).
    let base = start_json_server_lax().await;
    let url = format!("{}/echo.v1.Echo/Stream", base.replacen("http", "ws", 1));
    let mut ws = ws_connect(&url, None).await;

    // First frame text -> locked to JSON; opens the single stream.
    ws.send(text(serde_json::json!({ "message": {"message": "a"} }))).await.unwrap();
    // A later binary frame must be dropped (locked to text).
    ws.send(bin(Kind::Message(WsMessage { payload: b"junk".as_slice().into() }))).await.unwrap();
    ws.send(text(serde_json::json!({ "halfClose": true }))).await.unwrap();

    let mut echoed = Vec::new();
    while let Some(msg) = ws.next().await {
        match msg.unwrap() {
            TungMessage::Binary(_) => panic!("binary frame on a text-locked connection"),
            TungMessage::Text(t) => {
                let jf: serde_json::Value = serde_json::from_str(&t).unwrap();
                if jf.get("status").is_some() {
                    break;
                } else if let Some(m) = jf.get("message") {
                    echoed.push(m["message"].as_str().unwrap().to_string());
                }
            }
            _ => {}
        }
    }
    assert_eq!(echoed, vec!["a"]);
}

#[tokio::test]
async fn ws_locks_to_binary_on_first_binary_frame() {
    // Blank subprotocol -> codec inferred from the first frame (implicit/lax).
    let base = start_json_server_lax().await;
    let url = format!("{}/echo.v1.Echo/Stream", base.replacen("http", "ws", 1));
    let mut ws = ws_connect(&url, None).await;

    // First frame binary -> locked to protobuf; opens the stream (method from URL).
    ws.send(bin(Kind::Subscribe(Subscribe {
        method: String::new(), // ignored in single-stream mode; taken from the URL
        headers: vec![],
        timeout_millis: 0,
        initial_payload: EchoRequest { message: "a".into() }.encode_to_vec().into(),
    })))
    .await
    .unwrap();
    // A later text frame must be dropped (locked to binary).
    ws.send(text(serde_json::json!({ "message": {"message": "b"} }))).await.unwrap();
    ws.send(bin(Kind::HalfClose(HalfClose {}))).await.unwrap();

    let mut echoed = Vec::new();
    while let Some(msg) = ws.next().await {
        match msg.unwrap() {
            TungMessage::Text(_) => panic!("text frame on a binary-locked connection"),
            TungMessage::Binary(data) => match decode_frame(&data).unwrap().kind.unwrap() {
                Kind::Header(_) => {}
                Kind::Message(m) => echoed.push(EchoResponse::decode(&m.payload[..]).unwrap().message),
                Kind::Trailer(_) => break,
                other => panic!("unexpected: {other:?}"),
            },
            _ => {}
        }
    }
    assert_eq!(echoed, vec!["a"]);
}

#[tokio::test]
async fn ws_annotation_server_stream() {
    // A streaming method reached via its annotation URL: GET /v1/repeat/{message}?count=N.
    // Annotation routes accept a blank subprotocol and lock to text; the request is
    // built entirely from the URL (path `message`, query `count`).
    let base = start_json_server().await;
    let url = format!("{}/v1/repeat/hi?count=3", base.replacen("http", "ws", 1));
    let mut ws = ws_connect(&url, None).await;

    // An (empty) open frame starts the stream; the server injects the request from the URL.
    ws.send(text(serde_json::json!({}))).await.unwrap();

    let mut got = Vec::new();
    while let Some(msg) = ws.next().await {
        let TungMessage::Text(t) = msg.unwrap() else { continue };
        let jf: serde_json::Value = serde_json::from_str(&t).unwrap();
        assert!(jf.get("streamId").is_none(), "annotation routes are single-stream");
        if jf.get("status").is_some() {
            assert_eq!(jf["status"]["code"], 0);
            break;
        } else if let Some(m) = jf.get("message") {
            got.push(m["message"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(got, vec!["hi", "hi", "hi"]);
}

#[tokio::test]
async fn ws_annotation_bidi_with_body() {
    // A bidi method reached via `post: "/v1/stream" body:"*"`: each text frame's body
    // is a request message.
    let base = start_json_server().await;
    let url = format!("{}/v1/chat", base.replacen("http", "ws", 1));
    let mut ws = ws_connect(&url, Some("application/json")).await;

    ws.send(text(serde_json::json!({ "message": {"message": "a"} }))).await.unwrap();
    ws.send(text(serde_json::json!({ "message": {"message": "b"} }))).await.unwrap();
    ws.send(text(serde_json::json!({ "halfClose": true }))).await.unwrap();

    let mut got = Vec::new();
    while let Some(msg) = ws.next().await {
        let TungMessage::Text(t) = msg.unwrap() else { continue };
        let jf: serde_json::Value = serde_json::from_str(&t).unwrap();
        if jf.get("status").is_some() {
            break;
        } else if let Some(m) = jf.get("message") {
            got.push(m["message"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(got, vec!["a", "b"]);
}

#[tokio::test]
async fn ws_annotation_rejects_proto_subprotocol() {
    // Annotation routes are single-stream JSON: a binary (proto) subprotocol is the
    // wrong surface -> close 4009 (FAILED_PRECONDITION).
    let base = start_json_server().await;
    let url = format!("{}/v1/repeat/hi?count=1", base.replacen("http", "ws", 1));
    let mut ws = ws_connect(&url, Some("grpc-webnext+proto")).await;
    let mut closed = None;
    while let Some(msg) = ws.next().await {
        if let TungMessage::Close(cf) = msg.unwrap() {
            closed = cf;
            break;
        }
    }
    let cf = closed.expect("expected a close frame");
    assert_eq!(u16::from(cf.code), 4000 + tonic::Code::FailedPrecondition as u16);
}

#[tokio::test]
async fn ws_annotation_accepts_grpc_webnext_json() {
    // grpc-webnext+json is JSON, so it is accepted on a REST route (like blank /
    // application/json).
    let base = start_json_server().await;
    let url = format!("{}/v1/repeat/hi?count=2", base.replacen("http", "ws", 1));
    let mut ws = ws_connect(&url, Some("grpc-webnext+json")).await;
    ws.send(text(serde_json::json!({}))).await.unwrap();

    let mut got = Vec::new();
    while let Some(msg) = ws.next().await {
        let TungMessage::Text(t) = msg.unwrap() else { continue };
        let jf: serde_json::Value = serde_json::from_str(&t).unwrap();
        if jf.get("status").is_some() {
            break;
        } else if let Some(m) = jf.get("message") {
            got.push(m["message"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(got, vec!["hi", "hi"]);
}

//! WebSocket streaming path (the custom `Frame` protocol), shared by both surfaces.
//!
//! One WebSocket carries exactly ONE gRPC stream. The first frame is a `Subscribe`
//! (method from the WS URL); request messages are fed in from later WS frames and the
//! response is de-framed back into WS frames. Exactly one `Frame` per WebSocket message.
//! (The binary default runs real HTTP/2 over h2ts instead; see `crate::h2ts`.)

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use crate::json_frame::{
    decode_json_frame, encode_json_frame, json_frame_to_proto, json_open_to_subscribe,
    proto_frame_to_json,
};
use crate::pb::{frame::Kind, Frame, Header, Message as WsMessage, Reset, Trailer};
use crate::{decode_frame, encode_frame, grpc_frame, metadata, Deframer, WsBinding};
use http::uri::PathAndQuery;
use http::{HeaderMap, Request};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame as BodyFrame;
use hyper_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use hyper_tungstenite::tungstenite::protocol::CloseFrame;
use hyper_tungstenite::tungstenite::Message as TungMessage;
use hyper_tungstenite::HyperWebsocket;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body as TonicBody;
use tonic::metadata::MetadataMap;
use tonic::{Code, Status};

use crate::{Runtime, CT_GRPC, DEADLINE_GRACE};

/// How long to wait for the peer's answering close after we send ours. The close
/// handshake is politeness, not correctness — the terminal status was already delivered
/// in a `Trailer`/`Reset` frame — so it is bounded rather than awaited indefinitely.
const CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

struct StreamState {
    /// Feeds request messages into the gRPC request body; `None` after half-close.
    req_tx: Option<mpsc::Sender<Bytes>>,
    task: tokio::task::JoinHandle<()>,
}

/// Per-connection WebSocket parameters resolved from the handshake.
pub(crate) struct WsParams {
    /// `Some(true)` = JSON/text, `Some(false)` = proto/binary, `None` = infer from the
    /// first frame (only reachable with implicit codecs).
    pub codec: Option<bool>,
    /// The gRPC method from the WS URL path (the one stream's route).
    pub method: Option<String>,
    /// Annotation route: the WS URL matched a `google.api.http` binding.
    pub annotation: Option<Arc<WsBinding>>,
}

pub(crate) async fn serve(rt: Runtime, websocket: HyperWebsocket, reject: Option<Status>, params: WsParams) {
    let keepalive = rt.cfg.ws_keepalive;
    // Max silence tolerated before the peer is declared dead: one ping interval to provoke
    // a pong plus the ack timeout. `None` when keepalive is off.
    let max_silence = keepalive.map(|iv| iv + rt.cfg.ws_keepalive_timeout);
    let ws = match websocket.await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::debug!("ws upgrade failed: {e}");
            return;
        }
    };
    let (mut ws_sink, mut ws_stream) = ws.split();

    // Connection gate rejected this handshake: hand the client a readable gRPC status via a
    // private close code, then drop the socket without creating a stream.
    if let Some(status) = reject {
        let _ = ws_sink.send(close_for_status(&status)).await;
        // Read the peer out before closing, exactly as the main loop does. A client opens
        // and sends immediately — a browser's `onopen` fires the moment the 101 lands — so
        // by now there is very likely a frame in flight. Closing a socket that still has
        // unread bytes queued makes the OS answer with an RST, and an RST tells the peer's
        // kernel to discard its receive buffer *including the close frame it had not yet
        // parsed*. The client would then see a bare 1006 and report UNAVAILABLE instead of
        // the status this close was carrying, which is the whole point of the private code.
        let _ = tokio::time::timeout(CLOSE_GRACE, async {
            while let Some(Ok(msg)) = ws_stream.next().await {
                if matches!(msg, TungMessage::Close(_)) {
                    break;
                }
            }
        })
        .await;
        let _ = ws_sink.close().await;
        return;
    }

    // Ready-to-send WebSocket messages: each frame encodes in the connection codec before
    // sending, which is what lets a connection whose codec is inferred from the first frame work.
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<TungMessage>(64);
    // Signals the read loop that a close frame went out, so it can bound how long it
    // waits for the peer's answering close instead of waiting forever.
    let (closed_tx, mut closed_rx) = mpsc::channel::<()>(1);

    let writer = tokio::spawn(async move {
        let mut ping = keepalive.map(keepalive_interval);
        loop {
            tokio::select! {
                msg = outbound_rx.recv() => {
                    let Some(msg) = msg else { break };
                    let closing = matches!(msg, TungMessage::Close(_));
                    if ws_sink.send(msg).await.is_err() { break; }
                    // Nothing follows a close frame.
                    if closing { let _ = closed_tx.try_send(()); break; }
                }
                _ = next_tick(ping.as_mut()) => {
                    if ws_sink.send(TungMessage::Ping(Bytes::new())).await.is_err() { break; }
                }
            }
        }
    });

    let annotation = params.annotation;
    let method_url = params.method.unwrap_or_default();
    // Connection codec (true = JSON/text, false = proto/binary). A codec subprotocol pins
    // it up front; otherwise (implicit) it locks to the first frame's type.
    let mut codec: Option<bool> = params.codec;
    let mut opened = false;
    // The single stream on this connection.
    let mut stream: Option<StreamState> = None;
    let mut deadline = max_silence.map(|d| tokio::time::Instant::now() + d);
    // Set once the server starts draining, so the branch below fires exactly once.
    let mut draining = false;
    // Armed once we have sent a close frame; bounds the wait for the peer's answer.
    let mut close_deadline: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => {
                tracing::debug!("ws keepalive: no pong within timeout; dropping connection");
                break;
            }
            // We closed the socket and the peer never answered. A polite client echoes the
            // close and the `Close` arm below ends the loop first; this bounds the rude and
            // the dead ones, which would otherwise hold the connection — and, during a
            // drain, the whole shutdown — open indefinitely.
            _ = sleep_until(close_deadline) => {
                tracing::debug!("ws close not answered within {CLOSE_GRACE:?}; dropping connection");
                break;
            }
            Some(()) = closed_rx.recv(), if close_deadline.is_none() => {
                close_deadline = Some(tokio::time::Instant::now() + CLOSE_GRACE);
            }
            // Graceful shutdown. This socket carries exactly one stream, so "stop
            // accepting new streams" means: close it if it has not opened one yet, and
            // otherwise leave it alone — a live stream runs to its terminal frame, which
            // closes the socket anyway (see "Single-stream teardown" in PROTOCOL.md).
            _ = rt.drain.wait(), if !draining => {
                draining = true;
                if stream.is_none() {
                    let _ = outbound_tx.send(close_going_away()).await;
                    break;
                }
            }
            incoming = ws_stream.next() => {
                let Some(incoming) = incoming else { break };
                let msg = match incoming { Ok(m) => m, Err(_) => break };
                if let Some(d) = max_silence {
                    deadline = Some(tokio::time::Instant::now() + d);
                }
                let decoded = match msg {
                    TungMessage::Binary(data) => {
                        if *codec.get_or_insert(false) { continue } // locked to JSON/text
                        decode_binary(&data, &method_url, &mut opened).map(|f| (f, false))
                    }
                    TungMessage::Text(text) => {
                        if !*codec.get_or_insert(true) { continue } // locked to proto/binary
                        decode_text(&text, &method_url, &mut opened).map(|f| (f, true))
                    }
                    TungMessage::Close(_) => break,
                    _ => continue,
                };
                let Some((frame, json)) = decoded else { continue };
                handle_frame(&rt, frame, json, &annotation, &outbound_tx, &mut stream).await;
            }
        }
    }

    if let Some(state) = stream {
        state.task.abort();
    }
    drop(outbound_tx);
    let _ = writer.await;
}

/// Process one inbound `Frame`: open the stream on `Subscribe`, feed request messages,
/// half-close, or reset. A rejected `Subscribe` is answered with a `Reset` on the same
/// (still-open) connection — per-RPC authorization lives in the router (a tonic
/// interceptor), so there is no auth-driven connection teardown here.
async fn handle_frame(
    rt: &Runtime,
    frame: Frame,
    json: bool,
    annotation: &Option<Arc<WsBinding>>,
    outbound_tx: &mpsc::Sender<TungMessage>,
    stream: &mut Option<StreamState>,
) {
    match frame.kind {
        Some(Kind::Subscribe(sub)) => {
            if stream.is_some() {
                // Rejecting the duplicate open, not the live stream — which is still
                // running and will deliver its own terminal frame and close. This is the
                // one `Reset` that is *not* a stream's terminal frame, so it does not
                // tear the connection down.
                send_reset(outbound_tx, json, Code::InvalidArgument, "stream already open").await;
                return;
            }
            // An opening frame may carry the first message inline; hold it to the size limit.
            if sub.initial_payload.len() > rt.cfg.max_message_bytes {
                reject_stream(outbound_tx, json, Code::ResourceExhausted, "request message exceeds size limit").await;
                return;
            }
            let path: PathAndQuery = match sub.method.parse() {
                Ok(p) => p,
                Err(_) => {
                    reject_stream(outbound_tx, json, Code::InvalidArgument, "invalid method").await;
                    return;
                }
            };
            let md = metadata::metadata_vec_to_metadata(&sub.headers);

            let (req_tx, req_rx) = mpsc::channel::<Bytes>(16);
            if !sub.initial_payload.is_empty() {
                let _ = req_tx.send(sub.initial_payload).await;
            }
            // A no-body annotation route (GET-style server-stream) builds its one request
            // from the URL: inject one empty payload and half-close.
            let mut req_tx_state = Some(req_tx.clone());
            if let Some(ann) = annotation {
                if !ann.has_body() {
                    let _ = req_tx.send(Bytes::new()).await;
                    req_tx_state = None;
                }
            }

            let headers = md.into_headers();
            let timeout = metadata::grpc_timeout_from_millis(sub.timeout_millis);
            let task = tokio::spawn(run_stream(
                rt.clone(), path, headers, timeout, req_rx, outbound_tx.clone(), json, annotation.clone(),
            ));
            *stream = Some(StreamState { req_tx: req_tx_state, task });
        }
        Some(Kind::Message(msg)) => {
            if msg.payload.len() > rt.cfg.max_message_bytes {
                if let Some(state) = stream.take() {
                    state.task.abort();
                }
                reject_stream(outbound_tx, json, Code::ResourceExhausted, "request message exceeds size limit").await;
                return;
            }
            if let Some(state) = stream {
                if let Some(tx) = &state.req_tx {
                    let _ = tx.send(msg.payload).await;
                }
            }
        }
        Some(Kind::HalfClose(_)) => {
            if let Some(state) = stream {
                state.req_tx = None;
            }
        }
        Some(Kind::Reset(_)) => {
            if let Some(state) = stream.take() {
                state.task.abort();
            }
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_stream(
    rt: Runtime,
    path: PathAndQuery,
    headers: HeaderMap,
    timeout: Option<std::time::Duration>,
    req_rx: mpsc::Receiver<Bytes>,
    outbound_tx: mpsc::Sender<TungMessage>,
    json: bool,
    annotation: Option<Arc<WsBinding>>,
) {
    let method_path = path.path().to_string();

    // JSON needs a transcoder; resolve it in this per-stream task so a first reflection
    // round-trip never blocks the connection read loop. A capability gap (no schema /
    // unknown method) rejects the stream with a `Reset` before the RPC starts.
    let transcoder = if json {
        match rt.schema.transcoder(&method_path).await {
            Ok(tc) => Some(tc),
            Err(status) => {
                send_reset(&outbound_tx, json, status.code(), status.message()).await;
                let _ = outbound_tx.send(close_normal()).await;
                return;
            }
        }
    } else {
        None
    };

    // Request body: a live stream of gRPC-framed messages. An annotation route builds each
    // message from the URL + body; a plain `+json` stream transcodes each JSON message;
    // proto passes through.
    let req_tc = transcoder.clone();
    let req_ann = annotation.clone();
    let req_path = method_path.clone();
    let body_stream = ReceiverStream::new(req_rx).map(move |payload| {
        let proto = if let Some(ann) = &req_ann {
            ann.build_message(&payload).map(Bytes::from).unwrap_or_default()
        } else if json {
            req_tc.as_ref().unwrap().request_json_to_proto(&req_path, &payload).map(Bytes::from).unwrap_or_default()
        } else {
            payload
        };
        Ok::<_, Infallible>(BodyFrame::data(grpc_frame(&proto)))
    });
    let req_body = TonicBody::new(StreamBody::new(body_stream));

    let mut builder = Request::builder().method(http::Method::POST).uri(path);
    for (name, value) in headers.iter() {
        builder = builder.header(name.clone(), value.clone());
    }
    builder = builder.header(http::header::CONTENT_TYPE, CT_GRPC).header("te", "trailers");
    if let Some(d) = timeout {
        builder = builder.header("grpc-timeout", metadata::format_grpc_timeout(d + DEADLINE_GRACE));
    }
    let request = builder.body(req_body).expect("valid request");

    // Dispatch through the backend and pump responses to WS frames. On deadline expiry the
    // pump future is dropped, cancelling the call.
    let pump = async {
        let resp = match rt.backend.call(request).await {
            Ok(r) => r,
            Err(e) => {
                send_reset(&outbound_tx, json, Code::Unavailable, &format!("upstream: {e}")).await;
                return;
            }
        };
        let (parts, mut body) = resp.into_parts();

        let header = Header {
            headers: metadata::metadata_to_vec(&MetadataMap::from_headers(parts.headers.clone())),
        };
        let _ = outbound_tx.send(to_tung(&Frame { kind: Some(Kind::Header(header)) }, json)).await;

        let mut deframer = Deframer::new();
        let mut trailers = HeaderMap::new();
        while let Some(frame) = body.frame().await {
            let frame = match frame {
                Ok(f) => f,
                Err(_) => break,
            };
            if frame.is_data() {
                let data = frame.into_data().unwrap_or_default();
                deframer.push(&data);
                while let Some(msg) = deframer.next_message() {
                    let payload = if json {
                        // An annotation route may name a single response field.
                        let response_body = annotation.as_ref().map(|a| a.response_body()).unwrap_or("");
                        let tc = transcoder.as_ref().unwrap();
                        match tc.response_proto_to_json_body(&method_path, &msg, response_body) {
                            Ok(j) => j.into(),
                            Err(e) => {
                                send_reset(&outbound_tx, json, Code::Internal, &format!("json encode: {e}")).await;
                                return;
                            }
                        }
                    } else {
                        msg
                    };
                    let out = Frame { kind: Some(Kind::Message(WsMessage { payload })) };
                    if outbound_tx.send(to_tung(&out, json)).await.is_err() {
                        return;
                    }
                }
            } else if frame.is_trailers() {
                if let Ok(t) = frame.into_trailers() {
                    trailers = t;
                }
            }
        }

        let (status_code, status_message) = metadata::read_status(&trailers, &parts.headers);
        let trailer = Trailer {
            status_code,
            status_message,
            trailers: metadata::metadata_to_vec(&MetadataMap::from_headers(trailers)),
        };
        let _ = outbound_tx.send(to_tung(&Frame { kind: Some(Kind::Trailer(trailer)) }, json)).await;
    };

    match timeout {
        Some(d) => {
            if tokio::time::timeout(d, pump).await.is_err() {
                let trailer = Trailer {
                    status_code: Code::DeadlineExceeded as u32,
                    status_message: "deadline exceeded".into(),
                    trailers: vec![],
                };
                let _ = outbound_tx.send(to_tung(&Frame { kind: Some(Kind::Trailer(trailer)) }, json)).await;
            }
        }
        None => pump.await,
    }

    // One WebSocket carries exactly one stream: now that the RPC has terminated (a `Trailer`
    // or `Reset` was delivered above), close the socket — proto and json alike. This is the
    // stream-level teardown for a single-stream connection. (The h2ts binary path is a
    // separate, multiplexed connection we never own, so it is untouched by this.)
    let _ = outbound_tx.send(close_normal()).await;
}

/// Reject the stream: a terminal `Reset` followed by the normal close. A WebSocket carries
/// exactly one stream, so once that stream has ended — whether by `Trailer` or by `Reset` —
/// the socket has served its purpose (see PROTOCOL.md "Single-stream teardown").
async fn reject_stream(outbound_tx: &mpsc::Sender<TungMessage>, json: bool, code: Code, message: &str) {
    send_reset(outbound_tx, json, code, message).await;
    let _ = outbound_tx.send(close_normal()).await;
}

async fn send_reset(outbound_tx: &mpsc::Sender<TungMessage>, json: bool, code: Code, message: &str) {
    let frame = Frame {
        kind: Some(Kind::Reset(Reset {
            status_code: code as u32,
            status_message: message.to_string(),
        })),
    };
    let _ = outbound_tx.send(to_tung(&frame, json)).await;
}

/// A `1001 Going Away` close: the server is draining and this socket had not opened its
/// stream yet. It carries no gRPC status — no RPC ever started — and a client reading it
/// surfaces `UNAVAILABLE`, which is the retry-elsewhere signal a drain wants.
fn close_going_away() -> TungMessage {
    TungMessage::Close(Some(CloseFrame {
        code: CloseCode::Away,
        reason: "server draining".into(),
    }))
}

/// A normal WebSocket close (code `1000`): the single stream on this connection has
/// terminated (its status already delivered as a `Trailer`/`Reset`), so the socket is done.
fn close_normal() -> TungMessage {
    TungMessage::Close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: String::new().into(),
    }))
}

/// A close frame that carries a gRPC status to browser JS: private close code `4000 + code`
/// (gRPC codes 0..=16 ⇒ 4000..=4016), message as the reason (capped at 123 bytes).
fn close_for_status(status: &Status) -> TungMessage {
    let code = 4000u16 + status.code() as u16;
    TungMessage::Close(Some(CloseFrame {
        code: CloseCode::from(code),
        reason: truncate_utf8(status.message(), 123).to_string().into(),
    }))
}

/// Truncate to at most `max` bytes on a char boundary.
fn truncate_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

// --- Shared frame codec + keepalive helpers ---------------------------------

/// Decode an inbound binary (proto) WebSocket frame into an internal `Frame`. The first
/// frame must be a `Subscribe` (its method comes from the WS URL); a non-`Subscribe` before
/// the stream opens is dropped.
pub fn decode_binary(data: &[u8], method_url: &str, opened: &mut bool) -> Option<Frame> {
    let mut frame = decode_frame(data).ok()?;
    match frame.kind.as_mut()? {
        Kind::Subscribe(s) => {
            s.method = method_url.to_string();
            *opened = true;
        }
        _ if !*opened => return None,
        _ => {}
    }
    Some(frame)
}

/// Decode an inbound text (JSON) WebSocket frame. The first frame opens the one stream
/// (method from the URL); later frames are messages/half-close/reset.
pub fn decode_text(text: &str, method_url: &str, opened: &mut bool) -> Option<Frame> {
    let jf = decode_json_frame(text).ok()?;
    if !*opened {
        *opened = true;
        let sub = json_open_to_subscribe(jf, method_url.to_string());
        Some(Frame { kind: Some(Kind::Subscribe(sub)) })
    } else {
        Some(json_frame_to_proto(jf))
    }
}

/// Encode an outbound internal `Frame` as a WebSocket message in the connection codec.
pub fn to_tung(frame: &Frame, json: bool) -> TungMessage {
    if json {
        if let Some(jf) = proto_frame_to_json(frame) {
            return TungMessage::Text(encode_json_frame(&jf).into());
        }
    }
    TungMessage::Binary(encode_frame(frame))
}

/// A keepalive ticker whose first tick is one full period out and that skips missed ticks.
pub fn keepalive_interval(period: std::time::Duration) -> tokio::time::Interval {
    let mut i = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    i
}

/// Await the next keepalive tick, or never resolve when keepalive is disabled.
pub async fn next_tick(interval: Option<&mut tokio::time::Interval>) {
    match interval {
        Some(i) => {
            i.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// Resolve at `deadline`, or never when it is `None` (keepalive off).
pub async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

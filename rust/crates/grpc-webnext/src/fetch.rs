//! Fetch (HTTP) request handling + routing, shared by both surfaces.
//!
//! `handle` is the single entry every connection funnels through: it routes to the admin
//! endpoint, the WebSocket upgrade, native gRPC passthrough, `+proto` unary, or `+json` /
//! REST — dispatching the resulting gRPC call through the [`crate::Backend`].

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use crate::pb::Trailer;
use crate::{
    deframe_all, encode_response_body, encode_trailer_block, grpc_frame, metadata, Transcoder,
    EMPTY_MESSAGE_BLOCK, LEN_PREFIX,
};
use http::uri::PathAndQuery;
use http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame as BodyFrame, Incoming};
use tonic::body::Body as TonicBody;
use tonic::metadata::MetadataMap;
use tonic::Code;

use tonic::Status;

use crate::ws::{self, WsParams};
use crate::{
    ws_subprotocols, BoxError, ResBody, Runtime, CT_GRPC, CT_JSON, CT_PROTO,
    DEADLINE_GRACE, WS_SUBPROTOCOL, WS_SUBPROTOCOL_JSON, WS_SUBPROTOCOL_PROTO,
};

/// Route one inbound HTTP request.
pub(crate) async fn handle(rt: &Runtime, req: Request<Incoming>) -> Result<Response<ResBody>, Infallible> {
    // Management endpoint: force a reflection reload. Matched before routing so it can't
    // collide with a gRPC method path. (Only the proxy sets `admin_reload_path`.)
    if let Some(admin) = &rt.cfg.admin_reload_path {
        if req.uri().path() == admin {
            return Ok(handle_admin_reload(rt, req.method()).await);
        }
    }

    // WebSocket streaming path.
    if hyper_tungstenite::is_upgrade_request(&req) {
        return Ok(handle_ws_upgrade(rt, req).await);
    }

    let content_type = req
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if content_type == CT_PROTO {
        // Binary is the SDK contract on main paths. A REST-annotated URL is JSON-only, so
        // binary there is the wrong surface — reject explicitly.
        if let Ok(tc) = rt.schema.transcoder_any().await {
            if tc.match_ws(req.uri().path(), req.uri().query()).is_some() {
                return Ok(text_response(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "REST-annotated endpoints are JSON-only; use application/json or application/grpc-webnext+json",
                ));
            }
        }
        Ok(unary_proto(rt, req).await)
    } else if content_type == CT_JSON {
        Ok(json_fetch(rt, req, true).await)
    } else if content_type.starts_with(CT_GRPC) {
        Ok(passthrough(rt, req).await)
    } else if content_type == "application/json" || content_type.is_empty() {
        // Plain JSON reaches REST endpoints always and main paths only with the implicit
        // flag; `json_fetch` enforces that (and answers UNIMPLEMENTED with no transcoder).
        Ok(json_fetch(rt, req, false).await)
    } else {
        Ok(text_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported content-type: {content_type}"),
        ))
    }
}

/// Force an immediate reflection reload (proxy management hook). `POST` only.
async fn handle_admin_reload(rt: &Runtime, method: &http::Method) -> Response<ResBody> {
    if method != http::Method::POST {
        return text_response(StatusCode::METHOD_NOT_ALLOWED, "use POST to force a reflection reload");
    }
    match rt.schema.reload().await {
        Ok(()) => text_response(StatusCode::OK, "reflection reloaded"),
        Err(status) => {
            let http = if status.code() == Code::FailedPrecondition {
                StatusCode::CONFLICT
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            text_response(http, format!("reflection reload failed: {}", status.message()))
        }
    }
}

/// Forward a native gRPC request through the backend unchanged (same-port passthrough).
async fn passthrough(rt: &Runtime, req: Request<Incoming>) -> Response<ResBody> {
    let (parts, body) = req.into_parts();
    let req = Request::from_parts(parts, TonicBody::new(body));
    match rt.backend.call(req).await {
        Ok(resp) => resp,
        Err(e) => text_response(StatusCode::BAD_GATEWAY, format!("upstream: {e}")),
    }
}

/// The WebSocket handshake: resolve codec + REST annotation, run the connection
/// gate, then upgrade and hand off to `ws::serve`.
async fn handle_ws_upgrade(rt: &Runtime, mut req: Request<Incoming>) -> Response<ResBody> {
    let offered = ws_subprotocols(req.headers());

    // Binary path over real HTTP/2: the client offered the `h2ts` subprotocol, so it speaks
    // real gRPC over the tunnel — not the custom Frame protocol. Serve it directly (tonic
    // in-process) or byte-pump it to the upstream (proxy); no codec negotiation applies.
    if offered.iter().any(|p| p == h2ts_server::DEFAULT_SUBPROTOCOL) {
        return crate::h2ts::serve(rt, &mut req);
    }

    let has = |s: &str| offered.iter().any(|p| p == s);
    // `explicit` = a grpc-webnext codec subprotocol was offered (main-endpoint OK).
    let (codec, explicit) = if has(WS_SUBPROTOCOL_PROTO) {
        (Some(false), true)
    } else if has(WS_SUBPROTOCOL_JSON) {
        (Some(true), true)
    } else if has("application/json") {
        (Some(true), false)
    } else {
        (None, false)
    };

    // Annotation route: the WS URL matches a `google.api.http` binding.
    let ws_annotation = match rt.schema.transcoder_any().await {
        Ok(tc) => tc.match_ws(req.uri().path(), req.uri().query()).map(Arc::new),
        Err(_) => None,
    };

    // Surface gating (see PROTOCOL.md): REST routes are JSON-only; main routes need a
    // grpc-webnext codec subprotocol unless implicit codecs are allowed.
    let codec_reject = if ws_annotation.is_some() {
        (codec == Some(false)).then(|| {
            Status::failed_precondition(
                "REST WebSocket routes are JSON: use a blank, application/json, or grpc-webnext+json subprotocol",
            )
        })
    } else if explicit {
        None
    } else {
        (!rt.cfg.allow_implicit_codec).then(|| {
            Status::unimplemented("this websocket requires a grpc-webnext+proto or grpc-webnext+json subprotocol")
        })
    };
    // Pre-RPC rejection: only codec/surface gating (auth is per-RPC at the router, not a
    // handshake gate). A rejected handshake closes with a `4000 + code` close frame.
    let reject = codec_reject;

    let echo = [WS_SUBPROTOCOL_PROTO, WS_SUBPROTOCOL_JSON, WS_SUBPROTOCOL, "application/json"]
        .into_iter()
        .find(|&p| has(p));

    let params = match &ws_annotation {
        Some(ann) => WsParams {
            codec: Some(true),
            method: Some(ann.grpc_method().to_string()),
            annotation: ws_annotation.clone(),
        },
        None => WsParams {
            codec,
            method: Some(req.uri().path().to_string()),
            annotation: None,
        },
    };

    match hyper_tungstenite::upgrade(&mut req, None) {
        Ok((mut response, websocket)) => {
            if let Some(p) = echo {
                response
                    .headers_mut()
                    .insert(http::header::SEC_WEBSOCKET_PROTOCOL, http::HeaderValue::from_static(p));
            }
            let rt = rt.clone();
            tokio::spawn(async move { ws::serve(rt, websocket, reject, params).await });
            response.map(boxed_full)
        }
        Err(e) => text_response(StatusCode::BAD_REQUEST, format!("upgrade failed: {e}")),
    }
}

/// `+proto` unary over Fetch: stream the length-prefixed request into a gRPC frame, dispatch
/// through the backend under the local deadline, and stream the `[len|message][len|trailer]`
/// response back opaquely (never decoding the message).
async fn unary_proto(rt: &Runtime, req: Request<Incoming>) -> Response<ResBody> {
    let Some(path) = req.uri().path_and_query().cloned() else {
        return text_response(StatusCode::BAD_REQUEST, "missing method path");
    };
    let deadline = metadata::parse_grpc_timeout(req.headers());
    let (parts, body) = req.into_parts();

    let grpc_body = match frame_upstream_request(body, rt.cfg.max_message_bytes).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let mut builder = Request::builder().method(http::Method::POST).uri(path.clone());
    for (name, value) in parts.headers.iter() {
        if !metadata::is_denied(name) {
            builder = builder.header(name.clone(), value.clone());
        }
    }
    builder = builder.header(http::header::CONTENT_TYPE, CT_GRPC).header("te", "trailers");
    if let Some(d) = deadline {
        builder = builder.header("grpc-timeout", metadata::format_grpc_timeout(d + DEADLINE_GRACE));
    }
    let grpc_req = builder.body(grpc_body).expect("valid request");

    // Establish under the local deadline (dropping the future cancels the call).
    let established = match deadline {
        Some(d) => match tokio::time::timeout(d, rt.backend.call(grpc_req)).await {
            Ok(r) => r,
            Err(_) => return fetch_status(Code::DeadlineExceeded, "deadline exceeded"),
        },
        None => rt.backend.call(grpc_req).await,
    };
    let resp = match established {
        Ok(resp) => resp,
        Err(e) => return fetch_status(Code::Unavailable, &format!("upstream: {e}")),
    };
    let (resp_parts, mut resp_body) = resp.into_parts();
    let resp_headers = resp_parts.headers;
    // A trailers-only response (a gRPC error before any message) carries grpc-status in the
    // HEADERS block, and its metadata is *trailing*, not initial — route it to the trailer.
    let trailers_only = resp_headers.contains_key("grpc-status");

    let mut out = Response::builder().status(StatusCode::OK).header(http::header::CONTENT_TYPE, CT_PROTO);
    if !trailers_only {
        if let Some(headers) = out.headers_mut() {
            metadata::merge_metadata_into_headers(&MetadataMap::from_headers(resp_headers.clone()), headers);
        }
    }
    let deadline_at = deadline.map(|d| tokio::time::Instant::now() + d);
    let stream = async_stream::try_stream! {
        let mut skip = 1usize; // drop the gRPC compression-flag byte
        let mut saw_message = false;
        let mut trailer_headers = HeaderMap::new();
        let mut timed_out = false;
        loop {
            let next = match deadline_at {
                Some(at) => match tokio::time::timeout_at(at, resp_body.frame()).await {
                    Ok(f) => f,
                    Err(_) => { timed_out = true; break; }
                },
                None => resp_body.frame().await,
            };
            let Some(frame) = next else { break };
            let frame = frame.map_err(|e| -> BoxError { format!("upstream body: {e}").into() })?;
            match frame.into_data() {
                Ok(mut data) => {
                    while skip > 0 && !data.is_empty() {
                        let n = skip.min(data.len());
                        let _ = data.split_to(n);
                        skip -= n;
                    }
                    if !data.is_empty() {
                        saw_message = true;
                        yield BodyFrame::data(data);
                    }
                }
                Err(frame) => {
                    if let Ok(t) = frame.into_trailers() {
                        trailer_headers = t;
                    }
                }
            }
        }
        // A deadline firing mid-message can't be signalled cleanly (the block already
        // promised a length) — stop, letting the client see a truncated body. Before any
        // message we can still emit a clean status.
        if timed_out && saw_message {
            return;
        }
        if !saw_message {
            yield BodyFrame::data(Bytes::copy_from_slice(&EMPTY_MESSAGE_BLOCK));
        }
        let (status_code, status_message) = if timed_out {
            (Code::DeadlineExceeded as u32, "deadline exceeded".to_string())
        } else {
            metadata::read_status(&trailer_headers, &resp_headers)
        };
        // Trailers-only: the trailing metadata rode in the headers block, not a trailers block.
        let trailing = if trailer_headers.is_empty() && trailers_only {
            resp_headers.clone()
        } else {
            trailer_headers
        };
        let trailer = Trailer {
            status_code,
            status_message,
            trailers: metadata::metadata_to_vec(&MetadataMap::from_headers(trailing)),
        };
        yield BodyFrame::data(encode_trailer_block(&trailer));
    };

    out.body(StreamBody::new(stream).boxed_unsync()).expect("valid response")
}

/// `+json` / REST over Fetch: try a REST annotation binding first, then the main method
/// path; transcode JSON⇄proto around a binary backend call, returning native JSON.
async fn json_fetch(rt: &Runtime, req: Request<Incoming>, sdk_json: bool) -> Response<ResBody> {
    let tc = match rt.schema.transcoder_any().await {
        Ok(tc) => tc,
        Err(status) => return webnext_error(CT_JSON, status.code(), status.message()),
    };
    let resp_ct = if sdk_json { CT_JSON } else { "application/json" };
    let http_method = req.method().clone();
    let Some(pq) = req.uri().path_and_query().cloned() else {
        return webnext_error(resp_ct, Code::Internal, "missing request path");
    };
    let deadline = metadata::parse_grpc_timeout(req.headers());
    let (parts, body) = req.into_parts();
    let body_bytes = match collect_bounded(body, rt.cfg.max_message_bytes).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    // 1) REST annotation binding? Path/query/body build the method's request message.
    match tc.transcode_http_request(http_method.as_str(), pq.path(), pq.query(), &body_bytes) {
        Ok(Some(call)) => {
            let Ok(grpc_path) = call.grpc_method.parse::<PathAndQuery>() else {
                return webnext_error("application/json", Code::Internal, "bad transcoded method path");
            };
            return json_upstream(
                rt, &tc, grpc_path, call.message.into(), &parts.headers, "application/json", deadline,
                &call.response_body,
            )
            .await;
        }
        Ok(None) => {} // not a REST URL — fall through to the main method path
        Err(e) => return webnext_error("application/json", Code::InvalidArgument, &format!("bad REST request: {e}")),
    }

    // 2) Main gRPC method path. `+json` is the SDK contract; plain JSON needs the flag.
    if !sdk_json && !rt.cfg.allow_implicit_codec {
        return text_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "this gRPC method path requires content-type application/grpc-webnext+json (or set allow_implicit_codec to accept plain JSON here)",
        );
    }
    if !tc.has_method(pq.path()) {
        return webnext_error(resp_ct, Code::Unimplemented, &format!("no such method: {}", pq.path()));
    }
    let proto = match tc.request_json_to_proto(pq.path(), &body_bytes) {
        Ok(p) => p,
        Err(e) => return webnext_error(resp_ct, Code::InvalidArgument, &format!("bad json request: {e}")),
    };
    json_upstream(rt, &tc, pq, proto.into(), &parts.headers, resp_ct, deadline, "").await
}

/// Call the backend with an already-encoded binary request and render the binary response
/// as native JSON (bare body, status in `grpc-status`/`grpc-message` headers). JSON buffers
/// both messages (the transform is whole-message).
#[allow(clippy::too_many_arguments)]
async fn json_upstream(
    rt: &Runtime,
    tc: &Transcoder,
    grpc_path: PathAndQuery,
    message: Bytes,
    req_headers: &HeaderMap,
    resp_ct: &str,
    deadline: Option<std::time::Duration>,
    // A REST binding's `response_body`; empty for the whole message.
    response_body: &str,
) -> Response<ResBody> {
    let mut builder = Request::builder().method(http::Method::POST).uri(grpc_path.clone());
    for (name, value) in req_headers.iter() {
        if !metadata::is_denied(name) {
            builder = builder.header(name.clone(), value.clone());
        }
    }
    builder = builder.header(http::header::CONTENT_TYPE, CT_GRPC).header("te", "trailers");
    if let Some(d) = deadline {
        builder = builder.header("grpc-timeout", metadata::format_grpc_timeout(d + DEADLINE_GRACE));
    }
    let grpc_req = builder.body(TonicBody::new(Full::new(grpc_frame(&message)))).expect("valid request");

    let call = async {
        let resp = rt.backend.call(grpc_req).await.map_err(|e| format!("upstream: {e}"))?;
        let (parts, body) = resp.into_parts();
        let collected = body.collect().await.map_err(|e| format!("upstream body: {e}"))?;
        Ok::<_, String>((parts, collected))
    };
    let (resp_parts, collected) = match deadline {
        Some(d) => match tokio::time::timeout(d, call).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return webnext_error(resp_ct, Code::Unavailable, &e),
            Err(_) => return webnext_error(resp_ct, Code::DeadlineExceeded, "deadline exceeded"),
        },
        None => match call.await {
            Ok(v) => v,
            Err(e) => return webnext_error(resp_ct, Code::Unavailable, &e),
        },
    };

    let trailer_headers = collected.trailers().cloned().unwrap_or_default();
    let body_bytes = collected.to_bytes();
    let out_message = deframe_all(&body_bytes).into_iter().next().unwrap_or_default();
    let (status_code, status_message) = metadata::read_status(&trailer_headers, &resp_parts.headers);

    let json_body = if status_code == 0 && !out_message.is_empty() {
        match tc.response_proto_to_json_body(grpc_path.path(), &out_message, response_body) {
            Ok(j) => Bytes::from(j),
            Err(e) => return webnext_error(resp_ct, Code::Internal, &format!("bad json response: {e}")),
        }
    } else {
        Bytes::new()
    };
    json_fetch_response(resp_ct, json_body, status_code, &status_message, &resp_parts.headers, &trailer_headers)
}

// --- Response helpers -------------------------------------------------------

fn is_json_ct(ct: &str) -> bool {
    ct == CT_JSON || ct == "application/json"
}

/// A grpc-webnext error carried per the codec: `+json` puts status in headers; `+proto`
/// puts it in a trailer block.
pub(crate) fn webnext_error(content_type: &str, code: Code, message: &str) -> Response<ResBody> {
    if is_json_ct(content_type) {
        return json_fetch_response(content_type, Bytes::new(), code as u32, message, &HeaderMap::new(), &HeaderMap::new());
    }
    let trailer = Trailer { status_code: code as u32, status_message: message.to_string(), trailers: Vec::new() };
    let framed = encode_response_body(&[], &trailer);
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, content_type)
        .body(boxed_full(Full::new(framed)))
        .expect("valid response")
}

/// Render a native-JSON Fetch response: bare JSON body (empty on error), status in
/// `grpc-status`/`grpc-message`, metadata in headers.
fn json_fetch_response(
    content_type: &str,
    message: Bytes,
    status_code: u32,
    status_message: &str,
    initial: &HeaderMap,
    trailing: &HeaderMap,
) -> Response<ResBody> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, content_type)
        .header("grpc-status", status_code.to_string());
    if !status_message.is_empty() {
        if let Ok(v) = http::HeaderValue::from_str(&metadata::percent_encode(status_message)) {
            builder = builder.header("grpc-message", v);
        }
    }
    if let Some(headers) = builder.headers_mut() {
        let md = |h: &HeaderMap| MetadataMap::from_headers(h.clone());
        metadata::merge_metadata_into_headers(&md(initial), headers);
        metadata::merge_metadata_into_headers(&md(trailing), headers);
    }
    let body = if status_code == 0 { message } else { Bytes::new() };
    builder.body(boxed_full(Full::new(body))).expect("valid response")
}

/// A buffered `+proto` Fetch response carrying only a status (empty message block +
/// trailer), for failures before any response body is available.
fn fetch_status(code: Code, message: &str) -> Response<ResBody> {
    let trailer = Trailer { status_code: code as u32, status_message: message.to_string(), trailers: Vec::new() };
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, CT_PROTO)
        .body(boxed_full(Full::new(encode_response_body(&[], &trailer))))
        .expect("valid response")
}

/// Turn a length-prefixed `+proto` Fetch request body (`[u32 len | message]`) into a
/// streaming gRPC request body: peek the length prefix to enforce the size limit, then emit
/// `[1-byte flag]` + the client's block verbatim, without buffering to measure.
async fn frame_upstream_request(mut body: Incoming, max: usize) -> Result<TonicBody, Response<ResBody>> {
    let mut lead = bytes::BytesMut::new();
    while lead.len() < LEN_PREFIX {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    lead.extend_from_slice(&data);
                }
            }
            Some(Err(e)) => return Err(text_response(StatusCode::BAD_REQUEST, format!("read body: {e}"))),
            None => break,
        }
    }
    if lead.len() < LEN_PREFIX {
        return Err(text_response(StatusCode::BAD_REQUEST, "request body missing length prefix"));
    }
    let declared = u32::from_be_bytes([lead[0], lead[1], lead[2], lead[3]]) as usize;
    if declared > max {
        return Err(text_response(StatusCode::PAYLOAD_TOO_LARGE, "request message exceeds size limit"));
    }
    let lead = lead.freeze();
    let stream = async_stream::try_stream! {
        yield BodyFrame::data(Bytes::from_static(&[0u8])); // gRPC compression flag (uncompressed)
        yield BodyFrame::data(lead);                        // [u32 len | leading message bytes]
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| -> BoxError { format!("read body: {e}").into() })?;
            if let Ok(data) = frame.into_data() {
                yield BodyFrame::data(data);
            }
        }
    };
    let body: http_body_util::combinators::UnsyncBoxBody<Bytes, BoxError> = StreamBody::new(stream).boxed_unsync();
    Ok(TonicBody::new(body))
}

/// Buffer a request body into memory, bounded by `max` (the `+json` path holds the whole
/// message to transcode it).
async fn collect_bounded(mut body: Incoming, max: usize) -> Result<Bytes, Response<ResBody>> {
    let mut buf = bytes::BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| webnext_error(CT_JSON, Code::Internal, &format!("read body: {e}")))?;
        if let Ok(data) = frame.into_data() {
            if buf.len() + data.len() > max {
                return Err(webnext_error(CT_JSON, Code::ResourceExhausted, "request message exceeds size limit"));
            }
            buf.extend_from_slice(&data);
        }
    }
    Ok(buf.freeze())
}

pub(crate) fn boxed_full(body: Full<Bytes>) -> ResBody {
    body.map_err(|e: Infallible| match e {}).boxed_unsync()
}

fn text_response(status: StatusCode, message: impl Into<String>) -> Response<ResBody> {
    Response::builder()
        .status(status)
        .body(boxed_full(Full::new(Bytes::from(message.into()))))
        .expect("valid response")
}

//! grpc-webnext conformance server.
//!
//! Serves `grpc.webnext.conformance.v1.ConformanceService` over grpc-webnext
//! (Fetch + WebSocket) plus native gRPC on one port, so the language-neutral
//! conformance driver can run the declarative cases in `conformance/cases/*.yaml`
//! against a Rust implementation. The *request* carries a `ResponseDefinition`
//! telling the server exactly how to respond (payload, status, metadata, timing),
//! so this one generic service exercises every protocol feature.

use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use grpc_webnext::{bind_and_serve_in_process, ServerConfig, Transcoder};
use tonic::metadata::{
    AsciiMetadataKey, BinaryMetadataKey, KeyAndValueRef, MetadataMap, MetadataValue,
};
use tonic::service::Routes;
use tonic::{Code, Request, Response, Status, Streaming};

pub mod pb {
    tonic::include_proto!("grpc.webnext.conformance.v1");
}

const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/conformance_descriptor.bin"));

use pb::conformance_service_server::{ConformanceService, ConformanceServiceServer};
use pb::{
    metadatum, BidiStreamRequest, ClientStreamRequest, ClientStreamResponse, ConformancePayload,
    Metadatum, RequestInfo, ResponseDefinition, RestStreamRequest, RestUnaryRequest,
    ServerStreamRequest, UnaryRequest,
};

// --- Metadata / request-info mapping ---------------------------------------

/// Whether a header key is gRPC framing (content-type, te, grpc-timeout, …) rather
/// than user metadata. Reuses grpc-webnext's canonical denylist so the echoed
/// `request_info.request_headers` carries only what the client actually attached.
fn is_reserved_key(key: &str) -> bool {
    http::HeaderName::from_bytes(key.as_bytes())
        .map(|n| grpc_webnext::metadata::is_denied(&n))
        .unwrap_or(false)
}

/// Map a tonic `MetadataMap` to the conformance `Metadatum` list: ascii entries carry
/// `ascii_value`; `-bin` entries carry the *decoded* bytes in `bin_value`.
fn metadata_to_conformance(meta: &MetadataMap) -> Vec<Metadatum> {
    let mut out = Vec::new();
    for kv in meta.iter() {
        match kv {
            KeyAndValueRef::Ascii(key, value) => {
                let key = key.as_str().to_string();
                if is_reserved_key(&key) {
                    continue;
                }
                if let Ok(s) = value.to_str() {
                    out.push(Metadatum {
                        key,
                        value: Some(metadatum::Value::AsciiValue(s.to_string())),
                    });
                }
            }
            KeyAndValueRef::Binary(key, value) => {
                let key = key.as_str().to_string();
                if is_reserved_key(&key) {
                    continue;
                }
                if let Ok(bytes) = value.to_bytes() {
                    out.push(Metadatum {
                        key,
                        value: Some(metadatum::Value::BinValue(bytes.to_vec())),
                    });
                }
            }
        }
    }
    out
}

/// Map a conformance `Metadatum` list back to a tonic `MetadataMap` for emitting
/// response headers / trailers. `bin_value` is base64-encoded on the wire by tonic.
fn conformance_to_metadata(items: &[Metadatum]) -> MetadataMap {
    let mut md = MetadataMap::new();
    for m in items {
        match &m.value {
            Some(metadatum::Value::AsciiValue(s)) => {
                if let (Ok(k), Ok(v)) =
                    (m.key.parse::<AsciiMetadataKey>(), MetadataValue::try_from(s.as_str()))
                {
                    md.insert(k, v);
                }
            }
            Some(metadatum::Value::BinValue(b)) => {
                if let Ok(k) = m.key.parse::<BinaryMetadataKey>() {
                    md.insert_bin(k, MetadataValue::from_bytes(b));
                }
            }
            None => {}
        }
    }
    md
}

/// Best-effort read of the gRPC deadline the server observed (0 = none). The
/// `grpc-timeout` header reaches the in-process service on the deadline path.
fn observed_timeout_millis(meta: &MetadataMap) -> u32 {
    let headers = meta.clone().into_headers();
    match grpc_webnext::metadata::parse_grpc_timeout(&headers) {
        Some(d) => u32::try_from(d.as_millis()).unwrap_or(u32::MAX),
        None => 0,
    }
}

/// Observed request context, echoed for assertions. `json` is always false: the
/// in-process service can't see which codec terminated at the grpc-webnext edge.
fn request_info_from(meta: &MetadataMap) -> RequestInfo {
    RequestInfo {
        request_headers: metadata_to_conformance(meta),
        timeout_millis: observed_timeout_millis(meta),
        json: false,
    }
}

// --- Response construction --------------------------------------------------

/// Build the terminal error a non-zero `status_code` asks for, carrying the
/// `ResponseDefinition.trailers` as the status' trailing metadata.
fn make_status(rd: &ResponseDefinition) -> Status {
    let mut status = Status::new(Code::from(rd.status_code as i32), rd.status_message.clone());
    *status.metadata_mut() = conformance_to_metadata(&rd.trailers);
    status
}

/// The unary/echo payload: fabricated zeros when `oversize_response_bytes > 0`
/// (max-message-size cases), else the definition's payload, else the request's.
fn build_payload(rd: &ResponseDefinition, req_payload: Vec<u8>) -> Vec<u8> {
    if rd.oversize_response_bytes > 0 {
        vec![0u8; rd.oversize_response_bytes as usize]
    } else if !rd.payload.is_empty() {
        rd.payload.clone()
    } else {
        req_payload
    }
}

async fn maybe_delay(delay_ms: u32) {
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(u64::from(delay_ms))).await;
    }
}

// --- Service ----------------------------------------------------------------

type PayloadStream = Pin<Box<dyn Stream<Item = Result<ConformancePayload, Status>> + Send>>;

struct ConformanceSvc;

#[tonic::async_trait]
impl ConformanceService for ConformanceSvc {
    async fn unary(
        &self,
        request: Request<UnaryRequest>,
    ) -> Result<Response<ConformancePayload>, Status> {
        let request_info = request_info_from(request.metadata());
        let req = request.into_inner();
        let rd = req.response_definition.unwrap_or_default();

        maybe_delay(rd.delay_ms).await;
        if rd.status_code != 0 {
            return Err(make_status(&rd));
        }

        let payload = build_payload(&rd, req.payload);
        let mut resp = Response::new(ConformancePayload {
            payload,
            request_info: Some(request_info),
        });
        // Initial metadata. (tonic has no API to emit custom *trailing* metadata on a
        // successful unary response; no conformance case exercises OK-path trailers.)
        *resp.metadata_mut() = conformance_to_metadata(&rd.headers);
        Ok(resp)
    }

    type ServerStreamStream = PayloadStream;

    async fn server_stream(
        &self,
        request: Request<ServerStreamRequest>,
    ) -> Result<Response<Self::ServerStreamStream>, Status> {
        let request_info = request_info_from(request.metadata());
        let rd = request.into_inner().response_definition.unwrap_or_default();
        let headers = conformance_to_metadata(&rd.headers);

        let output = async_stream::try_stream! {
            let mut first = true;
            for sm in &rd.stream_messages {
                maybe_delay(sm.delay_ms).await;
                let request_info = if first {
                    first = false;
                    Some(request_info.clone())
                } else {
                    None
                };
                yield ConformancePayload { payload: sm.payload.clone(), request_info };
            }
            if rd.status_code != 0 {
                Err::<(), Status>(make_status(&rd))?;
            }
        };

        let mut resp = Response::new(Box::pin(output) as Self::ServerStreamStream);
        *resp.metadata_mut() = headers;
        Ok(resp)
    }

    async fn client_stream(
        &self,
        request: Request<Streaming<ClientStreamRequest>>,
    ) -> Result<Response<ClientStreamResponse>, Status> {
        let request_info = request_info_from(request.metadata());
        let mut inbound = request.into_inner();

        let mut received_count: u32 = 0;
        let mut received_bytes: u64 = 0;
        let mut rd: Option<ResponseDefinition> = None;
        let mut first = true;
        while let Some(msg) = inbound.next().await {
            let msg = msg?;
            if first {
                rd = msg.response_definition.clone();
                first = false;
            }
            received_count = received_count.saturating_add(1);
            received_bytes = received_bytes.saturating_add(msg.payload.len() as u64);
        }
        let rd = rd.unwrap_or_default();

        maybe_delay(rd.delay_ms).await;
        if rd.status_code != 0 {
            return Err(make_status(&rd));
        }

        let payload = if rd.payload.is_empty() { Vec::new() } else { rd.payload.clone() };
        let mut resp = Response::new(ClientStreamResponse {
            payload: Some(ConformancePayload {
                payload,
                request_info: Some(request_info),
            }),
            received_count,
            received_bytes,
        });
        *resp.metadata_mut() = conformance_to_metadata(&rd.headers);
        Ok(resp)
    }

    type BidiStreamStream = PayloadStream;

    async fn bidi_stream(
        &self,
        request: Request<Streaming<BidiStreamRequest>>,
    ) -> Result<Response<Self::BidiStreamStream>, Status> {
        let request_info = request_info_from(request.metadata());
        let mut inbound = request.into_inner();

        let output = async_stream::try_stream! {
            let mut first = true;
            let mut rd = ResponseDefinition::default();
            while let Some(msg) = inbound.next().await {
                // A client Reset surfaces here as Err(CANCELLED); propagate it as the
                // stream's terminal status and stop.
                let msg = msg?;
                let request_info = if first {
                    rd = msg.response_definition.clone().unwrap_or_default();
                    first = false;
                    Some(request_info.clone())
                } else {
                    None
                };
                yield ConformancePayload { payload: msg.payload, request_info };
            }
            if rd.status_code != 0 {
                Err::<(), Status>(make_status(&rd))?;
            }
        };

        Ok(Response::new(Box::pin(output) as Self::BidiStreamStream))
    }

    // --- REST / google.api.http URL binding ---------------------------------
    //
    // Deliberately trivial: these exist to prove the *routing* — that a URL built
    // a correct request message — so any logic here would only get in the way of
    // reading the assertion.

    async fn rest_unary(
        &self,
        request: Request<RestUnaryRequest>,
    ) -> Result<Response<ConformancePayload>, Status> {
        let request_info = request_info_from(request.metadata());
        let req = request.into_inner();
        if req.status_code != 0 {
            return Err(Status::new(Code::from(req.status_code as i32), req.status_message));
        }
        Ok(Response::new(ConformancePayload {
            payload: req.payload.into_bytes(),
            request_info: Some(request_info),
        }))
    }

    type RestServerStreamStream = PayloadStream;

    async fn rest_server_stream(
        &self,
        request: Request<RestStreamRequest>,
    ) -> Result<Response<Self::RestServerStreamStream>, Status> {
        let request_info = request_info_from(request.metadata());
        let req = request.into_inner();

        let output = async_stream::try_stream! {
            for i in 0..req.count {
                let request_info = (i == 0).then(|| request_info.clone());
                yield ConformancePayload { payload: req.payload.clone().into_bytes(), request_info };
            }
            if req.status_code != 0 {
                Err::<(), Status>(Status::new(Code::from(req.status_code as i32), ""))?;
            }
        };
        Ok(Response::new(Box::pin(output) as Self::RestServerStreamStream))
    }
}

// --- main -------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let routes = Routes::new(ConformanceServiceServer::new(ConformanceSvc));

    let mut config = ServerConfig::default();

    // Config profiles are selected by the conformance harness via env.
    if let Ok(v) = std::env::var("CONFORMANCE_MAX_MESSAGE_BYTES") {
        if let Ok(n) = v.parse::<usize>() {
            config.max_message_bytes = n;
        }
    }
    // `CONFORMANCE_TRANSCODER=0` disables the +json path (json-without-transcoder case).
    let transcoder_disabled =
        std::env::var("CONFORMANCE_TRANSCODER").map(|v| v == "0").unwrap_or(false);
    if !transcoder_disabled {
        config.transcoder = Some(Arc::new(Transcoder::from_file_descriptor_set(FILE_DESCRIPTOR_SET)?));
    }

    let (addr, handle) = bind_and_serve_in_process(routes, config).await?;

    // Readiness line parsed by the harness / conformance runner.
    let mut stdout = std::io::stdout();
    writeln!(stdout, "LISTENING http://{addr}")?;
    stdout.flush()?;

    handle.await??;
    Ok(())
}

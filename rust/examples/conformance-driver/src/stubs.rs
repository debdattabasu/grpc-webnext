//! The same cases, driven through **tonic's generated stubs** over the adapter.
//!
//! `run.rs` is the native reading of the wire: this crate's `Deframer`, `Metadata` and
//! `Status`. This module is tonic's reading of the same wire — its codec, its
//! `MetadataMap`, its `Status` — reaching the same servers through
//! `grpc_webnext_client`'s `tonic` feature. Two independent decoders, one YAML, and
//! the matcher in `run.rs` shared between them, which is deliberate: sharing the
//! assertions is what makes any disagreement attributable to the transport path
//! rather than to two different readings of the case file.
//!
//! ## What this mode cannot assert, and why it is a SKIP rather than a pass
//!
//! - **`header_timeout_millis`.** The case exists to prove the *server* enforces
//!   `grpc-timeout`, so it sends the header and arms no local timer — anything the
//!   client does would make the case pass whether or not the server did. The adapter
//!   deliberately *does* arm a local timer from that header (see `tonic_service`), so
//!   here the client would always fire first. Running it would be exactly the
//!   failure mode `doc/STATUS.md` records: "a case cannot test the server while the
//!   client is racing it."
//!
//! - **Header/trailer separation on a *successful* unary.** tonic merges trailing
//!   metadata into `Response::metadata()` for unary calls, so the two blocks are no
//!   longer distinguishable. `headers_contain` is therefore checked against the union
//!   — still a real assertion (the metadata must arrive) but weaker than the native
//!   driver's, which sees the blocks apart. The error path is unaffected:
//!   `Status::metadata()` is purely trailing metadata, which is where this repo has
//!   actually had bugs.

use futures::StreamExt;
use grpc_webnext_client::{Client, Code, Metadata, Status};

use crate::cases::{Case, Rpc};
use crate::pb;
use crate::pb::conformance_service_client::ConformanceServiceClient;
use crate::run::{request_definition, request_metadata_pairs, Outcome};

type Stub = ConformanceServiceClient<grpc_webnext_client::TonicService>;

/// Why this *mode* cannot run a case, on top of the driver-wide reasons.
pub fn skip_reason(c: &Case) -> Option<String> {
    if c.header_timeout_millis.is_some() {
        return Some(
            "server-enforced deadline — the adapter arms a local timer from `grpc-timeout`, \
             so the client would end the call regardless of the server"
                .into(),
        );
    }
    None
}

/// Build the request, attaching metadata and the deadline the same way `run.rs` does,
/// but in tonic's vocabulary.
fn request<T>(c: &Case, message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    for (key, value) in request_metadata_pairs(c) {
        match value {
            crate::run::Value::Ascii(v) => {
                if let (Ok(k), Ok(v)) = (
                    tonic::metadata::MetadataKey::from_bytes(key.as_bytes()),
                    v.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>(),
                ) {
                    request.metadata_mut().insert(k, v);
                }
            }
            crate::run::Value::Binary(raw) => {
                if let Ok(k) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
                    request
                        .metadata_mut()
                        .insert_bin(k, tonic::metadata::MetadataValue::from_bytes(&raw));
                }
            }
        }
    }
    if let Some(ms) = c.timeout_millis {
        request.set_timeout(std::time::Duration::from_millis(ms));
    }
    request
}

pub async fn run_case(client: &Client, c: &Case) -> Outcome {
    let mut stub = ConformanceServiceClient::new(client.clone().into_tonic());
    match c.rpc {
        Rpc::Unary => unary(&mut stub, c).await,
        Rpc::ServerStream => server_stream(&mut stub, c).await,
        Rpc::ClientStream => client_stream(&mut stub, c).await,
        Rpc::BidiStream => bidi_stream(&mut stub, c).await,
    }
}

async fn unary(stub: &mut Stub, c: &Case) -> Outcome {
    let message = pb::UnaryRequest {
        payload: c
            .request
            .as_ref()
            .and_then(|r| r.payload.as_ref())
            .map(|p| p.to_vec())
            .unwrap_or_default(),
        response_definition: request_definition(c.request.as_ref()),
    };
    match stub.unary(request(c, message)).await {
        Ok(response) => {
            // tonic has already merged the trailers in here; see the module docs.
            let metadata = metadata_of(response.metadata());
            Outcome {
                headers: metadata.clone(),
                trailers: metadata,
                response: Some(response.into_inner()),
                status: Status::new(Code::Ok, ""),
                ..Default::default()
            }
        }
        Err(status) => failed(status),
    }
}

async fn server_stream(stub: &mut Stub, c: &Case) -> Outcome {
    let message = pb::ServerStreamRequest { response_definition: request_definition(c.request.as_ref()) };
    match stub.server_stream(request(c, message)).await {
        Ok(response) => drain(response, c).await,
        Err(status) => failed(status),
    }
}

async fn client_stream(stub: &mut Stub, c: &Case) -> Outcome {
    // Only the FIRST request's ResponseDefinition is honored, per the service contract.
    let messages: Vec<pb::ClientStreamRequest> = c
        .requests
        .iter()
        .flatten()
        .enumerate()
        .map(|(i, r)| pb::ClientStreamRequest {
            payload: r.payload.as_ref().map(|p| p.to_vec()).unwrap_or_default(),
            response_definition: if i == 0 {
                crate::run::definition_of(r.response_definition.as_ref())
            } else {
                None
            },
        })
        .collect();

    match stub.client_stream(request(c, futures::stream::iter(messages))).await {
        Ok(response) => {
            let metadata = metadata_of(response.metadata());
            let decoded = response.into_inner();
            Outcome {
                headers: metadata.clone(),
                trailers: metadata,
                received_count: Some(decoded.received_count),
                response: decoded.payload,
                status: Status::new(Code::Ok, ""),
                ..Default::default()
            }
        }
        Err(status) => failed(status),
    }
}

async fn bidi_stream(stub: &mut Stub, c: &Case) -> Outcome {
    let messages: Vec<pb::BidiStreamRequest> = c
        .requests
        .iter()
        .flatten()
        .enumerate()
        .map(|(i, r)| pb::BidiStreamRequest {
            payload: r.payload.as_ref().map(|p| p.to_vec()).unwrap_or_default(),
            response_definition: if i == 0 {
                crate::run::definition_of(r.response_definition.as_ref())
            } else {
                None
            },
        })
        .collect();

    match stub.bidi_stream(request(c, futures::stream::iter(messages))).await {
        Ok(response) => drain(response, c).await,
        Err(status) => failed(status),
    }
}

/// Read a response stream to its end (or to the cancel point) and collect the outcome.
async fn drain(
    response: tonic::Response<tonic::Streaming<pb::ConformancePayload>>,
    c: &Case,
) -> Outcome {
    // On a streaming response tonic does *not* merge, so these really are the headers.
    let headers = metadata_of(response.metadata());
    let mut stream = response.into_inner();
    let mut messages = Vec::new();
    loop {
        match stream.next().await {
            Some(Ok(message)) => {
                messages.push(message);
                if let Some(n) = c.cancel_after_messages {
                    if messages.len() >= n {
                        // Cancellation is client behavior: dropping the stream resets the
                        // HTTP/2 stream (the adapter's response body does it on drop), and
                        // the caller's status is a locally synthesized CANCELLED — the same
                        // thing the native driver and the TS driver produce.
                        drop(stream);
                        return Outcome {
                            headers,
                            messages,
                            status: Status::new(Code::Cancelled, "cancelled by client"),
                            ..Default::default()
                        };
                    }
                }
            }
            None => {
                // The body ended cleanly; the terminal status is OK and the trailers are
                // whatever came with it.
                let trailers = match stream.trailers().await {
                    Ok(Some(map)) => metadata_of(&map),
                    _ => Metadata::new(),
                };
                return Outcome {
                    headers,
                    trailers,
                    messages,
                    status: Status::new(Code::Ok, ""),
                    ..Default::default()
                };
            }
            Some(Err(status)) => {
                let mut outcome = failed(status);
                outcome.headers = headers;
                outcome.messages = messages;
                return outcome;
            }
        }
    }
}

/// A failed call: tonic's `Status::metadata` is purely *trailing* metadata, which is
/// the same thing the native driver puts in `trailers`.
fn failed(status: tonic::Status) -> Outcome {
    let trailers = metadata_of(status.metadata());
    Outcome {
        trailers,
        status: Status::new(code_of(status.code()), status.message().to_string()),
        ..Default::default()
    }
}

/// tonic's `MetadataMap` in this driver's vocabulary, so `run::check` is shared.
fn metadata_of(map: &tonic::metadata::MetadataMap) -> Metadata {
    let mut out = Metadata::new();
    for entry in map.iter() {
        match entry {
            tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                if let Ok(value) = value.to_str() {
                    out.append(key.as_str(), value);
                }
            }
            tonic::metadata::KeyAndValueRef::Binary(key, value) => {
                if let Ok(raw) = value.to_bytes() {
                    out.insert_bin(key.as_str(), raw.to_vec());
                }
            }
        }
    }
    out
}

fn code_of(code: tonic::Code) -> Code {
    Code::from_i32(code as i32)
}

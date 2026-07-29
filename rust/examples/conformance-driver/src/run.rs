//! Driving one case through `grpc-webnext-client`, and asserting the outcome.

use grpc_webnext_client::{CallOptions, Client, Code, Metadata, Status};
use prost::Message as _;

use crate::cases::{Case, Expect, Metadatum, PayloadMatcher, ResponseDefinitionSpec, Rpc};
use crate::pb;

const SERVICE: &str = "/grpc.webnext.conformance.v1.ConformanceService";

/// What a case run observed. The TypeScript driver's `Result`, in Rust.
pub struct Outcome {
    pub headers: Metadata,
    pub trailers: Metadata,
    pub messages: Vec<pb::ConformancePayload>,
    pub response: Option<pb::ConformancePayload>,
    pub received_count: Option<u32>,
    pub status: Status,
}

impl Default for Outcome {
    fn default() -> Outcome {
        Outcome {
            headers: Metadata::new(),
            trailers: Metadata::new(),
            messages: Vec::new(),
            response: None,
            received_count: None,
            // Deliberately not OK. Every construction site sets the status explicitly,
            // and if one ever stops doing so this must read as "nothing was observed"
            // rather than silently asserting success.
            status: Status::new(Code::Unknown, "no status observed"),
        }
    }
}

// --- building the request ------------------------------------------------------------

fn metadata_of(list: &Option<Vec<Metadatum>>) -> Metadata {
    let mut md = Metadata::new();
    for m in list.iter().flatten() {
        match (&m.ascii, &m.b64) {
            (Some(v), _) => {
                md.insert(&m.key, v.clone());
            }
            (_, Some(b)) => {
                use base64::Engine as _;
                let raw = base64::engine::general_purpose::STANDARD
                    .decode(b)
                    .expect("case has invalid base64 metadata");
                md.insert_bin(&m.key, raw);
            }
            _ => {}
        }
    }
    md
}

/// Request metadata, plus a raw `grpc-timeout` when the case asks the *server* to
/// enforce the deadline. That is the whole point of `header_timeout_millis`: the
/// header goes out and no local timer is armed, so only the server can end the call.
fn request_metadata(c: &Case) -> Metadata {
    let mut md = metadata_of(&c.request_metadata);
    if let Some(ms) = c.header_timeout_millis {
        md.insert("grpc-timeout", format!("{ms}m"));
    }
    md
}

/// Call options. Deliberately ignores `header_timeout_millis` — arming a timer for it
/// would make the case pass whether or not the server did anything.
fn call_options(c: &Case) -> CallOptions {
    let mut opts = CallOptions::new().with_metadata(request_metadata(c));
    if let Some(ms) = c.timeout_millis {
        opts = opts.with_timeout(std::time::Duration::from_millis(ms));
    }
    opts
}

fn response_definition(spec: Option<&ResponseDefinitionSpec>) -> Option<pb::ResponseDefinition> {
    let s = spec?;
    Some(pb::ResponseDefinition {
        status_code: s.status_code.unwrap_or(0),
        status_message: s.status_message.clone().unwrap_or_default(),
        headers: s.headers.as_deref().map(pb_metadata).unwrap_or_default(),
        trailers: s.trailers.as_deref().map(pb_metadata).unwrap_or_default(),
        payload: s.payload.as_ref().map(|p| p.to_vec()).unwrap_or_default(),
        stream_messages: s
            .stream_messages
            .iter()
            .flatten()
            .map(|m| pb::StreamMessage {
                payload: m.payload.as_ref().map(|p| p.to_vec()).unwrap_or_default(),
                delay_ms: m.delay_ms.unwrap_or(0),
            })
            .collect(),
        delay_ms: s.delay_ms.unwrap_or(0),
        oversize_response_bytes: s.oversize_response_bytes.unwrap_or(0),
    })
}

fn pb_metadata(list: &[Metadatum]) -> Vec<pb::Metadatum> {
    list.iter()
        .map(|m| pb::Metadatum {
            key: m.key.clone(),
            value: match (&m.ascii, &m.b64) {
                (Some(v), _) => Some(pb::metadatum::Value::AsciiValue(v.clone())),
                (_, Some(b)) => {
                    use base64::Engine as _;
                    Some(pb::metadatum::Value::BinValue(
                        base64::engine::general_purpose::STANDARD.decode(b).unwrap(),
                    ))
                }
                _ => None,
            },
        })
        .collect()
}

// --- running ---------------------------------------------------------------------------

pub async fn run_case(client: &Client, c: &Case) -> Outcome {
    match c.rpc {
        Rpc::Unary => run_unary(client, c).await,
        Rpc::ServerStream => run_server_stream(client, c).await,
        Rpc::ClientStream => run_client_stream(client, c).await,
        Rpc::BidiStream => run_bidi_stream(client, c).await,
    }
}

async fn run_unary(client: &Client, c: &Case) -> Outcome {
    let request = pb::UnaryRequest {
        payload: c.request.as_ref().and_then(|r| r.payload.as_ref()).map(|p| p.to_vec()).unwrap_or_default(),
        response_definition: response_definition(
            c.request.as_ref().and_then(|r| r.response_definition.as_ref()),
        ),
    };
    match client.unary(&format!("{SERVICE}/Unary"), request.encode_to_vec(), call_options(c)).await {
        Ok(response) => Outcome {
            headers: response.headers,
            trailers: response.trailers,
            response: pb::ConformancePayload::decode(response.message.as_slice()).ok(),
            status: Status::new(Code::Ok, ""),
            ..Default::default()
        },
        Err(status) => Outcome {
            trailers: status.metadata.clone(),
            status,
            ..Default::default()
        },
    }
}

async fn run_server_stream(client: &Client, c: &Case) -> Outcome {
    let request = pb::ServerStreamRequest {
        response_definition: response_definition(
            c.request.as_ref().and_then(|r| r.response_definition.as_ref()),
        ),
    };
    let stream = client
        .server_streaming(&format!("{SERVICE}/ServerStream"), request.encode_to_vec(), call_options(c))
        .await;
    drain(stream, c).await
}

async fn run_client_stream(client: &Client, c: &Case) -> Outcome {
    // Only the FIRST request's ResponseDefinition is honored, per the service contract.
    let requests: Vec<Vec<u8>> = c
        .requests
        .iter()
        .flatten()
        .enumerate()
        .map(|(i, r)| {
            pb::ClientStreamRequest {
                payload: r.payload.as_ref().map(|p| p.to_vec()).unwrap_or_default(),
                response_definition: if i == 0 {
                    response_definition(r.response_definition.as_ref())
                } else {
                    None
                },
            }
            .encode_to_vec()
        })
        .collect();

    match client
        .client_streaming(
            &format!("{SERVICE}/ClientStream"),
            futures::stream::iter(requests),
            call_options(c),
        )
        .await
    {
        Ok(response) => {
            let decoded = pb::ClientStreamResponse::decode(response.message.as_slice()).ok();
            Outcome {
                headers: response.headers,
                trailers: response.trailers,
                received_count: decoded.as_ref().map(|d| d.received_count),
                response: decoded.and_then(|d| d.payload),
                status: Status::new(Code::Ok, ""),
                ..Default::default()
            }
        }
        Err(status) => Outcome {
            trailers: status.metadata.clone(),
            status,
            ..Default::default()
        },
    }
}

async fn run_bidi_stream(client: &Client, c: &Case) -> Outcome {
    let requests: Vec<Vec<u8>> = c
        .requests
        .iter()
        .flatten()
        .enumerate()
        .map(|(i, r)| {
            pb::BidiStreamRequest {
                payload: r.payload.as_ref().map(|p| p.to_vec()).unwrap_or_default(),
                response_definition: if i == 0 {
                    response_definition(r.response_definition.as_ref())
                } else {
                    None
                },
            }
            .encode_to_vec()
        })
        .collect();

    let stream = client
        .bidi_streaming(
            &format!("{SERVICE}/BidiStream"),
            futures::stream::iter(requests),
            call_options(c),
        )
        .await;
    drain(stream, c).await
}

/// Read a response stream to its end (or to the cancel point) and collect the outcome.
async fn drain(
    stream: Result<grpc_webnext_client::Streaming, Status>,
    c: &Case,
) -> Outcome {
    let mut stream = match stream {
        Ok(s) => s,
        // A trailers-only failure surfaces when the stream opens; there is no stream.
        Err(status) => {
            return Outcome { trailers: status.metadata.clone(), status, ..Default::default() }
        }
    };
    let headers = stream.headers.clone();
    let mut messages = Vec::new();
    loop {
        match stream.message().await {
            Ok(Some(bytes)) => {
                if let Ok(m) = pb::ConformancePayload::decode(bytes.as_slice()) {
                    messages.push(m);
                }
                if let Some(n) = c.cancel_after_messages {
                    if messages.len() >= n {
                        // Cancellation is *client* behavior: dropping the stream resets it
                        // (the peer is told), and the caller's status is a locally
                        // synthesized CANCELLED — the same thing the TS driver's
                        // `stream.cancel()` produces. That the reset actually reaches the
                        // server is proven separately, by the client's own e2e test
                        // `dropping_a_stream_cancels_it_at_the_server`; asserting it here
                        // would need a server hook the conformance contract does not have.
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
            Ok(None) => {
                let status = stream.status();
                return Outcome {
                    headers,
                    trailers: status.metadata.clone(),
                    messages,
                    status,
                    ..Default::default()
                };
            }
            Err(status) => {
                return Outcome {
                    headers,
                    trailers: status.metadata.clone(),
                    messages,
                    status,
                    ..Default::default()
                }
            }
        }
    }
}

// --- asserting ---------------------------------------------------------------------------

/// Check the outcome against `expect`, returning every failure rather than the first —
/// a report that stops at the first mismatch makes a broken implementation take one
/// round trip per bug to diagnose.
pub fn check(c: &Case, o: &Outcome) -> Vec<String> {
    let mut fails = Vec::new();
    let e: &Expect = &c.expect;

    if let Some(want) = &e.status {
        if want.not_ok == Some(true) {
            if o.status.is_ok() {
                fails.push("expected a non-OK status, got OK".to_string());
            }
        } else if let Some(code) = want.code {
            let got = o.status.code as i32;
            if got != code {
                fails.push(format!(
                    "status: want {code}, got {got} ({:?}: \"{}\")",
                    o.status.code, o.status.message
                ));
            }
        }
        if let Some(sub) = &want.message_contains {
            if !o.status.message.contains(sub) {
                fails.push(format!(
                    "status message: want it to contain {sub:?}, got {:?}",
                    o.status.message
                ));
            }
        }
    }

    if let Some(m) = &e.response {
        check_payload("response", m, o.response.as_ref(), &mut fails);
    }
    if let Some(want) = &e.messages {
        if o.messages.len() < want.len() {
            fails.push(format!("messages: want at least {}, got {}", want.len(), o.messages.len()));
        }
        for (i, m) in want.iter().enumerate() {
            check_payload(&format!("messages[{i}]"), m, o.messages.get(i), &mut fails);
        }
    }
    if let Some(n) = e.message_count {
        if o.messages.len() != n {
            fails.push(format!("message_count: want {n}, got {}", o.messages.len()));
        }
    }
    if let Some(n) = e.received_count {
        match o.received_count {
            Some(got) if got == n => {}
            Some(got) => fails.push(format!("received_count: want {n}, got {got}")),
            None => fails.push(format!("received_count: want {n}, but none was reported")),
        }
    }
    check_metadata("headers", &e.headers_contain, &o.headers, &mut fails);
    check_metadata("trailers", &e.trailers_contain, &o.trailers, &mut fails);
    fails
}

fn check_payload(
    label: &str,
    matcher: &PayloadMatcher,
    got: Option<&pb::ConformancePayload>,
    fails: &mut Vec<String>,
) {
    let Some(got) = got else {
        fails.push(format!("{label}: no payload received"));
        return;
    };
    if let Some(want) = &matcher.payload {
        let want = want.to_vec();
        if got.payload != want {
            fails.push(format!(
                "{label} payload: want {}, got {}",
                render(&want),
                render(&got.payload)
            ));
        }
    }
    let Some(ri) = &matcher.request_info else { return };
    let info = got.request_info.as_ref();
    if let Some(want) = &ri.request_headers_contain {
        for m in want {
            let found = info.map(|i| i.request_headers.iter().any(|h| header_matches(h, m)));
            if found != Some(true) {
                fails.push(format!("{label}: server did not report request header {:?}", m.key));
            }
        }
    }
    if let Some(want) = ri.timeout_present {
        let got_timeout = info.map(|i| i.timeout_millis > 0).unwrap_or(false);
        if got_timeout != want {
            fails.push(format!("{label}: timeout_present want {want}, got {got_timeout}"));
        }
    }
    if let Some(want) = ri.json {
        let got_json = info.map(|i| i.json).unwrap_or(false);
        if got_json != want {
            fails.push(format!("{label}: json want {want}, got {got_json}"));
        }
    }
}

fn header_matches(h: &pb::Metadatum, want: &Metadatum) -> bool {
    if !h.key.eq_ignore_ascii_case(&want.key) {
        return false;
    }
    match (&want.ascii, &want.b64, &h.value) {
        (Some(v), _, Some(pb::metadatum::Value::AsciiValue(got))) => got == v,
        (_, Some(b), Some(pb::metadatum::Value::BinValue(got))) => {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.decode(b).map(|w| &w == got).unwrap_or(false)
        }
        // A key-only matcher asserts presence.
        (None, None, _) => true,
        _ => false,
    }
}

fn check_metadata(
    label: &str,
    want: &Option<Vec<Metadatum>>,
    got: &Metadata,
    fails: &mut Vec<String>,
) {
    for m in want.iter().flatten() {
        match (&m.ascii, &m.b64) {
            (Some(v), _) => {
                if got.get(&m.key) != Some(v.as_str()) {
                    fails.push(format!(
                        "{label}[{}]: want {v:?}, got {:?}",
                        m.key,
                        got.get(&m.key)
                    ));
                }
            }
            (_, Some(b)) => {
                use base64::Engine as _;
                let expected = base64::engine::general_purpose::STANDARD.decode(b).unwrap();
                match got.get_bin(&m.key) {
                    Some(raw) if raw == expected.as_slice() => {}
                    other => fails.push(format!(
                        "{label}[{}]: want {}, got {}",
                        m.key,
                        render(&expected),
                        other.map(render).unwrap_or_else(|| "nothing".into())
                    )),
                }
            }
            _ => {
                if got.get(&m.key).is_none() && got.get_bin(&m.key).is_none() {
                    fails.push(format!("{label}[{}]: missing", m.key));
                }
            }
        }
    }
}

/// Bytes as something readable in a report: text when it is text, else a byte count.
fn render(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) if s.chars().all(|c| !c.is_control()) && s.len() <= 64 => format!("{s:?}"),
        _ => format!("<{} bytes>", bytes.len()),
    }
}

//! The grpc-webnext conformance runner — **Rust client driver**.
//!
//! The second implementation of the client driver contract in
//! `conformance/README.md`. Its value is precisely that it shares nothing with the
//! TypeScript one but the YAML on disk: two clients, written independently against
//! one spec, run over the real wire against every server. That is the only way a
//! client-side divergence gets caught, since a single driver cannot disagree with
//! itself.
//!
//! ```text
//! conformance-driver --base-url http://127.0.0.1:PORT --profile default CASES...
//! ```
//!
//! Prints one `PASS` / `FAIL` / `SKIP` line per case, a summary, and exits non-zero
//! if anything failed.
//!
//! ## What it covers, and what it cannot
//!
//! The Rust client speaks **only** the binary h2ts profile — real gRPC over the
//! tunnel. It has no JSON codec and no custom `Frame` WebSocket path, both of which
//! are deliberate (see the crate README). So this driver runs the `proto/h2ts`
//! profile and reports everything else as SKIP with a reason, which the contract
//! requires to be visible rather than silently passed. The TypeScript driver remains
//! the only one covering `proto/ws`, `json`, and REST.

mod cases;
mod run;

/// Generated conformance message types. Messages only — no service stubs: the client
/// dispatches on a method path and deals in bytes, which is the whole interface.
mod pb {
    // The generated set covers the whole service, including the REST-only messages
    // this driver never builds.
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/grpc.webnext.conformance.v1.rs"));
}

use std::path::PathBuf;

use futures::{SinkExt, StreamExt};
use grpc_webnext_client::{Client, ConnectOptions, Transport, TransportError, H2TS_SUBPROTOCOL};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use cases::{Case, SuiteFile};

struct Args {
    base_url: String,
    profile: String,
    files: Vec<PathBuf>,
}

fn parse_args() -> Args {
    let mut base_url = None;
    let mut profile = "default".to_string();
    let mut files = Vec::new();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--base-url" => base_url = it.next(),
            "--profile" => profile = it.next().expect("--profile needs a value"),
            other => files.push(PathBuf::from(other)),
        }
    }
    Args {
        base_url: base_url.expect("usage: conformance-driver --base-url URL [--profile KEY] CASES..."),
        profile,
        files,
    }
}

/// Adapt a real WebSocket to the h2ts byte transport.
///
/// This is what `grpc_webnext_client::connect` does in a browser with
/// `web_sys::WebSocket`; on the host the socket is `tokio-tungstenite`, and
/// everything above it — framing, HPACK, flow control, trailers — is the same code.
async fn dial(base_url: &str) -> Result<Client, String> {
    let ws_url = format!("ws{}", base_url.trim_start_matches("http"));
    let authority = base_url.trim_start_matches("http://").trim_start_matches("https://").to_string();

    let mut request = ws_url.into_client_request().map_err(|e| e.to_string())?;
    request
        .headers_mut()
        .insert("sec-websocket-protocol", H2TS_SUBPROTOCOL.parse().unwrap());
    let (ws, _) = tokio_tungstenite::connect_async(request).await.map_err(|e| e.to_string())?;
    let (mut ws_tx, ws_rx) = ws.split();

    let reader = ws_rx.filter_map(|msg| async move {
        match msg {
            Ok(WsMessage::Binary(data)) => Some(data.to_vec()),
            _ => None,
        }
    });
    let (tx, mut rx) = futures::channel::mpsc::unbounded::<Vec<u8>>();
    tokio::task::spawn_local(async move {
        while let Some(chunk) = rx.next().await {
            if ws_tx.send(WsMessage::Binary(chunk)).await.is_err() {
                break;
            }
        }
    });
    let writer = tx.sink_map_err(|e: futures::channel::mpsc::SendError| TransportError(e.to_string()));

    let transport = Transport::new(Box::pin(reader), Box::pin(writer));
    let (client, driver) = Client::over_transport(transport, authority, ConnectOptions::default());
    tokio::task::spawn_local(driver);
    Ok(client)
}

/// Why this driver cannot run a case, or `None` if it can.
fn skip_reason(c: &Case, profile: &str) -> Option<String> {
    if c.rest.is_some() {
        // Deliberate: a REST case is driven by a raw HTTP client, not a grpc-webnext
        // one — that is the claim being tested. Routing it through this client would
        // prove the opposite of what the case is for.
        return Some("REST case — driven by a raw HTTP client, not a grpc-webnext one".into());
    }
    if !c.covers_proto() {
        return Some("json-only case — this client has no JSON codec (h2ts is binary)".into());
    }
    if c.requires_key() != profile {
        return Some(format!("needs server profile {:?}, this run is {profile:?}", c.requires_key()));
    }
    None
}

fn main() {
    let args = parse_args();
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    // The client is `!Send` by design — a browser has no threads to hand it to — so
    // the whole driver runs on a LocalSet.
    let failed = tokio::task::LocalSet::new().block_on(&runtime, drive(args));
    if failed > 0 {
        eprintln!("\n{failed} case(s) FAILED");
        std::process::exit(1);
    }
}

async fn drive(args: Args) -> usize {
    let suites: Vec<SuiteFile> = args
        .files
        .iter()
        .map(|p| {
            let text = std::fs::read_to_string(p).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
            serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
        })
        .collect();

    let (mut passed, mut failed, mut skipped) = (0usize, 0usize, 0usize);

    for suite in &suites {
        for c in &suite.cases {
            let id = format!("{}/{}", suite.suite, c.name);
            if let Some(reason) = skip_reason(c, &args.profile) {
                println!("SKIP {id} [proto/h2ts] — {reason}");
                skipped += 1;
                continue;
            }
            // One connection per case: a case that cancels a stream or trips a size
            // limit can leave the tunnel in a state the next case would inherit, and
            // a driver that let cases interfere would report noise as divergence.
            let client = match dial(&args.base_url).await {
                Ok(c) => c,
                Err(e) => {
                    println!("FAIL {id} [proto/h2ts]\n      could not open the tunnel: {e}");
                    failed += 1;
                    continue;
                }
            };
            let outcome = run::run_case(&client, c).await;
            let fails = run::check(c, &outcome);
            if fails.is_empty() {
                println!("PASS {id} [proto/h2ts]");
                passed += 1;
            } else {
                println!("FAIL {id} [proto/h2ts]");
                for f in &fails {
                    println!("      {f}");
                }
                failed += 1;
            }
        }
    }

    println!("\n{passed} passed, {failed} failed, {skipped} skipped  (rust client driver, proto/h2ts)");
    failed
}

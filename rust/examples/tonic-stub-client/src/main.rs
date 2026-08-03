//! **tonic's own generated client stubs, over a grpc-webnext tunnel.**
//!
//! Everything below `GreeterClient` here is stock: `pb` is whatever
//! `tonic-prost-build` emitted from [`greeter.proto`](../../greeter.proto), with no
//! grpc-webnext-specific codegen anywhere. The only line that differs from a native
//! tonic program is the one that builds the transport —
//!
//! ```text
//! tonic:         GreeterClient::new(Channel::from_static("http://…").connect().await?)
//! grpc-webnext:  GreeterClient::new(client.into_tonic())
//! ```
//!
//! — because the wire is real HTTP/2 either way. Trailers, multiplexing and flow
//! control all survive the tunnel, so all four cardinalities work, including the two
//! (client- and bidi-streaming) that grpc-web cannot express at all.
//!
//! ```bash
//! cargo run -p example-tonic-stub-client                       # spawns the server
//! cargo run -p example-tonic-stub-client -- http://127.0.0.1:8080   # or point at one
//! ```

use std::time::Duration;

use example_tonic_stub_client::dial;
use example_tonic_stub_client::pb::greeter_client::GreeterClient;
use example_tonic_stub_client::pb::{ChatMessage, CountdownRequest, HelloRequest, SleepRequest};
use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, BufReader};

fn main() {
    // A `!Send` client belongs on a current-thread runtime — which is what a browser
    // is. Nothing here asserts `Send` to satisfy threads this target does not have.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let local = tokio::task::LocalSet::new();
    if let Err(e) = local.block_on(&runtime, run()) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (base_url, _server) = match std::env::args().nth(1) {
        Some(url) => (url, None),
        None => {
            let (url, child) = spawn_greeter_server().await?;
            (url, Some(child))
        }
    };
    println!("connected to {base_url}\n");

    // The one grpc-webnext-shaped line. In a browser it is
    // `grpc_webnext_client::connect(&base_url)?.into_tonic()`; `dial::client` is the
    // same thing over a host WebSocket. Nothing is dialed until the first call.
    let mut greeter = GreeterClient::new(dial::client(&base_url).into_tonic());

    // --- unary ---
    let reply = greeter.say_hello(HelloRequest { name: "world".into() }).await?;
    println!("[unary]         SayHello -> {:?}", reply.into_inner().message);

    // --- server streaming: the response body is pulled, so the server is throttled
    // by this loop rather than racing ahead of it ---
    let mut ticks = greeter.countdown(CountdownRequest { from: 3 }).await?.into_inner();
    print!("[server-stream] Countdown(3) ->");
    while let Some(tick) = ticks.next().await {
        print!(" {}", tick?.value);
    }
    println!();

    // --- client streaming: grpc-web cannot do this at all ---
    let words = futures::stream::iter(
        ["real", "HTTP/2", "in", "a", "browser"].map(|text| ChatMessage { text: text.into() }),
    );
    let joined = greeter.concat(words).await?.into_inner().message;
    println!("[client-stream] Concat -> {joined:?}");

    // --- bidi streaming: neither can grpc-web ---
    let outbound = futures::stream::iter(
        ["hi", "again"].map(|text| ChatMessage { text: text.into() }),
    );
    let mut chat = greeter.chat(outbound).await?.into_inner();
    print!("[bidi]          Chat ->");
    while let Some(message) = chat.next().await {
        print!(" {:?}", message?.text);
    }
    println!();

    // --- deadlines: `set_timeout` sends `grpc-timeout` *and* arms a local timer, so
    // the call ends even against a peer that ignores the header ---
    let mut slow = tonic::Request::new(SleepRequest { millis: 5_000 });
    slow.set_timeout(Duration::from_millis(200));
    let status = greeter.sleep(slow).await.expect_err("the deadline should end this call");
    println!("[deadline]      Sleep(5s) -> {:?}: {}", status.code(), status.message());

    // --- metadata, including the `-bin` leg that travels base64 ---
    let mut request = tonic::Request::new(HelloRequest { name: "metadata".into() });
    request.metadata_mut().insert("x-request-id", "abc-123".parse()?);
    request.metadata_mut().insert_bin(
        "x-trace-bin",
        tonic::metadata::MetadataValue::from_bytes(&[0, 1, 250]),
    );
    let reply = greeter.say_hello(request).await?;
    println!("[metadata]      SayHello -> {:?}", reply.into_inner().message);

    println!("\nDemo complete. ✅");
    Ok(())
}

/// Start `example-greeter-server` and wait for the `LISTENING <url>` line it prints,
/// so the demo is one command. Pass a URL instead to point at a server you already
/// have — including a Go one, which is the whole point of a language-neutral wire.
async fn spawn_greeter_server() -> Result<(String, tokio::process::Child), Box<dyn std::error::Error>>
{
    println!("no URL given — building and starting example-greeter-server…");
    let mut child = tokio::process::Command::new("cargo")
        .args(["run", "--quiet", "-p", "example-greeter-server"])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let stdout = child.stdout.take().expect("piped");
    let mut lines = BufReader::new(stdout).lines();
    while let Some(line) = lines.next_line().await? {
        if let Some(url) = line.strip_prefix("LISTENING ") {
            return Ok((url.trim().to_string(), child));
        }
    }
    Err("the server exited before it printed LISTENING".into())
}

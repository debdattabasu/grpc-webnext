//! Opening the tunnel **on the host**.
//!
//! In a browser this whole file is one line — `grpc_webnext_client::connect(url)`,
//! which is `wasm32`-only because it needs `web_sys::WebSocket`. Natively the socket
//! has to come from somewhere, so this is the same thing over `tokio-tungstenite`:
//! h2ts is a sans-I/O engine behind a pluggable byte transport, so everything above
//! this — HTTP/2, trailers, flow control, and every line of tonic's generated stub —
//! is the identical code path a browser runs.

use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use grpc_webnext_client::{
    open_tunnel, Client, ConnectOptions, Connector, H2Connection, Status, Transport,
    TransportError, H2TS_SUBPROTOCOL,
};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

/// A **reconnecting** client for `base_url`, the shape a browser gets from
/// `connect`: nothing is dialed until the first call, and a dropped tunnel is
/// redialed by the call after the one that found it dead.
pub fn client(base_url: &str) -> Client {
    let authority = base_url.trim_start_matches("http://").trim_start_matches("https://").to_string();
    let url = base_url.to_string();
    let connector: Connector = std::rc::Rc::new(move || {
        let url = url.clone();
        Box::pin(async move {
            dial(&url).await.ok_or_else(|| Status::unavailable("dial failed"))
        })
    });
    Client::with_connector(connector, authority)
}

/// Open one tunnel and spawn its driver.
async fn dial(base_url: &str) -> Option<H2Connection> {
    let ws_url = format!("ws{}", base_url.trim_start_matches("http"));
    let mut request = ws_url.into_client_request().ok()?;
    // The subprotocol is how the server tells an h2ts tunnel from the custom
    // single-stream `Frame` WebSocket on the same port.
    request.headers_mut().insert("sec-websocket-protocol", H2TS_SUBPROTOCOL.parse().ok()?);
    let (ws, _) = tokio_tungstenite::connect_async(request).await.ok()?;
    let (mut ws_tx, ws_rx) = ws.split();

    // Inbound: binary frames are tunnel bytes; anything else is not.
    let reader = ws_rx.filter_map(|message| async move {
        match message {
            Ok(Message::Binary(data)) => Some(data.to_vec()),
            _ => None,
        }
    });

    // Outbound: a channel the sink writes into, pumped into the socket.
    let (tx, mut rx) = mpsc::unbounded::<Vec<u8>>();
    tokio::task::spawn_local(async move {
        while let Some(chunk) = rx.next().await {
            if ws_tx.send(Message::Binary(chunk)).await.is_err() {
                break;
            }
        }
    });
    let writer = tx.sink_map_err(|e| TransportError(e.to_string()));

    let (connection, driver) = open_tunnel(
        Transport::new(Box::pin(reader), Box::pin(writer)),
        ConnectOptions::default(),
    );
    // Nothing moves until the driver is polled.
    tokio::task::spawn_local(driver);
    Some(connection)
}

//! Browser entry point: open the tunnel and hand back a [`Client`]. **wasm32 only.**

use h2ts_client::ConnectOptions;
use wasm_bindgen::JsValue;

use crate::client::Client;
use crate::status::Status;
use crate::url::websocket_url;

/// Connect to a grpc-webnext endpoint.
///
/// `base_url` is the ordinary endpoint URL (`https://api.example.com`) — the same
/// one native gRPC and the TypeScript client use, because grpc-webnext serves
/// every surface on one port. The scheme is mapped to `ws`/`wss`: the tunnelled
/// HTTP/2 is always cleartext h2c, since the outer WebSocket already carries the
/// transport security.
///
/// Returns immediately: like `tonic`'s lazy channel, the tunnel opens on the first
/// call and reopens if it drops. Errors that would have surfaced here surface on
/// that call instead, and [`Client::state`] reports where the channel is.
pub fn connect(base_url: &str) -> Result<Client, Status> {
    connect_with(base_url, ConnectOptions::default())
}

/// [`connect`], with control over the HTTP/2 settings (window sizes, frame size).
pub fn connect_with(base_url: &str, options: ConnectOptions) -> Result<Client, Status> {
    let (ws_url, authority) = websocket_url(base_url)?;
    // A connector rather than a connection: the client redials with it when the
    // tunnel drops, which is what makes this a gRPC channel and not a socket handle.
    let connector: crate::Connector = std::rc::Rc::new(move || {
        let ws_url = ws_url.clone();
        let options = options.clone();
        Box::pin(async move {
            h2ts_client::connect_websocket(&ws_url, &[crate::H2TS_SUBPROTOCOL], options)
                .await
                .map_err(|e| {
                    Status::unavailable(format!("could not open the tunnel: {}", describe(&e)))
                })
        })
    });
    Ok(Client::with_connector(connector, authority))
}

fn describe(value: &JsValue) -> String {
    value.as_string().unwrap_or_else(|| format!("{value:?}"))
}

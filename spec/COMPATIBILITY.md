# Transport Compatibility Notes

grpc-webnext aims for **identical gRPC semantics**. Everything that is *client-side
policy* matches standard gRPC exactly. A few things are properties of the HTTP/2
transport itself, which the browser does not expose — there we match the semantics
and the config surface, but the underlying mechanism differs by transport.

Legend: ✅ identical · ⚠️ semantics match, mechanism differs · ⛔ accepted for API
compatibility but inert on this transport.

Server-side rows apply to every in-process implementation (Rust and Go) and the Rust proxy
alike — the conformance matrix runs the same cases against each.

| Feature | Server (Rust · Go · proxy) | Node client | Browser — Fetch (unary) | Browser — WebSocket (stream) |
|---|---|---|---|---|
| Deadlines / timeouts | ✅ | ✅ | ✅ `grpc-timeout` + timer | ✅ envelope field + timer |
| Retries (service config, backoff, hedging) | ✅ | ✅ | ✅ client-side policy | ✅ client-side policy |
| Cancellation | ✅ | ✅ | ✅ `AbortController` | ✅ control frame |
| Wait-for-ready | ✅ | ✅ | ✅ | ✅ |
| Max message size / compression | ✅ | ✅ | ✅ | ✅ |
| Resolver (name → endpoint list) | ✅ | ✅ | ✅ endpoints are **URLs** | ✅ endpoints are **URLs** |
| LB policy (pick_first, round_robin, custom) | ✅ | ✅ | ✅ picks among URLs | ✅ picks among WS connections |
| Subchannel = managed transport connection | ✅ | ✅ | ⚠️ logical URL bucket; state **inferred** from responses, browser owns the socket pool | ✅ subchannel = a `WebSocket`, **real** connection state |
| Keepalive pings (`GRPC_ARG_KEEPALIVE_*`) | ✅ | ✅ | ⛔ browser owns the connection; no JS access to h2 PING | ⚠️ native WS ping/pong control frames, **server-driven** |
| DNS fan-out under one authority (many IPs → many subchannels) | ✅ | ✅ | ⛔ no per-IP pinning; resolver must emit distinct URLs | ⛔ same |

## Why the browser diverges

- **Subchannels (Fetch path).** `fetch(url)` selects a hostname; the browser does DNS
  + connection pooling and gives no way to pin a request to a specific resolved IP,
  and no persistent socket object to observe. So a "subchannel" on the fetch path is a
  logical routing bucket at **URL granularity**, and its connectivity state is inferred
  from request success/failure rather than read from the transport. The LB architecture
  (resolver → policy → picker) is fully faithful; only the subchannel↔connection binding
  degrades.
- **Subchannels (WebSocket path).** A subchannel *is* a `WebSocket` object, so
  `readyState` / `onopen` / `onclose` give real CONNECTING→READY→TRANSIENT_FAILURE
  transitions. This is a faithful port. The Node client (real sockets) matches on both
  transports.
- **Keepalive.** HTTP/2 PING is not reachable from browser JS, and a browser cannot send a
  WebSocket ping from JS either — but it *does* auto-answer a server ping with a pong. So on
  the WebSocket path keepalive is **server-driven**, using RFC 6455 ping/pong *control*
  frames rather than application frames (`ServerConfig::ws_keepalive` /
  `ws_keepalive_timeout`, mirroring gRPC's `keepalive_time` / `keepalive_timeout`); see
  [PROTOCOL.md](PROTOCOL.md#streaming--websocket). Accepted as config for compatibility on
  Fetch, where it is a no-op.

## Same-port serving (README point 9)

Content-type disambiguates the **request-based** RPCs on one HTTP/2 listener:
`application/grpc` (native) vs `application/grpc-webnext+proto` / `+json`. The
**WebSocket** streaming transport is *not* content-type disambiguated — it arrives as an
HTTP/1.1 `Upgrade: websocket` handshake, so the server must accept, on one socket: h2
gRPC, h2 grpc-webnext unary, and an h1 WebSocket upgrade. Browsers negotiate h2 only over
TLS (ALPN), so "same port" means a TLS port; plaintext h2c from a browser is not
available.

## Multiplexing (README points 10–11)

This is **not** HTTP/2-style framing. The rules are deliberately minimal:

- **1 gRPC message = 1 WebSocket message. No fragmentation.** No reassembly, no frame
  interleaving, no per-stream credit windows. Backpressure is TCP + `bufferedAmount`.
  Keeping messages atomic is also what keeps the browser DevTools Network → Messages
  panel readable.
- **No negotiation.** If a server has a feature disabled and the client opens a stream
  that needs it, the server replies with a `Reset`. Nothing is negotiated in the handshake.
- **One WebSocket per stream on this path; multiplexing lives elsewhere.** The custom
  `Frame` path opens a fresh WebSocket per stream, which under HTTP/1.1 is subject to the
  browser's ~6-connections/host cap. That cap is exactly why the **binary default runs real
  HTTP/2 over h2ts** — one WebSocket, many streams, multiplexed natively (see
  [PROTOCOL_H2TS.md](PROTOCOL_H2TS.md)). Over HTTP/2 the browser also
  multiplexes WebSockets for free (RFC 8441 extended CONNECT), though Safari does so only
  when it can reuse an already-open h2 connection.

### Consequences to design for

- **Hard max-message-size.** Because a message cannot span frames, an oversized message
  is one giant WS frame — both ends must enforce a configurable size limit (same knob as
  README point 5). This is a property of *this* path only: on the h2ts default the message
  rides bounded, flow-controlled HTTP/2 DATA frames and the tunnel forwards them sub-frame,
  so nothing materializes the whole message. Very large messages belong on that path — see
  [PROTOCOL.md](PROTOCOL.md#fragmentation-why-this-protocol-doesnt-have-it).
- **No cross-stream head-of-line blocking on this path.** Each stream has its own
  WebSocket, so a large message on one stream never delays another. (Within a stream, a
  large atomic message still occupies the socket for its transmit duration — bounded by
  max-message-size.)

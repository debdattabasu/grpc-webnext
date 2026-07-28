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
| Retries (backoff, pushback, throttling) | ✅ | ✅ | ✅ client-side policy | ✅ client-side policy |
| Retry config source | ✅ service config | ⚠️ client option, not per-method service config | ⚠️ same | ⚠️ same |
| Hedging | ✅ | ⛔ not implemented | ⛔ | ⛔ |
| Cancellation | ✅ | ✅ | ✅ `AbortController` | ✅ control frame |
| Wait-for-ready | ✅ | ✅ | ✅ | ✅ |
| Max message size / compression | ✅ | ✅ | ✅ | ✅ |
| Resolver (name → endpoint list) | ✅ | ✅ | ✅ endpoints are **URLs** | ✅ endpoints are **URLs** |
| LB policy (pick_first, round_robin, custom) | ✅ | ✅ | ✅ picks among URLs | ✅ picks among WS connections |
| Subchannel = managed transport connection | ✅ | ✅ | ⚠️ logical URL bucket; state **inferred** from responses, browser owns the socket pool | ✅ subchannel = a `WebSocket`, **real** connection state |
| Keepalive pings (`GRPC_ARG_KEEPALIVE_*`) | ✅ | ✅ | ⛔ browser owns the connection; no JS access to h2 PING | ⚠️ native WS ping/pong control frames, **server-driven** |
| DNS fan-out under one authority (many IPs → many subchannels) | ✅ | ✅ | ⛔ no per-IP pinning; resolver must emit distinct URLs | ⛔ same |
| Receive-side flow control (a slow consumer blocks the *server*) | ✅ | ✅ | ⛔ inert — one message, already buffered | ⚠️ **real on h2ts** (HTTP/2 `WINDOW_UPDATE`); ⛔ on the custom `Frame` path — messages queue client-side |

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
- **Retries.** The mechanism is gRPC's: exponential backoff with full jitter, a
  `retryableStatusCodes` set, `grpc-retry-pushback-ms` (including the negative value that
  forbids further attempts), and the token-bucket throttle that stops a failing server from
  being retried into the ground. Retries are **off unless configured**, as in gRPC, and the
  deadline bounds them because it aborts the call signal that both the attempt and the
  backoff wait on. Two deviations, both deliberate:

  - **Config is a client option, not per-method service config.** There is no service-config
    loader here and adding one would be a new configuration surface for a feature nothing in
    the tree can currently express — the same reasoning that declined `HttpRule.selector`
    (`doc/HTTPRULE_GAPS.md`). A per-method map is a compatible widening if a need appears.
  - **Hedging is not implemented.** It is a distinct feature — parallel attempts, not
    sequential ones — and it multiplies load by design, which is a thing to add on evidence
    rather than on spec-completeness.

  What *is* automatic, whatever the policy says, is replaying an RPC that never reached the
  server: a tunnel that died before the request was written. gRPC calls this a transparent
  retry, and it is what lets a client survive a blip without configuring anything. It lives
  in the transport, since only the transport knows whether the bytes went anywhere.

  **Streaming retries follow gRPC's commit rule**: a stream may be replayed until the caller
  has been shown something a replay would contradict — the first response message. After
  that its status is the call's status. Client-streaming and bidi are **not** retried at
  all: replaying them means holding every message the caller has written for as long as a
  replay is possible, and an unbounded client-side buffer is precisely what the flow-control
  work above declined to grow on the response side.
- **Receive-side flow control.** This is the one capability where the two streaming paths
  genuinely differ, and it is the browser's doing. `WebSocket` exposes no way to stop
  receiving: there is no `pause()`, no read side to leave unread, and `onmessage` fires
  whether or not anything is consuming. `bufferedAmount` is the *send* side. So on the
  custom `Frame` path a client cannot push back, and inbound messages queue in the client —
  the same bargain every browser WebSocket library makes, and why a market-data feed hands
  the application the job of keeping up. The grpc-webnext client surfaces the backlog as
  `ClientReadableStream.readableLength` and lets a consumer stop it accumulating further
  with `pause()`.

  On **h2ts** it is the genuine article, for free: the tunnel carries real HTTP/2 and h2ts
  response bodies are consumption-driven (`highWaterMark: 0` — a `WINDOW_UPDATE` goes out
  only as the application reads). A consumer that stops reading stops replenishing the
  window, and tonic/grpc-go blocks on its side. Nothing in grpc-webnext implements this;
  it is HTTP/2 doing its job, which is the same reason the backlog's *server*-side
  backpressure, large-payload streaming, and fragmentation items all dissolved.

  Deliberately **not** solved with a credit-based flow-control frame on the custom path:
  that would reimplement HTTP/2 flow control, worse, for the opt-out transport — the same
  reasoning that rejected a bespoke `fragment` frame kind (`doc/BACKLOG.md`). If you need
  a stream the server will throttle for you, the default path already is one.
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

# @grpc-webnext/client

**Full bidirectional gRPC in the browser (and Node) — real HTTP/2 tunneled over WebSockets via [h2ts](https://github.com/debdattabasu/h2ts), plus REST and JSON.**

[![npm](https://img.shields.io/npm/v/@grpc-webnext/client.svg)](https://www.npmjs.com/package/@grpc-webnext/client)
[![license](https://img.shields.io/npm/l/@grpc-webnext/client.svg)](#license)

The TypeScript client for [**grpc-webnext**](https://github.com/debdattabasu/grpc-webnext) —
all four call types, deadlines, metadata, trailers, and cancellation, with **no translation on
the default path**. The browser runs a real HTTP/2 stack tunneled over a WebSocket, straight into
an unmodified gRPC server. Want plaintext instead? Flip one switch for **JSON over Fetch +
WebSocket**. The API mirrors [`@grpc/grpc-js`](https://www.npmjs.com/package/@grpc/grpc-js) and
[Connect](https://connectrpc.com/) — a callback/EventEmitter flavor and a promise/async-iterable
flavor.

## Install

```bash
npm install @grpc-webnext/client
```

Works in the browser out of the box. In Node, pass the [`ws`](https://www.npmjs.com/package/ws)
package as `webSocketImpl` (see [Node.js](#nodejs) below).

## Quickstart

Generate a typed service definition with [`ts-proto`](https://github.com/stephenh/ts-proto)
(`outputServices=generic-definitions`), then pick a client flavor:

```ts
import { makeClient, makePromiseClient } from "@grpc-webnext/client";
import { GreeterDefinition } from "./gen/greeter.js";

// Callback / EventEmitter — mirrors @grpc/grpc-js
const cb = makeClient(GreeterDefinition, { baseUrl: "https://api.example.com" });
cb.sayHello({ name: "world" }, (err, reply) => console.log(reply.message));

// Promise / async-iterable — mirrors Connect / nice-grpc
const rpc = makePromiseClient(GreeterDefinition, { baseUrl: "https://api.example.com" });

const reply = await rpc.sayHello({ name: "world" });          // unary → Promise
for await (const t of rpc.countdown({ from: 3 })) log(t);     // server-stream → AsyncIterable
const sum = await rpc.concat(source);                         // client-stream → Promise
for await (const m of rpc.chat(source, { signal })) log(m);   // bidi, cancel via AbortSignal
```

Both flavors share one transport and every gRPC feature — `grpc-timeout` deadlines, metadata
(ASCII + `-bin`), canonical status codes, and cancellation via `AbortSignal`.

## Transports & codecs

The transport is a per-client config `{ codec, unary, streaming }`. The default is real gRPC over
h2ts; a single switch drops to plaintext JSON you can read in the browser's Network tab.

| Client options | Unary | Streaming | On the wire |
|---|---|---|---|
| `{}` &nbsp;*(default)* | h2ts | h2ts | **Real HTTP/2** over one WebSocket — multiplexed, unmodified gRPC |
| `{ unary: "fetch" }` | Fetch | h2ts | Fetch unary + multiplexed HTTP/2 streams |
| `{ streaming: "ws" }` | Fetch | one WS / stream | Custom `Frame` protocol — **binary** |
| `{ codec: "json" }` | Fetch | one WS / stream | Custom `Frame` protocol — **plaintext JSON** |

```ts
// Plaintext JSON — debuggable in the Network tab
const json = makePromiseClient(GreeterDefinition, { baseUrl, codec: "json" });
```

`codec: "json"` is locked to `{ unary: "fetch", streaming: "ws" }` — JSON stays plaintext and
never rides h2ts.

### REST (`google.api.http`) URLs

If your server annotates methods with `google.api.http`, those URLs are **not** part of this
client, deliberately: an annotated URL exists so that a caller needs no SDK, and this client
already reaches every method with JSON over the base endpoints. Call one directly — the
generated types still give you a typed result:

```ts
const res = await fetch(`${baseUrl}/v1/things/${encodeURIComponent(id)}`);
const thing = Thing.fromJSON(await res.json());   // ts-proto's decoder
```

Worth doing when the annotated route buys something the base endpoints cannot — a `GET` the
browser or a CDN will cache, or an API gateway that only admits documented paths.

## Node.js

Browsers provide `fetch` and `WebSocket` globally. In Node, supply a WebSocket implementation
(and, on older runtimes, `fetch`):

```ts
import { makePromiseClient } from "@grpc-webnext/client";
import WebSocket from "ws";

const rpc = makePromiseClient(GreeterDefinition, {
  baseUrl: "http://localhost:8080",
  webSocketImpl: WebSocket as unknown as typeof globalThis.WebSocket,
});
```

## Options

```ts
interface ClientOptions {
  baseUrl: string;                    // "http://localhost:8080" or "https://api.example.com"
  codec?: "proto" | "json";           // message codec (default "proto")
  unary?: "h2ts" | "fetch";           // unary transport
  streaming?: "h2ts" | "ws";          // streaming transport
  maxMessageBytes?: number;           // inbound message size limit
  readableHighWaterMark?: number;     // response messages queued before pushing back (default 32)
  retry?: RetryPolicy;                // off unless set (see below)
  retryThrottling?: RetryThrottling;  // token bucket shared by every call
  webSocketImpl?: typeof WebSocket;   // Node: pass the `ws` package
  fetch?: typeof fetch;               // override the fetch implementation
}
```

## Reconnecting and retries

**A dropped connection heals itself.** Every call on the default transport rides one
HTTP/2 tunnel; if it dies — a blip, a load balancer recycling, a server restart — the next
call redials, and a call that was handed to an already-dead tunnel is replayed
automatically. It never reached the server, so replaying it cannot duplicate anything.
That needs no configuration, and it is not the retry policy below.

**Retries are off unless you ask**, as in gRPC — a retry nobody asked for turns one
failing server into a thundering herd.

```ts
const client = makePromiseClient(GreeterDefinition, {
  baseUrl: "https://api.example.com",
  retry: {
    maxAttempts: 5,               // including the first
    initialBackoffMs: 100,        // grows ×2, jittered, up to maxBackoffMs
    maxBackoffMs: 2_000,
    backoffMultiplier: 2,
    retryableStatusCodes: [Status.UNAVAILABLE],
  },
  retryThrottling: { maxTokens: 10, tokenRatio: 0.1 },  // recommended alongside retry
});
```

Behavior worth knowing:

- **The deadline wins.** Retries stop when the call's deadline fires; they cannot extend it.
- **The server can override you.** A `grpc-retry-pushback-ms` trailer sets the delay, and a
  negative one stops retries even for a retryable status.
- **Streams are retried only until their first message.** After the caller has seen a
  message, replaying would contradict what it was shown, so the attempt is committed —
  gRPC's rule.
- **Client-streaming and bidi are not retried.** Replaying them means buffering every
  message you have written for as long as a replay is possible; that unbounded buffer is
  the thing this client avoids.
- **Hedging is not implemented.**

## Consuming a response stream

A server-streaming or bidi call returns a `ClientReadableStream`, consumable two ways —
`for await`, or a `data` listener. Pick one: attaching a `data` listener puts the stream in
flowing mode (as in Node), and the async iterator then has nothing to read.

```ts
for await (const reply of client.subscribe({ topic: "prices" })) {
  await handle(reply);           // slow work here pushes back on the server (see below)
}
```

**Backpressure.** On the default `h2ts` transport it is real: a consumer that stops reading
stops HTTP/2 window replenishment, so the *server* blocks instead of the tab filling with
messages. Slow work inside a `for await` body is all it takes.

On `streaming: "ws"` it cannot be. The browser `WebSocket` API has no way to stop
receiving — no `pause()`, no unread side, `onmessage` fires regardless — so a consumer that
falls behind queues messages client-side, exactly as any browser WebSocket library does.
Two levers:

```ts
stream.readableLength;   // messages received but not yet consumed — watch this
stream.pause();          // stop delivering (they keep queueing)
stream.resume();         // flush the backlog, in order
```

If you need the server throttled for you, use the default transport — that is what it is
for.

## Server side

This package is the client. To serve grpc-webnext, wrap a native gRPC server in-process or run
the schema-agnostic proxy — see the [main repository](https://github.com/debdattabasu/grpc-webnext).
The endpoint speaks grpc-webnext **and** native `application/grpc` on the same port, so native
gRPC clients pass through untouched.

## Links

- **Repository:** https://github.com/debdattabasu/grpc-webnext
- **Protocol spec (normative):** [`spec/PROTOCOL.md`](https://github.com/debdattabasu/grpc-webnext/blob/main/spec/PROTOCOL.md)
- **h2ts (HTTP/2 over WebSocket):** https://github.com/debdattabasu/h2ts

## License

Dual-licensed under either [Apache-2.0](https://github.com/debdattabasu/grpc-webnext/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/debdattabasu/grpc-webnext/blob/main/LICENSE-MIT), at your option.

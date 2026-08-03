# grpc-webnext conformance suite

**Purpose: keep independent implementations of one wire protocol from drifting.**

grpc-webnext is now implemented five times over — Rust server + proxy, Go server, Node
server, and TS + Rust clients — all speaking the single wire format in
[`/proto/grpc_webnext.proto`](../proto/grpc_webnext.proto) and
[`/spec/PROTOCOL.md`](../spec/PROTOCOL.md) + [`/spec/PROTOCOL_H2TS.md`](../spec/PROTOCOL_H2TS.md).
Across languages you cannot share code, so the
thing that guarantees they agree is a **language-neutral, wire-level conformance suite**:
declarative scenarios, run over the actual transports, against every implementation.

This is the polyglot analog of what unified the Rust proxy and server internally (one
`Backend` enum, so a protocol change is written once). That single audit found *four*
proxy/server divergences (see [`/doc/STATUS.md`](../doc/STATUS.md)). With three server
implementations and two clients, that class of bug can only be caught by running the
protocol, not by reading the code. This suite is that runner.

## The model

```
             cases/*.yaml  (declarative scenarios, this directory)
                   │
                   ▼
   ┌───────────────────────────────┐        grpc-webnext wire
   │  client driver (under test)   │  ────────────────────────────►  ┌──────────────────────┐
   │  TS driver | Rust driver | …  │   Fetch (unary) / WS (stream)    │  server (under test) │
   └───────────────────────────────┘  ◄────────────────────────────  │  Rust | Go | Node    │
                   │                                                  │  serves              │
                   ▼                                                  │  ConformanceService  │
              pass / fail / skip report                               └──────────────────────┘
```

Every **server** serves one fixed service —
[`ConformanceService`](proto/conformance.proto) — whose requests carry a
`ResponseDefinition` telling the server exactly how to respond (payload, status,
metadata, timing, oversize). One generic service therefore exercises every protocol
feature with no per-case server code.

Every **client driver** reads the case files, drives the RPCs over the requested
transport + codec, and asserts the observed wire behavior.

The guarantee is the **matrix**: `{client drivers} × {server impls} × {transports} × {codecs}`.

## A passing case can still test nothing

Two traps, both of which have hidden a real bug here:

- **A `transports: [websocket]` case only reaches the WebSocket if the RPC streams.** A
  unary call takes the Fetch path under every profile.
- **A case whose deadline the *client* enforces never tests the server.** Every
  `timeout_millis` case arms the TS client's own timer, so it passes whether or not the
  server honors `grpc-timeout` — which is how the Rust server's h2ts path came to receive
  the header and ignore it while Go honored it, with a green matrix throughout. Use
  `header_timeout_millis` to send the header and arm no local timer, so only the server can
  end the call.

The shape is the same both times: the driver quietly satisfying the assertion itself. When
adding a case, ask what would have to break for it to fail.

## The runner contract

An implementation joins the matrix by providing one (or both) of:

### A conformance **server**

An executable that:

1. Serves `grpc.webnext.conformance.v1.ConformanceService` over grpc-webnext (Fetch +
   WebSocket + native gRPC on one port, per the spec).
2. Is configured by a **profile** (see below), passed via environment variables.
3. Prints exactly `LISTENING http://<addr>` to stdout once ready, then runs until killed.
   *(This is the same readiness convention the Rust `devserver`/greeter examples already
   use, so existing harness code carries over.)*

### A conformance **client driver**

An executable that:

1. Takes a target base URL and a set of case files.
2. Runs each case (expanding `transports × codecs`) and attaches any `request_metadata`.
3. Emits a report: one `PASS` / `FAIL` / `SKIP` per expanded case, with a diff on failure.

The reference client driver is the **TS client** (`node/packages/client`), because it is
the mature reference implementation. The **Rust client** has one too, as of 2026-07-29:
`rust/examples/conformance-driver`, run from the same harness
(`node/packages/client/test/conformance-rust-driver.test.ts`) against every server.

```bash
conformance-driver --base-url http://127.0.0.1:PORT --profile default conformance/cases/*.yaml
```

It covers the `proto/h2ts` profile only — the Rust client has no JSON codec and no custom
`Frame` path, both deliberate — and reports everything else as SKIP **with a reason**. The two
drivers share nothing but the YAML on disk: separate case parsers, separate matchers, separate
clients. That is the point. A single driver cannot disagree with itself, so until there were
two, a client-side misreading of the protocol had nothing to fail against.

#### The Rust driver's two modes (2026-08-02)

`--stubs` runs the identical cases through **tonic's generated client stubs**, over
`grpc-webnext-client`'s `tonic` feature, instead of the native path-and-bytes API:

```bash
conformance-driver --base-url http://127.0.0.1:PORT --profile default --stubs conformance/cases/*.yaml
```

The client crate can decode a gRPC response two ways — its own `Deframer` / `Metadata` /
`Status`, or tonic's codec / `MetadataMap` / `Status` through the adapter — and only one of
them was ever run against a server other than Rust's. Two readings of one wire are worth
having only if something checks they still agree, so the harness runs both modes against
every server and asserts they pass **the same cases**, not merely the same *number* of them.
Report lines carry `[proto/h2ts]` or `[proto/h2ts+tonic]` so a run is never ambiguous about
which path produced it.

Unlike the two drivers, the modes deliberately **share the matcher and the case parser**.
That is the opposite choice for the opposite reason: here the thing under test is the
transport path, so anything else that differed would confound the comparison.

One case is SKIPped in `--stubs` mode, and the reason is the interesting part.
`deadline/unary/server-enforced` sends `grpc-timeout` as a raw header and arms no local
timer, precisely so that only the *server* can end the call. The adapter deliberately does
arm a local timer from that header, so through the stubs the client would always win the
race and the case would pass whether or not the server did anything — the exact failure mode
`doc/STATUS.md` records ("a case cannot test the server while the client is racing it").
Also weaker in this mode: on a *successful* unary tonic merges trailing metadata into the
response metadata, so `headers_contain` is checked against the union rather than the header
block alone. The error path is unaffected — `Status::metadata()` is purely trailing
metadata, and that is where this repo has actually had bugs.

## Server config profiles

Some cases only make sense under a specific server configuration (`requires:` in a case).
The harness starts the server in the matching profile via env vars; a server that cannot
honor a profile marks those cases **SKIPPED** — surfaced in the report, **never silently
passed**.

| `requires:` key        | env the harness sets            | meaning |
|------------------------|---------------------------------|---------|
| `max_message_bytes: N` | `CONFORMANCE_MAX_MESSAGE_BYTES` | server enforces an N-byte message cap |
| `transcoder: true`     | `CONFORMANCE_TRANSCODER=1`      | server has a `+json` transcoder for the conformance descriptors |
| `transcoder: false`    | `CONFORMANCE_TRANSCODER=0`      | server has **no** transcoder (capability-gap cases) |

## The cases

Declarative YAML validated by [`schema/case.schema.json`](schema/case.schema.json).
Byte values are `{ text: "…" }` (UTF-8) or `{ b64: "…" }`. See the schema for the full
grammar. Current coverage:

| Suite | File | Covers |
|-------|------|--------|
| unary | [cases/unary.yaml](cases/unary.yaml) | OK, empty payload, non-OK status + trailing metadata, response headers |
| streaming | [cases/streaming.yaml](cases/streaming.yaml) | server-stream (incl. messages-then-error), client-stream aggregate (with `received_count`), bidi echo, client cancel → CANCELLED |
| deadline | [cases/deadline.yaml](cases/deadline.yaml) | unary + stream `grpc-timeout` expiry (DEADLINE_EXCEEDED); within-deadline passes |
| limits | [cases/limits.yaml](cases/limits.yaml) | oversize request rejected on every path, large response intact, `+json` w/o transcoder → UNIMPLEMENTED, ASCII+`-bin` metadata round-trip on **both** Fetch and WebSocket |
| rest | [cases/rest.yaml](cases/rest.yaml) | `google.api.http` routes: `body:"*"` on Fetch and WebSocket, path/query binding, `additional_bindings`, bare `*`/`**` and multi-segment captures, `response_body`, binding precedence, status + metadata fidelity, deadlines, **multi-message** bidi and client-streaming routes, and both wrong-surface rejections |

Each case runs under every applicable **transport profile** — `proto/h2ts` (real gRPC over
the h2ts tunnel), `proto/ws` (the custom `Frame` path, unary over Fetch), and `json` (the
custom path, Fetch + WS) — against **every server implementation**. 82 case×profile runs per
server, Rust and Go, all green.

The **Rust client driver** adds a second and third pass over the same cases: 18 of the 54 are
within its reach (non-REST, proto codec), run against both servers on the native path and 17
of them again through tonic's generated stubs. When adding a case, note that it joins the TS
driver automatically, the Rust native mode only if it is neither REST nor json-only, and the
Rust stub mode only if it additionally sets no `header_timeout_millis` — the Rust harness
asserts the exact number of cases each mode runs, so a case drifting out of either set fails
rather than quietly shrinking the matrix.

### REST cases are different, on purpose

A `rest:` case names a URL, a verb, and a raw JSON body, and the driver issues an **ordinary
HTTP request** — it does not go through the grpc-webnext client at all. That is the claim
being tested: an annotated endpoint is supposed to be reachable by anything that speaks JSON
over HTTP, so proving it with a client that knows the protocol would prove nothing.

Consequently a REST case runs **once**, not once per profile: the URL already fixes the
codec (annotated endpoints are JSON-only) and the RPC's cardinality already fixes the
transport (a unary annotation URL is Fetch, a streaming one is a WebSocket — the spec's
rule, not the driver's choice). What the matrix still contributes is the part that matters:
every REST case runs against **every server implementation**.

Five conventions worth knowing before writing one:

- **Rejections.** A Fetch-surface refusal never becomes an RPC, so it is asserted with
  `expect.http_status` (e.g. `+proto` on a REST URL is `415`). A WebSocket refusal *does*
  carry a status — the `4000 + code` close — so it is asserted as an ordinary
  `expect.status.code`, exactly as a browser would read it.
- **One or many request messages.** `rest.body` is a single JSON document; `rest.bodies` is
  a list of them, for the client-streaming and bidi annotation routes — the first opens the
  stream, the rest follow as message frames, then the driver half-closes. Those are the only
  cases that prove an annotated URL is a real bidirectional stream rather than a fancy POST.
- **Client streaming is a WebSocket route**, not a Fetch one. It is a stream that happens to
  answer once, so its annotation URL behaves like every other streaming method's — routing it
  to Fetch is the easy mistake, and `client-stream-multi-message` is what catches it.
- **Raw bodies.** A `response_body` binding answers with a bare field value, not the RPC's
  response message, so those cases assert `expect.raw_body` / `expect.raw_messages` — the
  JSON verbatim — because no message matcher has anything to decode.
- **`cancel_after_messages` is not honored** on a REST case. Turning a mid-stream reset into
  a local `CANCELLED` is *client* behavior, and this driver is deliberately not a client; the
  main-surface bidi cancel case covers it instead.

**Design notes** (surfaced by the run — behavior pinned, not gaps):
- **Size limits are request-only, by design.** `max_message_bytes` bounds inbound *request*
  messages on every path — fetch/ws bound the request body; the h2ts path checks the gRPC
  frame length prefix above the tunnel (`GrpcSizeLimit` in `src/h2ts.rs`). Response size is
  **not** enforced and does not need to be (real gRPC over h2ts uses tonic's own limits), so
  there is deliberately no oversize-response case. Note a *mid-upload* request rejection
  surfaces as a stream/transport failure rather than a clean RESOURCE_EXHAUSTED on the
  Fetch/h2ts paths (inherent HTTP/2 semantics), so the oversize-request case asserts only
  that it was rejected.

The first run also **found two real bugs, now fixed**: trailing metadata on a trailers-only
(error) response was dropped on the h2ts client and the Fetch server path (both read
trailing metadata only from a trailers block, but a trailers-only response carries it in the
headers block).

Adding the **Go** server (2026-07-27) found two more, both in Rust — see
[`/doc/GO_SERVER.md`](../doc/GO_SERVER.md). One of them is a lesson about this suite's own
blind spots: `metadata-roundtrip` is a **unary** case, and a unary call takes the **Fetch**
path under *every* transport profile — even `proto/ws`, where only streaming uses the
WebSocket. So the WebSocket frame-metadata conversion was never executed by the matrix, and
`-bin` metadata was silently dropped there on Rust. The fix was a *streaming* twin of the
case (`metadata-roundtrip-websocket`). **When a case's `transports:` includes `websocket`,
check that the RPC is actually a streaming one** — otherwise the WebSocket surface goes
unproven.

Adding the **REST** suite (2026-07-27) found a third, and this one was in the *build* rather
than the protocol: `conformance-server/build.rs` compiled a proto living outside its own
package, and cargo's "rerun if the package changed" heuristic does not watch those. So the
Rust server kept serving **last build's descriptor set** — an annotation added to
`conformance.proto` simply never appeared, surfacing as a REST route that 415'd for no
reason. Both build scripts now declare their out-of-package inputs with
`cargo:rerun-if-changed`. Worth internalizing: a conformance suite that runs against a stale
artifact is not a weaker guard, it is a *lying* one, and only a case that failed for a
protocol reason it couldn't explain exposed it.

Extending the REST suite to multi-message routes also exposed a hole in an **existing**
case. `client-stream/aggregate` sent three requests and asserted only the response payload —
which comes from the *first* request's `ResponseDefinition` regardless, so a server that
silently dropped messages two and three would have passed. `ClientStreamResponse` has carried
`received_count` for exactly this since the proto was written; nothing read it. It is now an
`expect.received_count` matcher, asserted on both the main-surface and REST client-streaming
cases. Worth generalizing: **when a case's assertion would hold even if the thing it is named
after never happened, it is decoration.**

**Not yet covered** (tracked, not silently omitted): WebSocket keepalive/idle-timeout,
connection-level auth (Subscribe rejection), half-close ordering edge cases, mid-stream
cancellation over a REST route (see above), and the unsupported HttpRule features in
[`/doc/HTTPRULE_GAPS.md`](../doc/HTTPRULE_GAPS.md), which are shared by both implementations
and pinned by per-language tests instead. Add these as new suites; extend the table when you
do.

## Running

The harness is the TypeScript driver in
[`node/packages/client/test/conformance.test.ts`](../node/packages/client/test/conformance.test.ts):
it loads `cases/*.yaml`, builds and spawns each server implementation once per required
config profile, and drives every case across every transport profile via the TS client,
asserting the observed wire behavior. The server table (`SERVERS`) currently holds:

| Impl | Entry point | Toolchain needed |
|---|---|---|
| `rust` | [`rust/examples/conformance-server`](../rust/examples/conformance-server) | cargo |
| `go` | [`go/examples/conformance-server`](../go/examples/conformance-server) | go |

Each is built up front and then spawned **directly** (not through `cargo run`/`go run`), so
killing the process actually kills the server instead of orphaning a grandchild.

```bash
cd node/packages/client && npm test                              # the full suite (incl. conformance)
cd node/packages/client && npx vitest run test/conformance.test.ts   # just the matrix
```

Each server is thin: it implements `ConformanceService` on that language's grpc-webnext
in-process server. A further impl (Node) plugs in the same way (below) and the driver gains
it as another target by appending one entry to `SERVERS`.

## Adding an implementation

1. Implement `ConformanceService` on top of your language's grpc-webnext server.
2. Honor the config profiles (env vars above) and the `LISTENING http://<addr>` readiness line.
3. Register it in the harness server table.
4. Run the matrix; every applicable case must PASS or explicitly SKIP.

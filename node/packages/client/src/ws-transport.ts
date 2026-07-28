import { statusForAbort } from "./context.js";
import { decodeFrame, encodeFrame } from "./frame.js";
import { Metadata } from "./metadata.js";
import { Status } from "./status.js";
import type {
  StatusResult,
  StreamCall,
  StreamHandlers,
  Transport,
  TransportCallOptions,
  UnaryResponse,
} from "./transport.js";

/** Base subprotocol; the codec is appended. */
const WS_SUBPROTOCOL = "grpc-webnext";

export interface WebSocketTransportOptions {
  /** Base URL, e.g. "http://localhost:8080"; scheme is mapped to ws/wss. */
  baseUrl: string;
  /** Override the WebSocket constructor (node needs the `ws` package). */
  webSocketImpl?: typeof WebSocket;
  /** Message codec: "proto" (default) or "json". */
  codec?: "proto" | "json";
}

/**
 * Streaming transport over the custom `Frame` protocol: **one WebSocket per stream**,
 * connected to the method's URL. JSON frames are human-readable (no stream id / method).
 * (The binary default streams over h2ts instead — see `H2tsTransport`.)
 */
export class WebSocketTransport implements Transport {
  private readonly baseUrl: string;
  private readonly WS: typeof WebSocket;
  private readonly json: boolean;
  private readonly conns = new Set<SingleStreamConn>();

  constructor(options: WebSocketTransportOptions) {
    this.baseUrl = options.baseUrl;
    this.json = options.codec === "json";
    const impl = options.webSocketImpl ?? (globalThis as any).WebSocket;
    if (!impl) {
      throw new Error(
        "no WebSocket implementation; pass webSocketImpl (e.g. the `ws` package) in node",
      );
    }
    this.WS = impl;
  }

  unary(_path: string, _request: Uint8Array, _options: TransportCallOptions): Promise<UnaryResponse> {
    return Promise.reject(new Error("WebSocketTransport handles streaming only; unary uses Fetch"));
  }

  startStream(path: string, options: TransportCallOptions, handlers: StreamHandlers): StreamCall {
    const url = methodWsUrl(this.baseUrl, path);
    const conn = new SingleStreamConn(url, this.WS, this.json, options, handlers, () =>
      this.conns.delete(conn),
    );
    this.conns.add(conn);
    return conn.call;
  }

  close(): void {
    for (const conn of [...this.conns]) conn.close();
    this.conns.clear();
  }
}

/**
 * One WebSocket carrying exactly one stream. The method is the WS URL, so frames omit
 * any method/stream id.
 */
class SingleStreamConn {
  private readonly ws: WebSocket;
  private readonly enc = new TextEncoder();
  private readonly dec = new TextDecoder();
  private readonly pending: (string | Uint8Array)[] = [];
  private open_ = false;
  private finished = false;
  readonly call: StreamCall;

  constructor(
    url: string,
    WS: typeof WebSocket,
    private readonly json: boolean,
    options: TransportCallOptions,
    private readonly handlers: StreamHandlers,
    private readonly onDone: () => void,
  ) {
    const codecSub = json ? `${WS_SUBPROTOCOL}+json` : `${WS_SUBPROTOCOL}+proto`;
    const protocols = [WS_SUBPROTOCOL, codecSub];
    this.ws = new WS(url, protocols);
    this.ws.binaryType = "arraybuffer";
    this.ws.addEventListener("open", () => {
      this.open_ = true;
      for (const frame of this.pending) this.ws.send(frame);
      this.pending.length = 0;
    });
    this.ws.addEventListener("message", (ev: MessageEvent) => this.onMessage(ev));
    this.ws.addEventListener("close", (ev) => this.onClose(ev as CloseEvent));
    this.ws.addEventListener("error", () => this.onClose());

    // Eager open frame: carries metadata/deadline and unambiguously starts the stream.
    this.sendOpen(options);

    const signal = options.signal;
    if (signal) {
      const onAbort = () => this.terminate(statusForAbort(signal));
      if (signal.aborted) queueMicrotask(onAbort);
      else signal.addEventListener("abort", onAbort, { once: true });
    }

    this.call = {
      send: (message) => this.sendRaw(this.encodeMessage(message)),
      halfClose: () => this.sendRaw(this.encodeHalfClose()),
      cancel: () =>
        this.terminate({ code: Status.CANCELLED, details: "cancelled", metadata: new Metadata() }),
    };
  }

  close(): void {
    try {
      this.ws.close();
    } catch {
      /* ignore */
    }
  }

  private sendOpen(options: TransportCallOptions): void {
    const timeoutMillis = options.timeoutMillis ? Math.ceil(options.timeoutMillis) : 0;
    if (this.json) {
      const open: Record<string, unknown> = {};
      const metadata = metaToJson(options.metadata);
      if (Object.keys(metadata).length) open.metadata = metadata;
      if (timeoutMillis) open.timeoutMillis = timeoutMillis;
      this.sendRaw(JSON.stringify(open));
    } else {
      this.sendRaw(
        encodeFrame({
          subscribe: {
            method: "", // ignored by the server; taken from the URL
            headers: options.metadata.toMetadatumList(),
            timeoutMillis,
            initialPayload: new Uint8Array(),
          },
        }),
      );
    }
  }

  private encodeMessage(message: Uint8Array): string | Uint8Array {
    return this.json
      ? JSON.stringify({ message: JSON.parse(this.dec.decode(message)) })
      : encodeFrame({ message: { payload: message } });
  }

  private encodeHalfClose(): string | Uint8Array {
    return this.json ? JSON.stringify({ halfClose: true }) : encodeFrame({ halfClose: {} });
  }

  private encodeReset(status: StatusResult): string | Uint8Array {
    return this.json
      ? JSON.stringify({ status: { code: status.code, message: status.details } })
      : encodeFrame({ reset: { statusCode: status.code, statusMessage: status.details } });
  }

  private sendRaw(frame: string | Uint8Array): void {
    if (this.finished) return;
    if (this.open_) this.ws.send(frame);
    else this.pending.push(frame);
  }

  private terminate(status: StatusResult): void {
    if (this.finished) return;
    this.sendRaw(this.encodeReset(status));
    this.finish(status);
    this.close();
  }

  private finish(status: StatusResult): void {
    if (this.finished) return;
    this.finished = true;
    this.onDone();
    this.handlers.onStatus?.(status);
  }

  private onMessage(ev: MessageEvent): void {
    if (this.finished) return;
    const data = ev.data;
    if (typeof data === "string") {
      this.onJsonFrame(data);
      return;
    }
    const bytes =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : ArrayBuffer.isView(data)
          ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
          : null;
    if (!bytes) return;
    const frame = decodeFrame(bytes);
    if (frame.header) {
      this.handlers.onHeaders?.(Metadata.fromMetadatumList(frame.header.headers));
    } else if (frame.message) {
      this.handlers.onMessage?.(frame.message.payload);
    } else if (frame.trailer) {
      const t = frame.trailer;
      this.finish({
        code: t.statusCode as Status,
        details: t.statusMessage,
        metadata: Metadata.fromMetadatumList(t.trailers),
      });
    } else if (frame.reset) {
      const r = frame.reset;
      this.finish({ code: r.statusCode as Status, details: r.statusMessage, metadata: new Metadata() });
    }
  }

  private onJsonFrame(text: string): void {
    let jf: any;
    try {
      jf = JSON.parse(text);
    } catch {
      return;
    }
    if (jf.status) {
      this.finish({
        code: jf.status.code as Status,
        details: jf.status.message ?? "",
        metadata: jsonToMeta(jf.metadata),
      });
    } else if (jf.message !== undefined) {
      this.handlers.onMessage?.(this.enc.encode(JSON.stringify(jf.message)));
    } else if (jf.metadata) {
      this.handlers.onHeaders?.(jsonToMeta(jf.metadata));
    }
  }

  private onClose(event?: CloseEvent): void {
    this.open_ = false;
    this.finish(statusForClose(event));
  }
}

/** Private WebSocket close codes carry a gRPC status as `4000 + code` (0..=16). */
const CLOSE_STATUS_BASE = 4000;

/**
 * Reconstruct a gRPC status from a WebSocket close. The server encodes a rejected
 * handshake (and other pre-frame failures) into the close frame as private code
 * `4000 + gRPC code` — gRPC codes are 0..=16, so 4000..=4016 — with the message in
 * the reason (see PROTOCOL.md "Auth"). Any other close — a normal 1000, an abnormal
 * 1006, or an `error` event with no CloseEvent — is a transport failure and maps to
 * UNAVAILABLE.
 */
function statusForClose(event?: CloseEvent): StatusResult {
  const code = event?.code;
  if (code !== undefined && code >= CLOSE_STATUS_BASE && code <= CLOSE_STATUS_BASE + Status.UNAUTHENTICATED) {
    return {
      code: (code - CLOSE_STATUS_BASE) as Status,
      details: event?.reason ?? "",
      metadata: new Metadata(),
    };
  }
  return { code: Status.UNAVAILABLE, details: "websocket closed", metadata: new Metadata() };
}

/** Build the ws(s) URL for a method path (one WebSocket per stream). */
function methodWsUrl(baseUrl: string, path: string): string {
  const url = new URL(path, baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

/** Metadata -> JSON object (ASCII values only) for a JSON frame. */
function metaToJson(md: Metadata): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(md.getMap())) {
    if (typeof v === "string") out[k] = v;
  }
  return out;
}

/** JSON object -> Metadata. */
function jsonToMeta(obj?: Record<string, string>): Metadata {
  const md = new Metadata();
  for (const [k, v] of Object.entries(obj ?? {})) md.set(k, v);
  return md;
}

/**
 * Channel connectivity state, and watching it change.
 *
 * gRPC channels expose this (`getConnectivityState` / `watchConnectivityState` in
 * grpc-js) because a reconnecting channel is otherwise a black box: an app cannot
 * tell "the server is down" from "nothing has been asked of it yet". A browser
 * client needs it more than a backend does, because the answer is usually a banner.
 *
 * The Rust WASM client exposes the same states through `Client::state` /
 * `Client::state_changes`; keeping the two aligned is deliberate.
 */

/** Where the connection is. Mirrors grpc-js `ConnectivityState`. */
export enum ConnectivityState {
  /** No connection, and none being opened — before the first call, and after one drops. */
  IDLE = 0,
  /** A connection is being opened. */
  CONNECTING = 1,
  /** Connected; calls can be made. */
  READY = 2,
  /** The last attempt to connect failed. The next call tries again. */
  TRANSIENT_FAILURE = 3,
}

/** Called on every transition. Repeats are collapsed, so each call is a real change. */
export type ConnectivityListener = (state: ConnectivityState) => void;

/**
 * The current state plus a fan-out to anyone watching. A transport that keeps a
 * persistent connection owns one of these; see `Transport.connectivity`.
 */
export class ConnectivityWatch {
  private state = ConnectivityState.IDLE;
  private readonly listeners = new Set<ConnectivityListener>();

  get current(): ConnectivityState {
    return this.state;
  }

  /** Record a state. A repeat is not an event. */
  set(state: ConnectivityState): void {
    if (this.state === state) return;
    this.state = state;
    for (const listener of [...this.listeners]) {
      try {
        listener(state);
      } catch (e) {
        // A watcher is usually UI code. A render error in one subscriber must not
        // strand the others on a stale state, nor propagate into the transport's
        // connection lifecycle, which is what calls this.
        //
        // Reported rather than rethrown out of band: a rethrow would reach the app's
        // error handler, but it also manufactures an uncaught exception inside every
        // consumer's test run, for a fault that is theirs and already visible here.
        console.error("grpc-webnext: a connectivity listener threw", e);
      }
    }
  }

  /** Subscribe to transitions. Returns an unsubscribe. */
  watch(listener: ConnectivityListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
}

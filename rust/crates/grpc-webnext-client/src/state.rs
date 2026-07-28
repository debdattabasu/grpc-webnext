//! Connectivity state, and watching it change.
//!
//! gRPC channels expose this (`GetState` / `WaitForStateChange` in grpc-go) because a
//! reconnecting channel is otherwise a black box: an app cannot tell "the server is
//! down" from "nothing has been asked of it yet". tonic happens not to surface it;
//! a frontend needs it more than a backend does, because the answer is usually a
//! banner in the UI.

use std::cell::RefCell;
use std::rc::Rc;

use futures::channel::mpsc;
use futures::stream::Stream;

/// Where the tunnel is, using gRPC's connectivity-state vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectivityState {
    /// No tunnel, and none being opened — the state before the first call, and
    /// after one drops.
    Idle,
    /// A tunnel is being opened.
    Connecting,
    /// A tunnel is open and calls can be made.
    Ready,
    /// The last attempt to open a tunnel failed. The next call tries again.
    TransientFailure,
}

#[derive(Default)]
struct Inner {
    state: Option<ConnectivityState>,
    watchers: Vec<mpsc::UnboundedSender<ConnectivityState>>,
}

/// The current state plus a fan-out to anyone watching.
#[derive(Clone, Default)]
pub(crate) struct StateWatch {
    inner: Rc<RefCell<Inner>>,
}

impl StateWatch {
    pub(crate) fn get(&self) -> ConnectivityState {
        self.inner.borrow().state.unwrap_or(ConnectivityState::Idle)
    }

    /// Record a state. Repeats are dropped, so a watcher sees transitions rather
    /// than a stream of the same value.
    pub(crate) fn set(&self, state: ConnectivityState) {
        let mut inner = self.inner.borrow_mut();
        if inner.state == Some(state) {
            return;
        }
        inner.state = Some(state);
        // Drop watchers whose receiver is gone — otherwise a long-lived client
        // accumulates one dead sender per abandoned watcher.
        inner.watchers.retain(|w| w.unbounded_send(state).is_ok());
    }

    pub(crate) fn watch(&self) -> impl Stream<Item = ConnectivityState> + 'static {
        let (tx, rx) = mpsc::unbounded();
        self.inner.borrow_mut().watchers.push(tx);
        rx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn starts_idle() {
        assert_eq!(StateWatch::default().get(), ConnectivityState::Idle);
    }

    #[test]
    fn watchers_see_transitions_but_not_repeats() {
        futures::executor::block_on(async {
            let watch = StateWatch::default();
            let mut seen = watch.watch();

            watch.set(ConnectivityState::Connecting);
            watch.set(ConnectivityState::Connecting); // same value: not an event
            watch.set(ConnectivityState::Ready);
            watch.set(ConnectivityState::Idle);
            drop(watch);

            let all: Vec<_> = seen.by_ref().collect().await;
            assert_eq!(
                all,
                vec![
                    ConnectivityState::Connecting,
                    ConnectivityState::Ready,
                    ConnectivityState::Idle
                ]
            );
        });
    }

    #[test]
    fn a_watcher_added_later_sees_only_what_follows() {
        futures::executor::block_on(async {
            let watch = StateWatch::default();
            watch.set(ConnectivityState::Ready);
            let mut late = watch.watch();
            watch.set(ConnectivityState::Idle);
            drop(watch);

            let all: Vec<_> = late.by_ref().collect().await;
            assert_eq!(all, vec![ConnectivityState::Idle]);
        });
    }

    #[test]
    fn a_dropped_watcher_does_not_accumulate() {
        let watch = StateWatch::default();
        drop(watch.watch());
        watch.set(ConnectivityState::Connecting);
        watch.set(ConnectivityState::Ready);
        assert!(watch.inner.borrow().watchers.is_empty());
    }
}

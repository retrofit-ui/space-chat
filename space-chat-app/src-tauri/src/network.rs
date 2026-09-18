use space_chat_transport::bootstrap::TransportConfig;
use space_chat_transport::error::TransportError;
use space_chat_transport::identity::TransportIdentity;
use space_chat_transport::transport::{Transport, TransportEvent};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// At least one peer connection is currently live.
    Connected,
    /// No peer connections are currently live. This is also the initial
    /// value at bind time, before any dial has ever been attempted -- this
    /// type does NOT distinguish "never tried" from "tried and lost it." A
    /// later task's UI affordance that wants that distinction (e.g.
    /// "reconnecting..." only after a connection was lost, vs. no message at
    /// all before any dial) needs a separate signal (e.g. whether `dial` has
    /// ever been called), not this enum.
    Disconnected,
}

/// Thin app-specific wrapper around a real `space_chat_transport::Transport`.
/// Owns the `Transport` value and its raw `TransportEvent` receiver;
/// `AppState` (a later task) is the one place that actually consumes events
/// -- this type's job is just binding and exposing a `ConnectionStatus`
/// watch channel derived from `Connected`/`Disconnected` events, which is
/// cheap and independent of whatever `AppState` does with the rest of the
/// event stream.
pub struct AppNetwork {
    pub transport: Arc<Transport>,
    status_rx: watch::Receiver<ConnectionStatus>,
}

impl AppNetwork {
    /// Binds a real `iroh` endpoint. `config` is `TransportConfig { relay: None }`
    /// (real `n0` relay/discovery) in production; tests pass
    /// `TransportConfig { relay: Some((relay_map, relay_url)) }` against a
    /// local `iroh::test_utils::run_relay_server()`, exactly as every
    /// Milestone 3 test already does -- do not reintroduce a TCP stand-in.
    /// Returns the wrapper plus the raw event receiver, which the caller
    /// takes ownership of to drive its own persistence/spec-regeneration
    /// pipeline; `AppNetwork` itself only peeks at `Connected`/`Disconnected`
    /// via a `watch` channel fed by a small forwarding task, not by
    /// consuming the real receiver itself. The forwarding task exits (and
    /// `subscribe_status()` freezes at its last value) once the caller drops
    /// the returned receiver -- callers are expected to hold it for the
    /// process's lifetime.
    pub async fn bind(
        identity: &TransportIdentity,
        config: TransportConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<TransportEvent>), TransportError> {
        let (transport, mut events_rx) = Transport::bind(identity, config).await?;
        let transport = Arc::new(transport);

        // Forward Connected/Disconnected into a watch channel for cheap,
        // last-value-only status polling, while still handing the FULL
        // event stream on to the caller for everything else (IncomingChange,
        // JoinRequest) -- this requires a second channel the caller reads
        // from, since `mpsc::Receiver` has only one consumer. This function
        // creates its own forwarding task that reads `events_rx` and
        // re-sends every event onward on a fresh channel the caller gets
        // back, updating `status_tx` as a side effect for
        // `Connected`/`Disconnected` specifically.
        let (status_tx, status_rx) = watch::channel(ConnectionStatus::Disconnected);
        let (forward_tx, forward_rx) = mpsc::unbounded_channel::<TransportEvent>();
        tokio::spawn(async move {
            let mut live_connections: u32 = 0;
            while let Some(event) = events_rx.recv().await {
                match &event {
                    TransportEvent::Connected { .. } => {
                        live_connections += 1;
                        let _ = status_tx.send(ConnectionStatus::Connected);
                    }
                    TransportEvent::Disconnected { .. } => {
                        live_connections = live_connections.saturating_sub(1);
                        if live_connections == 0 {
                            let _ = status_tx.send(ConnectionStatus::Disconnected);
                        }
                    }
                    _ => {}
                }
                if forward_tx.send(event).is_err() {
                    break; // caller dropped its receiver -- nothing left to forward to
                }
            }
        });

        Ok((Self { transport, status_rx }, forward_rx))
    }

    pub fn subscribe_status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status_rx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_transport::bootstrap::TransportConfig;
    use space_chat_transport::identity::TransportIdentity;
    use std::time::Duration;

    /// Binds two real `AppNetwork`s over a local iroh test relay (mirroring
    /// `space-chat-transport/tests/multi_hop_convergence.rs`'s setup), dials
    /// one to the other, and asserts that `subscribe_status()` eventually
    /// observes `ConnectionStatus::Connected` -- proving the forwarding task
    /// really does derive connection status from the real `Transport`'s
    /// event stream rather than from a stand-in.
    #[tokio::test]
    async fn subscribe_status_observes_connected_after_real_dial() {
        let (relay_map, relay_url, _relay_server) =
            iroh::test_utils::run_relay_server().await.unwrap();

        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();

        let (alice, _alice_events) = AppNetwork::bind(
            &alice_identity,
            TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
        )
        .await
        .unwrap();
        let (bob, _bob_events) = AppNetwork::bind(
            &bob_identity,
            TransportConfig { relay: Some((relay_map, relay_url)) },
        )
        .await
        .unwrap();

        let mut alice_status = alice.subscribe_status();
        assert_eq!(*alice_status.borrow(), ConnectionStatus::Disconnected);

        alice.transport.dial(bob.transport.endpoint_addr()).await.unwrap();

        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if *alice_status.borrow() == ConnectionStatus::Connected {
                    break;
                }
                alice_status.changed().await.unwrap();
            }
        })
        .await
        .expect("alice's status should observe Connected after dialing bob");
    }
}

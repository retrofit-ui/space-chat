use crate::error::TransportError;
use crate::framing::{read_frame, write_frame};
use serde::{Deserialize, Serialize};

/// One space's sync-relevant state, per the protocol spec's sync flow step
/// 2 ("a lightweight per-space digest: epoch number + Automerge doc
/// heads"). `heads` are `automerge::ChangeHash` bytes -- kept as raw
/// `[u8; 32]` here rather than depending on `automerge` directly, since
/// this crate only ever compares/forwards them, never interprets them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpaceDigest {
    pub space_id: String,
    pub epoch: u64,
    pub heads: Vec<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlHello {
    pub digests: Vec<SpaceDigest>,
}

/// Runs the connection-level control-stream handshake: the dialing side
/// opens the stream (per the transport spec's "one connection-level
/// control stream, opened first, before any per-space stream sets exist");
/// the accepting side accepts it. Both sides then exchange their
/// `ControlHello` and learn the other's. This is the only network activity
/// that happens before either side knows which shared spaces are even
/// worth opening per-`(space_id, category)` streams for.
pub async fn exchange_digests(
    conn: &iroh::endpoint::Connection,
    is_dialer: bool,
    local: ControlHello,
) -> Result<ControlHello, TransportError> {
    let local_bytes = {
        let mut buf = Vec::new();
        ciborium::into_writer(&local, &mut buf).map_err(|e| TransportError::Codec(e.to_string()))?;
        buf
    };

    let (mut send, mut recv) = if is_dialer {
        conn.open_bi()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?
    } else {
        conn.accept_bi()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?
    };

    // Both sides write, then both sides read -- a fixed, symmetric order
    // avoids a dialer/accepter-specific deadlock (each side blocking on a
    // read the other hasn't sent yet).
    write_frame(&mut send, &local_bytes).await?;
    let remote_bytes = read_frame(&mut recv).await?;

    ciborium::from_reader(remote_bytes.as_slice()).map_err(|e| TransportError::Codec(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{bind_endpoint, TransportConfig, ALPN};
    use crate::identity::TransportIdentity;

    #[tokio::test]
    async fn both_sides_learn_the_others_digests_over_the_control_stream() {
        let (relay_map, relay_url, _relay_server) =
            iroh::test_utils::run_relay_server().await.unwrap();

        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let alice = bind_endpoint(
            &alice_identity,
            TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
        )
        .await
        .unwrap();
        let bob = bind_endpoint(
            &bob_identity,
            TransportConfig { relay: Some((relay_map, relay_url.clone())) },
        )
        .await
        .unwrap();
        // Built from the relay URL directly rather than `bob.addr()` -- see
        // bootstrap.rs's own test for why: right after `bind_endpoint`
        // returns, `bob`'s `EndpointAddr` watcher may not yet reflect relay
        // info, which empirically hangs the dial until QUIC's idle timeout.
        let bob_addr = iroh::EndpointAddr::new(bob.id()).with_relay_url(relay_url);

        let bob_digests = ControlHello {
            digests: vec![SpaceDigest { space_id: "space-1".to_string(), epoch: 0, heads: vec![[9u8; 32]] }],
        };
        let bob_digests_clone = bob_digests.clone();
        let bob_task = tokio::spawn(async move {
            let incoming = bob.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            let from_alice = exchange_digests(&conn, false, bob_digests_clone).await.unwrap();
            conn.closed().await;
            bob.close().await;
            from_alice
        });

        let alice_digests = ControlHello {
            digests: vec![SpaceDigest { space_id: "space-1".to_string(), epoch: 2, heads: vec![[1u8; 32], [2u8; 32]] }],
        };
        let conn = alice.connect(bob_addr, ALPN).await.unwrap();
        let from_bob = exchange_digests(&conn, true, alice_digests.clone()).await.unwrap();
        assert_eq!(from_bob, bob_digests);
        conn.close(0u32.into(), b"");
        alice.close().await;

        let from_alice = bob_task.await.unwrap();
        assert_eq!(from_alice, alice_digests);
    }

    #[tokio::test]
    async fn a_peer_with_zero_spaces_exchanges_an_empty_hello_without_error() {
        let (relay_map, relay_url, _relay_server) =
            iroh::test_utils::run_relay_server().await.unwrap();

        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let alice = bind_endpoint(
            &alice_identity,
            TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
        )
        .await
        .unwrap();
        let bob = bind_endpoint(
            &bob_identity,
            TransportConfig { relay: Some((relay_map, relay_url.clone())) },
        )
        .await
        .unwrap();
        let bob_addr = iroh::EndpointAddr::new(bob.id()).with_relay_url(relay_url);

        let bob_task = tokio::spawn(async move {
            let incoming = bob.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            let from_alice = exchange_digests(&conn, false, ControlHello { digests: vec![] })
                .await
                .unwrap();
            conn.closed().await;
            bob.close().await;
            from_alice
        });

        let conn = alice.connect(bob_addr, ALPN).await.unwrap();
        let from_bob = exchange_digests(&conn, true, ControlHello { digests: vec![] })
            .await
            .unwrap();
        assert_eq!(from_bob, ControlHello { digests: vec![] });
        conn.close(0u32.into(), b"");
        alice.close().await;

        let from_alice = bob_task.await.unwrap();
        assert_eq!(from_alice, ControlHello { digests: vec![] });
    }
}

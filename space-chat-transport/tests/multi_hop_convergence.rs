use space_chat_core::domain::{DeviceId, Message};
use space_chat_core::segment::Segment;
use space_chat_transport::bootstrap::TransportConfig;
use space_chat_transport::identity::TransportIdentity;
use space_chat_transport::transport::Transport;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Proves the transport spec's "multi-hop convergence for messages" claim:
/// Carol receives Alice's message purely because Bob independently syncs
/// with both of them against a *shared* local segment (Task 7's central
/// design insight) — Carol never dials, and is never given the address
/// of, Alice. No relay/forwarding code exists anywhere for this to work;
/// this test is the proof that none is needed.
///
/// Note on why Alice and Carol truly have no path to each other besides
/// Bob: `TransportConfig { relay: Some(..) }` (used by every peer here)
/// builds the underlying `iroh::Endpoint` on `presets::Minimal` (see
/// `bootstrap.rs`), which disables both the relay-based rendezvous *and*
/// n0's production pkarr/DNS discovery entirely. There is no discovery
/// mechanism active here through which Carol could ever learn Alice's
/// `EndpointAddr` on her own — the only way two of these peers connect is
/// an explicit `dial(addr)` call, and this test only ever calls
/// `alice.dial(bob..)` and `carol.dial(bob..)`.
#[tokio::test]
async fn carol_receives_alices_message_via_bob_without_ever_connecting_to_alice() {
    let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

    let alice_identity = TransportIdentity::generate();
    let bob_identity = TransportIdentity::generate();
    let carol_identity = TransportIdentity::generate();

    let (alice, _alice_events) = Transport::bind(
        &alice_identity,
        TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
    )
    .await
    .unwrap();
    let (bob, _bob_events) = Transport::bind(
        &bob_identity,
        TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
    )
    .await
    .unwrap();
    let (carol, _carol_events) =
        Transport::bind(&carol_identity, TransportConfig { relay: Some((relay_map, relay_url)) })
            .await
            .unwrap();

    let alice_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
    let bob_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
    let carol_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
    alice.add_space("space-1", 0, alice_segment.clone()).await;
    bob.add_space("space-1", 0, bob_segment.clone()).await;
    carol.add_space("space-1", 0, carol_segment.clone()).await;

    // Topology: alice<->bob, bob<->carol. Alice and Carol never dial each
    // other, and Carol is never handed alice's EndpointAddr at all.
    alice.dial(bob.endpoint_addr()).await.unwrap();
    carol.dial(bob.endpoint_addr()).await.unwrap();

    alice_segment.lock().await.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "hello from alice, three hops of trust but zero hops of relay code".to_string(),
        attachments: vec![],
    });
    alice.notify_local_change("space-1").await;

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if carol_segment.lock().await.message_count() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("carol should converge on alice's message via bob within the timeout");
}

use serde::{Deserialize, Serialize};
use space_chat_core::domain::DeviceId;

/// A future MLS integration (`space-chat-openmls`, not built yet — see
/// this plan's Architecture note) implements this for real, backed by
/// actual MLS group state. Until then, tests use an in-memory fake. This
/// crate never calls `members`/`endpoint_for` for any purpose other than
/// `elect_sequencer` routing (see `handle_join_request` in `transport.rs`).
pub trait SpaceMembership: Send + Sync {
    fn members(&self, space_id: &str) -> Vec<DeviceId>;
    fn endpoint_for(&self, device: DeviceId) -> Option<iroh::EndpointId>;
}

/// Carried opaquely end-to-end: this crate never inspects `payload`. A real
/// integration fills it with an MLS `KeyPackage`/proposal; here it is
/// exactly the bytes the joiner passed to `join_via_invite`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinRequest {
    pub space_id: String,
    pub joiner_endpoint_id: [u8; 32],
    pub payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::TransportConfig;
    use crate::identity::TransportIdentity;
    use crate::transport::{Transport, TransportEvent};
    use space_chat_core::domain::DeviceId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    struct FakeMembership {
        members: Vec<DeviceId>,
        endpoints: StdMutex<HashMap<DeviceId, iroh::EndpointId>>,
    }

    impl SpaceMembership for FakeMembership {
        fn members(&self, _space_id: &str) -> Vec<DeviceId> {
            self.members.clone()
        }
        fn endpoint_for(&self, device: DeviceId) -> Option<iroh::EndpointId> {
            self.endpoints.lock().unwrap().get(&device).copied()
        }
    }

    #[tokio::test]
    async fn a_join_request_to_a_non_sequencer_inviter_is_forwarded_to_the_elected_sequencer() {
        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

        // Device A has the lowest DeviceId -- it is the elected sequencer.
        let device_a = DeviceId([1u8; 32]);
        let device_b = DeviceId([2u8; 32]);

        let a_identity = TransportIdentity::generate();
        let b_identity = TransportIdentity::generate();
        let joiner_identity = TransportIdentity::generate();

        let (a, mut a_events) = Transport::bind(&a_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let (b, _b_events) = Transport::bind(&b_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let (joiner, _joiner_events) = Transport::bind(&joiner_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();

        let endpoints = StdMutex::new(HashMap::from([
            (device_a, a.endpoint_id()),
            (device_b, b.endpoint_id()),
        ]));
        let membership: Arc<dyn SpaceMembership> = Arc::new(FakeMembership {
            members: vec![device_a, device_b],
            endpoints,
        });

        a.configure_membership(device_a, membership.clone()).await;
        b.configure_membership(device_b, membership).await;

        // B (not the sequencer) generates the invite the joiner uses.
        let invite = b.generate_invite("space-1", "nonce-1");

        joiner.join_via_invite(&invite, b"opaque key package bytes".to_vec()).await.unwrap();

        let request = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match a_events.recv().await {
                    Some(TransportEvent::JoinRequest(req)) => return req,
                    Some(_) => continue,
                    None => panic!("a's event channel closed before a JoinRequest arrived"),
                }
            }
        })
        .await
        .expect("device A, the elected sequencer, should receive the forwarded join request");

        assert_eq!(request.space_id, "space-1");
        assert_eq!(request.joiner_endpoint_id, *joiner.endpoint_id().as_bytes());
        assert_eq!(request.payload, b"opaque key package bytes".to_vec());
    }

    /// Self-review-driven addition (not in the task brief's own test list):
    /// checks point 5 of Task 10's self-review checklist -- a join request
    /// landing on a peer that never called `configure_membership` at all
    /// (so it has no `SpaceMembership` source to resolve `elect_sequencer`
    /// with, for ANY space_id) must be dropped cleanly by
    /// `handle_join_request`'s early `let Some(...) = ... else { return }`,
    /// not hang or panic. `join_via_invite` itself is fire-and-forget (see
    /// its own doc comment: it returns once the frame is written, without
    /// waiting for any response), so the risk this guards against is
    /// entirely on the INVITER's spawned `handle_join_request` task -- both
    /// calls are wrapped in bounded timeouts so a regression here fails
    /// this test cleanly instead of hanging the whole suite.
    #[tokio::test]
    async fn join_request_to_a_peer_with_no_membership_configured_is_dropped_cleanly() {
        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

        let inviter_identity = TransportIdentity::generate();
        let joiner_identity = TransportIdentity::generate();
        let (inviter, mut inviter_events) = Transport::bind(&inviter_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let (joiner, _joiner_events) = Transport::bind(&joiner_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();

        // Deliberately never calls `inviter.configure_membership(..)`.
        let invite = inviter.generate_invite("space-nobody-tracks", "nonce-2");

        tokio::time::timeout(
            Duration::from_secs(10),
            joiner.join_via_invite(&invite, b"key package".to_vec()),
        )
        .await
        .expect("join_via_invite should not hang even though the inviter has no membership configured")
        .expect(
            "join_via_invite is fire-and-forget -- it succeeds once the frame is written, \
             regardless of how the inviter's handler ends up treating it",
        );

        let saw_join_request = tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                match inviter_events.recv().await {
                    Some(TransportEvent::JoinRequest(_)) => return true,
                    Some(_) => continue,
                    None => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            !saw_join_request,
            "an inviter with no membership configured should never surface a JoinRequest event"
        );
    }

    /// Self-review-driven addition: the same check as above, but for a peer
    /// that HAS called `configure_membership`, just with a
    /// `SpaceMembership` source that returns an empty `members()` list for
    /// the requested `space_id` (e.g. a space this device doesn't actually
    /// track). `elect_sequencer` returns `None` for an empty slice (see
    /// `space_chat_core::sequencer`), which `handle_join_request` must also
    /// treat as a clean drop, not a panic on `.unwrap()`-ing `None`.
    #[tokio::test]
    async fn join_request_for_a_space_with_no_known_members_is_dropped_cleanly() {
        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

        let inviter_identity = TransportIdentity::generate();
        let joiner_identity = TransportIdentity::generate();
        let (inviter, mut inviter_events) = Transport::bind(&inviter_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let (joiner, _joiner_events) = Transport::bind(&joiner_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();

        let membership: Arc<dyn SpaceMembership> = Arc::new(FakeMembership {
            members: vec![], // no known members for any space
            endpoints: StdMutex::new(HashMap::new()),
        });
        inviter.configure_membership(DeviceId([9u8; 32]), membership).await;

        let invite = inviter.generate_invite("space-nobody-tracks", "nonce-3");

        tokio::time::timeout(
            Duration::from_secs(10),
            joiner.join_via_invite(&invite, b"key package".to_vec()),
        )
        .await
        .expect("join_via_invite should not hang")
        .expect("join_via_invite is fire-and-forget and should still succeed");

        let saw_join_request = tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                match inviter_events.recv().await {
                    Some(TransportEvent::JoinRequest(_)) => return true,
                    Some(_) => continue,
                    None => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            !saw_join_request,
            "a space with no known members (elect_sequencer returns None) should never surface a JoinRequest event"
        );
    }
}

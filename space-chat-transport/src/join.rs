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
    /// Critical #2 fix: incremented by `handle_join_request` (in
    /// `transport.rs`) each time this request is forwarded on to a newly
    /// elected sequencer. Starts at `0` from `Transport::join_via_invite`.
    /// The design assumes every peer computes the same sequencer for a
    /// given space via `elect_sequencer`, so a request should normally
    /// converge in one hop -- but two devices at different MLS-state
    /// epochs can have different `members()` views and each elect the
    /// OTHER as sequencer, forwarding the same request back and forth
    /// forever. `handle_join_request` drops (rather than forwards) any
    /// request whose `hops` has already reached `JoinRequest::MAX_HOPS`,
    /// bounding the worst case.
    pub hops: u8,
}

impl JoinRequest {
    /// Upper bound on how many times a `JoinRequest` may be forwarded
    /// between peers before it is dropped instead of forwarded further.
    /// This is a P2P invite-join path (normally converging in a single
    /// hop), not a general message-routing protocol, so a small constant
    /// cap is generous headroom for the inconsistent-membership-views case
    /// while still guaranteeing termination.
    pub const MAX_HOPS: u8 = 3;
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

    /// Critical #2 regression test (hop-count cap): the design assumes every
    /// peer computes the same sequencer for a space via `elect_sequencer`,
    /// so a normal join request converges in one forwarding hop. Two
    /// devices with inconsistent membership views could otherwise forward
    /// the same request back and forth forever. Rather than actually
    /// constructing such a loop (which would require racing two diverging
    /// `SpaceMembership` views and risks flakiness), this test proves the
    /// termination mechanism directly: a `JoinRequest` already at
    /// `JoinRequest::MAX_HOPS` must be DROPPED by a non-sequencer peer, not
    /// forwarded on. `Transport::join_via_invite` always starts a fresh
    /// request at `hops: 0`, so this test bypasses it and speaks the wire
    /// protocol directly (mirroring the pattern `transport.rs`'s own
    /// `dialer_reports_disconnected_only_after_the_peer_actually_disconnects`
    /// test uses for a raw, non-`Transport` peer) in order to deliver a
    /// `JoinRequest` whose `hops` field is already at the cap.
    #[tokio::test]
    async fn a_join_request_at_the_hop_cap_is_dropped_instead_of_forwarded() {
        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

        // Device A has the lowest DeviceId -- it is the elected sequencer.
        // Device B is deliberately NOT the sequencer, so absent the hop-cap
        // fix it would forward the request on to A.
        let device_a = DeviceId([1u8; 32]);
        let device_b = DeviceId([2u8; 32]);

        let a_identity = TransportIdentity::generate();
        let b_identity = TransportIdentity::generate();

        let (a, mut a_events) = Transport::bind(&a_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let (b, _b_events) = Transport::bind(&b_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();

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

        // A raw endpoint (not wrapped in a `Transport`) plays the role of a
        // peer delivering a `JoinRequest` directly to B, over the same wire
        // protocol `join_via_invite` uses, but with `hops` already at
        // `JoinRequest::MAX_HOPS` -- something `join_via_invite` itself can
        // never produce (it always starts at `hops: 0`).
        let raw_identity = TransportIdentity::generate();
        let raw_endpoint = crate::bootstrap::bind_endpoint(
            &raw_identity,
            TransportConfig { relay: Some((relay_map, relay_url)) },
        )
        .await
        .unwrap();
        let conn = raw_endpoint
            .connect(b.endpoint_addr(), crate::bootstrap::ALPN)
            .await
            .unwrap();
        // Speak just enough of the control-stream digest handshake (Task 5)
        // to unblock B's accepter-side `run_connection` past
        // `exchange_digests`, exactly like `join_via_invite`'s own
        // connections do under the hood.
        crate::control::exchange_digests(&conn, true, crate::control::ControlHello { digests: vec![] })
            .await
            .unwrap();

        let manager = crate::streams::StreamManager::new(conn);
        let mut handle = manager
            .open("space-1", crate::envelope::Category::MlsControl)
            .await
            .unwrap();
        let request = JoinRequest {
            space_id: "space-1".to_string(),
            joiner_endpoint_id: [7u8; 32],
            payload: b"key package".to_vec(),
            hops: JoinRequest::MAX_HOPS, // already at the cap
        };
        let mut bytes = Vec::new();
        ciborium::into_writer(&request, &mut bytes).unwrap();
        crate::framing::write_frame(&mut handle.send, &bytes).await.unwrap();

        let saw_join_request = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match a_events.recv().await {
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
            "a JoinRequest already at JoinRequest::MAX_HOPS should be dropped by B (a non-sequencer), \
             not forwarded on to A (the elected sequencer)"
        );
    }

    /// Important I1 regression test: the stream envelope's `space_id` (used
    /// to look up membership / elect the sequencer) and the `JoinRequest`
    /// payload's own `space_id` field are two independently peer-controlled
    /// values. A peer could set the envelope's `space_id` to name one space
    /// (so routing/elect_sequencer uses that space's membership) while the
    /// payload names a different one. This test delivers exactly that
    /// mismatch directly to the elected sequencer (so it isn't even a
    /// forwarding case) and confirms the request is dropped cleanly rather
    /// than surfaced as a `TransportEvent::JoinRequest`.
    #[tokio::test]
    async fn a_join_request_with_mismatched_envelope_and_payload_space_id_is_dropped() {
        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

        let device_a = DeviceId([1u8; 32]);
        let a_identity = TransportIdentity::generate();
        let (a, mut a_events) = Transport::bind(&a_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();

        let endpoints = StdMutex::new(HashMap::from([(device_a, a.endpoint_id())]));
        let membership: Arc<dyn SpaceMembership> = Arc::new(FakeMembership {
            members: vec![device_a],
            endpoints,
        });
        a.configure_membership(device_a, membership).await;

        let raw_identity = TransportIdentity::generate();
        let raw_endpoint = crate::bootstrap::bind_endpoint(
            &raw_identity,
            TransportConfig { relay: Some((relay_map, relay_url)) },
        )
        .await
        .unwrap();
        let conn = raw_endpoint
            .connect(a.endpoint_addr(), crate::bootstrap::ALPN)
            .await
            .unwrap();
        crate::control::exchange_digests(&conn, true, crate::control::ControlHello { digests: vec![] })
            .await
            .unwrap();

        let manager = crate::streams::StreamManager::new(conn);
        // The stream ENVELOPE names "space-envelope" -- this is what
        // `handle_join_request` uses to look up membership and elect the
        // sequencer (device A, above, is the sole/elected sequencer for
        // every space_id `FakeMembership` is asked about).
        let mut handle = manager
            .open("space-envelope", crate::envelope::Category::MlsControl)
            .await
            .unwrap();
        // The PAYLOAD names a different space, "space-payload".
        let request = JoinRequest {
            space_id: "space-payload".to_string(),
            joiner_endpoint_id: [7u8; 32],
            payload: b"key package".to_vec(),
            hops: 0,
        };
        let mut bytes = Vec::new();
        ciborium::into_writer(&request, &mut bytes).unwrap();
        crate::framing::write_frame(&mut handle.send, &bytes).await.unwrap();

        let saw_join_request = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match a_events.recv().await {
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
            "a JoinRequest whose payload space_id disagrees with its stream envelope's space_id \
             must be dropped, not surfaced as a JoinRequest event"
        );
    }
}

use space_chat_core::domain::{DeviceId, Message};
use space_chat_core::segment::{sync_state, Segment};
use space_chat_core::sequencer::elect_sequencer;

fn send(segment: &mut Segment, sender: DeviceId, content: &str) {
    segment.append_message(&Message {
        sender,
        content: content.to_string(),
        attachments: vec![],
    });
}

fn sync_pair(
    a: &mut Segment,
    a_state: &mut automerge::sync::State,
    b: &mut Segment,
    b_state: &mut automerge::sync::State,
) {
    loop {
        let a_to_b = a.generate_sync_message(a_state);
        let b_to_a = b.generate_sync_message(b_state);
        let (a_done, b_done) = (a_to_b.is_none(), b_to_a.is_none());
        if let Some(msg) = a_to_b {
            b.receive_sync_message(b_state, msg).unwrap();
        }
        if let Some(msg) = b_to_a {
            a.receive_sync_message(a_state, msg).unwrap();
        }
        if a_done && b_done {
            break;
        }
    }
}

#[test]
fn three_members_converge_after_partition_and_reconnect() {
    let alice_id = DeviceId([1u8; 32]);
    let bob_id = DeviceId([2u8; 32]);
    let carol_id = DeviceId([3u8; 32]);

    // Sanity check on Task 5's sequencer logic in a 3-member context —
    // this is what the protocol spec's membership-change routing depends on.
    assert_eq!(elect_sequencer(&[alice_id, bob_id, carol_id]), alice_id);

    let mut alice = Segment::new();
    let mut bob = Segment::new();
    let mut carol = Segment::new();

    // IMPORTANT: `automerge::sync::State` tracks one *directed peer
    // relationship*, not "this segment's sync state" in general -- see the
    // doc comment on `segment::sync_state`: "A Segment needs one sync::State
    // per remote peer it exchanges sync messages with." With three members
    // there are three peer relationships (alice<->bob, bob<->carol,
    // alice<->carol), and each side of each relationship needs its own
    // state -- six states total, not three.
    //
    // An earlier version of this test used only three states (one per
    // device, e.g. a single `b_state` for both of Bob's relationships) on
    // the assumption that a `sync::State` belongs to a device rather than to
    // a specific peer pairing. That reuse silently broke convergence: after
    // syncing Bob with Alice, Bob's shared state carried Alice's "already
    // seen" heads into the *unrelated* Bob<->Carol relationship, causing
    // `generate_sync_message` to wrongly conclude Carol already had
    // messages she'd never actually received. Carol's message count stayed
    // at 0 through every resync attempt. Giving every directed pairing its
    // own state (as below) fixes it -- this was a mistake in the test's use
    // of the sync API, not a bug in `Segment`/`elect_sequencer`.
    let mut alice_bob_a = sync_state();
    let mut alice_bob_b = sync_state();
    let mut bob_carol_b = sync_state();
    let mut bob_carol_c = sync_state();
    let mut alice_carol_a = sync_state();
    let mut alice_carol_c = sync_state();

    // All three start in sync.
    sync_pair(&mut alice, &mut alice_bob_a, &mut bob, &mut alice_bob_b);
    sync_pair(&mut bob, &mut bob_carol_b, &mut carol, &mut bob_carol_c);
    sync_pair(
        &mut alice,
        &mut alice_carol_a,
        &mut carol,
        &mut alice_carol_c,
    );

    // Simulate a partition: Alice, Bob, and Carol all send messages that others don't see yet.
    send(&mut alice, alice_id, "from alice during partition");
    send(&mut bob, bob_id, "from bob during partition");
    send(&mut carol, carol_id, "from carol during partition");

    // Reconnect: sync every pair until all three match.
    sync_pair(&mut alice, &mut alice_bob_a, &mut bob, &mut alice_bob_b);
    sync_pair(&mut bob, &mut bob_carol_b, &mut carol, &mut bob_carol_c);
    sync_pair(
        &mut alice,
        &mut alice_carol_a,
        &mut carol,
        &mut alice_carol_c,
    );
    // One more pass, since Carol's new state from Bob may need to reach Alice.
    sync_pair(
        &mut alice,
        &mut alice_carol_a,
        &mut carol,
        &mut alice_carol_c,
    );

    assert_eq!(alice.message_count(), 3);
    assert_eq!(bob.message_count(), 3);
    assert_eq!(carol.message_count(), 3);

    // Matching counts alone would also pass if each peer somehow ended up
    // with a different set of 2 messages (e.g. a lost message masked by a
    // duplicate). Since every message lives at its own unique "msg:<uuid>"
    // key (see the `Segment` doc comment), comparing the sorted key sets
    // across all three peers is a stronger, still public-API-only check
    // that they hold *the same* messages, not just the same count --
    // directly exercising the milestone's "identical conversation state"
    // exit criterion.
    let mut alice_keys: Vec<String> = alice.message_keys().collect();
    let mut bob_keys: Vec<String> = bob.message_keys().collect();
    let mut carol_keys: Vec<String> = carol.message_keys().collect();
    alice_keys.sort();
    bob_keys.sort();
    carol_keys.sort();
    assert_eq!(alice_keys, bob_keys);
    assert_eq!(bob_keys, carol_keys);
}

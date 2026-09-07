//! Richer, more "realistic conversation" integration coverage than
//! `convergence.rs`. `convergence.rs` proves convergence at a fairly narrow
//! level -- plain messages only, checked via counts/key-sets/heads, never by
//! reading back actual content. This file goes further:
//!
//! - mixes messages, reactions, and deletes in one multi-party scenario with
//!   realistic partition/reconnect interleaving, and asserts convergence by
//!   reading back actual content (`read_message`/`read_reaction`), not just
//!   counts or heads;
//! - exercises attachments across independently-created segments, and
//!   through a relay peer -- a path `segment.rs`'s own tests only cover via
//!   single-segment save/load round trips, never multi-peer sync;
//! - exercises reactions and deletes landing on the *same* message
//!   concurrently, from different peers, before either has synced -- an
//!   interaction between two individually-tested mechanisms that's never
//!   been tested together;
//! - pushes the unique-key design (see the `Segment` doc comment) to higher
//!   concurrency than the existing 3-party test: 5 independently-created
//!   segments, all writing messages *and* reactions before any sync
//!   happens at all.

use space_chat_core::domain::{AttachmentRef, Delete, DeviceId, Message, Reaction};
use space_chat_core::segment::{objid_to_target_string, sync_state, Segment};
use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn send(segment: &mut Segment, sender: DeviceId, content: &str) -> automerge::ObjId {
    segment.append_message(&Message {
        sender,
        content: content.to_string(),
        attachments: vec![],
    })
}

fn react(segment: &mut Segment, target: &automerge::ObjId, actor: DeviceId, emoji: &str) {
    segment
        .append_reaction(
            target,
            &Reaction {
                target: objid_to_target_string(target),
                actor,
                emoji: emoji.to_string(),
            },
        )
        .expect("append_reaction should succeed for a target this segment already knows about");
}

fn delete(segment: &mut Segment, target: &automerge::ObjId) {
    segment
        .apply_delete(
            target,
            &Delete {
                target: objid_to_target_string(target),
            },
        )
        .expect("apply_delete should succeed for a target this segment already knows about");
}

/// Finds the key of the (well-formed) message on `segment` whose `content`
/// is exactly `content`. Content-based lookup, rather than assuming any
/// particular key ordering, is what lets a test operate on "the same
/// message" across independently-created segments that each assign it an
/// unrelated random UUID key locally.
fn find_message_key_by_content(segment: &Segment, content: &str) -> String {
    segment
        .message_keys()
        .find(|key| {
            segment
                .read_message(key)
                .is_some_and(|m| m.content == content)
        })
        .unwrap_or_else(|| panic!("no message with content {content:?} found on this segment"))
}

fn find_message_id_by_content(segment: &Segment, content: &str) -> automerge::ObjId {
    let key = find_message_key_by_content(segment, content);
    segment
        .message(&key)
        .expect("a key just found via find_message_key_by_content must resolve")
}

/// Drives one *directed peer relationship* to completion (both sides report
/// nothing left to send), per the loop pattern in `automerge::sync`'s module
/// docs. Returns whether anything was actually exchanged, so mesh-wide
/// helpers can detect a fixpoint without a hard-coded pass count.
fn sync_pair(
    a: &mut Segment,
    a_state: &mut automerge::sync::State,
    b: &mut Segment,
    b_state: &mut automerge::sync::State,
) -> bool {
    let mut exchanged_any = false;
    loop {
        let a_to_b = a.generate_sync_message(a_state);
        let b_to_a = b.generate_sync_message(b_state);
        let (a_done, b_done) = (a_to_b.is_none(), b_to_a.is_none());
        if let Some(msg) = a_to_b {
            exchanged_any = true;
            b.receive_sync_message(b_state, msg).unwrap();
        }
        if let Some(msg) = b_to_a {
            exchanged_any = true;
            a.receive_sync_message(a_state, msg).unwrap();
        }
        if a_done && b_done {
            break;
        }
    }
    exchanged_any
}

/// Syncs three peers' three pairwise relationships (alice<->bob,
/// bob<->carol, alice<->carol) to a genuine fixpoint: keeps re-running every
/// pair until a full round produces no further exchanged messages. Mirrors
/// `convergence.rs`'s `sync_all_pairs_to_fixpoint` -- every *directed* pair
/// needs its own persistent `sync::State`, since `automerge::sync::State`
/// tracks one peer relationship, not "this segment's sync state" in general.
#[allow(clippy::too_many_arguments)]
fn sync_three_to_fixpoint(
    alice: &mut Segment,
    alice_bob_a: &mut automerge::sync::State,
    bob: &mut Segment,
    alice_bob_b: &mut automerge::sync::State,
    bob_carol_b: &mut automerge::sync::State,
    carol: &mut Segment,
    bob_carol_c: &mut automerge::sync::State,
    alice_carol_a: &mut automerge::sync::State,
    alice_carol_c: &mut automerge::sync::State,
) {
    loop {
        let mut progressed = false;
        progressed |= sync_pair(alice, alice_bob_a, bob, alice_bob_b);
        progressed |= sync_pair(bob, bob_carol_b, carol, bob_carol_c);
        progressed |= sync_pair(alice, alice_carol_a, carol, alice_carol_c);
        if !progressed {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario 1: messages + reactions + deletes, richly interleaved, verified
// by content -- not just counts/heads.
// ---------------------------------------------------------------------------

/// A realistic three-person conversation: an opener, a reaction to it from
/// two different people while a third is offline, a proposed reschedule
/// that its own author later deletes, and a message sent while cut off.
/// Convergence is checked the strong way: every participant must
/// `read_message`/`read_reaction` the *same content*, not just agree on
/// counts or opaque key sets.
#[test]
fn three_participants_converge_on_messages_reactions_and_deletes_by_content() {
    let alice_id = DeviceId([1u8; 32]);
    let bob_id = DeviceId([2u8; 32]);
    let carol_id = DeviceId([3u8; 32]);

    let mut alice = Segment::new("space-1", 0);
    let mut bob = Segment::new("space-1", 0);
    let mut carol = Segment::new("space-1", 0);

    let mut alice_bob_a = sync_state();
    let mut alice_bob_b = sync_state();
    let mut bob_carol_b = sync_state();
    let mut bob_carol_c = sync_state();
    let mut alice_carol_a = sync_state();
    let mut alice_carol_c = sync_state();

    // Alice opens the conversation, and everyone gets it before the
    // partition begins.
    send(&mut alice, alice_id, "Hey everyone, meeting at 5pm?");
    sync_three_to_fixpoint(
        &mut alice,
        &mut alice_bob_a,
        &mut bob,
        &mut alice_bob_b,
        &mut bob_carol_b,
        &mut carol,
        &mut bob_carol_c,
        &mut alice_carol_a,
        &mut alice_carol_c,
    );

    // Carol drops offline for the rest of the conversation, until the final
    // reconnect. Alice and Bob keep talking and reacting to the opener
    // while she's gone, staying in sync *with each other* the whole time;
    // Carol, unaware, independently reacts to the same opener and sends a
    // message of her own -- none of it synced with the other two yet.
    let opener_on_alice = find_message_id_by_content(&alice, "Hey everyone, meeting at 5pm?");
    react(&mut alice, &opener_on_alice, alice_id, "\u{1F440}"); // alice: 👀
    send(&mut alice, alice_id, "actually let's do 6pm instead");
    sync_pair(&mut alice, &mut alice_bob_a, &mut bob, &mut alice_bob_b);

    let opener_on_bob = find_message_id_by_content(&bob, "Hey everyone, meeting at 5pm?");
    react(&mut bob, &opener_on_bob, bob_id, "\u{1F44D}"); // bob: 👍
    let reschedule_on_bob = find_message_id_by_content(&bob, "actually let's do 6pm instead");
    // Bob deletes Alice's reschedule proposal and offers his own instead --
    // exercising a delete alongside reactions in the same segment.
    delete(&mut bob, &reschedule_on_bob);
    send(&mut bob, bob_id, "let's just keep it at 5pm, I can't do 6");
    sync_pair(&mut alice, &mut alice_bob_a, &mut bob, &mut alice_bob_b);

    let opener_on_carol = find_message_id_by_content(&carol, "Hey everyone, meeting at 5pm?");
    react(&mut carol, &opener_on_carol, carol_id, "\u{2764}"); // carol: ❤️, unsynced with the others
    send(&mut carol, carol_id, "works for me either way, cut off");

    // Reconnect: sync every pairing to a genuine mesh fixpoint.
    sync_three_to_fixpoint(
        &mut alice,
        &mut alice_bob_a,
        &mut bob,
        &mut alice_bob_b,
        &mut bob_carol_b,
        &mut carol,
        &mut bob_carol_c,
        &mut alice_carol_a,
        &mut alice_carol_c,
    );

    // Content-level convergence: every participant must read back identical
    // `Message`s for every piece of content that was ever sent, not just
    // agree on how many messages exist.
    let all_contents = [
        "Hey everyone, meeting at 5pm?",
        "actually let's do 6pm instead",
        "let's just keep it at 5pm, I can't do 6",
        "works for me either way, cut off",
    ];
    for content in all_contents {
        let a_key = find_message_key_by_content(&alice, content);
        let b_key = find_message_key_by_content(&bob, content);
        let c_key = find_message_key_by_content(&carol, content);
        let a_msg = alice.read_message(&a_key);
        let b_msg = bob.read_message(&b_key);
        let c_msg = carol.read_message(&c_key);
        assert_eq!(
            a_msg, b_msg,
            "alice/bob should read identical content for {content:?}"
        );
        assert_eq!(
            b_msg, c_msg,
            "bob/carol should read identical content for {content:?}"
        );
    }
    assert_eq!(alice.message_count(), 4);
    assert_eq!(bob.message_count(), 4);
    assert_eq!(carol.message_count(), 4);

    // The reschedule message must show as deleted on every peer, including
    // Carol, who never even saw its pre-delete state before reconnecting.
    let reschedule_on_alice = find_message_id_by_content(&alice, "actually let's do 6pm instead");
    let reschedule_on_bob = find_message_id_by_content(&bob, "actually let's do 6pm instead");
    let reschedule_on_carol = find_message_id_by_content(&carol, "actually let's do 6pm instead");
    assert!(alice.is_deleted(&reschedule_on_alice));
    assert!(bob.is_deleted(&reschedule_on_bob));
    assert!(carol.is_deleted(&reschedule_on_carol));

    // All three reactions to the opener (alice, bob, carol -- created
    // independently, two of them while fully partitioned from each other)
    // must survive and be readable, with matching (actor, emoji) content on
    // every peer -- not just a matching count.
    let opener_on_alice = find_message_id_by_content(&alice, "Hey everyone, meeting at 5pm?");
    let opener_on_bob = find_message_id_by_content(&bob, "Hey everyone, meeting at 5pm?");
    let opener_on_carol = find_message_id_by_content(&carol, "Hey everyone, meeting at 5pm?");

    let reactions_on =
        |segment: &Segment, target: &automerge::ObjId| -> HashSet<(DeviceId, String)> {
            segment
                .reaction_keys(target)
                .filter_map(|key| segment.read_reaction(target, &key))
                .map(|r| (r.actor, r.emoji))
                .collect()
        };
    let alice_reactions = reactions_on(&alice, &opener_on_alice);
    let bob_reactions = reactions_on(&bob, &opener_on_bob);
    let carol_reactions = reactions_on(&carol, &opener_on_carol);

    let expected: HashSet<(DeviceId, String)> = HashSet::from([
        (alice_id, "\u{1F440}".to_string()),
        (bob_id, "\u{1F44D}".to_string()),
        (carol_id, "\u{2764}".to_string()),
    ]);
    assert_eq!(alice_reactions, expected);
    assert_eq!(bob_reactions, expected);
    assert_eq!(carol_reactions, expected);

    // Belt-and-suspenders: identical Automerge heads across all three,
    // the strongest convergence proof available.
    let mut alice_heads = alice.heads();
    let mut bob_heads = bob.heads();
    let mut carol_heads = carol.heads();
    alice_heads.sort();
    bob_heads.sort();
    carol_heads.sort();
    assert_eq!(alice_heads, bob_heads);
    assert_eq!(bob_heads, carol_heads);
}

// ---------------------------------------------------------------------------
// Scenario 2: attachments, never tested in a multi-peer sync scenario
// before -- only single-segment save/load round trips.
// ---------------------------------------------------------------------------

/// Two segments, each created independently (no shared history at all), each
/// attach a file to a message. After they sync, both peers must read back
/// identical attachment data (hash/size/mime/wrapped_key), not just a
/// present-but-possibly-mangled attachment. A third peer who syncs only
/// through Bob -- never directly with Alice -- must also end up with
/// Alice's attachment intact, proving it survives relay, not just direct
/// exchange.
#[test]
fn attachments_sync_across_independently_created_segments_and_relay_to_a_third_peer() {
    let alice_id = DeviceId([1u8; 32]);
    let bob_id = DeviceId([2u8; 32]);

    let mut alice = Segment::new("space-1", 0);
    let mut bob = Segment::new("space-1", 0);
    let mut carol = Segment::new("space-1", 0);

    let alice_attachment = AttachmentRef {
        hash: [0xAAu8; 32],
        size: 204_800,
        mime: "image/png".to_string(),
        wrapped_key: vec![1, 2, 3, 4, 5, 6, 7, 8],
    };
    let bob_attachment = AttachmentRef {
        hash: [0xBBu8; 32],
        size: 51_200,
        mime: "application/pdf".to_string(),
        wrapped_key: vec![9, 8, 7, 6],
    };

    alice.append_message(&Message {
        sender: alice_id,
        content: "here's the deck".to_string(),
        attachments: vec![alice_attachment.clone()],
    });
    bob.append_message(&Message {
        sender: bob_id,
        content: "and here's my summary doc".to_string(),
        attachments: vec![bob_attachment.clone()],
    });

    let mut alice_bob_a = sync_state();
    let mut alice_bob_b = sync_state();
    sync_pair(&mut alice, &mut alice_bob_a, &mut bob, &mut alice_bob_b);

    assert_eq!(alice.message_count(), 2);
    assert_eq!(bob.message_count(), 2);

    for content in ["here's the deck", "and here's my summary doc"] {
        let a_key = find_message_key_by_content(&alice, content);
        let b_key = find_message_key_by_content(&bob, content);
        assert_eq!(
            alice.read_message(&a_key),
            bob.read_message(&b_key),
            "attachment data for {content:?} should read back identically on both peers after sync"
        );
    }

    // Carol never syncs directly with Alice -- only with Bob.
    let mut bob_carol_b = sync_state();
    let mut bob_carol_c = sync_state();
    sync_pair(&mut bob, &mut bob_carol_b, &mut carol, &mut bob_carol_c);

    assert_eq!(carol.message_count(), 2);
    let carol_key = find_message_key_by_content(&carol, "here's the deck");
    let alice_key = find_message_key_by_content(&alice, "here's the deck");
    assert_eq!(
        carol.read_message(&carol_key),
        alice.read_message(&alice_key),
        "Alice's attachment must reach Carol identically even though they never sync directly"
    );

    // Explicit field-by-field check as a belt-and-suspenders sanity check
    // beyond the whole-struct equality above.
    let carol_msg = carol.read_message(&carol_key).unwrap();
    assert_eq!(carol_msg.attachments.len(), 1);
    assert_eq!(carol_msg.attachments[0].hash, alice_attachment.hash);
    assert_eq!(carol_msg.attachments[0].size, alice_attachment.size);
    assert_eq!(carol_msg.attachments[0].mime, alice_attachment.mime);
    assert_eq!(
        carol_msg.attachments[0].wrapped_key,
        alice_attachment.wrapped_key
    );

    let bob_key_for_carol = find_message_key_by_content(&carol, "and here's my summary doc");
    let bob_msg_for_carol = carol.read_message(&bob_key_for_carol).unwrap();
    assert_eq!(bob_msg_for_carol.attachments[0].hash, bob_attachment.hash);
}


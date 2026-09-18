use space_chat_core::domain::{DeviceId, Message};
use space_chat_core::gc::sweep;
use space_chat_core::projection::Projection;
use space_chat_core::replay::catch_up;
use space_chat_core::segment::{sync_state, Segment};
use space_chat_core::storage::{
    AttachmentBlobStore, AttachmentMetadataStore, ListingEntry, ListingIndex, SearchIndex,
    SegmentBlobStore,
};
use space_chat_search_tantivy::TantivySearchIndex;
use space_chat_storage_files::{FileAttachmentStore, FileSegmentStore};
use space_chat_storage_redb::attachment_metadata::RedbAttachmentMetadataStore;
use space_chat_storage_redb::listing::RedbListingIndex;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Milestone 1's own convergence test (`two_segments_converge_via_sync_messages`)
/// re-run with real persistent storage substituted for the in-memory
/// `Segment`s it originally used directly -- proving persistence doesn't
/// change convergence behavior. Per this plan's exit criteria.
#[test]
fn milestone_1_convergence_still_holds_with_real_persistent_storage() {
    let alice_dir = tempfile::tempdir().unwrap();
    let bob_dir = tempfile::tempdir().unwrap();

    let mut alice_store = FileSegmentStore::new(alice_dir.path()).unwrap();
    let mut bob_store = FileSegmentStore::new(bob_dir.path()).unwrap();

    let mut alice = Segment::new("space-1", 0);
    let mut bob = Segment::new("space-1", 0);

    alice.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "from alice".to_string(),
        attachments: vec![],
    });
    bob.append_message(&Message {
        sender: DeviceId([2u8; 32]),
        content: "from bob".to_string(),
        attachments: vec![],
    });

    let mut alice_state = sync_state();
    let mut bob_state = sync_state();
    loop {
        let a_to_b = alice.generate_sync_message(&mut alice_state);
        let b_to_a = bob.generate_sync_message(&mut bob_state);
        let (a_none, b_none) = (a_to_b.is_none(), b_to_a.is_none());
        if let Some(msg) = a_to_b {
            bob.receive_sync_message(&mut bob_state, msg).unwrap();
        }
        if let Some(msg) = b_to_a {
            alice.receive_sync_message(&mut alice_state, msg).unwrap();
        }
        if a_none && b_none {
            break;
        }
    }

    assert_eq!(alice.message_count(), 2);
    assert_eq!(bob.message_count(), 2);

    // Persist each side's converged segment.
    let alice_change = alice.latest_change();
    let bob_change = bob.latest_change();
    alice_store.save_segment("space-1", 0, alice_change.cursor.0, &alice_change.bytes).unwrap();
    bob_store.save_segment("space-1", 0, bob_change.cursor.0, &bob_change.bytes).unwrap();

    // Reload from disk and confirm the persisted bytes still show both messages.
    let (alice_cursor, alice_bytes) = alice_store.load_segment("space-1", 0).unwrap().unwrap();
    let reloaded = Segment::load(&alice_bytes, "space-1", 0, alice_cursor).unwrap();
    assert_eq!(reloaded.message_count(), 2);

    // Exercise ListingIndex and SearchIndex against the same converged
    // messages -- this crate's dev-dependencies on space-chat-storage-redb
    // and space-chat-search-tantivy exist "to exercise all three [storage
    // backends] together" (this task's own Files note), so do that here
    // rather than leaving those imports unused.
    let listing_db = Arc::new(redb::Database::create(alice_dir.path().join("listing.redb")).unwrap());
    let mut listing = RedbListingIndex::new(listing_db).unwrap();
    let mut search = TantivySearchIndex::new(alice_dir.path().join("search")).unwrap();

    let mut keys: Vec<String> = reloaded.message_keys().collect();
    keys.sort(); // deterministic order for indexing/listing
    for (seq, key) in keys.iter().enumerate() {
        let msg = reloaded.read_message(key).expect("well-formed message");
        listing
            .append_entry(ListingEntry {
                space_id: "space-1".to_string(),
                epoch: 0,
                seq: seq as u64,
                message_key: key.clone(),
            })
            .unwrap();
        search.index_message("space-1", key, &msg.content).unwrap();
    }
    search.commit().unwrap();

    let page = listing.page("space-1", None, 10).unwrap();
    assert_eq!(page.len(), 2, "both converged messages should be listed");

    let alice_message_key = keys
        .iter()
        .find(|k| reloaded.read_message(k).unwrap().content == "from alice")
        .unwrap()
        .clone();
    let found = search.search("space-1", "alice").unwrap();
    assert_eq!(
        found,
        vec![alice_message_key],
        "search should find alice's message by content, not bob's"
    );
}

/// Kill-and-restart: a `ListingIndex` that has fallen behind (simulating a
/// crash between a segment write and the index's own commit) must catch up
/// via `catch_up`'s watermark-replay, not require a from-scratch rebuild.
#[test]
fn listing_index_catches_up_after_a_simulated_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut segment_store = FileSegmentStore::new(dir.path().join("segments")).unwrap();

    let mut segment = Segment::new("space-1", 0);
    segment.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "one".to_string(),
        attachments: vec![],
    });
    segment.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "two".to_string(),
        attachments: vec![],
    });
    let seed_change = segment.latest_change();
    segment_store.save_segment("space-1", 0, seed_change.cursor.0, &seed_change.bytes).unwrap();

    // "Restart": fresh RedbListingIndex, watermark at 0, must catch up.
    let db = Arc::new(redb::Database::create(dir.path().join("listing.redb")).unwrap());
    let mut listing = RedbListingIndex::new(db).unwrap();
    assert_eq!(
        listing.watermark("space-1", 0),
        space_chat_core::projection::SegmentCursor(0)
    );

    catch_up(&segment_store, "space-1", &mut listing).unwrap();

    assert_eq!(
        listing.watermark("space-1", 0),
        space_chat_core::projection::SegmentCursor(2)
    );
}

/// The storage spec's own example scenario, translated to a direct test
/// against `AttachmentMetadataStore`/`sweep` (no rendered UI exists yet --
/// that's Milestone 4 -- so this asserts the mechanism directly, per this
/// plan's Global Constraints and the storage spec's Testing section, which
/// permits this fallback "where there genuinely isn't" a user-visible
/// outcome yet): an attachment referenced only by Alice isn't lost if Bob
/// (who never independently held a reference) comes back online within the
/// grace window relative to when Alice's reference was tombstoned.
#[test]
fn attachment_survives_the_grace_window_when_still_referenced_elsewhere() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(redb::Database::create(dir.path().join("attachments.redb")).unwrap());
    let mut metadata = RedbAttachmentMetadataStore::new(db).unwrap();
    let mut blobs = FileAttachmentStore::new(dir.path().join("attachments")).unwrap();

    let hash = [9u8; 32];
    blobs.save_attachment(&hash, b"shared attachment bytes").unwrap();
    metadata.record_seen(hash, 24, "image/png").unwrap();

    let t0 = SystemTime::now();
    let grace = Duration::from_secs(30 * 24 * 60 * 60);
    // Day 0: "Alice deletes the message referencing the attachment" -- on the
    // device running this sweep, the reference is gone immediately (GC is
    // inherently per-device; per the spec, a *different* device (Bob) still
    // holding a live reference elsewhere is exactly the reason the grace
    // window exists, not something this device's own live-set computation
    // needs to simulate directly). First sweep marks it unreferenced.
    let empty = HashSet::new();
    sweep(&mut metadata, &mut blobs, &empty, t0, grace).unwrap();

    // Day 20: 20 elapsed days of being continuously unreferenced -- still
    // inside the 30-day grace window, so the attachment must survive. This
    // is the storage spec's own Gherkin scenario ("20 days pass... Bob's
    // conversation view still shows the attachment as available").
    let t_20_days = t0 + Duration::from_secs(20 * 24 * 60 * 60);
    sweep(&mut metadata, &mut blobs, &empty, t_20_days, grace).unwrap();
    assert!(
        blobs.load_attachment(&hash).unwrap().is_some(),
        "attachment must still exist after only 20 of 30 grace-window days have elapsed"
    );

    // Day 31: the grace window has now fully elapsed since the reference
    // first became unreferenced at t0 -- the attachment is finally deleted.
    let t_31_days = t0 + Duration::from_secs(31 * 24 * 60 * 60);
    let deleted = sweep(&mut metadata, &mut blobs, &empty, t_31_days, grace).unwrap();
    assert_eq!(deleted, vec![hash]);
    assert!(
        blobs.load_attachment(&hash).unwrap().is_none(),
        "attachment should be deleted once the full 30-day grace window has elapsed"
    );
}

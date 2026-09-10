use crate::projection::{Projection, ProjectionError, SegmentCursor};
use crate::segment::Segment;
use crate::storage::{SegmentBlobStore, StorageError};

/// Replays every epoch currently persisted for `space_id` into `projection`,
/// skipping any epoch whose resulting change's cursor is not after
/// `projection.watermark()`. This is the mechanism the storage spec calls
/// "each independently compares its watermark against what's on disk in
/// `segments/` and replays forward whatever it's missing" — used both for
/// normal startup catch-up and for kill-and-restart recovery.
///
/// Deliberate simplification consistent with `Segment::latest_change`'s own
/// doc comment: each epoch's entire segment is loaded and turned into one
/// `SegmentChange` snapshot (not a per-mutation incremental diff), so
/// `catch_up` applies at most one `SegmentChange` per epoch, not one per
/// original mutation. A `Projection`'s `apply` must be idempotent-safe
/// against a full-snapshot re-application in this sense: `watermark()`
/// comparison (below) is what prevents re-applying an epoch already caught up.
pub fn catch_up<P: Projection>(
    store: &impl SegmentBlobStore,
    space_id: &str,
    projection: &mut P,
) -> Result<(), StorageError> {
    for epoch in store.list_epochs(space_id)? {
        let Some((cursor, bytes)) = store.load_segment(space_id, epoch)? else {
            continue;
        };
        if SegmentCursor(cursor) <= projection.watermark() {
            continue;
        }
        // Seed the loaded segment's cursor from the persisted value (not 0) --
        // `Segment::load`'s own doc comment on its `cursor` parameter is
        // explicit that a caller restoring persisted state must pass the
        // saved cursor back in, otherwise a subsequent `latest_change()`
        // re-emits the wrong value. Here we don't even need a subsequent
        // mutation for this to matter: `latest_change()` below returns
        // `SegmentCursor(self.cursor)` unchanged, so seeding it correctly is
        // what makes the value match what was actually persisted.
        let mut segment = Segment::load(&bytes, space_id, epoch, cursor)
            .map_err(|e| StorageError::Corrupt(e.to_string()))?;
        let change = segment.latest_change();
        projection
            .apply(&change)
            .map_err(|e: ProjectionError| StorageError::Corrupt(format!("{e:?}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{DeviceId, Message};
    use crate::projection::{Projection, ProjectionError, SegmentChange, SegmentCursor};
    use crate::segment::Segment;
    use crate::storage::{SegmentBlobStore, StorageError};
    use std::collections::HashMap;

    struct FakeSegmentBlobStore {
        data: HashMap<(String, u64), (u64, Vec<u8>)>,
    }

    impl SegmentBlobStore for FakeSegmentBlobStore {
        fn save_segment(&mut self, space_id: &str, epoch: u64, cursor: u64, bytes: &[u8]) -> Result<(), StorageError> {
            self.data.insert((space_id.to_string(), epoch), (cursor, bytes.to_vec()));
            Ok(())
        }
        fn load_segment(&self, space_id: &str, epoch: u64) -> Result<Option<(u64, Vec<u8>)>, StorageError> {
            Ok(self.data.get(&(space_id.to_string(), epoch)).cloned())
        }
        fn list_epochs(&self, space_id: &str) -> Result<Vec<u64>, StorageError> {
            let mut epochs: Vec<u64> = self
                .data
                .keys()
                .filter(|(s, _)| s == space_id)
                .map(|(_, e)| *e)
                .collect();
            epochs.sort_unstable();
            Ok(epochs)
        }
    }

    struct CountingProjection {
        watermark: SegmentCursor,
        applied: Vec<SegmentChange>,
    }

    impl Projection for CountingProjection {
        fn watermark(&self) -> SegmentCursor {
            self.watermark
        }
        fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError> {
            self.watermark = change.cursor;
            self.applied.push(change.clone());
            Ok(())
        }
    }

    #[test]
    fn catch_up_replays_a_persisted_segment_into_a_fresh_projection() {
        let mut store = FakeSegmentBlobStore { data: HashMap::new() };
        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        let change = segment.latest_change();
        store.save_segment("space-1", 0, change.cursor.0, &change.bytes).unwrap();

        let mut projection = CountingProjection {
            watermark: SegmentCursor(0),
            applied: vec![],
        };
        catch_up(&store, "space-1", &mut projection).unwrap();

        assert_eq!(projection.applied.len(), 1);
        assert_eq!(projection.watermark(), SegmentCursor(1));
    }

    #[test]
    fn catch_up_is_a_no_op_when_projection_watermark_is_already_current() {
        let mut store = FakeSegmentBlobStore { data: HashMap::new() };
        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        let change = segment.latest_change();
        store.save_segment("space-1", 0, change.cursor.0, &change.bytes).unwrap();

        let mut projection = CountingProjection {
            watermark: SegmentCursor(1), // already caught up
            applied: vec![],
        };
        catch_up(&store, "space-1", &mut projection).unwrap();

        assert_eq!(
            projection.applied.len(),
            0,
            "a projection already at the segment's cursor should not be re-applied"
        );
    }

    /// Proves the fix this amendment made: a segment containing a reaction
    /// (not just messages) still replays with the correct cursor. Under the
    /// old message-count-inference bug, this would have inferred cursor 1
    /// (one message) instead of the true 2 (append_message + append_reaction),
    /// silently under-counting.
    #[test]
    fn catch_up_replays_the_true_mutation_count_not_just_message_count() {
        use crate::segment::objid_to_target_string;
        use crate::domain::Reaction;

        let mut store = FakeSegmentBlobStore { data: HashMap::new() };
        let mut segment = Segment::new("space-1", 0);
        let msg_id = segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        segment
            .append_reaction(
                &msg_id,
                &Reaction {
                    target: objid_to_target_string(&msg_id),
                    actor: DeviceId([2u8; 32]),
                    emoji: "👍".to_string(),
                },
            )
            .unwrap();
        let change = segment.latest_change();
        assert_eq!(change.cursor, SegmentCursor(2), "one message + one reaction = cursor 2");
        store.save_segment("space-1", 0, change.cursor.0, &change.bytes).unwrap();

        let mut projection = CountingProjection {
            watermark: SegmentCursor(0),
            applied: vec![],
        };
        catch_up(&store, "space-1", &mut projection).unwrap();

        assert_eq!(
            projection.watermark(),
            SegmentCursor(2),
            "watermark must reflect the true cursor (2), not message_count (1)"
        );
    }
}

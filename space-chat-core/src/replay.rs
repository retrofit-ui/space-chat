use crate::projection::{Projection, ProjectionError};
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
        let Some(bytes) = store.load_segment(space_id, epoch)? else {
            continue;
        };
        // Load segment with cursor 0 to peek at the message count.
        // The cursor needs to reflect the number of mutations in the document
        // so that latest_change() returns the correct cursor for watermark
        // comparison, but it's not persisted in the bytes. We infer it from
        // the document content: message_count represents the number of
        // mutations (simplified assumption that each epoch contains only
        // messages for this snapshot-based replay).
        let segment = Segment::load(&bytes, space_id, epoch, 0)
            .map_err(|e| StorageError::Corrupt(e.to_string()))?;
        let cursor = segment.message_count() as u64;
        let mut segment = Segment::load(&bytes, space_id, epoch, cursor)
            .map_err(|e| StorageError::Corrupt(e.to_string()))?;
        let change = segment.latest_change();
        if change.cursor <= projection.watermark() {
            continue;
        }
        projection
            .apply(&change)
            .map_err(|e: ProjectionError| StorageError::Corrupt(format!("{e:?}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::catch_up;
    use crate::domain::{DeviceId, Message};
    use crate::projection::{Projection, ProjectionError, SegmentChange, SegmentCursor};
    use crate::segment::Segment;
    use crate::storage::{SegmentBlobStore, StorageError};
    use std::collections::HashMap;

    struct FakeSegmentBlobStore {
        data: HashMap<(String, u64), Vec<u8>>,
    }

    impl SegmentBlobStore for FakeSegmentBlobStore {
        fn save_segment(&mut self, space_id: &str, epoch: u64, bytes: &[u8]) -> Result<(), StorageError> {
            self.data.insert((space_id.to_string(), epoch), bytes.to_vec());
            Ok(())
        }
        fn load_segment(&self, space_id: &str, epoch: u64) -> Result<Option<Vec<u8>>, StorageError> {
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
        store.save_segment("space-1", 0, &change.bytes).unwrap();

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
        store.save_segment("space-1", 0, &change.bytes).unwrap();

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
}

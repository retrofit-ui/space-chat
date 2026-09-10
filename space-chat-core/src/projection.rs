#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentCursor(pub u64);

#[derive(Debug, Clone)]
pub struct SegmentChange {
    pub space_id: String,
    pub epoch: u64,
    pub cursor: SegmentCursor,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionError {
    ApplyFailed(String),
}

pub trait Projection {
    /// The last-applied cursor for the given `(space_id, epoch)` pair, or
    /// `SegmentCursor(0)` if nothing has been applied for it yet.
    ///
    /// Scoped by both dimensions because a single `Projection` instance
    /// (e.g. one `ListingIndex`/`SearchIndex` per app) serves every space and
    /// every epoch within a space, not just one. A single unscoped scalar
    /// here silently conflates unrelated spaces'/epochs' progress: a
    /// `Segment`'s own `cursor` counter restarts from 0 at every new epoch
    /// (see `Segment::new`), so a later epoch's low cursor could compare
    /// `<=` an earlier epoch's already-advanced watermark and be silently
    /// skipped by a naive replay driver -- caught in space-chat's Milestone 2
    /// final whole-branch review, where `catch_up` (space-chat-core's
    /// `replay` module) never applied any epoch after the first for exactly
    /// this reason, and no test caught it because every test used epoch 0.
    fn watermark(&self, space_id: &str, epoch: u64) -> SegmentCursor;
    fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct CountingProjection {
        counts: HashMap<(String, u64), u64>,
    }

    impl Projection for CountingProjection {
        fn watermark(&self, space_id: &str, epoch: u64) -> SegmentCursor {
            SegmentCursor(
                *self
                    .counts
                    .get(&(space_id.to_string(), epoch))
                    .unwrap_or(&0),
            )
        }
        fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError> {
            *self
                .counts
                .entry((change.space_id.clone(), change.epoch))
                .or_insert(0) += 1;
            Ok(())
        }
    }

    #[test]
    fn projection_advances_watermark_on_apply() {
        let mut p = CountingProjection {
            counts: HashMap::new(),
        };
        let change = SegmentChange {
            space_id: "space-1".to_string(),
            epoch: 0,
            cursor: SegmentCursor(1),
            bytes: vec![1, 2, 3],
        };
        p.apply(&change).unwrap();
        assert_eq!(p.watermark("space-1", 0), SegmentCursor(1));
    }

    #[test]
    fn watermark_is_scoped_independently_per_space_and_epoch() {
        let mut p = CountingProjection {
            counts: HashMap::new(),
        };
        p.apply(&SegmentChange {
            space_id: "space-1".to_string(),
            epoch: 0,
            cursor: SegmentCursor(10),
            bytes: vec![],
        })
        .unwrap();
        p.apply(&SegmentChange {
            space_id: "space-1".to_string(),
            epoch: 1,
            cursor: SegmentCursor(3),
            bytes: vec![],
        })
        .unwrap();
        p.apply(&SegmentChange {
            space_id: "space-2".to_string(),
            epoch: 0,
            cursor: SegmentCursor(7),
            bytes: vec![],
        })
        .unwrap();

        assert_eq!(p.watermark("space-1", 0), SegmentCursor(1));
        assert_eq!(p.watermark("space-1", 1), SegmentCursor(1));
        assert_eq!(p.watermark("space-2", 0), SegmentCursor(1));
        assert_eq!(
            p.watermark("space-1", 2),
            SegmentCursor(0),
            "an (space_id, epoch) pair with no applied changes yet must read as 0"
        );
    }
}

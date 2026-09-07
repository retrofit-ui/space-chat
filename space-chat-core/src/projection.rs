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
    fn watermark(&self) -> SegmentCursor;
    fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CountingProjection {
        count: u64,
    }

    impl Projection for CountingProjection {
        fn watermark(&self) -> SegmentCursor {
            SegmentCursor(self.count)
        }
        fn apply(&mut self, _change: &SegmentChange) -> Result<(), ProjectionError> {
            self.count += 1;
            Ok(())
        }
    }

    #[test]
    fn projection_advances_watermark_on_apply() {
        let mut p = CountingProjection { count: 0 };
        let change = SegmentChange {
            space_id: "space-1".to_string(),
            epoch: 0,
            cursor: SegmentCursor(1),
            bytes: vec![1, 2, 3],
        };
        p.apply(&change).unwrap();
        assert_eq!(p.watermark(), SegmentCursor(1));
    }
}

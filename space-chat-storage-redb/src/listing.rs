use redb::{Database, TableDefinition};
use space_chat_core::projection::{Projection, ProjectionError, SegmentChange, SegmentCursor};
use space_chat_core::storage::{ListingEntry, ListingIndex, StorageError};
use std::sync::Arc;

// Key: big-endian-encoded (space_id, epoch, seq) so redb's natural byte-order
// range scan gives ascending (epoch, seq) order per space_id for free.
// Value: message_key.
const ENTRIES: TableDefinition<&[u8], &str> = TableDefinition::new("listing_entries");
// Keyed by (space_id, epoch) -- see `Projection::watermark`'s doc comment for
// why a single shared row is wrong (this table originally used one fixed
// "watermark" string key; fixed as part of the Milestone 2 final
// whole-branch review, which found `catch_up` silently skipped every epoch
// after the first as a result). Persisted (not held only in memory) so it
// survives a reopen -- per the storage spec, ListingIndex must compare its
// watermark against what's on disk at startup.
const WATERMARK: TableDefinition<&[u8], u64> = TableDefinition::new("listing_watermark");

fn redb_err(e: impl std::fmt::Display) -> StorageError {
    StorageError::Io(e.to_string())
}

/// Length-prefixed, not separator-delimited: a fixed-width `u32` BE length
/// field followed by exactly that many `space_id` bytes, then epoch/seq.
/// **Amendment:** the original version used a single `0x00` separator byte,
/// which is broken -- Rust `str`/`String` permits an embedded NUL, so
/// `space_id = "a\0"` at (epoch=5, seq=7) produced a key that fell inside
/// `page("a", ...)`'s range scan, leaking one space's entries into another's
/// results. Length-prefixing removes the ambiguity: two different
/// `space_id`s either encode a different length (differing in the first 4
/// bytes, which resolves byte-lexicographic ordering before any content is
/// compared) or the same length or with genuinely identical content (i.e.
/// the same `space_id`) -- there is no byte sequence a shorter/longer
/// `space_id`'s key can produce that falls inside another's range.
fn encode_key(space_id: &str, epoch: u64, seq: u64) -> Vec<u8> {
    let space_bytes = space_id.as_bytes();
    let mut key = (space_bytes.len() as u32).to_be_bytes().to_vec();
    key.extend_from_slice(space_bytes);
    key.extend_from_slice(&epoch.to_be_bytes());
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

/// Same length-prefixing rationale as `encode_key`, one field shorter (no
/// `seq` -- a watermark is per-epoch, not per-entry).
fn encode_watermark_key(space_id: &str, epoch: u64) -> Vec<u8> {
    let space_bytes = space_id.as_bytes();
    let mut key = (space_bytes.len() as u32).to_be_bytes().to_vec();
    key.extend_from_slice(space_bytes);
    key.extend_from_slice(&epoch.to_be_bytes());
    key
}

pub struct RedbListingIndex {
    db: Arc<Database>,
}

impl RedbListingIndex {
    pub fn new(db: Arc<Database>) -> Result<Self, StorageError> {
        // Ensure both tables exist so read-only opens elsewhere don't fail.
        let txn = db.begin_write().map_err(redb_err)?;
        {
            txn.open_table(ENTRIES).map_err(redb_err)?;
            txn.open_table(WATERMARK).map_err(redb_err)?;
        }
        txn.commit().map_err(redb_err)?;
        Ok(Self { db })
    }
}

impl ListingIndex for RedbListingIndex {
    fn append_entry(&mut self, entry: ListingEntry) -> Result<(), StorageError> {
        let key = encode_key(&entry.space_id, entry.epoch, entry.seq);
        let txn = self.db.begin_write().map_err(redb_err)?;
        {
            let mut table = txn.open_table(ENTRIES).map_err(redb_err)?;
            table
                .insert(key.as_slice(), entry.message_key.as_str())
                .map_err(redb_err)?;
        }
        txn.commit().map_err(redb_err)
    }

    fn page(
        &self,
        space_id: &str,
        before: Option<(u64, u64)>,
        limit: usize,
    ) -> Result<Vec<ListingEntry>, StorageError> {
        let txn = self.db.begin_read().map_err(redb_err)?;
        let table = txn.open_table(ENTRIES).map_err(redb_err)?;

        let start = encode_key(space_id, 0, 0);
        // Amendment: previously used a separate, shorter "no upper bound"
        // key for the `before: None` case, built from `space_id` bytes alone
        // -- broken along with the old separator-based `encode_key` (see
        // that function's amendment note). With length-prefixed keys, the
        // simplest correct upper bound is always a real `encode_key` call:
        // `before: None` means "up to the maximum possible (epoch, seq)."
        // This excludes a real entry only in the practically-impossible case
        // where one exists at exactly `(u64::MAX, u64::MAX)` -- a documented
        // edge case, not a design fork worth solving for.
        let (before_epoch, before_seq) = before.unwrap_or((u64::MAX, u64::MAX));
        let end = encode_key(space_id, before_epoch, before_seq);

        // `.rev().take(limit)` instead of collecting the whole `[start, end)`
        // range and truncating afterward -- the original version allocated
        // one `ListingEntry` per entry in the ENTIRE space before returning
        // `limit` of them (found in the Milestone 2 final whole-branch
        // review: `page(space, None, 50)` on a space with a long history
        // would materialize its entire backlog just to return the newest
        // 50). redb's `Range` implements `DoubleEndedIterator`, so reversing
        // the iterator itself (not a `Vec` built from it) lets `.take(limit)`
        // stop scanning as soon as it has enough.
        let mut entries = Vec::new();
        for result in table
            .range(start.as_slice()..end.as_slice())
            .map_err(redb_err)?
            .rev()
            .take(limit)
        {
            // Propagate a decode error instead of silently dropping the
            // entry (the original version's `.filter_map(|res| res.ok())`
            // masked corruption rather than reporting it).
            let (key, value) = result.map_err(redb_err)?;
            let key_bytes = key.value();
            let epoch = u64::from_be_bytes(key_bytes[key_bytes.len() - 16..key_bytes.len() - 8].try_into().unwrap());
            let seq = u64::from_be_bytes(key_bytes[key_bytes.len() - 8..].try_into().unwrap());
            entries.push(ListingEntry {
                space_id: space_id.to_string(),
                epoch,
                seq,
                message_key: value.value().to_string(),
            });
        }

        Ok(entries) // already newest-first: the range scan is ascending, reversed above
    }
}

impl Projection for RedbListingIndex {
    fn watermark(&self, space_id: &str, epoch: u64) -> SegmentCursor {
        let key = encode_watermark_key(space_id, epoch);
        let txn = self.db.begin_read().expect("redb read transaction should not fail");
        let table = txn
            .open_table(WATERMARK)
            .expect("watermark table is created in RedbListingIndex::new");
        SegmentCursor(table.get(key.as_slice()).ok().flatten().map(|v| v.value()).unwrap_or(0))
    }

    fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError> {
        // Milestone 2 scope note: this stores only the watermark advance
        // here. Actually deriving ListingEntry values from `change.bytes`
        // (an Automerge segment snapshot) belongs to the composition root
        // that owns both a `Segment` and this index together (Milestone 4's
        // `space-chat-app`, or Task 8's integration test in this plan) --
        // see Task 8, which drives `append_entry` directly from decoded
        // `Segment` contents rather than teaching `RedbListingIndex` to
        // decode Automerge bytes itself. This keeps `space-chat-storage-redb`
        // free of an `automerge` dependency, per the storage spec's crate
        // layout.
        let key = encode_watermark_key(&change.space_id, change.epoch);
        let txn = self.db.begin_write().map_err(|e| ProjectionError::ApplyFailed(e.to_string()))?;
        {
            let mut table = txn
                .open_table(WATERMARK)
                .map_err(|e| ProjectionError::ApplyFailed(e.to_string()))?;
            table
                .insert(key.as_slice(), change.cursor.0)
                .map_err(|e| ProjectionError::ApplyFailed(e.to_string()))?;
        }
        txn.commit().map_err(|e| ProjectionError::ApplyFailed(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::projection::{Projection, SegmentChange, SegmentCursor};
    use space_chat_core::storage::{ListingEntry, ListingIndex};
    use std::sync::Arc;

    fn fresh_index() -> (tempfile::TempDir, RedbListingIndex) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(redb::Database::create(dir.path().join("listing.redb")).unwrap());
        let index = RedbListingIndex::new(db).unwrap();
        (dir, index)
    }

    #[test]
    fn appended_entries_page_back_newest_first() {
        let (_dir, mut index) = fresh_index();
        for seq in 0..3u64 {
            index
                .append_entry(ListingEntry {
                    space_id: "space-1".to_string(),
                    epoch: 0,
                    seq,
                    message_key: format!("msg:{seq}"),
                })
                .unwrap();
        }

        let page = index.page("space-1", None, 10).unwrap();
        let seqs: Vec<u64> = page.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![2, 1, 0], "page() should return newest-first");
    }

    #[test]
    fn page_respects_before_cursor_and_limit() {
        let (_dir, mut index) = fresh_index();
        for seq in 0..5u64 {
            index
                .append_entry(ListingEntry {
                    space_id: "space-1".to_string(),
                    epoch: 0,
                    seq,
                    message_key: format!("msg:{seq}"),
                })
                .unwrap();
        }

        let page = index.page("space-1", Some((0, 3)), 2).unwrap();
        let seqs: Vec<u64> = page.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![2, 1], "page() should return entries strictly before (epoch=0, seq=3), newest-first, limited to 2");
    }

    /// Regression test for the encode_key amendment: a `space_id` containing
    /// an embedded NUL byte (`"a\0"`) must never leak its entries into
    /// `page("a", ...)`'s results, and vice versa. Under the original
    /// separator-based `encode_key`, `"a\0"` at (epoch=5, seq=7) fell inside
    /// `page("a", None, ...)`'s byte range.
    #[test]
    fn page_never_returns_entries_from_a_different_space_id() {
        let (_dir, mut index) = fresh_index();
        index
            .append_entry(ListingEntry {
                space_id: "a".to_string(),
                epoch: 0,
                seq: 0,
                message_key: "msg:a-0".to_string(),
            })
            .unwrap();
        index
            .append_entry(ListingEntry {
                space_id: "a\0".to_string(),
                epoch: 5,
                seq: 7,
                message_key: "msg:a-nul-5-7".to_string(),
            })
            .unwrap();

        let page_a = index.page("a", None, 10).unwrap();
        assert_eq!(
            page_a.iter().map(|e| e.message_key.clone()).collect::<Vec<_>>(),
            vec!["msg:a-0".to_string()],
            "page(\"a\", ...) must not include \"a\\0\"'s entry"
        );

        let page_a_nul = index.page("a\0", None, 10).unwrap();
        assert_eq!(
            page_a_nul.iter().map(|e| e.message_key.clone()).collect::<Vec<_>>(),
            vec!["msg:a-nul-5-7".to_string()],
            "page(\"a\\0\", ...) must not include \"a\"'s entry"
        );
    }

    #[test]
    fn watermark_advances_on_apply_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("listing.redb");
        {
            let db = Arc::new(redb::Database::create(&db_path).unwrap());
            let mut index = RedbListingIndex::new(db).unwrap();
            assert_eq!(index.watermark("space-1", 0), SegmentCursor(0));
            index
                .apply(&SegmentChange {
                    space_id: "space-1".to_string(),
                    epoch: 0,
                    cursor: SegmentCursor(5),
                    bytes: vec![],
                })
                .unwrap();
            assert_eq!(index.watermark("space-1", 0), SegmentCursor(5));
        }
        // Reopen against the same file -- watermark must persist, not reset.
        let db = Arc::new(redb::Database::open(&db_path).unwrap());
        let index = RedbListingIndex::new(db).unwrap();
        assert_eq!(index.watermark("space-1", 0), SegmentCursor(5));
    }

    /// Regression test for the Milestone 2 final whole-branch review: a
    /// single shared watermark row conflated every space and epoch that
    /// shared this `RedbListingIndex` instance. Two different (space_id,
    /// epoch) pairs must track independently, including a case where the
    /// second-applied cursor is numerically LOWER than the first.
    #[test]
    fn watermark_is_tracked_independently_per_space_and_epoch() {
        let (_dir, mut index) = fresh_index();

        index
            .apply(&SegmentChange {
                space_id: "space-1".to_string(),
                epoch: 0,
                cursor: SegmentCursor(10),
                bytes: vec![],
            })
            .unwrap();
        index
            .apply(&SegmentChange {
                space_id: "space-1".to_string(),
                epoch: 1,
                cursor: SegmentCursor(3),
                bytes: vec![],
            })
            .unwrap();
        index
            .apply(&SegmentChange {
                space_id: "space-2".to_string(),
                epoch: 0,
                cursor: SegmentCursor(7),
                bytes: vec![],
            })
            .unwrap();

        assert_eq!(index.watermark("space-1", 0), SegmentCursor(10));
        assert_eq!(
            index.watermark("space-1", 1),
            SegmentCursor(3),
            "epoch 1's watermark must not be conflated with epoch 0's, even though it's numerically lower"
        );
        assert_eq!(index.watermark("space-2", 0), SegmentCursor(7));
        assert_eq!(
            index.watermark("space-2", 1),
            SegmentCursor(0),
            "an untouched (space_id, epoch) pair must read as 0"
        );
    }
}

use redb::{Database, TableDefinition};
use space_chat_core::projection::{Projection, ProjectionError, SegmentChange, SegmentCursor};
use space_chat_core::storage::{ListingEntry, ListingIndex, StorageError};
use std::sync::Arc;

// Key: big-endian-encoded (space_id, epoch, seq) so redb's natural byte-order
// range scan gives ascending (epoch, seq) order per space_id for free.
// Value: message_key.
const ENTRIES: TableDefinition<&[u8], &str> = TableDefinition::new("listing_entries");
// Single-row table holding the watermark cursor, so it survives a reopen --
// per the storage spec, ListingIndex must compare its watermark against
// what's on disk at startup, which requires the watermark itself to be
// persisted, not held only in memory.
const WATERMARK: TableDefinition<&str, u64> = TableDefinition::new("listing_watermark");

fn redb_err(e: impl std::fmt::Display) -> StorageError {
    StorageError::Io(e.to_string())
}

fn encode_key(space_id: &str, epoch: u64, seq: u64) -> Vec<u8> {
    let mut key = space_id.as_bytes().to_vec();
    key.push(0); // separator, since space_id is variable-length
    key.extend_from_slice(&epoch.to_be_bytes());
    key.extend_from_slice(&seq.to_be_bytes());
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
        let end = match before {
            Some((epoch, seq)) => encode_key(space_id, epoch, seq),
            None => {
                let mut end = space_id.as_bytes().to_vec();
                end.push(1); // byte after the 0 separator, bounds all epochs/seqs for this space_id
                end
            }
        };

        let mut entries: Vec<ListingEntry> = table
            .range(start.as_slice()..end.as_slice())
            .map_err(redb_err)?
            .filter_map(|res| res.ok())
            .map(|(key, value)| {
                let key_bytes = key.value();
                let epoch = u64::from_be_bytes(key_bytes[key_bytes.len() - 16..key_bytes.len() - 8].try_into().unwrap());
                let seq = u64::from_be_bytes(key_bytes[key_bytes.len() - 8..].try_into().unwrap());
                ListingEntry {
                    space_id: space_id.to_string(),
                    epoch,
                    seq,
                    message_key: value.value().to_string(),
                }
            })
            .collect();

        entries.reverse(); // ascending scan -> newest-first
        entries.truncate(limit);
        Ok(entries)
    }
}

impl Projection for RedbListingIndex {
    fn watermark(&self) -> SegmentCursor {
        let txn = self.db.begin_read().expect("redb read transaction should not fail");
        let table = txn
            .open_table(WATERMARK)
            .expect("watermark table is created in RedbListingIndex::new");
        SegmentCursor(table.get("watermark").ok().flatten().map(|v| v.value()).unwrap_or(0))
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
        let txn = self.db.begin_write().map_err(|e| ProjectionError::ApplyFailed(e.to_string()))?;
        {
            let mut table = txn
                .open_table(WATERMARK)
                .map_err(|e| ProjectionError::ApplyFailed(e.to_string()))?;
            table
                .insert("watermark", change.cursor.0)
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

    #[test]
    fn watermark_advances_on_apply_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("listing.redb");
        {
            let db = Arc::new(redb::Database::create(&db_path).unwrap());
            let mut index = RedbListingIndex::new(db).unwrap();
            assert_eq!(index.watermark(), SegmentCursor(0));
            index
                .apply(&SegmentChange {
                    space_id: "space-1".to_string(),
                    epoch: 0,
                    cursor: SegmentCursor(5),
                    bytes: vec![],
                })
                .unwrap();
            assert_eq!(index.watermark(), SegmentCursor(5));
        }
        // Reopen against the same file -- watermark must persist, not reset.
        let db = Arc::new(redb::Database::open(&db_path).unwrap());
        let index = RedbListingIndex::new(db).unwrap();
        assert_eq!(index.watermark(), SegmentCursor(5));
    }
}

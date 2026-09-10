use crate::projection::Projection;
use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq)]
pub enum StorageError {
    Io(String),
    Corrupt(String),
    NotFound,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(msg) => write!(f, "storage io error: {msg}"),
            StorageError::Corrupt(msg) => write!(f, "storage corruption: {msg}"),
            StorageError::NotFound => write!(f, "not found"),
        }
    }
}

impl std::error::Error for StorageError {}

/// Irreplaceable source-of-truth storage for Automerge segment bytes, one
/// entry per `(space_id, epoch)`, per the storage spec's data-placement
/// table (`segments/<space_id>/<epoch>.automerge`). Persists `cursor`
/// alongside `bytes` — `cursor` is a mutation counter
/// (`Segment`/`SegmentChange`'s own bookkeeping, from `Segment::latest_change()`),
/// not something recoverable by inspecting segment content after the fact
/// (message count alone undercounts reactions/deletes, which also advance
/// it). A caller always has both in hand together, from the same
/// `Segment::latest_change()` call that produces a `SegmentChange` to persist.
pub trait SegmentBlobStore {
    fn save_segment(&mut self, space_id: &str, epoch: u64, cursor: u64, bytes: &[u8]) -> Result<(), StorageError>;
    /// Returns `(cursor, bytes)` for the epoch, or `None` if never saved.
    fn load_segment(&self, space_id: &str, epoch: u64) -> Result<Option<(u64, Vec<u8>)>, StorageError>;
    /// All epochs currently persisted for `space_id`, ascending.
    fn list_epochs(&self, space_id: &str) -> Result<Vec<u64>, StorageError>;
}

/// Irreplaceable source-of-truth storage for content-addressed attachment
/// bytes. No hash-to-bytes reconstruction exists if a blob is lost, per the
/// storage spec's GC section.
pub trait AttachmentBlobStore {
    fn save_attachment(&mut self, hash: &[u8; 32], bytes: &[u8]) -> Result<(), StorageError>;
    fn load_attachment(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, StorageError>;
    fn delete_attachment(&mut self, hash: &[u8; 32]) -> Result<(), StorageError>;
}

/// One page-able entry in the primary, immediately-consistent
/// conversation-listing view, ordered by `(space_id, epoch, seq)` per the
/// storage spec. `message_key` is a `Segment`'s `"msg:<uuid>"` key
/// (see `space_chat_core::segment::Segment::message_keys`).
#[derive(Debug, Clone, PartialEq)]
pub struct ListingEntry {
    pub space_id: String,
    pub epoch: u64,
    pub seq: u64,
    pub message_key: String,
}

/// The primary, always-synchronous, immediately-consistent conversation
/// listing/pagination view. A derived `Projection` — rebuildable by
/// replaying segments — not a source of truth.
pub trait ListingIndex: Projection {
    fn append_entry(&mut self, entry: ListingEntry) -> Result<(), StorageError>;
    /// Returns up to `limit` entries for `space_id`, ordered newest-first,
    /// strictly before `before` (exclusive) if given, or from the newest
    /// entry if `before` is `None` — the pagination contract
    /// `fetch_older_page` (app-shell spec) relies on.
    fn page(
        &self,
        space_id: &str,
        before: Option<(u64, u64)>,
        limit: usize,
    ) -> Result<Vec<ListingEntry>, StorageError>;
}

/// The near-real-time full-text search view. A derived `Projection` —
/// rebuildable by replaying segments — not a source of truth.
pub trait SearchIndex: Projection {
    fn index_message(
        &mut self,
        space_id: &str,
        message_key: &str,
        content: &str,
    ) -> Result<(), StorageError>;
    /// Message keys matching `query` within `space_id`, most-relevant-first.
    fn search(&self, space_id: &str, query: &str) -> Result<Vec<String>, StorageError>;
}

/// Metadata + GC liveness bookkeeping for attachment blobs. A derived
/// projection, not a source of truth (rebuildable by replaying segments
/// for the reference-liveness half; `first_seen_unreferenced` bookkeeping
/// itself has no ground truth to rebuild from other than re-running the
/// sweep, which is fine per the storage spec's GC section).
#[derive(Debug, Clone, PartialEq)]
pub struct AttachmentMetadata {
    pub hash: [u8; 32],
    pub size: u64,
    pub mime: String,
    pub first_seen_unreferenced: Option<SystemTime>,
}

pub trait AttachmentMetadataStore {
    /// Registers (or re-confirms) an attachment's size/mime the first time
    /// it's seen referenced. Idempotent.
    fn record_seen(&mut self, hash: [u8; 32], size: u64, mime: &str) -> Result<(), StorageError>;
    fn get(&self, hash: &[u8; 32]) -> Result<Option<AttachmentMetadata>, StorageError>;
    /// All hashes currently tracked (regardless of liveness state) — the
    /// mark-and-sweep driver walks this to reset/set `first_seen_unreferenced`.
    fn all_hashes(&self) -> Result<Vec<[u8; 32]>, StorageError>;
    /// Sets `first_seen_unreferenced` to `now` only if it isn't already set
    /// (preserves the earliest unreferenced timestamp across sweeps).
    fn mark_unreferenced_if_unset(&mut self, hash: [u8; 32], now: SystemTime) -> Result<(), StorageError>;
    /// Clears `first_seen_unreferenced` (a hash marked live again).
    fn clear_unreferenced(&mut self, hash: [u8; 32]) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // A minimal in-memory fake, used only to prove the trait signatures are
    // usable before any real backend exists.
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

    #[test]
    fn segment_blob_store_round_trips_cursor_and_bytes() {
        let mut store = FakeSegmentBlobStore { data: HashMap::new() };
        store.save_segment("space-1", 0, 3, b"hello").unwrap();
        assert_eq!(store.load_segment("space-1", 0).unwrap(), Some((3, b"hello".to_vec())));
        assert_eq!(store.load_segment("space-1", 1).unwrap(), None);
        assert_eq!(store.list_epochs("space-1").unwrap(), vec![0]);
    }
}

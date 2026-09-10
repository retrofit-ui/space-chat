use crate::storage::{AttachmentBlobStore, AttachmentMetadataStore, StorageError};
use std::collections::HashSet;
use std::time::{Duration, SystemTime};

/// Runs one mark-and-sweep pass, per the storage spec's GC section:
/// - Every hash in `metadata.all_hashes()` not present in `live_hashes` gets
///   `first_seen_unreferenced` set via `mark_unreferenced_if_unset` (a no-op
///   if already set, preserving the earliest timestamp).
/// - Every hash in `live_hashes` gets `clear_unreferenced` (a no-op if
///   already clear).
/// - A hash is actually deleted (metadata + blob) only if
///   `now - first_seen_unreferenced >= grace_window`, i.e. it has been
///   continuously unreferenced across every sweep spanning the full window,
///   not merely seen unreferenced more than once.
///
/// Deliberately not incremental reference counting -- see the storage
/// spec's rationale (avoids drift from double-counting during a projection
/// replay). Callers are expected to invoke this on a periodic schedule
/// (e.g. daily) with `live_hashes` freshly computed from current segment
/// state each time.
pub fn sweep<M: AttachmentMetadataStore, B: AttachmentBlobStore>(
    metadata: &mut M,
    blobs: &mut B,
    live_hashes: &HashSet<[u8; 32]>,
    now: SystemTime,
    grace_window: Duration,
) -> Result<Vec<[u8; 32]>, StorageError> {
    let mut deleted = vec![];
    for hash in metadata.all_hashes()? {
        if live_hashes.contains(&hash) {
            metadata.clear_unreferenced(hash)?;
            continue;
        }
        metadata.mark_unreferenced_if_unset(hash, now)?;
        let meta = metadata.get(&hash)?.ok_or(StorageError::NotFound)?;
        if let Some(first_seen) = meta.first_seen_unreferenced {
            let elapsed = now.duration_since(first_seen).unwrap_or(Duration::ZERO);
            if elapsed >= grace_window {
                blobs.delete_attachment(&hash)?;
                // Forget the metadata row too, not just the blob -- otherwise
                // this hash lingers in `all_hashes()` forever and every
                // future sweep re-processes (a harmless but pointless no-op
                // re-delete of) the same already-gone blob.
                metadata.forget(hash)?;
                deleted.push(hash);
            }
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{AttachmentBlobStore, AttachmentMetadata, AttachmentMetadataStore, StorageError};
    use std::collections::{HashMap, HashSet};
    use std::time::{Duration, SystemTime};

    #[derive(Default)]
    struct FakeMetadataStore {
        data: HashMap<[u8; 32], AttachmentMetadata>,
    }

    impl AttachmentMetadataStore for FakeMetadataStore {
        fn record_seen(&mut self, hash: [u8; 32], size: u64, mime: &str) -> Result<(), StorageError> {
            self.data.entry(hash).or_insert(AttachmentMetadata {
                hash,
                size,
                mime: mime.to_string(),
                first_seen_unreferenced: None,
            });
            Ok(())
        }
        fn get(&self, hash: &[u8; 32]) -> Result<Option<AttachmentMetadata>, StorageError> {
            Ok(self.data.get(hash).cloned())
        }
        fn all_hashes(&self) -> Result<Vec<[u8; 32]>, StorageError> {
            Ok(self.data.keys().copied().collect())
        }
        fn mark_unreferenced_if_unset(&mut self, hash: [u8; 32], now: SystemTime) -> Result<(), StorageError> {
            if let Some(meta) = self.data.get_mut(&hash) {
                if meta.first_seen_unreferenced.is_none() {
                    meta.first_seen_unreferenced = Some(now);
                }
            }
            Ok(())
        }
        fn clear_unreferenced(&mut self, hash: [u8; 32]) -> Result<(), StorageError> {
            if let Some(meta) = self.data.get_mut(&hash) {
                meta.first_seen_unreferenced = None;
            }
            Ok(())
        }
        fn forget(&mut self, hash: [u8; 32]) -> Result<(), StorageError> {
            self.data.remove(&hash);
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeBlobStore {
        blobs: HashMap<[u8; 32], Vec<u8>>,
    }

    impl AttachmentBlobStore for FakeBlobStore {
        fn save_attachment(&mut self, hash: &[u8; 32], bytes: &[u8]) -> Result<(), StorageError> {
            self.blobs.insert(*hash, bytes.to_vec());
            Ok(())
        }
        fn load_attachment(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.blobs.get(hash).cloned())
        }
        fn delete_attachment(&mut self, hash: &[u8; 32]) -> Result<(), StorageError> {
            self.blobs.remove(hash);
            Ok(())
        }
    }

    const GRACE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

    #[test]
    fn a_live_hash_is_never_marked_unreferenced() {
        let mut metadata = FakeMetadataStore::default();
        let mut blobs = FakeBlobStore::default();
        let hash = [1u8; 32];
        metadata.record_seen(hash, 10, "a").unwrap();
        blobs.save_attachment(&hash, b"data").unwrap();

        let live = HashSet::from([hash]);
        let deleted = sweep(&mut metadata, &mut blobs, &live, SystemTime::now(), GRACE).unwrap();

        assert!(deleted.is_empty());
        assert_eq!(metadata.get(&hash).unwrap().unwrap().first_seen_unreferenced, None);
        assert!(blobs.load_attachment(&hash).unwrap().is_some());
    }

    #[test]
    fn an_unreferenced_hash_is_not_deleted_before_the_grace_window_elapses() {
        let mut metadata = FakeMetadataStore::default();
        let mut blobs = FakeBlobStore::default();
        let hash = [2u8; 32];
        metadata.record_seen(hash, 10, "a").unwrap();
        blobs.save_attachment(&hash, b"data").unwrap();

        let t0 = SystemTime::UNIX_EPOCH;
        sweep(&mut metadata, &mut blobs, &HashSet::new(), t0, GRACE).unwrap();

        let t_20_days = t0 + Duration::from_secs(20 * 24 * 60 * 60);
        let deleted = sweep(&mut metadata, &mut blobs, &HashSet::new(), t_20_days, GRACE).unwrap();

        assert!(deleted.is_empty(), "20 days is inside the 30-day grace window");
        assert!(blobs.load_attachment(&hash).unwrap().is_some());
    }

    #[test]
    fn an_unreferenced_hash_is_deleted_once_the_grace_window_fully_elapses() {
        let mut metadata = FakeMetadataStore::default();
        let mut blobs = FakeBlobStore::default();
        let hash = [3u8; 32];
        metadata.record_seen(hash, 10, "a").unwrap();
        blobs.save_attachment(&hash, b"data").unwrap();

        let t0 = SystemTime::UNIX_EPOCH;
        sweep(&mut metadata, &mut blobs, &HashSet::new(), t0, GRACE).unwrap();

        let t_31_days = t0 + Duration::from_secs(31 * 24 * 60 * 60);
        let deleted = sweep(&mut metadata, &mut blobs, &HashSet::new(), t_31_days, GRACE).unwrap();

        assert_eq!(deleted, vec![hash]);
        assert!(blobs.load_attachment(&hash).unwrap().is_none());
    }

    #[test]
    fn a_hash_that_becomes_live_again_before_the_window_elapses_has_its_timer_cleared() {
        let mut metadata = FakeMetadataStore::default();
        let mut blobs = FakeBlobStore::default();
        let hash = [4u8; 32];
        metadata.record_seen(hash, 10, "a").unwrap();
        blobs.save_attachment(&hash, b"data").unwrap();

        let t0 = SystemTime::UNIX_EPOCH;
        sweep(&mut metadata, &mut blobs, &HashSet::new(), t0, GRACE).unwrap(); // unreferenced at t0

        let t_20_days = t0 + Duration::from_secs(20 * 24 * 60 * 60);
        let live = HashSet::from([hash]);
        sweep(&mut metadata, &mut blobs, &live, t_20_days, GRACE).unwrap(); // live again -- timer cleared
        assert_eq!(metadata.get(&hash).unwrap().unwrap().first_seen_unreferenced, None);

        // Now unreferenced again; a further 31 days from *this* point must
        // elapse before deletion -- the original t0 timer must not resurface.
        let t_51_days = t0 + Duration::from_secs(51 * 24 * 60 * 60);
        let deleted = sweep(&mut metadata, &mut blobs, &HashSet::new(), t_51_days, GRACE).unwrap();
        assert!(
            deleted.is_empty(),
            "only 31 days have passed since the timer was cleared at t_20_days, not since t0"
        );
    }

    /// Proves the fix this amendment made: once a hash is actually deleted,
    /// its metadata row is gone too -- `all_hashes()` no longer returns it,
    /// and a subsequent sweep does not re-report it in the returned `Vec`
    /// (under the original bug, the metadata row lingered forever and every
    /// future sweep call re-"deleted" -- a no-op -- the same hash again).
    #[test]
    fn a_deleted_hash_is_forgotten_not_rediscovered_on_the_next_sweep() {
        let mut metadata = FakeMetadataStore::default();
        let mut blobs = FakeBlobStore::default();
        let hash = [5u8; 32];
        metadata.record_seen(hash, 10, "a").unwrap();
        blobs.save_attachment(&hash, b"data").unwrap();

        let t0 = SystemTime::UNIX_EPOCH;
        sweep(&mut metadata, &mut blobs, &HashSet::new(), t0, GRACE).unwrap();

        let t_31_days = t0 + Duration::from_secs(31 * 24 * 60 * 60);
        let deleted = sweep(&mut metadata, &mut blobs, &HashSet::new(), t_31_days, GRACE).unwrap();
        assert_eq!(deleted, vec![hash]);
        assert!(
            metadata.get(&hash).unwrap().is_none(),
            "metadata row must be removed, not just the blob"
        );
        assert!(!metadata.all_hashes().unwrap().contains(&hash));

        let t_62_days = t0 + Duration::from_secs(62 * 24 * 60 * 60);
        let deleted_again = sweep(&mut metadata, &mut blobs, &HashSet::new(), t_62_days, GRACE).unwrap();
        assert!(
            deleted_again.is_empty(),
            "an already-forgotten hash must not be re-reported as deleted on a later sweep"
        );
    }
}

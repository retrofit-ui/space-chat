use redb::{Database, ReadableTable, TableDefinition};
use space_chat_core::storage::{AttachmentMetadata, AttachmentMetadataStore, StorageError};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Value layout: size (8 bytes BE) || mime_len (2 bytes BE) || mime bytes ||
// has_unreferenced (1 byte) || unreferenced_unix_secs (8 bytes BE, only
// meaningful if has_unreferenced == 1). A hand-rolled encoding rather than a
// second dependency (e.g. bincode) -- this table has exactly one value shape
// and it's small enough not to warrant a serialization crate.
const ATTACHMENTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("attachment_metadata");

fn redb_err(e: impl std::fmt::Display) -> StorageError {
    StorageError::Io(e.to_string())
}

fn encode(size: u64, mime: &str, first_seen_unreferenced: Option<SystemTime>) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&size.to_be_bytes());
    let mime_bytes = mime.as_bytes();
    debug_assert!(
        mime_bytes.len() <= u16::MAX as usize,
        "mime string too long to encode with a u16 length prefix"
    );
    buf.extend_from_slice(&(mime_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(mime_bytes);
    match first_seen_unreferenced {
        Some(t) => {
            buf.push(1);
            let secs = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs();
            buf.extend_from_slice(&secs.to_be_bytes());
        }
        None => {
            buf.push(0);
            buf.extend_from_slice(&0u64.to_be_bytes());
        }
    }
    buf
}

fn decode(hash: [u8; 32], bytes: &[u8]) -> AttachmentMetadata {
    let size = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let mime_len = u16::from_be_bytes(bytes[8..10].try_into().unwrap()) as usize;
    let mime = String::from_utf8_lossy(&bytes[10..10 + mime_len]).to_string();
    let has_unreferenced = bytes[10 + mime_len] == 1;
    let secs = u64::from_be_bytes(bytes[10 + mime_len + 1..10 + mime_len + 9].try_into().unwrap());
    let first_seen_unreferenced = has_unreferenced.then(|| UNIX_EPOCH + Duration::from_secs(secs));
    AttachmentMetadata { hash, size, mime, first_seen_unreferenced }
}

pub struct RedbAttachmentMetadataStore {
    db: Arc<Database>,
}

impl RedbAttachmentMetadataStore {
    pub fn new(db: Arc<Database>) -> Result<Self, StorageError> {
        let txn = db.begin_write().map_err(redb_err)?;
        {
            txn.open_table(ATTACHMENTS).map_err(redb_err)?;
        }
        txn.commit().map_err(redb_err)?;
        Ok(Self { db })
    }

    fn read_raw(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, StorageError> {
        let txn = self.db.begin_read().map_err(redb_err)?;
        let table = txn.open_table(ATTACHMENTS).map_err(redb_err)?;
        Ok(table.get(hash.as_slice()).map_err(redb_err)?.map(|v| v.value().to_vec()))
    }
}

impl AttachmentMetadataStore for RedbAttachmentMetadataStore {
    fn record_seen(&mut self, hash: [u8; 32], size: u64, mime: &str) -> Result<(), StorageError> {
        // Idempotent: if already present, preserve its first_seen_unreferenced.
        let existing = self.read_raw(&hash)?.map(|b| decode(hash, &b));
        let first_seen_unreferenced = existing.and_then(|m| m.first_seen_unreferenced);
        let value = encode(size, mime, first_seen_unreferenced);
        let txn = self.db.begin_write().map_err(redb_err)?;
        {
            let mut table = txn.open_table(ATTACHMENTS).map_err(redb_err)?;
            table.insert(hash.as_slice(), value.as_slice()).map_err(redb_err)?;
        }
        txn.commit().map_err(redb_err)
    }

    fn get(&self, hash: &[u8; 32]) -> Result<Option<AttachmentMetadata>, StorageError> {
        Ok(self.read_raw(hash)?.map(|b| decode(*hash, &b)))
    }

    fn all_hashes(&self) -> Result<Vec<[u8; 32]>, StorageError> {
        let txn = self.db.begin_read().map_err(redb_err)?;
        let table = txn.open_table(ATTACHMENTS).map_err(redb_err)?;
        table
            .iter()
            .map_err(redb_err)?
            .map(|res| {
                let (key, _) = res.map_err(redb_err)?;
                let bytes = key.value();
                let hash: [u8; 32] = bytes.try_into().map_err(|_| StorageError::Corrupt("bad hash key length".to_string()))?;
                Ok(hash)
            })
            .collect()
    }

    fn mark_unreferenced_if_unset(&mut self, hash: [u8; 32], now: SystemTime) -> Result<(), StorageError> {
        let Some(existing) = self.read_raw(&hash)?.map(|b| decode(hash, &b)) else {
            return Err(StorageError::NotFound);
        };
        if existing.first_seen_unreferenced.is_some() {
            return Ok(()); // already set -- preserve the earliest timestamp
        }
        let value = encode(existing.size, &existing.mime, Some(now));
        let txn = self.db.begin_write().map_err(redb_err)?;
        {
            let mut table = txn.open_table(ATTACHMENTS).map_err(redb_err)?;
            table.insert(hash.as_slice(), value.as_slice()).map_err(redb_err)?;
        }
        txn.commit().map_err(redb_err)
    }

    fn clear_unreferenced(&mut self, hash: [u8; 32]) -> Result<(), StorageError> {
        let Some(existing) = self.read_raw(&hash)?.map(|b| decode(hash, &b)) else {
            return Err(StorageError::NotFound);
        };
        let value = encode(existing.size, &existing.mime, None);
        let txn = self.db.begin_write().map_err(redb_err)?;
        {
            let mut table = txn.open_table(ATTACHMENTS).map_err(redb_err)?;
            table.insert(hash.as_slice(), value.as_slice()).map_err(redb_err)?;
        }
        txn.commit().map_err(redb_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::storage::AttachmentMetadataStore;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    fn fresh_store() -> (tempfile::TempDir, RedbAttachmentMetadataStore) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(redb::Database::create(dir.path().join("attachments.redb")).unwrap());
        let store = RedbAttachmentMetadataStore::new(db).unwrap();
        (dir, store)
    }

    #[test]
    fn record_seen_then_get_round_trips_size_and_mime() {
        let (_dir, mut store) = fresh_store();
        let hash = [3u8; 32];
        store.record_seen(hash, 1234, "image/png").unwrap();

        let meta = store.get(&hash).unwrap().expect("should be present");
        assert_eq!(meta.size, 1234);
        assert_eq!(meta.mime, "image/png");
        assert_eq!(meta.first_seen_unreferenced, None);
    }

    #[test]
    fn mark_unreferenced_if_unset_only_sets_the_timestamp_once() {
        let (_dir, mut store) = fresh_store();
        let hash = [4u8; 32];
        store.record_seen(hash, 10, "text/plain").unwrap();

        let t1 = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        store.mark_unreferenced_if_unset(hash, t1).unwrap();
        assert_eq!(store.get(&hash).unwrap().unwrap().first_seen_unreferenced, Some(t1));

        let t2 = SystemTime::UNIX_EPOCH + Duration::from_secs(200);
        store.mark_unreferenced_if_unset(hash, t2).unwrap();
        assert_eq!(
            store.get(&hash).unwrap().unwrap().first_seen_unreferenced,
            Some(t1),
            "an already-set first_seen_unreferenced must not be overwritten by a later sweep"
        );
    }

    #[test]
    fn clear_unreferenced_resets_the_timestamp() {
        let (_dir, mut store) = fresh_store();
        let hash = [5u8; 32];
        store.record_seen(hash, 10, "text/plain").unwrap();
        store.mark_unreferenced_if_unset(hash, SystemTime::now()).unwrap();

        store.clear_unreferenced(hash).unwrap();
        assert_eq!(store.get(&hash).unwrap().unwrap().first_seen_unreferenced, None);
    }

    #[test]
    fn all_hashes_lists_every_recorded_hash() {
        let (_dir, mut store) = fresh_store();
        store.record_seen([1u8; 32], 1, "a").unwrap();
        store.record_seen([2u8; 32], 2, "b").unwrap();

        let mut hashes = store.all_hashes().unwrap();
        hashes.sort();
        assert_eq!(hashes, vec![[1u8; 32], [2u8; 32]]);
    }

    // --- Additional tests, added during self-review of the hand-rolled
    // binary encoding (not from the brief): the brief's own tests only ever
    // exercise mime strings of length <= 10 ("image/png", "text/plain") or
    // single characters ("a", "b"). Neither exercises a mime string whose
    // length needs more than one byte to represent (i.e. > 255), which is
    // exactly the boundary most likely to hide an off-by-one in the u16
    // length-prefix logic. Also directly checks `encode`/`decode` round-trip
    // in isolation (not just through the store), including an empty mime and
    // `first_seen_unreferenced: None`.

    #[test]
    fn encode_decode_round_trips_empty_mime_and_none_timestamp() {
        let hash = [7u8; 32];
        let bytes = encode(0, "", None);
        let meta = decode(hash, &bytes);
        assert_eq!(meta.size, 0);
        assert_eq!(meta.mime, "");
        assert_eq!(meta.first_seen_unreferenced, None);
    }

    #[test]
    fn encode_decode_round_trips_long_mime_spanning_two_length_bytes() {
        // 300 > 255, so the u16 length prefix's high byte is nonzero --
        // this is the case a single-byte length field would truncate.
        let long_mime: String = "x".repeat(300);
        let hash = [8u8; 32];
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(42);
        let bytes = encode(u64::MAX, &long_mime, Some(ts));
        let meta = decode(hash, &bytes);
        assert_eq!(meta.size, u64::MAX);
        assert_eq!(meta.mime, long_mime);
        assert_eq!(meta.first_seen_unreferenced, Some(ts));
    }

    #[test]
    fn store_round_trips_long_mime_via_record_seen_and_get() {
        let (_dir, mut store) = fresh_store();
        let hash = [9u8; 32];
        let long_mime: String = "application/vnd.custom+very-long-subtype-name-".repeat(10);
        store.record_seen(hash, 42, &long_mime).unwrap();
        let meta = store.get(&hash).unwrap().unwrap();
        assert_eq!(meta.mime, long_mime);
        assert_eq!(meta.size, 42);
    }
}

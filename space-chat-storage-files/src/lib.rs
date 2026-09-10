use space_chat_core::storage::{AttachmentBlobStore, SegmentBlobStore, StorageError};
use std::fs;
use std::path::PathBuf;

fn io_err(e: std::io::Error) -> StorageError {
    StorageError::Io(e.to_string())
}

/// `SegmentBlobStore` at `<root>/segments/<space_id>/<epoch>.automerge`, per
/// the storage spec's data-placement table.
pub struct FileSegmentStore {
    root: PathBuf,
}

impl FileSegmentStore {
    pub fn new(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("segments"))?;
        Ok(Self { root })
    }

    fn space_dir(&self, space_id: &str) -> PathBuf {
        self.root.join("segments").join(space_id)
    }

    fn epoch_path(&self, space_id: &str, epoch: u64) -> PathBuf {
        self.space_dir(space_id).join(format!("{epoch}.automerge"))
    }
}

impl SegmentBlobStore for FileSegmentStore {
    fn save_segment(&mut self, space_id: &str, epoch: u64, cursor: u64, bytes: &[u8]) -> Result<(), StorageError> {
        fs::create_dir_all(self.space_dir(space_id)).map_err(io_err)?;
        // File layout: 8-byte BE cursor prefix, then the raw Automerge
        // segment bytes. `cursor` is bookkeeping this store owns (per the
        // `SegmentBlobStore` doc comment, it's not recoverable from the
        // Automerge content itself), so it travels with the file rather than
        // needing a second file or an index.
        let mut contents = Vec::with_capacity(8 + bytes.len());
        contents.extend_from_slice(&cursor.to_be_bytes());
        contents.extend_from_slice(bytes);
        fs::write(self.epoch_path(space_id, epoch), contents).map_err(io_err)
    }

    fn load_segment(&self, space_id: &str, epoch: u64) -> Result<Option<(u64, Vec<u8>)>, StorageError> {
        match fs::read(self.epoch_path(space_id, epoch)) {
            Ok(contents) => {
                if contents.len() < 8 {
                    return Err(StorageError::Corrupt(format!(
                        "segment file for {space_id}/{epoch} is shorter than the 8-byte cursor prefix"
                    )));
                }
                let cursor = u64::from_be_bytes(contents[..8].try_into().unwrap());
                Ok(Some((cursor, contents[8..].to_vec())))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err(e)),
        }
    }

    fn list_epochs(&self, space_id: &str) -> Result<Vec<u64>, StorageError> {
        let dir = self.space_dir(space_id);
        if !dir.exists() {
            return Ok(vec![]);
        }
        let mut epochs = vec![];
        for entry in fs::read_dir(&dir).map_err(io_err)? {
            let entry = entry.map_err(io_err)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stem) = name.strip_suffix(".automerge") {
                if let Ok(epoch) = stem.parse::<u64>() {
                    epochs.push(epoch);
                }
            }
        }
        epochs.sort_unstable();
        Ok(epochs)
    }
}

/// `AttachmentBlobStore` at `<root>/attachments/<first 2 hex chars>/<full
/// hex hash>`, git-object-store style, per the storage spec.
pub struct FileAttachmentStore {
    root: PathBuf,
}

impl FileAttachmentStore {
    pub fn new(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("attachments"))?;
        Ok(Self { root })
    }

    fn hash_hex(hash: &[u8; 32]) -> String {
        hash.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn attachment_path(&self, hash: &[u8; 32]) -> PathBuf {
        let hex = Self::hash_hex(hash);
        self.root
            .join("attachments")
            .join(&hex[..2])
            .join(&hex)
    }
}

impl AttachmentBlobStore for FileAttachmentStore {
    fn save_attachment(&mut self, hash: &[u8; 32], bytes: &[u8]) -> Result<(), StorageError> {
        let path = self.attachment_path(hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        fs::write(path, bytes).map_err(io_err)
    }

    fn load_attachment(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, StorageError> {
        match fs::read(self.attachment_path(hash)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err(e)),
        }
    }

    fn delete_attachment(&mut self, hash: &[u8; 32]) -> Result<(), StorageError> {
        match fs::remove_file(self.attachment_path(hash)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::storage::{AttachmentBlobStore, SegmentBlobStore};

    #[test]
    fn segment_store_round_trips_cursor_and_bytes_across_a_fresh_instance() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = FileSegmentStore::new(dir.path()).unwrap();
            store.save_segment("space-1", 3, 7, b"epoch three bytes").unwrap();
        }
        // Fresh instance, same directory -- proves persistence survives restart.
        let store = FileSegmentStore::new(dir.path()).unwrap();
        assert_eq!(
            store.load_segment("space-1", 3).unwrap(),
            Some((7, b"epoch three bytes".to_vec()))
        );
        assert_eq!(store.list_epochs("space-1").unwrap(), vec![3]);
    }

    #[test]
    fn segment_store_returns_none_for_missing_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSegmentStore::new(dir.path()).unwrap();
        assert_eq!(store.load_segment("space-1", 0).unwrap(), None);
        assert_eq!(store.list_epochs("space-1").unwrap(), Vec::<u64>::new());
    }

    #[test]
    fn attachment_store_round_trips_and_deletes_content_addressed_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = FileAttachmentStore::new(dir.path()).unwrap();
        let hash = [7u8; 32];
        store.save_attachment(&hash, b"attachment bytes").unwrap();
        assert_eq!(
            store.load_attachment(&hash).unwrap(),
            Some(b"attachment bytes".to_vec())
        );

        store.delete_attachment(&hash).unwrap();
        assert_eq!(store.load_attachment(&hash).unwrap(), None);
    }
}

use space_chat_core::projection::{Projection, ProjectionError, SegmentChange, SegmentCursor};
use space_chat_core::storage::{SearchIndex, StorageError};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tantivy::doc;
use tantivy::schema::{Schema, Value, STORED, STRING, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};

fn tantivy_err(e: impl std::fmt::Display) -> StorageError {
    StorageError::Io(e.to_string())
}

pub struct TantivySearchIndex {
    index: Index,
    writer: IndexWriter,
    space_id_field: tantivy::schema::Field,
    message_key_field: tantivy::schema::Field,
    content_field: tantivy::schema::Field,
    // Milestone 2 scope note: the watermark is held in memory, not persisted
    // to disk. Per the storage spec's error-handling section, a lost/corrupt
    // derived index (this one included) is recoverable by a full replay from
    // segments -- a startup-time full rebuild here is an acceptable
    // consequence of that same tradeoff, not a bug, so persisting this
    // counter isn't required for correctness. Task 8's kill-and-restart test
    // exercises exactly this path for `ListingIndex` (which does persist its
    // watermark, since it's the immediately-consistent primary view); a
    // `SearchIndex` restart in this milestone always replays from cursor 0.
    watermark: AtomicU64,
}

impl TantivySearchIndex {
    pub fn new(index_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let mut schema_builder = Schema::builder();
        let space_id_field = schema_builder.add_text_field("space_id", STRING | STORED);
        let message_key_field = schema_builder.add_text_field("message_key", STRING | STORED);
        let content_field = schema_builder.add_text_field("content", TEXT);
        let schema = schema_builder.build();

        std::fs::create_dir_all(index_dir.as_ref()).map_err(|e| StorageError::Io(e.to_string()))?;
        let index = Index::open_or_create(
            tantivy::directory::MmapDirectory::open(index_dir).map_err(tantivy_err)?,
            schema,
        )
        .map_err(tantivy_err)?;
        let writer = index.writer(15_000_000).map_err(tantivy_err)?;

        Ok(Self {
            index,
            writer,
            space_id_field,
            message_key_field,
            content_field,
            watermark: AtomicU64::new(0),
        })
    }

    /// Forces a commit + reader reload so a just-indexed message becomes
    /// searchable immediately, instead of waiting for tantivy's normal
    /// near-real-time refresh interval. Test-only: production code accepts
    /// the near-real-time gap the storage spec explicitly says is fine for
    /// search (unlike `ListingIndex`, which must be synchronous).
    ///
    /// Note: `search()` below always builds a brand-new `IndexReader` per
    /// call (`self.index.reader()`), and a freshly-constructed reader always
    /// opens the currently-committed segments synchronously (it doesn't wait
    /// for the `ReloadPolicy::OnCommitWithDelay` background watch to fire) --
    /// so a plain `writer.commit()` is sufficient here; no separate manual
    /// `reader.reload()` call is needed for a *new* reader to observe it.
    pub fn commit_for_test(&mut self) -> Result<(), StorageError> {
        self.writer.commit().map_err(tantivy_err)?;
        Ok(())
    }
}

impl SearchIndex for TantivySearchIndex {
    fn index_message(&mut self, space_id: &str, message_key: &str, content: &str) -> Result<(), StorageError> {
        self.writer
            .add_document(doc!(
                self.space_id_field => space_id,
                self.message_key_field => message_key,
                self.content_field => content,
            ))
            .map_err(tantivy_err)?;
        Ok(())
    }

    fn search(&self, space_id: &str, query: &str) -> Result<Vec<String>, StorageError> {
        let reader = self.index.reader().map_err(tantivy_err)?;
        let searcher = reader.searcher();
        let query_parser = tantivy::query::QueryParser::for_index(&self.index, vec![self.content_field]);
        let parsed_query = query_parser.parse_query(query).map_err(tantivy_err)?;

        let top_docs = searcher
            .search(&parsed_query, &tantivy::collector::TopDocs::with_limit(50))
            .map_err(tantivy_err)?;

        let mut results = vec![];
        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address).map_err(tantivy_err)?;
            let doc_space_id = doc
                .get_first(self.space_id_field)
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if doc_space_id != space_id {
                continue;
            }
            if let Some(key) = doc.get_first(self.message_key_field).and_then(|v| v.as_str()) {
                results.push(key.to_string());
            }
        }
        Ok(results)
    }
}

impl Projection for TantivySearchIndex {
    fn watermark(&self) -> SegmentCursor {
        SegmentCursor(self.watermark.load(Ordering::SeqCst))
    }

    fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError> {
        // Milestone 2 scope note: as with RedbListingIndex::apply (see
        // Task 4), decoding `change.bytes` into individual messages to index
        // is the composition root's job (Task 8's integration test, and
        // later space-chat-app), not this crate's -- keeps
        // space-chat-search-tantivy free of an `automerge` dependency.
        self.watermark.store(change.cursor.0, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::projection::{Projection, SegmentChange, SegmentCursor};
    use space_chat_core::storage::SearchIndex;

    #[test]
    fn indexed_message_is_found_by_a_matching_query_after_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TantivySearchIndex::new(dir.path()).unwrap();

        index.index_message("space-1", "msg:1", "hello from the search test").unwrap();
        index.commit_for_test().unwrap(); // near-real-time -- test forces a commit rather than sleeping

        let results = index.search("space-1", "search").unwrap();
        assert_eq!(results, vec!["msg:1".to_string()]);
    }

    #[test]
    fn search_is_scoped_to_the_given_space_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TantivySearchIndex::new(dir.path()).unwrap();

        index.index_message("space-1", "msg:1", "shared keyword here").unwrap();
        index.index_message("space-2", "msg:2", "shared keyword here").unwrap();
        index.commit_for_test().unwrap();

        let results = index.search("space-1", "keyword").unwrap();
        assert_eq!(results, vec!["msg:1".to_string()]);
    }

    #[test]
    fn watermark_starts_at_zero_and_advances_on_apply() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TantivySearchIndex::new(dir.path()).unwrap();
        assert_eq!(index.watermark(), SegmentCursor(0));

        index
            .apply(&SegmentChange {
                space_id: "space-1".to_string(),
                epoch: 0,
                cursor: SegmentCursor(2),
                bytes: vec![],
            })
            .unwrap();
        assert_eq!(index.watermark(), SegmentCursor(2));
    }

    // Adversarial: same message content/term appears in both spaces, and
    // space-2's message is indexed (and thus has a higher tantivy doc id)
    // *after* space-1's -- guards against a filter that accidentally keys
    // off insertion order, doc id, or only checks the first/last match
    // instead of genuinely filtering every hit by its stored space_id.
    #[test]
    fn cross_space_search_never_leaks_the_other_spaces_matching_message() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TantivySearchIndex::new(dir.path()).unwrap();

        index
            .index_message("space-1", "msg:1", "the quick brown fox jumps")
            .unwrap();
        index
            .index_message("space-2", "msg:2", "the quick brown fox jumps")
            .unwrap();
        index
            .index_message("space-2", "msg:3", "the quick brown fox jumps again")
            .unwrap();
        index.commit_for_test().unwrap();

        let space_1_results = index.search("space-1", "quick brown fox").unwrap();
        assert_eq!(space_1_results, vec!["msg:1".to_string()]);

        let space_2_results = index.search("space-2", "quick brown fox").unwrap();
        assert_eq!(space_2_results.len(), 2);
        assert!(space_2_results.contains(&"msg:2".to_string()));
        assert!(space_2_results.contains(&"msg:3".to_string()));
        assert!(!space_2_results.contains(&"msg:1".to_string()));

        // A space with no indexed messages at all must get nothing, even
        // though the term matches documents that exist in other spaces.
        let space_3_results = index.search("space-3", "quick brown fox").unwrap();
        assert!(space_3_results.is_empty());
    }
}

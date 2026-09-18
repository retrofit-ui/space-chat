use space_chat_core::projection::{Projection, ProjectionError, SegmentChange, SegmentCursor};
use space_chat_core::storage::{SearchIndex, StorageError};
use std::collections::HashMap;
use std::path::Path;
use tantivy::doc;
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, Value, STORED, STRING, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument, Term};

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
    // consequence of that same tradeoff PROVIDED replay is idempotent, which
    // `index_message` below now guarantees (see its own doc comment; this
    // was flagged as a real gap by the Milestone 2 final whole-branch
    // review when the watermark was in-memory-only but re-indexing was NOT
    // idempotent, which would have duplicated every message on every
    // restart). Keyed by (space_id, epoch), not a single scalar -- see
    // `Projection::watermark`'s doc comment for why.
    watermarks: HashMap<(String, u64), u64>,
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
            watermarks: HashMap::new(),
        })
    }

    /// Flushes the `IndexWriter` so indexed-but-uncommitted messages become
    /// searchable. This is a real production method, not test-only: nothing
    /// else in this crate ever calls `IndexWriter::commit`, so a caller
    /// (Milestone 4's composition root) must invoke this periodically --
    /// on a timer, or after a batch of `index_message` calls -- for search
    /// to ever observe new content at all. "Near-real-time" (the storage
    /// spec's accepted staleness for search, unlike `ListingIndex`'s
    /// immediate consistency) describes the gap between indexing and the
    /// next scheduled call to this method, not "commits automatically."
    ///
    /// `search()` below always builds a brand-new `IndexReader` per call
    /// (`self.index.reader()`), and a freshly-constructed reader always
    /// opens the currently-committed segments synchronously (it doesn't
    /// wait for the `ReloadPolicy::OnCommitWithDelay` background watch to
    /// fire) -- so a plain `writer.commit()` is sufficient here; no separate
    /// manual `reader.reload()` call is needed for a *new* reader to
    /// observe it.
    pub fn commit(&mut self) -> Result<(), StorageError> {
        self.writer.commit().map_err(tantivy_err)?;
        Ok(())
    }
}

impl SearchIndex for TantivySearchIndex {
    fn index_message(&mut self, space_id: &str, message_key: &str, content: &str) -> Result<(), StorageError> {
        // Amendment (Milestone 2 final whole-branch review): tantivy has no
        // upsert -- re-indexing the same `message_key` without deleting the
        // old document first adds a SECOND document. Combined with this
        // index's in-memory-only watermark, every restart used to replay
        // from cursor 0 and re-index every message, duplicating the entire
        // index on every restart. Deleting any existing document for this
        // key first makes re-indexing idempotent regardless of why it
        // happens (a genuine edit -- not currently possible per the
        // protocol spec's immutable-message design, but harmless to handle
        // -- or a replay).
        let term = Term::from_field_text(self.message_key_field, message_key);
        self.writer.delete_term(term);
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
        let query_parser = QueryParser::for_index(&self.index, vec![self.content_field]);
        let parsed_query = query_parser.parse_query(query).map_err(tantivy_err)?;

        // Amendment: the original version ran `parsed_query` alone through
        // `TopDocs::with_limit(50)` across the WHOLE (multi-tenant) index,
        // then filtered by `space_id` only after collecting the top 50
        // globally-ranked hits. Under real multi-space load with shared
        // vocabulary, another space's documents could fill that window
        // before this space's genuine matches were ever seen -- a
        // completeness bug, not a leak (small adversarial tests didn't hit
        // the 50-doc window, so they passed anyway). Fixed by pushing the
        // `space_id` constraint into the query itself via a `BooleanQuery`,
        // so `TopDocs` only ever ranks documents already scoped to this
        // space -- no post-hoc filter needed.
        let space_term = Term::from_field_text(self.space_id_field, space_id);
        let space_query: Box<dyn Query> = Box::new(TermQuery::new(space_term, IndexRecordOption::Basic));
        let combined_query = BooleanQuery::new(vec![(Occur::Must, space_query), (Occur::Must, parsed_query)]);

        let top_docs = searcher
            .search(&combined_query, &tantivy::collector::TopDocs::with_limit(50))
            .map_err(tantivy_err)?;

        let mut results = vec![];
        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address).map_err(tantivy_err)?;
            if let Some(key) = doc.get_first(self.message_key_field).and_then(|v| v.as_str()) {
                results.push(key.to_string());
            }
        }
        Ok(results)
    }
}

impl Projection for TantivySearchIndex {
    fn watermark(&self, space_id: &str, epoch: u64) -> SegmentCursor {
        SegmentCursor(
            *self
                .watermarks
                .get(&(space_id.to_string(), epoch))
                .unwrap_or(&0),
        )
    }

    fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError> {
        // Milestone 2 scope note: as with RedbListingIndex::apply (see
        // Task 4), decoding `change.bytes` into individual messages to index
        // is the composition root's job (Task 8's integration test, and
        // later space-chat-app), not this crate's -- keeps
        // space-chat-search-tantivy free of an `automerge` dependency.
        self.watermarks
            .insert((change.space_id.clone(), change.epoch), change.cursor.0);
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
        index.commit().unwrap(); // near-real-time -- test forces a commit rather than sleeping

        let results = index.search("space-1", "search").unwrap();
        assert_eq!(results, vec!["msg:1".to_string()]);
    }

    #[test]
    fn search_is_scoped_to_the_given_space_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TantivySearchIndex::new(dir.path()).unwrap();

        index.index_message("space-1", "msg:1", "shared keyword here").unwrap();
        index.index_message("space-2", "msg:2", "shared keyword here").unwrap();
        index.commit().unwrap();

        let results = index.search("space-1", "keyword").unwrap();
        assert_eq!(results, vec!["msg:1".to_string()]);
    }

    #[test]
    fn watermark_starts_at_zero_and_advances_on_apply() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TantivySearchIndex::new(dir.path()).unwrap();
        assert_eq!(index.watermark("space-1", 0), SegmentCursor(0));

        index
            .apply(&SegmentChange {
                space_id: "space-1".to_string(),
                epoch: 0,
                cursor: SegmentCursor(2),
                bytes: vec![],
            })
            .unwrap();
        assert_eq!(index.watermark("space-1", 0), SegmentCursor(2));
    }

    /// Regression test for the Milestone 2 final whole-branch review:
    /// `index_message` used to have no upsert semantics -- re-indexing the
    /// same `message_key` (e.g. from a replay after a restart, since this
    /// index's watermark is in-memory-only) added a second document,
    /// duplicating search results and growing the index unboundedly.
    #[test]
    fn reindexing_the_same_message_key_does_not_duplicate_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TantivySearchIndex::new(dir.path()).unwrap();

        index.index_message("space-1", "msg:1", "original content").unwrap();
        index.commit().unwrap();
        index.index_message("space-1", "msg:1", "original content").unwrap(); // simulates a replay
        index.commit().unwrap();

        let results = index.search("space-1", "original").unwrap();
        assert_eq!(
            results,
            vec!["msg:1".to_string()],
            "re-indexing the same message_key must not duplicate it"
        );
    }

    /// Regression test for the search-scoping amendment: a space's genuine
    /// match must not be crowded out of `TopDocs::with_limit(50)` by a large
    /// number of same-vocabulary documents belonging to a DIFFERENT space.
    /// Under the original post-hoc-filter design, this scenario risked the
    /// one relevant `space-1` document never appearing in the (globally
    /// ranked, then filtered) top-50 window at all once 60 other-space
    /// documents with identical content compete for the same ranking slots.
    /// With the space_id constraint pushed into the query itself, `space-1`
    /// has exactly one matching document in its own scope, so it's always
    /// found regardless of how many unrelated documents exist elsewhere.
    #[test]
    fn a_matching_message_is_found_even_when_outnumbered_by_another_spaces_documents() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TantivySearchIndex::new(dir.path()).unwrap();

        for i in 0..60 {
            index
                .index_message("space-other", &format!("msg:other-{i}"), "the quick brown fox jumps")
                .unwrap();
        }
        index
            .index_message("space-1", "msg:mine", "the quick brown fox jumps")
            .unwrap();
        index.commit().unwrap();

        let results = index.search("space-1", "quick brown fox").unwrap();
        assert_eq!(results, vec!["msg:mine".to_string()]);
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
        index.commit().unwrap();

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

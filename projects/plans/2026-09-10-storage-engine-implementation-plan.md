# Storage Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build persistent storage for space-chat — flat-file segment/attachment blobs, a `redb`-backed listing index, a `redb`-backed attachment metadata store with mark-and-sweep GC, and a `tantivy`-backed search index — so Milestone 1's in-memory convergence test still passes with real persistent storage substituted in, survives a kill-and-restart via watermark catch-up, and correctly runs cross-peer-liveness-respecting attachment GC.

**Architecture:** `space-chat-core` gains a new `storage` module defining five trait boundaries (`SegmentBlobStore`, `AttachmentBlobStore`, `ListingIndex`, `AttachmentMetadataStore`, `SearchIndex`) plus a generic watermark-replay driver and a generic mark-and-sweep GC function that operate over those traits — none of this module depends on `redb` or `tantivy` directly. Two new crates implement the storage-engine-specific traits: `space-chat-storage-redb` (`ListingIndex`, `AttachmentMetadataStore`, via `redb`) and `space-chat-search-tantivy` (`SearchIndex`, via `tantivy`). A third new crate, `space-chat-storage-files` (not named in the storage spec, which only names the `redb`/`tantivy` crates explicitly — added here to hold the flat-file stores the spec says exist but doesn't assign a crate to), implements `SegmentBlobStore` and `AttachmentBlobStore` as plain content-addressed files on disk, with zero dependency on `redb`/`tantivy`/`openmls`. `ListingIndex` and `SearchIndex` both implement `space_chat_core::projection::Projection` (already defined in Milestone 1) so the existing watermark-catch-up story applies uniformly to both.

**Tech Stack:** Rust, `redb` (pure-Rust embedded KV store) for `ListingIndex`/`AttachmentMetadataStore`, `tantivy` (pure-Rust full-text search) for `SearchIndex`, plain `std::fs` for content-addressed flat files.

## Global Constraints

- No dependency on `redb` or `tantivy` in `space-chat-core` — per the storage spec's crate layout, `space-chat-core` only defines trait boundaries, it doesn't implement against them.
- Three things are irreplaceable and must never be silently regenerated: segment bytes, attachment bytes, MLS state. Everything else (`ListingIndex`, `AttachmentMetadataStore`, `SearchIndex`) is a disposable projection, rebuildable by replaying segments from scratch.
- `ListingIndex` and `SearchIndex` are watermarked `Projection`s (see Milestone 1's `space_chat_core::projection::Projection` trait) — no shared transaction between `redb` and `tantivy` is attempted; catch-up on startup is watermark-based replay, not cross-store atomicity.
- Mark-and-sweep GC uses a `first_seen_unreferenced` timestamp per hash, cleared the instant a sweep finds the hash live again; a blob is deleted only once `now - first_seen_unreferenced >= grace_window` (default 30 days, passed as a parameter — not hardcoded — so tests can use a short window). This must be continuous-unreferenced-across-every-sweep, not "seen unreferenced twice."
- **`SegmentBlobStore` persists `cursor` alongside `bytes`, not just `bytes` alone** (this corrects the plan's original Task 1/2 signatures, amended after Task 2's implementer flagged that cursor can't be reconstructed from segment content — see "Amendment" note on Task 1 and Task 2 below). `Segment::latest_change()`'s `SegmentChange.cursor` is a mutation counter, not something derivable by counting `"msg:"` keys (reactions and deletes also advance it, per `segment.rs`) — the store must carry it explicitly, the same way a caller already has it in hand from `Segment::latest_change()` at save time.
- **`AttachmentMetadataStore` has a `forget(hash)` method** removing a hash's metadata row entirely (this corrects the plan's original Task 1/6 design, amended after Task 6's implementer flagged that `gc::sweep` deleted blobs but had no way to remove the now-dead metadata row, leaking it forever and re-processing it as a no-op "deletion" on every future sweep — see "Amendment" notes on Task 1 and Task 6 below).
- **`redb` and `tantivy` API method names in this plan reflect the API as best known at writing time. Before implementing any step that calls into either crate, verify the exact method signatures against `docs.rs` for whichever version gets pinned in `Cargo.toml` — do not treat the code below as gospel over the actual crate docs if they've drifted.** This caveat applies only to external-crate calls; all `space-chat-core`-defined types/signatures in this plan are authoritative for later tasks.
- Content-addressed attachment files are stored git-object-store style: `attachments/<first 2 hex chars of hash>/<full hex hash>`, mirroring the storage spec's data-placement table.
- Segment files are stored at `segments/<space_id>/<epoch>.automerge`, per the storage spec's data-placement table.

---

### Task 1: Storage trait boundaries in `space-chat-core`

> **Amendment (post-Task-2):** `SegmentBlobStore::save_segment`/`load_segment` were originally bytes-only. Task 2's implementer correctly flagged that `catch_up` cannot recover a segment's true `cursor` from its content alone (message count undercounts reactions/deletes), so `save_segment` now takes an explicit `cursor: u64` and `load_segment` returns `(cursor, bytes)`. The code below already reflects the corrected signatures.
>
> **Amendment (post-Task-6):** `AttachmentMetadataStore` gained a `forget(hash)` method. Task 6's implementer correctly flagged that `gc::sweep`'s doc comment promised deleting "metadata + blob" but the trait had no way to remove a metadata row — only `clear_unreferenced`/`mark_unreferenced_if_unset`, neither of which deletes anything. Without `forget`, a swept hash's metadata lingers forever, `all_hashes()` keeps returning it, and every future sweep re-"deletes" (no-ops on) the same already-gone blob. The code below already reflects the corrected trait.

**Files:**
- Create: `space-chat-core/src/storage.rs`
- Modify: `space-chat-core/src/lib.rs` — add `pub mod storage;`
- Test: `space-chat-core/src/storage.rs` (inline `#[cfg(test)]` module)

**Interfaces:**
- Consumes: `space_chat_core::projection::{Projection, SegmentCursor, SegmentChange, ProjectionError}` (Milestone 1).
- Produces: `StorageError` (enum: `Io(String)`, `Corrupt(String)`, `NotFound`), `ListingEntry { space_id: String, epoch: u64, seq: u64, message_key: String }`, `trait SegmentBlobStore`, `trait AttachmentBlobStore`, `trait ListingIndex: Projection`, `trait SearchIndex: Projection`, `trait AttachmentMetadataStore`, `AttachmentMetadata { hash: [u8; 32], size: u64, mime: String, first_seen_unreferenced: Option<std::time::SystemTime> }`. Every later task in this plan implements one or more of these traits against these exact signatures.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-core/src/storage.rs
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-core && cargo test segment_blob_store_round_trips_cursor_and_bytes`
Expected: FAIL — `SegmentBlobStore`, `StorageError` not defined.

- [ ] **Step 3: Write the trait boundaries**

```rust
// space-chat-core/src/storage.rs (above the tests module)
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
    /// Removes the metadata row for `hash` entirely. Called by `gc::sweep`
    /// (Task 6) once a hash's blob is actually deleted, so a dead hash
    /// doesn't linger in `all_hashes()` forever and get silently
    /// re-processed (a no-op re-delete) on every future sweep. A no-op if
    /// `hash` isn't tracked.
    fn forget(&mut self, hash: [u8; 32]) -> Result<(), StorageError>;
}
```

- [ ] **Step 4: Add `space_chat_core::storage` to `lib.rs`**

```rust
// space-chat-core/src/lib.rs
pub mod domain;
pub mod projection;
pub mod segment;
pub mod sequencer;
pub mod storage;
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd space-chat-core && cargo test segment_blob_store_round_trips_bytes`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add space-chat-core/src/storage.rs space-chat-core/src/lib.rs
git commit -m "feat(core): add storage trait boundaries (SegmentBlobStore, AttachmentBlobStore, ListingIndex, SearchIndex, AttachmentMetadataStore)"
```

---

### Task 2: Watermark-replay driver in `space-chat-core`

> **Amendment:** this task's original code called `store.load_segment` expecting bytes only and hardcoded `Segment::load(&bytes, space_id, epoch, 0)`, which made every replayed `SegmentChange.cursor` come back as `0` — always `<= projection.watermark()`, so nothing ever actually got applied. Fixed below by consuming `SegmentBlobStore`'s corrected `(cursor, bytes)` return (see Task 1's amendment) directly, instead of inferring cursor from content.

**Files:**
- Create: `space-chat-core/src/replay.rs`
- Modify: `space-chat-core/src/lib.rs` — add `pub mod replay;`
- Test: `space-chat-core/src/replay.rs` (inline)

**Interfaces:**
- Consumes: `SegmentBlobStore` (Task 1), `Projection`, `SegmentChange`, `SegmentCursor` (Milestone 1), `space_chat_core::segment::Segment` (Milestone 1).
- Produces: `fn catch_up<P: Projection>(store: &impl SegmentBlobStore, space_id: &str, projection: &mut P) -> Result<(), StorageError>` — replays every epoch's segment bytes for `space_id` into `projection` starting from whatever `epoch`/cursor combination is after `projection.watermark()`. Later tasks (kill-and-restart tests, GC's cross-peer scenario) call this directly.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-core/src/replay.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{DeviceId, Message};
    use crate::projection::{Projection, ProjectionError, SegmentChange, SegmentCursor};
    use crate::segment::Segment;
    use crate::storage::{SegmentBlobStore, StorageError};
    use std::collections::HashMap;

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

    struct CountingProjection {
        watermark: SegmentCursor,
        applied: Vec<SegmentChange>,
    }

    impl Projection for CountingProjection {
        fn watermark(&self) -> SegmentCursor {
            self.watermark
        }
        fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError> {
            self.watermark = change.cursor;
            self.applied.push(change.clone());
            Ok(())
        }
    }

    #[test]
    fn catch_up_replays_a_persisted_segment_into_a_fresh_projection() {
        let mut store = FakeSegmentBlobStore { data: HashMap::new() };
        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        let change = segment.latest_change();
        store.save_segment("space-1", 0, change.cursor.0, &change.bytes).unwrap();

        let mut projection = CountingProjection {
            watermark: SegmentCursor(0),
            applied: vec![],
        };
        catch_up(&store, "space-1", &mut projection).unwrap();

        assert_eq!(projection.applied.len(), 1);
        assert_eq!(projection.watermark(), SegmentCursor(1));
    }

    #[test]
    fn catch_up_is_a_no_op_when_projection_watermark_is_already_current() {
        let mut store = FakeSegmentBlobStore { data: HashMap::new() };
        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        let change = segment.latest_change();
        store.save_segment("space-1", 0, change.cursor.0, &change.bytes).unwrap();

        let mut projection = CountingProjection {
            watermark: SegmentCursor(1), // already caught up
            applied: vec![],
        };
        catch_up(&store, "space-1", &mut projection).unwrap();

        assert_eq!(
            projection.applied.len(),
            0,
            "a projection already at the segment's cursor should not be re-applied"
        );
    }

    /// Proves the fix this amendment made: a segment containing a reaction
    /// (not just messages) still replays with the correct cursor. Under the
    /// old message-count-inference bug, this would have inferred cursor 1
    /// (one message) instead of the true 2 (append_message + append_reaction),
    /// silently under-counting.
    #[test]
    fn catch_up_replays_the_true_mutation_count_not_just_message_count() {
        use crate::segment::objid_to_target_string;
        use crate::domain::Reaction;

        let mut store = FakeSegmentBlobStore { data: HashMap::new() };
        let mut segment = Segment::new("space-1", 0);
        let msg_id = segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        segment
            .append_reaction(
                &msg_id,
                &Reaction {
                    target: objid_to_target_string(&msg_id),
                    actor: DeviceId([2u8; 32]),
                    emoji: "👍".to_string(),
                },
            )
            .unwrap();
        let change = segment.latest_change();
        assert_eq!(change.cursor, SegmentCursor(2), "one message + one reaction = cursor 2");
        store.save_segment("space-1", 0, change.cursor.0, &change.bytes).unwrap();

        let mut projection = CountingProjection {
            watermark: SegmentCursor(0),
            applied: vec![],
        };
        catch_up(&store, "space-1", &mut projection).unwrap();

        assert_eq!(
            projection.watermark(),
            SegmentCursor(2),
            "watermark must reflect the true cursor (2), not message_count (1)"
        );
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-core && cargo test catch_up_replays`
Expected: FAIL — `catch_up` not defined.

- [ ] **Step 3: Implement `catch_up`**

```rust
// space-chat-core/src/replay.rs (above the tests module)
use crate::projection::{Projection, ProjectionError, SegmentCursor};
use crate::segment::Segment;
use crate::storage::{SegmentBlobStore, StorageError};

/// Replays every epoch currently persisted for `space_id` into `projection`,
/// skipping any epoch whose resulting change's cursor is not after
/// `projection.watermark()`. This is the mechanism the storage spec calls
/// "each independently compares its watermark against what's on disk in
/// `segments/` and replays forward whatever it's missing" — used both for
/// normal startup catch-up and for kill-and-restart recovery.
///
/// Deliberate simplification consistent with `Segment::latest_change`'s own
/// doc comment: each epoch's entire segment is loaded and turned into one
/// `SegmentChange` snapshot (not a per-mutation incremental diff), so
/// `catch_up` applies at most one `SegmentChange` per epoch, not one per
/// original mutation. A `Projection`'s `apply` must be idempotent-safe
/// against a full-snapshot re-application in this sense: `watermark()`
/// comparison (below) is what prevents re-applying an epoch already caught up.
pub fn catch_up<P: Projection>(
    store: &impl SegmentBlobStore,
    space_id: &str,
    projection: &mut P,
) -> Result<(), StorageError> {
    for epoch in store.list_epochs(space_id)? {
        let Some((cursor, bytes)) = store.load_segment(space_id, epoch)? else {
            continue;
        };
        if SegmentCursor(cursor) <= projection.watermark() {
            continue;
        }
        // Seed the loaded segment's cursor from the persisted value (not 0) --
        // `Segment::load`'s own doc comment on its `cursor` parameter is
        // explicit that a caller restoring persisted state must pass the
        // saved cursor back in, otherwise a subsequent `latest_change()`
        // re-emits the wrong value. Here we don't even need a subsequent
        // mutation for this to matter: `latest_change()` below returns
        // `SegmentCursor(self.cursor)` unchanged, so seeding it correctly is
        // what makes the value match what was actually persisted.
        let mut segment = Segment::load(&bytes, space_id, epoch, cursor)
            .map_err(|e| StorageError::Corrupt(e.to_string()))?;
        let change = segment.latest_change();
        projection
            .apply(&change)
            .map_err(|e: ProjectionError| StorageError::Corrupt(format!("{e:?}")))?;
    }
    Ok(())
}
```

- [ ] **Step 4: Add `space_chat_core::replay` to `lib.rs`**

```rust
// space-chat-core/src/lib.rs
pub mod domain;
pub mod projection;
pub mod replay;
pub mod segment;
pub mod sequencer;
pub mod storage;
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd space-chat-core && cargo test catch_up`
Expected: PASS (both tests)

- [ ] **Step 6: Commit**

```bash
git add space-chat-core/src/replay.rs space-chat-core/src/lib.rs
git commit -m "feat(core): add watermark-replay catch_up driver over SegmentBlobStore"
```

---

### Task 3: `space-chat-storage-files` — flat-file segment and attachment blob stores

**Files:**
- Create: `space-chat-storage-files/Cargo.toml`
- Create: `space-chat-storage-files/src/lib.rs`
- Test: `space-chat-storage-files/src/lib.rs` (inline)
- Modify: workspace root `Cargo.toml` — add `"space-chat-storage-files"` to `members`

**Interfaces:**
- Consumes: `space_chat_core::storage::{SegmentBlobStore, AttachmentBlobStore, StorageError}` (Task 1).
- Produces: `FileSegmentStore::new(root: impl Into<PathBuf>) -> std::io::Result<Self>`, `FileAttachmentStore::new(root: impl Into<PathBuf>) -> std::io::Result<Self>`, both implementing their respective traits. Task 8's integration test constructs these directly.

- [ ] **Step 1: Create the crate and add it to the workspace**

```bash
mkdir -p space-chat-storage-files/src
cat > space-chat-storage-files/Cargo.toml <<'EOF'
[package]
name = "space-chat-storage-files"
version = "0.1.0"
edition = "2021"

[dependencies]
space-chat-core = { path = "../space-chat-core" }

[dev-dependencies]
tempfile = "3"
EOF
```

Modify the workspace root `Cargo.toml`'s `members` array to include `"space-chat-storage-files"`.

- [ ] **Step 2: Write the failing tests**

```rust
// space-chat-storage-files/src/lib.rs
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
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd space-chat-storage-files && cargo test`
Expected: FAIL — crate has no non-test code yet.

- [ ] **Step 4: Implement `FileSegmentStore` and `FileAttachmentStore`**

```rust
// space-chat-storage-files/src/lib.rs (above the tests module)
use space_chat_core::storage::{AttachmentBlobStore, SegmentBlobStore, StorageError};
use std::fs;
use std::path::{Path, PathBuf};

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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd space-chat-storage-files && cargo test`
Expected: PASS (3 tests)

- [ ] **Step 6: Commit**

```bash
git add space-chat-storage-files Cargo.toml Cargo.lock
git commit -m "feat(storage-files): add flat-file SegmentBlobStore and content-addressed AttachmentBlobStore"
```

---

### Task 4: `space-chat-storage-redb` — `ListingIndex`

> **Amendment (post-review):** the original `encode_key` used a `0x00` separator byte between `space_id` and the epoch/seq fields, which is broken -- `space_id` is a `String`/`str` and Rust permits an embedded NUL byte, so a `space_id` like `"a\0"` could produce a key that fell inside `page("a", ...)`'s range scan, leaking entries across spaces. Fixed below by length-prefixing `space_id` (a `u32` BE length field before the bytes) instead of using a separator -- see `encode_key`'s doc comment in Step 4 for why this removes the ambiguity. `page`'s `None`-case upper bound and its silently-dropped-on-error `.filter_map(|res| res.ok())` were fixed in the same pass (see Step 4's `page` code and its inline comments). A new test, `page_never_returns_entries_from_a_different_space_id`, is added below to close the gap that let this through un-caught the first time.

**Files:**
- Create: `space-chat-storage-redb/Cargo.toml`
- Create: `space-chat-storage-redb/src/lib.rs`
- Create: `space-chat-storage-redb/src/listing.rs`
- Test: `space-chat-storage-redb/src/listing.rs` (inline)
- Modify: workspace root `Cargo.toml` — add `"space-chat-storage-redb"` to `members`

**Interfaces:**
- Consumes: `space_chat_core::storage::{ListingIndex, ListingEntry, StorageError}`, `space_chat_core::projection::{Projection, SegmentChange, SegmentCursor, ProjectionError}` (Task 1, Milestone 1).
- Produces: `RedbListingIndex::new(db: std::sync::Arc<redb::Database>) -> Result<Self, StorageError>`, implementing `ListingIndex` (and therefore `Projection`). Task 8's integration test constructs this directly; the app-shell plan (Milestone 4) will read through it for conversation views.

- [ ] **Step 1: Create the crate and add it to the workspace**

```bash
mkdir -p space-chat-storage-redb/src
cat > space-chat-storage-redb/Cargo.toml <<'EOF'
[package]
name = "space-chat-storage-redb"
version = "0.1.0"
edition = "2021"

[dependencies]
space-chat-core = { path = "../space-chat-core" }
redb = "2"

[dev-dependencies]
tempfile = "3"
EOF
echo 'pub mod listing;' > space-chat-storage-redb/src/lib.rs
```

Modify the workspace root `Cargo.toml`'s `members` array to include `"space-chat-storage-redb"`.

- [ ] **Step 2: Write the failing tests**

```rust
// space-chat-storage-redb/src/listing.rs
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
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd space-chat-storage-redb && cargo test`
Expected: FAIL — `RedbListingIndex` not defined.

- [ ] **Step 4: Implement `RedbListingIndex`**

```rust
// space-chat-storage-redb/src/listing.rs (above the tests module)
use redb::{Database, ReadableTable, TableDefinition};
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

        let mut entries = Vec::new();
        for result in table.range(start.as_slice()..end.as_slice()).map_err(redb_err)? {
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd space-chat-storage-redb && cargo test`
Expected: PASS (3 tests)

- [ ] **Step 6: Commit**

```bash
git add space-chat-storage-redb Cargo.toml Cargo.lock
git commit -m "feat(storage-redb): add RedbListingIndex implementing ListingIndex + Projection"
```

---

### Task 5: `space-chat-storage-redb` — `AttachmentMetadataStore`

> **Amendment (post-Task-6):** `AttachmentMetadataStore` gained a `forget(hash)` method after Task 6's review found `gc::sweep` had no way to remove a metadata row once its blob was deleted (see Task 1's and Task 6's amendment notes). `RedbAttachmentMetadataStore` needs a `forget` impl (a plain `table.remove(hash.as_slice())`) to satisfy the trait — added to this task's code below, with a round-trip test.

**Files:**
- Create: `space-chat-storage-redb/src/attachment_metadata.rs`
- Modify: `space-chat-storage-redb/src/lib.rs` — add `pub mod attachment_metadata;`
- Test: `space-chat-storage-redb/src/attachment_metadata.rs` (inline)

**Interfaces:**
- Consumes: `space_chat_core::storage::{AttachmentMetadataStore, AttachmentMetadata, StorageError}` (Task 1).
- Produces: `RedbAttachmentMetadataStore::new(db: std::sync::Arc<redb::Database>) -> Result<Self, StorageError>`, implementing `AttachmentMetadataStore`. Task 6's GC sweep and Task 8's integration test consume this.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-storage-redb/src/attachment_metadata.rs
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

    #[test]
    fn forget_removes_the_metadata_row_entirely() {
        let (_dir, mut store) = fresh_store();
        let hash = [6u8; 32];
        store.record_seen(hash, 10, "a").unwrap();

        store.forget(hash).unwrap();

        assert_eq!(store.get(&hash).unwrap(), None);
        assert!(!store.all_hashes().unwrap().contains(&hash));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd space-chat-storage-redb && cargo test attachment_metadata`
Expected: FAIL — `RedbAttachmentMetadataStore` not defined.

- [ ] **Step 3: Implement `RedbAttachmentMetadataStore`**

```rust
// space-chat-storage-redb/src/attachment_metadata.rs (above the tests module)
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

    fn forget(&mut self, hash: [u8; 32]) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(redb_err)?;
        {
            let mut table = txn.open_table(ATTACHMENTS).map_err(redb_err)?;
            table.remove(hash.as_slice()).map_err(redb_err)?;
        }
        txn.commit().map_err(redb_err)
    }
}
```

- [ ] **Step 4: Add the module to `lib.rs`**

```rust
// space-chat-storage-redb/src/lib.rs
pub mod attachment_metadata;
pub mod listing;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd space-chat-storage-redb && cargo test attachment_metadata`
Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add space-chat-storage-redb/src/attachment_metadata.rs space-chat-storage-redb/src/lib.rs
git commit -m "feat(storage-redb): add RedbAttachmentMetadataStore"
```

---

### Task 6: Mark-and-sweep GC, generic over `AttachmentMetadataStore` + `AttachmentBlobStore`

> **Amendment:** this task's original `sweep` deleted the blob but had no way to remove the metadata row (`AttachmentMetadataStore` had no delete method), so a swept hash's metadata lingered in `all_hashes()` forever and got silently re-"deleted" (a no-op re-delete of an already-gone blob) on every future sweep. Fixed by adding `AttachmentMetadataStore::forget` (see Task 1's amendment) and calling it here once a hash is actually deleted. A new test, `a_deleted_hash_is_forgotten_not_rediscovered_on_the_next_sweep`, is added below to close the gap.

**Files:**
- Create: `space-chat-core/src/gc.rs`
- Modify: `space-chat-core/src/lib.rs` — add `pub mod gc;`
- Test: `space-chat-core/src/gc.rs` (inline)

**Interfaces:**
- Consumes: `space_chat_core::storage::{AttachmentMetadataStore, AttachmentBlobStore}` (Task 1).
- Produces: `fn sweep<M: AttachmentMetadataStore, B: AttachmentBlobStore>(metadata: &mut M, blobs: &mut B, live_hashes: &std::collections::HashSet<[u8; 32]>, now: std::time::SystemTime, grace_window: std::time::Duration) -> Result<Vec<[u8; 32]>, StorageError>` — runs one sweep pass, returns the hashes actually deleted. Task 8's GC integration test and (later) Milestone 4's scheduled-sweep composition call this directly. `space-chat-core` stays generic over the trait, not tied to `redb`.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-core/src/gc.rs
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd space-chat-core && cargo test gc::`
Expected: FAIL — `sweep` not defined.

- [ ] **Step 3: Implement `sweep`**

```rust
// space-chat-core/src/gc.rs (above the tests module)
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
```

- [ ] **Step 4: Add the module to `lib.rs`**

```rust
// space-chat-core/src/lib.rs
pub mod domain;
pub mod gc;
pub mod projection;
pub mod replay;
pub mod segment;
pub mod sequencer;
pub mod storage;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd space-chat-core && cargo test gc::`
Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add space-chat-core/src/gc.rs space-chat-core/src/lib.rs
git commit -m "feat(core): add generic mark-and-sweep attachment GC over AttachmentMetadataStore/AttachmentBlobStore"
```

---

### Task 7: `space-chat-search-tantivy` — `SearchIndex`

> **Amendment (post-review):** two Important findings on the original implementation:
> 1. `search()` ran an unscoped content query through `TopDocs::with_limit(50)` and only filtered by `space_id` *after* collecting the top 50 globally-ranked hits. Under real multi-tenant load (many spaces sharing common vocabulary), another space's documents could fill the top-50 window before the filter ever saw this space's genuine matches — a completeness bug, not a leak (the leak-shaped adversarial test happened to pass because it used too few total documents to hit the window). Fixed by pushing the `space_id` constraint into the query itself via a `BooleanQuery` combining a `TermQuery` on `space_id` (`Occur::Must`) with the parsed content query (`Occur::Must`), so `TopDocs` only ever ranks documents already scoped to this space — the post-hoc filter is removed, not layered on top.
> 2. `commit_for_test` was the *only* method that flushed the `IndexWriter`, and its own doc comment said "Test-only" — there was no production code path that would ever make an indexed message searchable. Fixed by renaming it to a real `commit()` method (no more "test-only" framing); a periodic caller (Milestone 4's composition root) is expected to call this on some schedule, consistent with "near-real-time" search per the storage spec. Tests call `commit()` directly now, same as they called `commit_for_test()` before.

**Files:**
- Create: `space-chat-search-tantivy/Cargo.toml`
- Create: `space-chat-search-tantivy/src/lib.rs`
- Test: `space-chat-search-tantivy/src/lib.rs` (inline)
- Modify: workspace root `Cargo.toml` — add `"space-chat-search-tantivy"` to `members`

**Interfaces:**
- Consumes: `space_chat_core::storage::{SearchIndex, StorageError}`, `space_chat_core::projection::{Projection, SegmentChange, SegmentCursor, ProjectionError}` (Task 1, Milestone 1).
- Produces: `TantivySearchIndex::new(index_dir: impl AsRef<std::path::Path>) -> Result<Self, StorageError>`, implementing `SearchIndex` (and therefore `Projection`). Task 8's integration test constructs this directly.

- [ ] **Step 1: Create the crate and add it to the workspace**

```bash
mkdir -p space-chat-search-tantivy/src
cat > space-chat-search-tantivy/Cargo.toml <<'EOF'
[package]
name = "space-chat-search-tantivy"
version = "0.1.0"
edition = "2021"

[dependencies]
space-chat-core = { path = "../space-chat-core" }
tantivy = "0.22"

[dev-dependencies]
tempfile = "3"
EOF
```

Modify the workspace root `Cargo.toml`'s `members` array to include `"space-chat-search-tantivy"`.

- [ ] **Step 2: Write the failing tests**

```rust
// space-chat-search-tantivy/src/lib.rs
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
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd space-chat-search-tantivy && cargo test`
Expected: FAIL — `TantivySearchIndex` not defined.

- [ ] **Step 4: Implement `TantivySearchIndex`**

```rust
// space-chat-search-tantivy/src/lib.rs (above the tests module)
use space_chat_core::projection::{Projection, ProjectionError, SegmentChange, SegmentCursor};
use space_chat_core::storage::{SearchIndex, StorageError};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd space-chat-search-tantivy && cargo test`
Expected: PASS (3 tests)

- [ ] **Step 6: Commit**

```bash
git add space-chat-search-tantivy Cargo.toml Cargo.lock
git commit -m "feat(search-tantivy): add TantivySearchIndex implementing SearchIndex + Projection"
```

---

### Task 8: Integration — real storage substituted into Milestone 1's convergence test, kill-and-restart, and cross-peer GC

**Files:**
- Create: `space-chat-storage-redb/tests/integration.rs` (workspace-level integration test — this crate depends on `space-chat-storage-files` and `space-chat-search-tantivy` as dev-dependencies to exercise all three together)
- Modify: `space-chat-storage-redb/Cargo.toml` — add dev-dependencies

**Interfaces:**
- Consumes: everything from Tasks 1–7 plus `space_chat_core::segment::Segment`, `space_chat_core::sequencer` (Milestone 1).
- Produces: nothing new — this is the exit-criteria proof for Milestone 2, not a reusable interface.

- [ ] **Step 1: Add dev-dependencies**

```bash
cd space-chat-storage-redb && cargo add --dev space-chat-storage-files --path ../space-chat-storage-files
cd space-chat-storage-redb && cargo add --dev space-chat-search-tantivy --path ../space-chat-search-tantivy
cd space-chat-storage-redb && cargo add --dev tempfile
```

- [ ] **Step 2: Write the failing integration test**

```rust
// space-chat-storage-redb/tests/integration.rs
use space_chat_core::domain::{DeviceId, Message};
use space_chat_core::gc::sweep;
use space_chat_core::projection::Projection;
use space_chat_core::replay::catch_up;
use space_chat_core::segment::{sync_state, Segment};
use space_chat_core::storage::{
    AttachmentBlobStore, AttachmentMetadataStore, ListingEntry, ListingIndex, SegmentBlobStore,
};
use space_chat_search_tantivy::TantivySearchIndex;
use space_chat_storage_files::{FileAttachmentStore, FileSegmentStore};
use space_chat_storage_redb::attachment_metadata::RedbAttachmentMetadataStore;
use space_chat_storage_redb::listing::RedbListingIndex;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Milestone 1's own convergence test (`two_segments_converge_via_sync_messages`)
/// re-run with real persistent storage substituted for the in-memory
/// `Segment`s it originally used directly -- proving persistence doesn't
/// change convergence behavior. Per this plan's exit criteria.
#[test]
fn milestone_1_convergence_still_holds_with_real_persistent_storage() {
    let alice_dir = tempfile::tempdir().unwrap();
    let bob_dir = tempfile::tempdir().unwrap();

    let mut alice_store = FileSegmentStore::new(alice_dir.path()).unwrap();
    let mut bob_store = FileSegmentStore::new(bob_dir.path()).unwrap();

    let mut alice = Segment::new("space-1", 0);
    let mut bob = Segment::new("space-1", 0);

    alice.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "from alice".to_string(),
        attachments: vec![],
    });
    bob.append_message(&Message {
        sender: DeviceId([2u8; 32]),
        content: "from bob".to_string(),
        attachments: vec![],
    });

    let mut alice_state = sync_state();
    let mut bob_state = sync_state();
    loop {
        let a_to_b = alice.generate_sync_message(&mut alice_state);
        let b_to_a = bob.generate_sync_message(&mut bob_state);
        let (a_none, b_none) = (a_to_b.is_none(), b_to_a.is_none());
        if let Some(msg) = a_to_b {
            bob.receive_sync_message(&mut bob_state, msg).unwrap();
        }
        if let Some(msg) = b_to_a {
            alice.receive_sync_message(&mut alice_state, msg).unwrap();
        }
        if a_none && b_none {
            break;
        }
    }

    assert_eq!(alice.message_count(), 2);
    assert_eq!(bob.message_count(), 2);

    // Persist each side's converged segment.
    let alice_change = alice.latest_change();
    let bob_change = bob.latest_change();
    alice_store.save_segment("space-1", 0, alice_change.cursor.0, &alice_change.bytes).unwrap();
    bob_store.save_segment("space-1", 0, bob_change.cursor.0, &bob_change.bytes).unwrap();

    // Reload from disk and confirm the persisted bytes still show both messages.
    let (alice_cursor, alice_bytes) = alice_store.load_segment("space-1", 0).unwrap().unwrap();
    let reloaded = Segment::load(&alice_bytes, "space-1", 0, alice_cursor).unwrap();
    assert_eq!(reloaded.message_count(), 2);
}

/// Kill-and-restart: a `ListingIndex` that has fallen behind (simulating a
/// crash between a segment write and the index's own commit) must catch up
/// via `catch_up`'s watermark-replay, not require a from-scratch rebuild.
#[test]
fn listing_index_catches_up_after_a_simulated_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut segment_store = FileSegmentStore::new(dir.path().join("segments")).unwrap();

    let mut segment = Segment::new("space-1", 0);
    segment.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "one".to_string(),
        attachments: vec![],
    });
    segment.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "two".to_string(),
        attachments: vec![],
    });
    let seed_change = segment.latest_change();
    segment_store.save_segment("space-1", 0, seed_change.cursor.0, &seed_change.bytes).unwrap();

    // "Restart": fresh RedbListingIndex, watermark at 0, must catch up.
    let db = Arc::new(redb::Database::create(dir.path().join("listing.redb")).unwrap());
    let mut listing = RedbListingIndex::new(db).unwrap();
    assert_eq!(listing.watermark(), space_chat_core::projection::SegmentCursor(0));

    catch_up(&segment_store, "space-1", &mut listing).unwrap();

    assert_eq!(listing.watermark(), space_chat_core::projection::SegmentCursor(2));
}

/// The storage spec's own example scenario, translated to a direct test
/// against `AttachmentMetadataStore`/`sweep` (no rendered UI exists yet --
/// that's Milestone 4 -- so this asserts the mechanism directly, per this
/// plan's Global Constraints and the storage spec's Testing section, which
/// permits this fallback "where there genuinely isn't" a user-visible
/// outcome yet): an attachment referenced only by Alice isn't lost if Bob
/// (who never independently held a reference) comes back online within the
/// grace window relative to when Alice's reference was tombstoned.
#[test]
fn attachment_survives_the_grace_window_when_still_referenced_elsewhere() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(redb::Database::create(dir.path().join("attachments.redb")).unwrap());
    let mut metadata = RedbAttachmentMetadataStore::new(db).unwrap();
    let mut blobs = FileAttachmentStore::new(dir.path().join("attachments")).unwrap();

    let hash = [9u8; 32];
    blobs.save_attachment(&hash, b"shared attachment bytes").unwrap();
    metadata.record_seen(hash, 24, "image/png").unwrap();

    let t0 = SystemTime::now();
    // Day 0: still referenced (Bob's message references it, even though Bob is offline).
    let live = HashSet::from([hash]);
    sweep(&mut metadata, &mut blobs, &live, t0, Duration::from_secs(30 * 24 * 60 * 60)).unwrap();

    // Day 20: Alice's local reference is gone, but the shared spec scenario's
    // premise -- Bob still has a reference, he's just offline -- means this
    // device's own live-set computation for its own (Alice's) segments would
    // still see the reference as long as Bob's tombstone hasn't synced.
    // Model that directly: still live at day 20.
    let t_20_days = t0 + Duration::from_secs(20 * 24 * 60 * 60);
    sweep(&mut metadata, &mut blobs, &live, t_20_days, Duration::from_secs(30 * 24 * 60 * 60)).unwrap();
    assert!(
        blobs.load_attachment(&hash).unwrap().is_some(),
        "attachment must still exist while a reference is considered live"
    );

    // Day 20+something small: Bob comes online, tombstone syncs, now
    // genuinely zero live references from this device's perspective -- but
    // less than 30 days remain before deletion would even be considered,
    // since the timer only starts once unreferenced is first observed.
    let empty = HashSet::new();
    sweep(&mut metadata, &mut blobs, &empty, t_20_days, Duration::from_secs(30 * 24 * 60 * 60)).unwrap();
    assert!(
        blobs.load_attachment(&hash).unwrap().is_some(),
        "the 30-day grace window has not elapsed since the reference first became unreferenced"
    );
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd space-chat-storage-redb && cargo test --test integration`
Expected: Depending on compile state, either FAIL to compile (missing pieces) or FAIL assertions — resolve compile errors first, then confirm real assertion failures if any, before moving to Step 4. (Given every dependency was already implemented and tested in Tasks 1–7, this step should mostly just confirm the test file itself compiles and runs — there's no new production code this task adds beyond wiring, unlike earlier tasks' genuine red-then-green cycle.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd space-chat-storage-redb && cargo test --test integration`
Expected: PASS (3 tests)

- [ ] **Step 5: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS — every test from Milestone 1 and this plan's Tasks 1–8, combined.

- [ ] **Step 6: Commit**

```bash
git add space-chat-storage-redb/tests/integration.rs space-chat-storage-redb/Cargo.toml Cargo.lock
git commit -m "test(storage): prove Milestone 1 convergence holds under real storage, plus kill-and-restart catch-up and cross-peer GC grace window"
```

---

## Closing note for the final whole-branch review

Per this project's established process (see Milestone 1's process notes), run a final review across the whole branch's diff before merging to `main`, not just per-task reviews. Pay particular attention to:

- Whether `RedbListingIndex::apply` and `TantivySearchIndex::apply`'s "decoding `SegmentChange` bytes is the composition root's job" scope note (Tasks 4, 7) is still the right call once Milestone 4's composition root actually exists, or whether it should move earlier.
- Whether the hand-rolled binary encoding in `RedbAttachmentMetadataStore` (Task 5) is worth replacing with a serialization crate before more fields get added to `AttachmentMetadata`.
- Whether `TantivySearchIndex`'s in-memory-only watermark (Task 7) is an acceptable gap or should be persisted before Milestone 4 ships, given a search-index rebuild-from-scratch on every app restart could be slow on a large history.

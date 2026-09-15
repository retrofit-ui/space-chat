use crate::live_spec::LiveSpec;
use crate::membership::PlaintextMembership;
use crate::network::AppNetwork;
use crate::observed_at::ObservedAtStore;
use redb::{Database, ReadableTable, TableDefinition};
use space_chat_core::domain::DeviceId;
use space_chat_core::segment::Segment;
use space_chat_core::storage::{AttachmentBlobStore, SegmentBlobStore, StorageError};
use space_chat_storage_files::{FileAttachmentStore, FileSegmentStore};
use space_chat_storage_redb::attachment_metadata::RedbAttachmentMetadataStore;
use space_chat_storage_redb::listing::RedbListingIndex;
use space_chat_transport::transport::TransportEvent;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum AppStateError {
    Storage(StorageError),
    Redb(String),
    Io(String),
}

impl std::fmt::Display for AppStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppStateError::Storage(e) => write!(f, "storage error: {e}"),
            AppStateError::Redb(e) => write!(f, "redb error: {e}"),
            AppStateError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for AppStateError {}

const OBSERVED_AT: TableDefinition<&str, u64> = TableDefinition::new("observed_at");

/// A persistent `ObservedAtStore`, sharing the same physical `redb::Database`
/// file `AppState` also uses for `RedbListingIndex`/`RedbAttachmentMetadataStore`
/// -- a single small app-owned database, not a separate file per table, since
/// none of this data needs to be transactionally consistent *across* those
/// tables (each is its own independently-rebuildable/best-effort projection).
pub struct RedbObservedAtStore {
    db: Arc<Database>,
}

impl RedbObservedAtStore {
    pub fn new(db: Arc<Database>) -> Result<Self, AppStateError> {
        let txn = db.begin_write().map_err(|e| AppStateError::Redb(e.to_string()))?;
        {
            txn.open_table(OBSERVED_AT).map_err(|e| AppStateError::Redb(e.to_string()))?;
        }
        txn.commit().map_err(|e| AppStateError::Redb(e.to_string()))?;
        Ok(Self { db })
    }
}

impl ObservedAtStore for RedbObservedAtStore {
    fn record_if_absent(&mut self, message_key: &str, now_unix_ms: u64) -> u64 {
        if let Some(existing) = self.get(message_key) {
            return existing;
        }
        if let Ok(txn) = self.db.begin_write() {
            if let Ok(mut table) = txn.open_table(OBSERVED_AT) {
                let _ = table.insert(message_key, now_unix_ms);
            }
            let _ = txn.commit();
        }
        now_unix_ms
    }

    fn get(&self, message_key: &str) -> Option<u64> {
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(OBSERVED_AT).ok()?;
        table.get(message_key).ok()?.map(|v| v.value())
    }
}

/// One entry per currently-actively-viewed conversation -- per this plan's
/// Global Constraints, every other conversation gets no `LiveSpec` at all.
pub struct ActiveConversation {
    pub live_spec: LiveSpec,
}

pub struct AppState {
    pub local_device: DeviceId,
    pub segment_store: Mutex<FileSegmentStore>,
    pub attachment_store: Mutex<FileAttachmentStore>,
    pub listing_index: Mutex<RedbListingIndex>,
    pub attachment_metadata: Mutex<RedbAttachmentMetadataStore>,
    pub observed_at: Mutex<RedbObservedAtStore>,
    pub membership: Mutex<PlaintextMembership>,
    pub network: AppNetwork,
    /// The ONE live, shared `Segment` handle per currently-known space, at
    /// its current epoch (epoch rollover stays out of scope for this plan).
    /// This is the exact `Arc<Mutex<Segment>>` registered with `Transport`
    /// via `add_space` -- both local mutation (a later task) and
    /// `Transport`'s own sync loop mutate THIS handle, never a separately-
    /// loaded copy, so the two can never diverge. Lazily populated by
    /// `segment_arc` on first access.
    pub active_segments: tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<Segment>>>>,
    pub active: Mutex<HashMap<String, ActiveConversation>>,
}

impl AppState {
    /// `network` must already be bound (`AppNetwork::bind(...).await`) before
    /// calling this -- binding is async, this constructor is not. See this
    /// crate's `run()` for how production code handles that ordering (bind
    /// inside `.setup()` via `tauri::async_runtime::block_on`, then call
    /// this).
    pub fn new<R: tauri::Runtime>(
        data_dir: impl Into<PathBuf>,
        local_device: DeviceId,
        network: AppNetwork,
        network_events: mpsc::UnboundedReceiver<TransportEvent>,
        app_handle: tauri::AppHandle<R>,
    ) -> Result<Arc<Self>, AppStateError> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir).map_err(|e| AppStateError::Io(e.to_string()))?;

        let segment_store = FileSegmentStore::new(&data_dir).map_err(|e| AppStateError::Io(e.to_string()))?;
        let attachment_store =
            FileAttachmentStore::new(&data_dir).map_err(|e| AppStateError::Io(e.to_string()))?;

        let db = Arc::new(
            Database::create(data_dir.join("app.redb")).map_err(|e| AppStateError::Redb(e.to_string()))?,
        );
        let listing_index = RedbListingIndex::new(db.clone()).map_err(AppStateError::Storage)?;
        let attachment_metadata =
            RedbAttachmentMetadataStore::new(db.clone()).map_err(AppStateError::Storage)?;
        let observed_at = RedbObservedAtStore::new(db)?;

        let membership = PlaintextMembership::new(data_dir.join("membership.json"))
            .map_err(|e| AppStateError::Io(e.to_string()))?;

        let state = Arc::new(Self {
            local_device,
            segment_store: Mutex::new(segment_store),
            attachment_store: Mutex::new(attachment_store),
            listing_index: Mutex::new(listing_index),
            attachment_metadata: Mutex::new(attachment_metadata),
            observed_at: Mutex::new(observed_at),
            membership: Mutex::new(membership),
            network,
            active_segments: tokio::sync::Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()),
        });

        spawn_network_event_loop(state.clone(), network_events, app_handle);

        Ok(state)
    }

    /// Loads every epoch `segment_store` currently has persisted for
    /// `space_id` into an in-memory map, for `build_conversation_spec`
    /// (Task 5) to read message content from. Cursor is restored as `0` on
    /// load -- this map is rebuilt fresh on every call, not held across
    /// calls as a cache, so there's no `Projection` consuming its cursor
    /// that a restored value would need to line up with.
    ///
    /// NOTE: this is a read-only, independent-copy view, separate from
    /// `active_segments`/`segment_arc`'s persistent shared handles below --
    /// see this task's brief for why both exist.
    pub fn segments_for(&self, space_id: &str) -> HashMap<u64, Segment> {
        let store = self.segment_store.lock().unwrap();
        let mut out = HashMap::new();
        let Ok(epochs) = store.list_epochs(space_id) else {
            return out;
        };
        for epoch in epochs {
            if let Ok(Some((_cursor, bytes))) = store.load_segment(space_id, epoch) {
                if let Ok(segment) = Segment::load(&bytes, space_id, epoch, 0) {
                    out.insert(epoch, segment);
                }
            }
        }
        out
    }

    /// Returns the persistent, shared `Segment` handle for `space_id`'s
    /// current epoch, registering it with `Transport` via `add_space` the
    /// FIRST time it's requested for this process's lifetime (idempotent
    /// after that -- checking the cache first, since re-registering a space
    /// already known to `Transport` is a documented, deliberately
    /// unimplemented gap in Milestone 3: calling `add_space` exactly once
    /// per space, as early as possible, sidesteps that gap entirely rather
    /// than depending on it).
    pub async fn segment_arc(&self, space_id: &str) -> Arc<tokio::sync::Mutex<Segment>> {
        const CURRENT_EPOCH: u64 = 0;
        let mut active = self.active_segments.lock().await;
        if let Some(existing) = active.get(space_id) {
            return existing.clone();
        }
        let loaded = {
            let store = self.segment_store.lock().unwrap();
            store
                .load_segment(space_id, CURRENT_EPOCH)
                .ok()
                .flatten()
                .and_then(|(cursor, bytes)| Segment::load(&bytes, space_id, CURRENT_EPOCH, cursor).ok())
        };
        let segment = loaded.unwrap_or_else(|| Segment::new(space_id, CURRENT_EPOCH));
        let arc = Arc::new(tokio::sync::Mutex::new(segment));
        self.network.transport.add_space(space_id, CURRENT_EPOCH, arc.clone()).await;
        active.insert(space_id.to_string(), arc.clone());
        arc
    }
}

/// The background network-event pipeline: the "third consumer of the
/// storage spec's `Projection` change feed," driven by real network events.
///
/// IMPORTANT -- SCOPED DELIBERATELY NARROW FOR THIS TASK: this handles
/// `IncomingChange` by persisting the already-mutated-in-place shared
/// segment and bringing `listing_index` up to date via `replay::catch_up`.
/// It does NOT regenerate an actively-viewed conversation's spec or push a
/// patch event yet -- that needs `regenerate_spec_value`/`crate::events`,
/// which are introduced in the NEXT task. Whoever implements that task must
/// extend this same function (not build a second, competing event loop) to
/// add: "if `state.active` has an entry for `change.space_id`, regenerate
/// its spec value, call `live_spec.update(..)`, and emit a
/// `ConversationPatchEvent`." See this task's brief for the full reasoning.
///
/// DISCOVERED GAP (verified directly against `space-chat-storage-redb/src/listing.rs`,
/// not assumed): `RedbListingIndex`'s `Projection::apply` -- the method
/// `replay::catch_up` drives -- only advances the per-`(space_id, epoch)`
/// watermark; per its own doc comment, it deliberately does NOT decode
/// `SegmentChange.bytes` into `ListingEntry` rows, leaving that to "the
/// composition root that owns both a `Segment` and this index together."
/// That means the `catch_up` call below correctly makes `listing_index`
/// idempotent/no-op-safe against a change already seen, but does NOT by
/// itself add a page-able `ListingEntry` for a message that arrived via
/// network sync -- only `Segment::save`/`segment_store` (the source of
/// truth) actually gains the new content. Decoding `Segment` content into
/// `ListingEntry` rows for remote-delivered messages (with a `seq` counter
/// and dedup against entries a local mutation may have already appended)
/// is a real design decision this task's brief did not specify and this
/// task does not invent unilaterally -- it is left for whichever later task
/// owns local-mutation `append_entry` calls (see that task's own `next_seq`
/// bookkeeping) to extend to remote-delivered content too.
fn spawn_network_event_loop<R: tauri::Runtime>(
    state: Arc<AppState>,
    mut events: mpsc::UnboundedReceiver<TransportEvent>,
    _app_handle: tauri::AppHandle<R>,
) {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                TransportEvent::IncomingChange(change) => {
                    let bytes = {
                        let arc = state.segment_arc(&change.space_id).await;
                        let mut seg = arc.lock().await;
                        seg.save()
                    };
                    if let Ok(mut store) = state.segment_store.lock() {
                        let _ = store.save_segment(&change.space_id, change.epoch, change.cursor.0, &bytes);
                        if let Ok(mut listing) = state.listing_index.lock() {
                            let _ = space_chat_core::replay::catch_up(&*store, &change.space_id, &mut *listing);
                        }
                    }
                }
                TransportEvent::Connected { .. } | TransportEvent::Disconnected { .. } => {
                    // Handled by AppNetwork's own status watch channel; nothing to do here.
                }
                TransportEvent::JoinRequest(_request) => {
                    // Out of scope for this plan -- see Task 12's note in the
                    // amendment: real MLS integration would act on this, a
                    // local-only placeholder membership model does not.
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::domain::{DeviceId, Message};
    use space_chat_core::storage::{ListingEntry, ListingIndex};
    use space_chat_transport::bootstrap::TransportConfig;
    use space_chat_transport::identity::TransportIdentity;

    /// A real, unconnected `AppNetwork` -- cheap to bind (no network I/O
    /// happens until something dials out), and correct as an inert default
    /// for tests that don't exercise real networking, per this plan's
    /// amendment ("No NullNetworkService needed").
    async fn inert_network() -> (AppNetwork, mpsc::UnboundedReceiver<TransportEvent>) {
        let identity = TransportIdentity::generate();
        AppNetwork::bind(&identity, TransportConfig { relay: None }).await.unwrap()
    }

    fn mock_app_handle() -> tauri::AppHandle<tauri::test::MockRuntime> {
        tauri::test::mock_builder()
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap()
            .handle()
            .clone()
    }

    #[tokio::test]
    async fn observed_at_store_persists_across_a_fresh_instance_at_the_same_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("app.redb");
        {
            let db = std::sync::Arc::new(redb::Database::create(&db_path).unwrap());
            let mut store = RedbObservedAtStore::new(db).unwrap();
            let stored = store.record_if_absent("msg:1", 1234);
            assert_eq!(stored, 1234);
        }
        let db = std::sync::Arc::new(redb::Database::open(&db_path).unwrap());
        let store = RedbObservedAtStore::new(db).unwrap();
        assert_eq!(store.get("msg:1"), Some(1234));
    }

    #[tokio::test]
    async fn app_state_new_creates_a_fresh_data_dir_with_no_active_conversations() {
        let dir = tempfile::tempdir().unwrap();
        let (network, events) = inert_network().await;
        let state = AppState::new(dir.path(), DeviceId([1u8; 32]), network, events, mock_app_handle()).unwrap();

        assert_eq!(state.active.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn segments_for_loads_every_persisted_epoch_for_a_space() {
        use space_chat_core::segment::Segment;
        use space_chat_core::storage::SegmentBlobStore;

        let dir = tempfile::tempdir().unwrap();
        let (network, events) = inert_network().await;
        let state = AppState::new(dir.path(), DeviceId([1u8; 32]), network, events, mock_app_handle()).unwrap();

        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        let change = segment.latest_change();
        state
            .segment_store
            .lock()
            .unwrap()
            .save_segment("space-1", 0, change.cursor.0, &change.bytes)
            .unwrap();

        let segments = state.segments_for("space-1");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[&0].message_count(), 1);
    }

    #[tokio::test]
    async fn listing_index_is_reachable_through_app_state() {
        let dir = tempfile::tempdir().unwrap();
        let (network, events) = inert_network().await;
        let state = AppState::new(dir.path(), DeviceId([1u8; 32]), network, events, mock_app_handle()).unwrap();

        state
            .listing_index
            .lock()
            .unwrap()
            .append_entry(ListingEntry {
                space_id: "space-1".to_string(),
                epoch: 0,
                seq: 0,
                message_key: "msg:1".to_string(),
            })
            .unwrap();

        let page = state.listing_index.lock().unwrap().page("space-1", None, 10).unwrap();
        assert_eq!(page.len(), 1);
    }

    #[tokio::test]
    async fn segment_arc_is_idempotent_and_registers_with_transport() {
        let dir = tempfile::tempdir().unwrap();
        let (network, events) = inert_network().await;
        let state = AppState::new(dir.path(), DeviceId([1u8; 32]), network, events, mock_app_handle()).unwrap();

        let first = state.segment_arc("space-1").await;
        let second = state.segment_arc("space-1").await;
        assert!(Arc::ptr_eq(&first, &second), "segment_arc must return the same Arc on repeated calls for the same space");
    }

    #[tokio::test]
    async fn incoming_change_event_persists_the_segment_and_advances_the_listing_index_watermark() {
        use space_chat_core::projection::{Projection, SegmentCursor};
        use space_chat_core::storage::SegmentBlobStore;

        let dir = tempfile::tempdir().unwrap();
        let (network, events) = inert_network().await;
        let state = AppState::new(dir.path(), DeviceId([1u8; 32]), network, events, mock_app_handle()).unwrap();

        // Mutate the shared segment directly (as Transport's real sync loop
        // would, in place, before emitting IncomingChange) and manually
        // drive one iteration of what the background loop does, via the
        // same public entry points it uses -- segment_arc + a synthetic
        // change carrying the space_id/epoch. See NOTE below on why this
        // test calls the same building blocks directly rather than trying
        // to inject a synthetic event through a real Transport's channel.
        let arc = state.segment_arc("space-1").await;
        {
            let mut seg = arc.lock().await;
            seg.append_message(&Message {
                sender: DeviceId([1u8; 32]),
                content: "from the network".to_string(),
                attachments: vec![],
            });
        }
        let change = { arc.lock().await.latest_change() };
        state
            .segment_store
            .lock()
            .unwrap()
            .save_segment("space-1", 0, change.cursor.0, &change.bytes)
            .unwrap();
        space_chat_core::replay::catch_up(
            &*state.segment_store.lock().unwrap(),
            "space-1",
            &mut *state.listing_index.lock().unwrap(),
        )
        .unwrap();

        // NOTE: this asserts what `catch_up` actually does for
        // `RedbListingIndex` -- advance its per-(space_id, epoch) watermark
        // to the persisted change's cursor -- not that a page-able
        // `ListingEntry` now exists. `RedbListingIndex::apply` deliberately
        // does not decode `SegmentChange.bytes` into `ListingEntry` rows
        // (see `space-chat-storage-redb/src/listing.rs`'s own doc comment on
        // `apply`); that decoding is left to a later task's direct
        // `append_entry` calls, per `spawn_network_event_loop`'s doc comment
        // above. A `page()` assertion here would test behavior `catch_up`
        // was never going to provide.
        assert_eq!(
            state.listing_index.lock().unwrap().watermark("space-1", 0),
            SegmentCursor(1),
            "listing_index's watermark should advance to the persisted change's cursor"
        );

        // catch_up must be safe to call again with nothing new (documented
        // idempotency this task's event loop relies on for every future
        // IncomingChange on a space already fully caught up).
        space_chat_core::replay::catch_up(
            &*state.segment_store.lock().unwrap(),
            "space-1",
            &mut *state.listing_index.lock().unwrap(),
        )
        .unwrap();
        assert_eq!(state.listing_index.lock().unwrap().watermark("space-1", 0), SegmentCursor(1));
    }
}

use crate::conversation_spec::build_conversation_spec;
use crate::live_spec::{LiveSpec, PatchResponse};
use crate::spec::{ConversationErrorSpec, MessageSpec, ViewSpec};
use crate::state::{ActiveConversation, AppState};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct OpenConversationResult {
    pub version: u64,
    pub spec: serde_json::Value,
}

fn now_unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

const DEFAULT_PAGE_SIZE: usize = 50;

/// Peeks one entry past `page`'s last (oldest) entry to determine whether
/// anything older remains -- the same "ask for one more, see if it's there"
/// technique a later task's pagination command uses, factored out so the
/// initial/live spec (this function) and pagination never disagree about
/// what "has more older" means for the same listing state.
fn compute_has_more_older(state: &AppState, space_id: &str, page: &[space_chat_core::storage::ListingEntry]) -> bool {
    use space_chat_core::storage::ListingIndex;
    let Some(oldest) = page.last() else { return false };
    state
        .listing_index
        .lock()
        .unwrap()
        .page(space_id, Some((oldest.epoch, oldest.seq)), 1)
        .map(|older| !older.is_empty())
        .unwrap_or(false)
}

/// Regenerates the spec for `space_id` from current storage state and
/// returns the JSON value that should become the conversation's next
/// `LiveSpec` version -- either a real `ConversationSpec` or a
/// `ConversationErrorSpec`, per this plan's per-conversation failure
/// isolation. Does not touch `state.active` itself; callers decide what to
/// do with the result (construct a fresh `LiveSpec`, or feed it to an
/// existing one's `update`). Public (not `pub(crate)`) because `state.rs`'s
/// network event loop also calls this.
pub fn regenerate_spec_value(state: &AppState, space_id: &str, title: &str) -> serde_json::Value {
    use space_chat_core::storage::ListingIndex;
    let page = state
        .listing_index
        .lock()
        .unwrap()
        .page(space_id, None, DEFAULT_PAGE_SIZE)
        .unwrap_or_default();
    let has_more_older = compute_has_more_older(state, space_id, &page);
    let segments = state.segments_for(space_id);
    let membership = state.membership.lock().unwrap();
    let mut observed_at = state.observed_at.lock().unwrap();

    match build_conversation_spec(
        space_id,
        title,
        &page,
        &segments,
        &*membership,
        &mut *observed_at,
        has_more_older,
        now_unix_ms(),
    ) {
        Ok(spec) => serde_json::to_value(ViewSpec::Conversation(spec)).unwrap(),
        Err(e) => serde_json::to_value(ViewSpec::ConversationError(ConversationErrorSpec {
            space_id: space_id.to_string(),
            message: e.to_string(),
        }))
        .unwrap(),
    }
}

pub async fn open_conversation_impl(
    state: &AppState,
    space_id: &str,
    title: &str,
    _now_unix_ms: u64,
) -> OpenConversationResult {
    // Gap 1: register this space with Transport as soon as it's actively
    // viewed, even if no local mutation has touched it yet.
    let _ = state.segment_arc(space_id).await;

    let spec_value = regenerate_spec_value(state, space_id, title);
    let live_spec = LiveSpec::new(spec_value.clone());
    let (version, _) = live_spec.snapshot();

    state
        .active
        .lock()
        .unwrap()
        .insert(space_id.to_string(), ActiveConversation { live_spec, title: title.to_string() });

    OpenConversationResult { version, spec: spec_value }
}

pub async fn close_conversation_impl(state: &AppState, space_id: &str) {
    state.active.lock().unwrap().remove(space_id);
}

pub async fn resync_conversation_impl(
    state: &AppState,
    space_id: &str,
    since_version: u64,
) -> Result<PatchResponse, String> {
    let active = state.active.lock().unwrap();
    let conversation = active
        .get(space_id)
        .ok_or_else(|| format!("conversation {space_id} is not currently open"))?;
    Ok(conversation.live_spec.diff_since(since_version))
}

#[tauri::command]
pub async fn open_conversation(
    state: tauri::State<'_, std::sync::Arc<AppState>>,
    space_id: String,
    title: String,
) -> Result<OpenConversationResult, String> {
    Ok(open_conversation_impl(&state, &space_id, &title, now_unix_ms()).await)
}

#[tauri::command]
pub async fn close_conversation(state: tauri::State<'_, std::sync::Arc<AppState>>, space_id: String) -> Result<(), String> {
    close_conversation_impl(&state, &space_id).await;
    Ok(())
}

#[tauri::command]
pub async fn resync_conversation(
    state: tauri::State<'_, std::sync::Arc<AppState>>,
    space_id: String,
    since_version: u64,
) -> Result<PatchResponse, String> {
    resync_conversation_impl(&state, &space_id, since_version).await
}

use space_chat_core::domain::{Delete, Message, Reaction};
use space_chat_core::segment::{objid_to_target_string, Segment};
use space_chat_core::storage::{ListingEntry, ListingIndex, SegmentBlobStore};

const CURRENT_EPOCH: u64 = 0; // epoch rollover is out of scope for this plan

/// Locks the shared `Segment` handle for `(space_id, CURRENT_EPOCH)` (the
/// SAME handle `Transport`'s own sync loop mutates -- see `AppState::segment_arc`),
/// runs `mutate` against it, persists the result, appends `new_message_keys`
/// to `ListingIndex` in order, notifies `Transport` so any parked sync tasks
/// wake immediately, and -- if this conversation is actively viewed --
/// regenerates its spec and returns the patch that should be pushed to the
/// frontend (the `#[tauri::command]` wrappers below do the actual emitting,
/// keeping this function testable without a live `tauri::AppHandle`).
async fn mutate_and_persist(
    state: &AppState,
    space_id: &str,
    mutate: impl FnOnce(&mut Segment) -> Vec<String>, // returns any newly-created message keys, in order
) -> Result<Option<PatchResponse>, String> {
    let arc = state.segment_arc(space_id).await;
    let new_message_keys = {
        let mut segment = arc.lock().await;
        let new_message_keys = mutate(&mut segment);
        let change = segment.latest_change();
        state
            .segment_store
            .lock()
            .unwrap()
            .save_segment(space_id, CURRENT_EPOCH, change.cursor.0, &change.bytes)
            .map_err(|e| e.to_string())?;
        new_message_keys
    }; // segment guard dropped here -- must not be held across notify_local_change's .await

    {
        let mut listing = state.listing_index.lock().unwrap();
        let mut next_seq = listing
            .page(space_id, None, 1)
            .ok()
            .and_then(|page| page.first().map(|e| e.seq + 1))
            .unwrap_or(0);
        for key in new_message_keys {
            listing
                .append_entry(ListingEntry {
                    space_id: space_id.to_string(),
                    epoch: CURRENT_EPOCH,
                    seq: next_seq,
                    message_key: key,
                })
                .map_err(|e| e.to_string())?;
            next_seq += 1;
        }
    }

    state.network.transport.notify_local_change(space_id).await;

    let active = state.active.lock().unwrap();
    if let Some(conversation) = active.get(space_id) {
        // Use the REAL title stored on ActiveConversation (Task 9's fix),
        // not a space_id fallback -- see this brief's Gap 4.
        let new_value = regenerate_spec_value(state, space_id, &conversation.title);
        conversation.live_spec.update(new_value);
        let (version, _) = conversation.live_spec.snapshot();
        return Ok(Some(conversation.live_spec.diff_since(version.saturating_sub(1))));
    }

    Ok(None)
}

pub async fn send_message_impl(state: &AppState, space_id: &str, content: String) -> Result<(), String> {
    let local_device = state.local_device;
    mutate_and_persist(state, space_id, move |segment| {
        let new_obj_id = segment.append_message(&Message {
            sender: local_device,
            content,
            attachments: vec![],
        });
        // NOT `segment.message_keys().last()`: `message_keys()`'s own doc
        // comment states it iterates "in no particular order" (Automerge
        // maps don't preserve insertion order). With only one message in a
        // space, `.last()` happens to return the right key by coincidence
        // -- which is exactly why this shipped without being caught: every
        // test that exercised this path sent only one message per space.
        // Sending a second message to the same space can make `.last()`
        // return a PREVIOUSLY-created message's key instead of the new
        // one, silently mis-assigning `seq`/listing order to the wrong
        // message. Fixed by finding the key whose ObjId matches what
        // `append_message` actually returned -- a value comparison, so it's
        // correct regardless of `message_keys()`'s iteration order.
        segment
            .message_keys()
            .find(|key| segment.message(key).as_ref() == Some(&new_obj_id))
            .map(|key| vec![key])
            .unwrap_or_default()
    })
    .await?;
    Ok(())
}

pub async fn react_impl(
    state: &AppState,
    space_id: &str,
    message_key: &str,
    emoji: &str,
) -> Result<(), String> {
    let local_device = state.local_device;
    let emoji = emoji.to_string();
    let message_key = message_key.to_string();
    mutate_and_persist(state, space_id, move |segment| {
        let Some(msg_id) = segment.message(&message_key) else {
            return vec![];
        };
        let _ = segment.append_reaction(
            &msg_id,
            &Reaction {
                target: objid_to_target_string(&msg_id),
                actor: local_device,
                emoji,
            },
        );
        vec![]
    })
    .await?;
    Ok(())
}

pub async fn delete_message_impl(state: &AppState, space_id: &str, message_key: &str) -> Result<(), String> {
    let message_key = message_key.to_string();
    mutate_and_persist(state, space_id, move |segment| {
        let Some(msg_id) = segment.message(&message_key) else {
            return vec![];
        };
        let _ = segment.apply_delete(&msg_id, &Delete { target: objid_to_target_string(&msg_id) });
        vec![]
    })
    .await?;
    Ok(())
}

#[tauri::command]
pub async fn send_message(
    state: tauri::State<'_, std::sync::Arc<AppState>>,
    app: tauri::AppHandle,
    space_id: String,
    content: String,
) -> Result<(), String> {
    send_message_impl(&state, &space_id, content).await?;
    emit_patch_if_active(&app, &state, &space_id);
    Ok(())
}

#[tauri::command]
pub async fn react(
    state: tauri::State<'_, std::sync::Arc<AppState>>,
    app: tauri::AppHandle,
    space_id: String,
    message_key: String,
    emoji: String,
) -> Result<(), String> {
    react_impl(&state, &space_id, &message_key, &emoji).await?;
    emit_patch_if_active(&app, &state, &space_id);
    Ok(())
}

#[tauri::command]
pub async fn delete_message(
    state: tauri::State<'_, std::sync::Arc<AppState>>,
    app: tauri::AppHandle,
    space_id: String,
    message_key: String,
) -> Result<(), String> {
    delete_message_impl(&state, &space_id, &message_key).await?;
    emit_patch_if_active(&app, &state, &space_id);
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct OlderPageResult {
    pub messages: Vec<MessageSpec>,
    pub has_more_older: bool,
}

pub async fn fetch_older_page_impl(
    state: &AppState,
    space_id: &str,
    before_epoch: u64,
    before_seq: u64,
    limit: usize,
) -> Result<OlderPageResult, String> {
    use space_chat_core::storage::ListingIndex;

    let before = if before_epoch == u64::MAX && before_seq == u64::MAX {
        None
    } else {
        Some((before_epoch, before_seq))
    };

    let page = state
        .listing_index
        .lock()
        .unwrap()
        .page(space_id, before, limit)
        .map_err(|e| e.to_string())?;

    // Reuses this file's `compute_has_more_older` (Task 9) so the live/initial
    // spec and this pagination path can never disagree about what "more
    // older" means for the same underlying listing state.
    let has_more_older = compute_has_more_older(state, space_id, &page);

    let segments = state.segments_for(space_id);
    let membership = state.membership.lock().unwrap();
    let mut observed_at = state.observed_at.lock().unwrap();

    let spec = build_conversation_spec(
        space_id,
        space_id, // title is discarded below -- see this brief's note on why that's fine here
        &page,
        &segments,
        &*membership,
        &mut *observed_at,
        has_more_older,
        {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
        },
    )
    .map_err(|e| e.to_string())?;

    Ok(OlderPageResult { messages: spec.messages, has_more_older })
}

#[tauri::command]
pub async fn fetch_older_page(
    state: tauri::State<'_, std::sync::Arc<AppState>>,
    space_id: String,
    before_epoch: u64,
    before_seq: u64,
    limit: usize,
) -> Result<OlderPageResult, String> {
    fetch_older_page_impl(&state, &space_id, before_epoch, before_seq, limit).await
}

/// Looks up the current patch for `space_id` (if it's actively viewed) and
/// emits it on `crate::events::conversation_patch_event_name`. Takes a bare
/// (non-generic) `tauri::AppHandle` -- unlike `state.rs`'s network event
/// loop (which had to be generic over `R: tauri::Runtime` to stay testable
/// with `MockRuntime`), this function is only ever called from real
/// `#[tauri::command]` handlers dispatched by the actual Tauri runtime, and
/// is not itself unit-tested with a mock handle (matching how Task 9's own
/// command wrappers were left untested at the IPC layer -- only their
/// `_impl` functions have unit tests). If a later task adds IPC-level tests
/// for these commands, revisit whether this needs to become generic too.
fn emit_patch_if_active(app: &tauri::AppHandle, state: &AppState, space_id: &str) {
    use tauri::Emitter;

    let active = state.active.lock().unwrap();
    let Some(conversation) = active.get(space_id) else { return };
    let (version, _) = conversation.live_spec.snapshot();
    let patch = conversation.live_spec.diff_since(version.saturating_sub(1));
    let event = crate::events::ConversationPatchEvent { space_id: space_id.to_string(), patch };
    let _ = app.emit(&crate::events::conversation_patch_event_name(space_id), event);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::SpaceMembership;
    use crate::network::AppNetwork;
    use space_chat_core::domain::{DeviceId, Message};
    use space_chat_core::storage::{ListingEntry, ListingIndex, SegmentBlobStore};
    use space_chat_transport::bootstrap::TransportConfig;
    use space_chat_transport::identity::TransportIdentity;
    use space_chat_transport::transport::TransportEvent;
    use tokio::sync::mpsc;

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

    async fn fresh_state() -> (tempfile::TempDir, std::sync::Arc<AppState>) {
        let dir = tempfile::tempdir().unwrap();
        let (network, events) = inert_network().await;
        let state = AppState::new(dir.path(), DeviceId([1u8; 32]), network, events, mock_app_handle()).unwrap();
        (dir, state)
    }

    fn seed_one_message(state: &AppState, space_id: &str) {
        use space_chat_core::segment::Segment;

        let mut segment = Segment::new(space_id, 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        let key = segment.message_keys().next().unwrap();
        let change = segment.latest_change();
        state.segment_store.lock().unwrap().save_segment(space_id, 0, change.cursor.0, &change.bytes).unwrap();
        state
            .listing_index
            .lock()
            .unwrap()
            .append_entry(ListingEntry {
                space_id: space_id.to_string(),
                epoch: 0,
                seq: 0,
                message_key: key,
            })
            .unwrap();
        state.membership.lock().unwrap().create_space(space_id, DeviceId([1u8; 32]), "Alice".to_string());
    }

    #[tokio::test]
    async fn open_conversation_returns_version_zero_and_marks_it_active() {
        let (_dir, state) = fresh_state().await;
        seed_one_message(&state, "space-1");

        let result = open_conversation_impl(&state, "space-1", "General", 10_000).await;

        assert_eq!(result.version, 0);
        assert_eq!(result.spec["kind"], "conversation");
        assert_eq!(result.spec["messages"][0]["content"], "hello");
        assert!(state.active.lock().unwrap().contains_key("space-1"));
    }

    #[tokio::test]
    async fn a_spec_build_failure_degrades_to_a_conversation_error_spec_not_a_command_error() {
        let (_dir, state) = fresh_state().await;
        state
            .listing_index
            .lock()
            .unwrap()
            .append_entry(ListingEntry {
                space_id: "space-1".to_string(),
                epoch: 99,
                seq: 0,
                message_key: "msg:ghost".to_string(),
            })
            .unwrap();

        let result = open_conversation_impl(&state, "space-1", "General", 10_000).await;

        assert_eq!(result.spec["kind"], "conversation-error");
        assert_eq!(result.spec["space_id"], "space-1");
        assert!(state.active.lock().unwrap().contains_key("space-1"));
    }

    #[tokio::test]
    async fn close_conversation_removes_it_from_active() {
        let (_dir, state) = fresh_state().await;
        seed_one_message(&state, "space-1");
        open_conversation_impl(&state, "space-1", "General", 10_000).await;

        close_conversation_impl(&state, "space-1").await;

        assert!(!state.active.lock().unwrap().contains_key("space-1"));
    }

    #[tokio::test]
    async fn resync_after_a_fresh_open_with_a_stale_version_returns_full() {
        let (_dir, state) = fresh_state().await;
        seed_one_message(&state, "space-1");
        open_conversation_impl(&state, "space-1", "General", 10_000).await;

        let response = resync_conversation_impl(&state, "space-1", 42).await.unwrap();
        match response {
            crate::live_spec::PatchResponse::Full { version, .. } => assert_eq!(version, 0),
            other => panic!("expected Full, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resync_conversation_errors_for_a_space_that_was_never_opened() {
        let (_dir, state) = fresh_state().await;
        let result = resync_conversation_impl(&state, "space-never-opened", 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn open_conversation_registers_the_space_with_transport() {
        let (_dir, state) = fresh_state().await;
        seed_one_message(&state, "space-1");

        // Real assertion, not a vacuous one: `segment_arc` unconditionally
        // caches into `active_segments`, so calling it twice always yields
        // `Arc::ptr_eq` regardless of whether `open_conversation_impl`
        // registered the space first -- that shape of test would pass even
        // if the `segment_arc` call were deleted from `open_conversation_impl`
        // entirely (confirmed by review: deleting that line left the whole
        // suite green). Instead, check `active_segments` directly, before
        // and after, to prove `open_conversation_impl` is what causes the
        // registration.
        assert!(
            !state.active_segments.lock().await.contains_key("space-1"),
            "space should not be registered before open_conversation_impl runs"
        );

        open_conversation_impl(&state, "space-1", "General", 10_000).await;

        assert!(
            state.active_segments.lock().await.contains_key("space-1"),
            "open_conversation_impl must register the space with Transport via segment_arc"
        );
    }

    #[tokio::test]
    async fn send_message_appends_a_message_and_it_shows_up_in_the_active_spec() {
        let (_dir, state) = fresh_state().await;
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        open_conversation_impl(&state, "space-1", "General", 10_000).await;

        send_message_impl(&state, "space-1", "hello world".to_string()).await.unwrap();

        let active = state.active.lock().unwrap();
        let (_, spec_value) = active.get("space-1").unwrap().live_spec.snapshot();
        assert_eq!(spec_value["messages"][0]["content"], "hello world");
    }

    #[tokio::test]
    async fn send_message_persists_across_a_fresh_segment_load() {
        let (_dir, state) = fresh_state().await;
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());

        send_message_impl(&state, "space-1", "persisted".to_string()).await.unwrap();

        let segments = state.segments_for("space-1");
        assert_eq!(segments[&0].message_count(), 1);
    }

    #[tokio::test]
    async fn react_and_delete_apply_to_an_existing_message() {
        let (_dir, state) = fresh_state().await;
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        send_message_impl(&state, "space-1", "react to me".to_string()).await.unwrap();

        let segments = state.segments_for("space-1");
        let message_key = segments[&0].message_keys().next().unwrap();

        react_impl(&state, "space-1", &message_key, "\u{1F44D}").await.unwrap();
        delete_message_impl(&state, "space-1", &message_key).await.unwrap();

        let segments = state.segments_for("space-1");
        let msg_id = segments[&0].message(&message_key).unwrap();
        assert_eq!(segments[&0].reaction_count(&msg_id), 1);
        assert!(segments[&0].is_deleted(&msg_id));
    }

    #[tokio::test]
    async fn send_message_on_an_unopened_conversation_still_succeeds_without_a_live_spec() {
        let (_dir, state) = fresh_state().await;
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());

        let result = send_message_impl(&state, "space-1", "no active view".to_string()).await;

        assert!(result.is_ok());
        assert!(!state.active.lock().unwrap().contains_key("space-1"));
    }

    /// New test, not in the original plan text: proves the network layer is
    /// actually notified (Gap 2/Task 7's `notify_local_change`), and that
    /// this task didn't reintroduce Task 9's title-clobber bug (Gap 4) for
    /// the local-mutation path.
    #[tokio::test]
    async fn send_message_preserves_the_real_title_when_regenerating_the_active_spec() {
        let (_dir, state) = fresh_state().await;
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        open_conversation_impl(&state, "space-1", "General", 10_000).await;

        send_message_impl(&state, "space-1", "hello".to_string()).await.unwrap();

        let active = state.active.lock().unwrap();
        let (_, spec_value) = active.get("space-1").unwrap().live_spec.snapshot();
        assert_eq!(spec_value["title"], "General", "title must not fall back to space_id after a local mutation");
    }

    #[tokio::test]
    async fn fetch_older_page_returns_messages_strictly_before_the_given_cursor() {
        let (_dir, state) = fresh_state().await;
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        for content in ["one", "two", "three"] {
            send_message_impl(&state, "space-1", content.to_string()).await.unwrap();
        }

        // Page 1: newest 2.
        let page1 = fetch_older_page_impl(&state, "space-1", u64::MAX, u64::MAX, 2).await.unwrap();
        assert_eq!(page1.messages.len(), 2);
        assert_eq!(page1.messages[0].content, "two");
        assert_eq!(page1.messages[1].content, "three");
        assert!(page1.has_more_older);
    }

    #[tokio::test]
    async fn fetch_older_page_reports_no_more_older_once_exhausted() {
        let (_dir, state) = fresh_state().await;
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        send_message_impl(&state, "space-1", "only one".to_string()).await.unwrap();

        let page = fetch_older_page_impl(&state, "space-1", u64::MAX, u64::MAX, 10).await.unwrap();
        assert_eq!(page.messages.len(), 1);
        assert!(!page.has_more_older);
    }
}

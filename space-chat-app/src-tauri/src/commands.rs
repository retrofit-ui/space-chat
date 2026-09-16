use crate::conversation_spec::build_conversation_spec;
use crate::live_spec::{LiveSpec, PatchResponse};
use crate::spec::{ConversationErrorSpec, ViewSpec};
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
}

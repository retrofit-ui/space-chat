use serde::Serialize;

/// Emitted on the per-conversation event name `format!("conversation-patch:{space_id}")`
/// whenever `LiveSpec::update` produces a new version for an actively-viewed
/// conversation.
#[derive(Debug, Clone, Serialize)]
pub struct ConversationPatchEvent {
    pub space_id: String,
    #[serde(flatten)]
    pub patch: crate::live_spec::PatchResponse,
}

pub fn conversation_patch_event_name(space_id: &str) -> String {
    format!("conversation-patch:{space_id}")
}

pub fn attachment_ready_event_name(hash_hex: &str) -> String {
    format!("attachment-ready:{hash_hex}")
}

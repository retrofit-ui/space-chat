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

/// The single, app-wide event carrying `network::ConnectionStatus` changes to
/// the frontend, for UI affordances like "reconnecting" (the app-shell spec's
/// Composition & Tauri IPC section). Unlike the two helpers above it takes no
/// argument -- connection status is per-process, not per-space or
/// per-attachment -- but it stays a function rather than a `const` so callers
/// on both sides reference one definition and the name can't drift.
pub fn connection_status_event_name() -> &'static str {
    "connection-status"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_status_event_name_is_stable() {
        assert_eq!(connection_status_event_name(), "connection-status");
    }
}

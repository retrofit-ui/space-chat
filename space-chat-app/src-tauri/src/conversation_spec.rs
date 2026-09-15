use crate::membership::SpaceMembership;
use crate::observed_at::ObservedAtStore;
use crate::spec::{AttachmentSpec, ConversationSpec, MessageSpec, ReactionSpec};
use space_chat_core::segment::Segment;
use space_chat_core::storage::ListingEntry;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum SpecBuildError {
    MissingSegment { epoch: u64 },
    MalformedMessage { message_key: String },
}

impl std::fmt::Display for SpecBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpecBuildError::MissingSegment { epoch } => write!(f, "missing segment for epoch {epoch}"),
            SpecBuildError::MalformedMessage { message_key } => {
                write!(f, "malformed message at key {message_key:?}")
            }
        }
    }
}

impl std::error::Error for SpecBuildError {}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Buckets elapsed time between `sent_at_unix_ms` and `now_unix_ms` into a
/// coarse human-readable label. `sent_at_unix_ms` here is really "observed
/// at" -- see `crate::observed_at`'s doc comment.
pub fn relative_time(observed_at_unix_ms: u64, now_unix_ms: u64) -> String {
    let delta_secs = now_unix_ms.saturating_sub(observed_at_unix_ms) / 1000;
    match delta_secs {
        0..=9 => "just now".to_string(),
        10..=59 => format!("{delta_secs}s ago"),
        60..=3_599 => format!("{}m ago", delta_secs / 60),
        3_600..=86_399 => format!("{}h ago", delta_secs / 3_600),
        _ => format!("{}d ago", delta_secs / 86_400),
    }
}

/// Builds a `ConversationSpec` from a `ListingIndex::page` window (the
/// ordering source of truth -- see this plan's Global Constraints) plus the
/// `Segment`s that actually hold each entry's message content. `segments`
/// maps epoch -> the `Segment` covering it; the composition root (Task 8)
/// keeps every epoch a space has ever used loaded so this lookup never
/// needs to hit disk mid-call.
#[allow(clippy::too_many_arguments)]
pub fn build_conversation_spec(
    space_id: &str,
    title: &str,
    page: &[ListingEntry],
    segments: &HashMap<u64, Segment>,
    membership: &dyn SpaceMembership,
    observed_at: &mut dyn ObservedAtStore,
    has_more_older: bool,
    now_unix_ms: u64,
) -> Result<ConversationSpec, SpecBuildError> {
    let mut messages = Vec::with_capacity(page.len());

    // `page` is newest-first (ListingIndex::page's documented contract);
    // reverse it here so the spec's `messages` reads oldest-first, the
    // order a chat transcript is displayed in.
    for entry in page.iter().rev() {
        let segment = segments
            .get(&entry.epoch)
            .ok_or(SpecBuildError::MissingSegment { epoch: entry.epoch })?;

        let msg = segment.read_message(&entry.message_key).ok_or_else(|| {
            SpecBuildError::MalformedMessage { message_key: entry.message_key.clone() }
        })?;
        let msg_obj_id = segment.message(&entry.message_key).ok_or_else(|| {
            SpecBuildError::MalformedMessage { message_key: entry.message_key.clone() }
        })?;

        let reactions = segment
            .reaction_keys(&msg_obj_id)
            .filter_map(|key| segment.read_reaction(&msg_obj_id, &key))
            .map(|r| ReactionSpec {
                emoji: r.emoji,
                actor_name: membership.display_name(&r.actor),
            })
            .collect();

        let attachments = msg
            .attachments
            .iter()
            .map(|a| AttachmentSpec {
                url: format!("spacechat://attachment/{}", hex(&a.hash)),
                mime: a.mime.clone(),
                size: a.size,
            })
            .collect();

        let observed_ms = observed_at.record_if_absent(&entry.message_key, now_unix_ms);

        messages.push(MessageSpec {
            id: entry.message_key.clone(),
            sender_id: hex(&msg.sender.0),
            sender_name: membership.display_name(&msg.sender),
            content: msg.content,
            relative_time: relative_time(observed_ms, now_unix_ms),
            attachments,
            reactions,
            deleted: segment.is_deleted(&msg_obj_id),
        });
    }

    Ok(ConversationSpec {
        space_id: space_id.to_string(),
        title: title.to_string(),
        messages,
        has_more_older,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::SpaceMembership;
    use crate::observed_at::{InMemoryObservedAtStore, ObservedAtStore};
    use space_chat_core::domain::{DeviceId, Message, Reaction};
    use space_chat_core::segment::{objid_to_target_string, Segment};
    use space_chat_core::storage::ListingEntry;
    use std::collections::HashMap;

    struct FakeMembership;
    impl SpaceMembership for FakeMembership {
        fn members(&self, _space_id: &str) -> Vec<DeviceId> {
            vec![]
        }
        fn display_name(&self, id: &DeviceId) -> String {
            if id.0 == [1u8; 32] {
                "Alice".to_string()
            } else {
                "Unknown".to_string()
            }
        }
        fn add_member(&mut self, _space_id: &str, _id: DeviceId, _name: String) {}
        fn create_space(&mut self, _space_id: &str, _local_device: DeviceId, _name: String) {}
        fn sequencer(&self, _space_id: &str) -> Option<DeviceId> {
            None
        }
    }

    #[test]
    fn builds_a_conversation_spec_from_a_listing_page_and_segment() {
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
                    actor: DeviceId([1u8; 32]),
                    emoji: "\u{1F44D}".to_string(),
                },
            )
            .unwrap();
        let message_key = segment.message_keys().next().unwrap();

        let page = vec![ListingEntry {
            space_id: "space-1".to_string(),
            epoch: 0,
            seq: 0,
            message_key: message_key.clone(),
        }];
        let mut segments = HashMap::new();
        segments.insert(0u64, segment);

        let membership = FakeMembership;
        let mut observed_at = InMemoryObservedAtStore::default();

        let spec = build_conversation_spec(
            "space-1",
            "General",
            &page,
            &segments,
            &membership,
            &mut observed_at,
            false,
            5_000,
        )
        .unwrap();

        assert_eq!(spec.space_id, "space-1");
        assert_eq!(spec.messages.len(), 1);
        let msg = &spec.messages[0];
        assert_eq!(msg.content, "hello");
        assert_eq!(msg.sender_name, "Alice");
        assert_eq!(msg.reactions.len(), 1);
        assert_eq!(msg.reactions[0].emoji, "\u{1F44D}");
        assert!(!msg.deleted);
    }

    #[test]
    fn newest_first_listing_page_is_reversed_to_oldest_first_display_order() {
        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "first".to_string(),
            attachments: vec![],
        });
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "second".to_string(),
            attachments: vec![],
        });
        let mut keys: Vec<String> = segment.message_keys().collect();
        keys.sort();

        // ListingIndex::page's documented contract is newest-first -- feed
        // the page in that order regardless of which key holds which
        // content, then assert display order comes out oldest-first.
        let page = vec![
            ListingEntry { space_id: "space-1".to_string(), epoch: 0, seq: 1, message_key: keys[1].clone() },
            ListingEntry { space_id: "space-1".to_string(), epoch: 0, seq: 0, message_key: keys[0].clone() },
        ];
        let mut segments = HashMap::new();
        segments.insert(0u64, segment);
        let membership = FakeMembership;
        let mut observed_at = InMemoryObservedAtStore::default();

        let spec = build_conversation_spec(
            "space-1", "General", &page, &segments, &membership, &mut observed_at, false, 1_000,
        )
        .unwrap();

        assert_eq!(spec.messages.len(), 2);
        assert_eq!(spec.messages[0].id, keys[0], "oldest entry (seq 0) must render first");
        assert_eq!(spec.messages[1].id, keys[1], "newest entry (seq 1) must render second");
    }

    #[test]
    fn missing_epoch_segment_produces_a_spec_build_error_not_a_panic() {
        let page = vec![ListingEntry {
            space_id: "space-1".to_string(),
            epoch: 7,
            seq: 0,
            message_key: "msg:does-not-exist".to_string(),
        }];
        let segments = HashMap::new(); // epoch 7 never inserted
        let membership = FakeMembership;
        let mut observed_at = InMemoryObservedAtStore::default();

        let result = build_conversation_spec(
            "space-1", "General", &page, &segments, &membership, &mut observed_at, false, 1_000,
        );

        assert_eq!(result, Err(SpecBuildError::MissingSegment { epoch: 7 }));
    }

    #[test]
    fn relative_time_buckets_by_elapsed_duration() {
        assert_eq!(relative_time(1_000, 1_005), "just now");
        assert_eq!(relative_time(1_000, 31_000), "30s ago");
        assert_eq!(relative_time(1_000, 121_000), "2m ago");
        assert_eq!(relative_time(1_000, 3_601_000 + 1_000), "1h ago");
        assert_eq!(relative_time(1_000, 2 * 86_400_000 + 1_000), "2d ago");
    }
}

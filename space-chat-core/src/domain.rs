use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeviceId(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttachmentRef {
    pub hash: [u8; 32],
    pub size: u64,
    pub mime: String,
    pub wrapped_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub sender: DeviceId,
    pub content: String,
    pub attachments: Vec<AttachmentRef>,
}

/// `target` is redundant with the `target: &ObjId` parameter that
/// `Segment::append_reaction` already takes to know *where* to attach the
/// reaction -- so why does this field exist too? Resolution (see Finding 6
/// of the Milestone 1 final review): it's kept, deliberately, as
/// defense-in-depth, not silently ignored. `Segment::append_reaction`
/// parses this field back into an `ObjId` (via the `pub`
/// `segment::target_string_to_objid`, so a caller can do the same
/// independent decode a wire-supplied target string needs) and rejects the
/// call (`segment::SegmentError::TargetMismatch`) if it doesn't match the
/// `target` parameter. That catches a real bug class -- a caller resolving the
/// wrong local `ObjId` for a wire-supplied target string (e.g. after a
/// later milestone parses `target` out of a network envelope) -- at the
/// point of the mistake, rather than silently attaching the reaction to
/// the wrong object.
///
/// The value stored here is the stable hex-encoded form of
/// `automerge::ObjId::to_bytes()` (see `segment::objid_to_target_string`),
/// *not* `{:?}` (`Debug`) -- `Debug`'s output isn't a documented stable
/// format and isn't safe to persist or round-trip.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reaction {
    pub target: String,
    pub actor: DeviceId,
    pub emoji: String,
}

/// See the doc comment on [`Reaction::target`] -- the same reasoning
/// applies here: `Segment::apply_delete` validates this field against its
/// `target: &ObjId` parameter as defense-in-depth, using the same stable
/// hex-encoded `ObjId::to_bytes()` form, not `Debug`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub target: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_round_trips_through_serde() {
        let msg = Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        };
        let bytes = serde_json::to_vec(&msg).unwrap();
        let back: Message = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(msg, back);
    }
}

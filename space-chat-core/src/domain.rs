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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reaction {
    pub target: String, // Automerge ObjId, serialized as its string form
    pub actor: DeviceId,
    pub emoji: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub target: String, // Automerge ObjId, serialized as its string form
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

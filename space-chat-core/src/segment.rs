use automerge::{transaction::Transactable, AutoCommit, ObjId, ObjType, ReadDoc, ROOT};
use crate::domain::Message;

/// One epoch's worth of messages, backed by an Automerge document.
///
/// Messages are stored append-only in a "messages" list at the document
/// root, keeping the doc append-mostly per the protocol spec's no-edit
/// decision. Later tasks (Reaction/Delete application, sync exchange) build
/// on the exact shape of this list.
pub struct Segment {
    doc: AutoCommit,
}

impl Segment {
    pub fn new() -> Self {
        let mut doc = AutoCommit::new();
        doc.put_object(ROOT, "messages", ObjType::List)
            .expect("creating the root messages list cannot fail on a fresh doc");
        Self { doc }
    }

    /// Appends `msg` as a new map at the end of the root "messages" list and
    /// returns the Automerge object ID of the newly created map, which
    /// later tasks use as the addressable target for Reactions/Deletes.
    pub fn append_message(&mut self, msg: &Message) -> ObjId {
        let messages = self
            .doc
            .get(ROOT, "messages")
            .expect("get on a known key cannot fail")
            .expect("messages list was created in Segment::new")
            .1;
        let idx = self.doc.length(&messages);
        let entry = self
            .doc
            .insert_object(&messages, idx, ObjType::Map)
            .expect("inserting into the messages list cannot fail");
        self.doc
            .put(&entry, "sender", msg.sender.0.to_vec())
            .expect("put on a freshly-inserted map cannot fail");
        self.doc
            .put(&entry, "content", msg.content.clone())
            .expect("put on a freshly-inserted map cannot fail");
        entry
    }

    /// Number of messages currently stored in this segment.
    pub fn message_count(&self) -> usize {
        let messages = self
            .doc
            .get(ROOT, "messages")
            .expect("get on a known key cannot fail")
            .expect("messages list was created in Segment::new")
            .1;
        self.doc.length(&messages)
    }

    /// Serializes the full document (compacted) to bytes for persistence or
    /// transfer to another peer.
    pub fn save(&mut self) -> Vec<u8> {
        self.doc.save()
    }

    /// Reconstructs a `Segment` from bytes previously produced by `save`.
    pub fn load(bytes: &[u8]) -> Result<Self, automerge::AutomergeError> {
        Ok(Self {
            doc: AutoCommit::load(bytes)?,
        })
    }
}

impl Default for Segment {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{DeviceId, Message};

    #[test]
    fn appended_message_survives_save_and_load() {
        let mut segment = Segment::new();
        let msg = Message {
            sender: DeviceId([7u8; 32]),
            content: "hello segment".to_string(),
            attachments: vec![],
        };
        segment.append_message(&msg);
        let bytes = segment.save();

        let loaded = Segment::load(&bytes).unwrap();
        assert_eq!(loaded.message_count(), 1);
    }
}

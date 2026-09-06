use crate::domain::Message;
use automerge::{transaction::Transactable, AutoCommit, ObjId, ObjType, ReadDoc, Value, ROOT};
use std::fmt;

/// One epoch's worth of messages, backed by an Automerge document.
///
/// Messages are stored append-only in a "messages" list at the document
/// root, keeping the doc append-mostly per the protocol spec's no-edit
/// decision. Later tasks (Reaction/Delete application, sync exchange) build
/// on the exact shape of this list.
pub struct Segment {
    doc: AutoCommit,
    /// Object ID of the root "messages" list. Cached at construction time
    /// (`new` or `load`) once its presence and shape have been validated, so
    /// every other method can rely on it existing without re-checking.
    messages: ObjId,
}

/// Error returned by [`Segment::load`] when `bytes` decode as a valid
/// Automerge document but don't have the schema `Segment` expects (e.g. no
/// "messages" list at the document root). This is distinct from
/// `automerge::AutomergeError`, which only covers malformed Automerge bytes,
/// not a mismatched application schema.
#[derive(Debug)]
pub enum SegmentLoadError {
    /// The bytes could not be decoded as an Automerge document at all.
    Automerge(automerge::AutomergeError),
    /// The bytes decoded fine, but there is no "messages" list at ROOT.
    MissingMessagesList,
}

impl fmt::Display for SegmentLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SegmentLoadError::Automerge(e) => write!(f, "failed to decode Automerge document: {e}"),
            SegmentLoadError::MissingMessagesList => {
                write!(
                    f,
                    "document is missing the expected \"messages\" list at ROOT"
                )
            }
        }
    }
}

impl std::error::Error for SegmentLoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SegmentLoadError::Automerge(e) => Some(e),
            SegmentLoadError::MissingMessagesList => None,
        }
    }
}

impl From<automerge::AutomergeError> for SegmentLoadError {
    fn from(e: automerge::AutomergeError) -> Self {
        SegmentLoadError::Automerge(e)
    }
}

impl Segment {
    pub fn new() -> Self {
        let mut doc = AutoCommit::new();
        let messages = doc
            .put_object(ROOT, "messages", ObjType::List)
            .expect("creating the root messages list cannot fail on a fresh doc");
        Self { doc, messages }
    }

    /// Looks up and validates the "messages" list at ROOT of an already
    /// loaded document, returning its object ID. Used by `load` to restore
    /// the invariant (normally established by `new`) that a constructed
    /// `Segment` always has a well-formed "messages" list.
    fn find_messages_list(doc: &AutoCommit) -> Result<ObjId, SegmentLoadError> {
        match doc.get(ROOT, "messages")? {
            Some((Value::Object(ObjType::List), id)) => Ok(id),
            _ => Err(SegmentLoadError::MissingMessagesList),
        }
    }

    /// Appends `msg` as a new map at the end of the root "messages" list and
    /// returns the Automerge object ID of the newly created map, which
    /// later tasks use as the addressable target for Reactions/Deletes.
    pub fn append_message(&mut self, msg: &Message) -> ObjId {
        let idx = self.doc.length(&self.messages);
        let entry = self
            .doc
            .insert_object(&self.messages, idx, ObjType::Map)
            .expect("inserting into the messages list cannot fail");
        self.doc
            .put(&entry, "sender", msg.sender.0.to_vec())
            .expect("put on a freshly-inserted map cannot fail");
        self.doc
            .put(&entry, "content", msg.content.clone())
            .expect("put on a freshly-inserted map cannot fail");

        let attachments = self
            .doc
            .put_object(&entry, "attachments", ObjType::List)
            .expect("put_object on a freshly-inserted map cannot fail");
        for (i, attachment) in msg.attachments.iter().enumerate() {
            let att_entry = self
                .doc
                .insert_object(&attachments, i, ObjType::Map)
                .expect("inserting into a freshly-created list cannot fail");
            self.doc
                .put(&att_entry, "hash", attachment.hash.to_vec())
                .expect("put on a freshly-inserted map cannot fail");
            self.doc
                .put(&att_entry, "size", attachment.size)
                .expect("put on a freshly-inserted map cannot fail");
            self.doc
                .put(&att_entry, "mime", attachment.mime.clone())
                .expect("put on a freshly-inserted map cannot fail");
            self.doc
                .put(&att_entry, "wrapped_key", attachment.wrapped_key.clone())
                .expect("put on a freshly-inserted map cannot fail");
        }

        entry
    }

    /// Number of messages currently stored in this segment.
    pub fn message_count(&self) -> usize {
        self.doc.length(&self.messages)
    }

    /// Serializes the full document (compacted) to bytes for persistence or
    /// transfer to another peer.
    pub fn save(&mut self) -> Vec<u8> {
        self.doc.save()
    }

    /// Reconstructs a `Segment` from bytes previously produced by `save`.
    ///
    /// Returns `Err(SegmentLoadError)` if `bytes` don't decode as an
    /// Automerge document, or decode fine but lack the "messages" list at
    /// ROOT that every other `Segment` method relies on. `bytes` may
    /// originate from another peer over the network (see sync exchange), so
    /// this validates rather than trusting the input's shape.
    pub fn load(bytes: &[u8]) -> Result<Self, SegmentLoadError> {
        let doc = AutoCommit::load(bytes)?;
        let messages = Self::find_messages_list(&doc)?;
        Ok(Self { doc, messages })
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

    #[test]
    fn appended_message_attachments_survive_save_and_load() {
        use crate::domain::AttachmentRef;

        let mut segment = Segment::new();
        let attachment = AttachmentRef {
            hash: [9u8; 32],
            size: 1234,
            mime: "image/png".to_string(),
            wrapped_key: vec![1, 2, 3, 4],
        };
        let msg = Message {
            sender: DeviceId([7u8; 32]),
            content: "look at this".to_string(),
            attachments: vec![attachment.clone()],
        };
        segment.append_message(&msg);
        let bytes = segment.save();

        let loaded = Segment::load(&bytes).unwrap();
        assert_eq!(loaded.message_count(), 1);

        let entry = loaded.doc.get(&loaded.messages, 0).unwrap().unwrap().1;
        let attachments_obj = loaded.doc.get(&entry, "attachments").unwrap().unwrap().1;
        assert_eq!(loaded.doc.length(&attachments_obj), 1);

        let att0 = loaded.doc.get(&attachments_obj, 0).unwrap().unwrap().1;
        let hash = loaded
            .doc
            .get(&att0, "hash")
            .unwrap()
            .unwrap()
            .0
            .into_scalar()
            .unwrap()
            .into_bytes()
            .unwrap();
        assert_eq!(hash, attachment.hash.to_vec());

        let size = loaded
            .doc
            .get(&att0, "size")
            .unwrap()
            .unwrap()
            .0
            .into_scalar()
            .unwrap()
            .to_u64()
            .unwrap();
        assert_eq!(size, attachment.size);

        let mime = loaded
            .doc
            .get(&att0, "mime")
            .unwrap()
            .unwrap()
            .0
            .into_scalar()
            .unwrap()
            .into_string()
            .unwrap();
        assert_eq!(mime, attachment.mime);

        let wrapped_key = loaded
            .doc
            .get(&att0, "wrapped_key")
            .unwrap()
            .unwrap()
            .0
            .into_scalar()
            .unwrap()
            .into_bytes()
            .unwrap();
        assert_eq!(wrapped_key, attachment.wrapped_key);
    }

    #[test]
    fn load_rejects_document_without_a_messages_list() {
        // Build a valid Automerge document by hand that simply doesn't have
        // our expected "messages" list at ROOT.
        let mut doc = AutoCommit::new();
        doc.put(ROOT, "not_messages", "surprise")
            .expect("put on a fresh doc cannot fail");
        let bytes = doc.save();

        let result = Segment::load(&bytes);
        assert!(
            result.is_err(),
            "expected Segment::load to reject a document without a messages list"
        );
    }
}

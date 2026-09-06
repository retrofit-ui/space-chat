use crate::domain::Message;
use automerge::{
    sync::{self, SyncDoc},
    transaction::Transactable,
    AutoCommit, ObjId, ObjType, ReadDoc, Value, ROOT,
};
use uuid::Uuid;

/// Prefix marking a top-level ROOT key as a message entry, distinguishing it
/// from any other top-level content this document might hold in the future.
const MESSAGE_KEY_PREFIX: &str = "msg:";

/// One epoch's worth of messages, backed by an Automerge document.
///
/// Each message is stored as its own map object at a unique
/// `"msg:<uuid>"` key directly under `ROOT` -- not nested inside any shared
/// list or map container. This is deliberate: two independently-created
/// `Segment`s (e.g. two devices' fresh segments at an epoch boundary, never
/// synced before) must be able to each create messages and then sync
/// without conflict. `ROOT` itself always exists in every Automerge
/// document -- nobody "creates" it, so there's nothing to conflict over --
/// and two peers writing to two different (random-UUID) keys under `ROOT`
/// is never a conflict, regardless of shared causal history. A shared
/// "messages" list container was tried first and rejected: two peers each
/// independently calling `put_object(ROOT, "messages", ObjType::List)` is a
/// concurrent write to the *same* key from two unrelated objects, which
/// Automerge resolves via last-writer-wins at the key level -- silently
/// discarding one entire side's list, not merging it.
pub struct Segment {
    doc: AutoCommit,
}

impl Segment {
    pub fn new() -> Self {
        Self {
            doc: AutoCommit::new(),
        }
    }

    /// Generates a fresh, unique key for a new message entry. Uses a random
    /// UUID so concurrently-created messages from different senders (who
    /// share no coordination beyond both writing under the same `ROOT`)
    /// cannot collide.
    fn new_message_key() -> String {
        format!("{MESSAGE_KEY_PREFIX}{}", Uuid::new_v4())
    }

    /// Returns the Automerge object ID of the message stored at `key`, or
    /// `None` if `key` doesn't hold a well-formed message entry -- e.g.
    /// there's nothing there, the value isn't a map, or the map is missing
    /// the required `content` field. This never panics, even against a
    /// foreign/malformed/adversarial document: callers use it to skip bad
    /// entries rather than crash the whole segment.
    pub fn message(&self, key: &str) -> Option<ObjId> {
        let (value, id) = self.doc.get(ROOT, key).ok()??;
        if !matches!(value, Value::Object(ObjType::Map)) {
            return None;
        }
        // A well-formed message always has a "content" field; treat its
        // absence as a sign this entry isn't one of ours.
        self.doc.get(&id, "content").ok()?.map(|_| id)
    }

    /// Iterates the `"msg:"`-prefixed keys directly under `ROOT`, in no
    /// particular order. Includes keys that turn out to be malformed when
    /// passed to [`Segment::message`]; callers that need only well-formed
    /// entries should filter through `message`.
    pub fn message_keys(&self) -> impl Iterator<Item = String> + '_ {
        self.doc
            .keys(ROOT)
            .filter(|key| key.starts_with(MESSAGE_KEY_PREFIX))
    }

    /// Appends `msg` as a new map at a fresh unique key under `ROOT` and
    /// returns the Automerge object ID of the newly created map, which
    /// later tasks use as the addressable target for Reactions/Deletes.
    pub fn append_message(&mut self, msg: &Message) -> ObjId {
        let key = Self::new_message_key();
        let entry = self
            .doc
            .put_object(ROOT, &key, ObjType::Map)
            .expect("creating a message map at a fresh UUID key cannot fail");
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

    /// Number of well-formed messages currently stored in this segment.
    ///
    /// Counts only `"msg:"`-prefixed keys under `ROOT` whose value passes
    /// the same shape check as [`Segment::message`]; a malformed entry
    /// (e.g. from a foreign/adversarial document) is silently skipped
    /// rather than counted or causing a panic.
    pub fn message_count(&self) -> usize {
        self.message_keys()
            .filter(|key| self.message(key).is_some())
            .count()
    }

    /// Serializes the full document (compacted) to bytes for persistence or
    /// transfer to another peer.
    pub fn save(&mut self) -> Vec<u8> {
        self.doc.save()
    }

    /// Reconstructs a `Segment` from bytes previously produced by `save`.
    ///
    /// Returns `Err` only if `bytes` don't decode as an Automerge document
    /// at all. Unlike the prior "messages list" design, there is no single
    /// required container whose absence makes the whole document invalid:
    /// a document with zero `"msg:"`-prefixed keys is simply an empty
    /// segment, and individual malformed message entries are handled
    /// per-entry by [`Segment::message`] / [`Segment::message_count`], not
    /// rejected at load time. `bytes` may originate from another peer over
    /// the network (see sync exchange), so those per-entry checks matter,
    /// but they don't belong at load time since a message-shaped entry
    /// could legitimately arrive *after* load, via a later sync message.
    pub fn load(bytes: &[u8]) -> Result<Self, automerge::AutomergeError> {
        let doc = AutoCommit::load(bytes)?;
        Ok(Self { doc })
    }

    /// Generates the next sync message to send to the peer tracked by
    /// `state`, or `None` if there is nothing new to send (either we're
    /// waiting on an in-flight message, or the peer is already up to date).
    ///
    /// Note: `automerge::AutoCommit` doesn't implement `sync::SyncDoc`
    /// directly; it exposes sync via a `sync()` wrapper method that first
    /// closes out any in-progress transaction. We go through that wrapper
    /// here rather than the trait impl the plan sketched directly on `doc`.
    pub fn generate_sync_message(&mut self, state: &mut sync::State) -> Option<sync::Message> {
        self.doc.sync().generate_sync_message(state)
    }

    /// Applies a sync message received from the peer tracked by `state`,
    /// merging in any changes it carries.
    pub fn receive_sync_message(
        &mut self,
        state: &mut sync::State,
        msg: sync::Message,
    ) -> Result<(), automerge::AutomergeError> {
        self.doc.sync().receive_sync_message(state, msg)
    }
}

/// Creates a fresh sync-protocol state for tracking one peer relationship.
/// A `Segment` needs one `sync::State` per remote peer it exchanges sync
/// messages with (see `Segment::generate_sync_message` /
/// `receive_sync_message`).
pub fn sync_state() -> sync::State {
    sync::State::new()
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

        let key = loaded
            .message_keys()
            .next()
            .expect("expected exactly one message key");
        let entry = loaded
            .message(&key)
            .expect("message entry should be well-formed");
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

    /// Drives the sync protocol between `alice` and `bob` to completion
    /// (both sides report nothing left to send), per the loop pattern in
    /// `automerge::sync`'s own module docs.
    fn run_sync_to_completion(
        alice: &mut Segment,
        alice_state: &mut sync::State,
        bob: &mut Segment,
        bob_state: &mut sync::State,
    ) {
        loop {
            let a_to_b = alice.generate_sync_message(alice_state);
            let b_to_a = bob.generate_sync_message(bob_state);
            let (a_none, b_none) = (a_to_b.is_none(), b_to_a.is_none());
            if let Some(msg) = a_to_b {
                bob.receive_sync_message(bob_state, msg).unwrap();
            }
            if let Some(msg) = b_to_a {
                alice.receive_sync_message(alice_state, msg).unwrap();
            }
            if a_none && b_none {
                break;
            }
        }
    }

    /// Proves the sync-message plumbing itself is correct: two
    /// independently-created `Segment`s that each make local changes
    /// converge on the same set of Automerge changes (same heads, i.e. the
    /// same content-addressed change-hash frontier) after exchanging sync
    /// messages to completion. This is the CRDT-level convergence guarantee
    /// `generate_sync_message` / `receive_sync_message` / `sync_state` are
    /// responsible for. (`save()` bytes are *not* used for this comparison:
    /// they embed each actor's local actor-ID ordering metadata, which
    /// legitimately differs between independently-created docs even when
    /// their logical content is identical.)
    #[test]
    fn two_segments_converge_to_the_same_document_via_sync_messages() {
        let mut alice = Segment::new();
        let mut bob = Segment::new();

        alice.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "from alice".to_string(),
            attachments: vec![],
        });
        bob.append_message(&Message {
            sender: DeviceId([2u8; 32]),
            content: "from bob".to_string(),
            attachments: vec![],
        });

        let mut alice_state = sync_state();
        let mut bob_state = sync_state();
        run_sync_to_completion(&mut alice, &mut alice_state, &mut bob, &mut bob_state);

        let mut alice_heads = alice.doc.get_heads();
        let mut bob_heads = bob.doc.get_heads();
        alice_heads.sort();
        bob_heads.sort();
        assert_eq!(
            alice_heads, bob_heads,
            "after sync, both peers should agree on the same change-hash frontier"
        );
        assert!(!alice_heads.is_empty());
    }

    /// This is the convergence test from the Task 4 plan. It used to fail
    /// (see git history / task-4-report.md for the original `#[ignore]`
    /// reason): `Segment::new` used to call
    /// `put_object(ROOT, "messages", ObjType::List)` independently on each
    /// side, and two independently-created segments syncing was a
    /// concurrent write to the *same* ROOT map key from two unrelated
    /// objects, which Automerge resolves as a conflict, discarding one
    /// side's entire list. Now that each message lives at its own unique
    /// `"msg:<uuid>"` key directly under `ROOT` (see the `Segment` doc
    /// comment), two peers appending concurrently write to two different
    /// keys, which is never a conflict -- so both messages survive sync on
    /// both sides.
    #[test]
    fn two_segments_converge_via_sync_messages() {
        let mut alice = Segment::new();
        let mut bob = Segment::new();

        alice.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "from alice".to_string(),
            attachments: vec![],
        });
        bob.append_message(&Message {
            sender: DeviceId([2u8; 32]),
            content: "from bob".to_string(),
            attachments: vec![],
        });

        let mut alice_state = sync_state();
        let mut bob_state = sync_state();
        run_sync_to_completion(&mut alice, &mut alice_state, &mut bob, &mut bob_state);

        assert_eq!(alice.message_count(), 2);
        assert_eq!(bob.message_count(), 2);
    }

    #[test]
    fn load_accepts_a_document_with_no_messages_at_all() {
        // A document with unrelated top-level content and zero "msg:"-keyed
        // entries is a legitimate, freshly-created-and-never-appended-to
        // segment, not an error: there's no single "messages container"
        // left to be missing.
        let mut doc = AutoCommit::new();
        doc.put(ROOT, "not_a_message", "surprise")
            .expect("put on a fresh doc cannot fail");
        let bytes = doc.save();

        let loaded = Segment::load(&bytes).expect("a plain Automerge doc should load fine");
        assert_eq!(loaded.message_count(), 0);
    }

    #[test]
    fn load_rejects_undecodable_bytes() {
        let result = Segment::load(b"not an automerge document");
        assert!(
            result.is_err(),
            "expected Segment::load to reject bytes that aren't a valid Automerge document"
        );
    }

    #[test]
    fn message_count_ignores_non_message_keys_at_root() {
        let mut segment = Segment::new();
        segment
            .doc
            .put(ROOT, "some_other_top_level_key", "unrelated content")
            .expect("put on ROOT cannot fail");

        assert_eq!(
            segment.message_count(),
            0,
            "a top-level key without the \"msg:\" prefix must not be counted as a message"
        );
    }

    #[test]
    fn message_count_skips_malformed_message_entries_without_panicking() {
        let mut segment = Segment::new();

        // A "msg:"-prefixed key whose value is a plain scalar, not a map at
        // all -- e.g. from a foreign/adversarial/corrupted document.
        segment
            .doc
            .put(ROOT, "msg:not-a-map", "surprise")
            .expect("put on ROOT cannot fail");

        // A "msg:"-prefixed key whose value is a map, but missing the
        // required "content" field.
        segment
            .doc
            .put_object(ROOT, "msg:missing-content", ObjType::Map)
            .expect("put_object on ROOT cannot fail");

        // Neither malformed entry should panic or be counted.
        assert_eq!(segment.message_count(), 0);

        // A real, well-formed message alongside the malformed entries is
        // still counted correctly.
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "a real message".to_string(),
            attachments: vec![],
        });
        assert_eq!(segment.message_count(), 1);
    }

    #[test]
    fn message_accessor_returns_none_for_malformed_or_missing_keys() {
        let mut segment = Segment::new();
        segment
            .doc
            .put(ROOT, "msg:not-a-map", "surprise")
            .expect("put on ROOT cannot fail");

        assert!(segment.message("msg:not-a-map").is_none());
        assert!(segment.message("msg:does-not-exist").is_none());
    }
}

use crate::domain::{Delete, Message, Reaction};
use crate::projection::{SegmentChange, SegmentCursor};
use automerge::{
    sync::{self, SyncDoc},
    transaction::Transactable,
    AutoCommit, AutomergeError, ObjId, ObjType, ReadDoc, ScalarValue, Value, ROOT,
};
use uuid::Uuid;

/// Prefix marking a top-level ROOT key as a message entry, distinguishing it
/// from any other top-level content this document might hold in the future.
const MESSAGE_KEY_PREFIX: &str = "msg:";

/// Prefix marking a key on a message map as a reaction entry, mirroring
/// `MESSAGE_KEY_PREFIX`'s role at `ROOT`.
const REACTION_KEY_PREFIX: &str = "reaction:";

/// Serializes an Automerge `ObjId` to a stable string form suitable for the
/// wire-facing `target` field on [`Reaction`]/[`Delete`]: hex-encoded
/// `ObjId::to_bytes()`, automerge's own documented stable serialization
/// (see `automerge::ObjId::to_bytes`'s doc comment) -- unlike `{:?}`
/// (`Debug`), which is not a stable format and isn't safe to persist or
/// round-trip.
pub fn objid_to_target_string(id: &ObjId) -> String {
    id.to_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

/// Parses the hex-encoded form produced by [`objid_to_target_string`] back
/// into an `ObjId`. Returns `None` (never panics) for any malformed input --
/// wrong-length hex, non-hex characters, or bytes that don't decode as a
/// valid `ObjId` -- since this handles untrusted, potentially wire-supplied
/// strings.
fn target_string_to_objid(s: &str) -> Option<ObjId> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let bytes = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    ObjId::try_from(bytes.as_slice()).ok()
}

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
    /// Identifies which space this segment belongs to. Paired with `epoch`
    /// to give a `Segment` the identity a [`SegmentChange`] needs to
    /// reference it -- see [`Segment::latest_change`].
    space_id: String,
    /// Which epoch (of `space_id`) this segment covers.
    epoch: u64,
    /// Monotonically increasing count of successful mutating calls
    /// (`append_message`, a successful `append_reaction`, a successful
    /// `apply_delete`) made on this segment so far. Feeds
    /// [`SegmentChange::cursor`] via [`Segment::latest_change`].
    cursor: u64,
}

impl Segment {
    pub fn new(space_id: impl Into<String>, epoch: u64) -> Self {
        Self {
            doc: AutoCommit::new(),
            space_id: space_id.into(),
            epoch,
            cursor: 0,
        }
    }

    /// Generates a fresh, unique key for a new message entry. Uses a random
    /// UUID so concurrently-created messages from different senders (who
    /// share no coordination beyond both writing under the same `ROOT`)
    /// cannot collide.
    fn new_message_key() -> String {
        format!("{MESSAGE_KEY_PREFIX}{}", Uuid::new_v4())
    }

    /// Generates a fresh, unique key for a new reaction entry, for the same
    /// reason [`Segment::new_message_key`] does: two peers who both already
    /// have `target` (via prior sync) and independently react to it before
    /// hearing about each other's reaction must not collide. A shared
    /// "reactions" list container was rejected for the same reason a
    /// shared "messages" list was -- see the `Segment` doc comment.
    fn new_reaction_key() -> String {
        format!("{REACTION_KEY_PREFIX}{}", Uuid::new_v4())
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

    /// Returns the Automerge object ID of the reaction stored at `key` on
    /// `target`, or `None` if `key` doesn't hold a well-formed reaction
    /// entry -- mirroring [`Segment::message`]'s shape check one level
    /// deeper (map + has the field a well-formed entry of this kind always
    /// has). Never panics, for the same reason `message` doesn't.
    pub fn reaction(&self, target: &ObjId, key: &str) -> Option<ObjId> {
        let (value, id) = self.doc.get(target, key).ok()??;
        if !matches!(value, Value::Object(ObjType::Map)) {
            return None;
        }
        // A well-formed reaction always has an "emoji" field; treat its
        // absence as a sign this entry isn't one of ours.
        self.doc.get(&id, "emoji").ok()?.map(|_| id)
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

        self.cursor += 1;
        entry
    }

    /// Reconstructs the [`Message`] stored at `key`, or `None` if `key`
    /// doesn't hold a well-formed message entry, or any field on it is
    /// missing/mistyped/malformed. Never panics -- built on top of
    /// [`Segment::message`]'s same shape checking.
    pub fn read_message(&self, key: &str) -> Option<Message> {
        let id = self.message(key)?;

        let sender = self.doc.get(&id, "sender").ok()??.0.into_bytes().ok()?;
        let sender: [u8; 32] = sender.try_into().ok()?;

        let content = self.doc.get(&id, "content").ok()??.0.into_string().ok()?;

        let attachments_obj = self.doc.get(&id, "attachments").ok()??.1;
        let len = self.doc.length(&attachments_obj);
        let mut attachments = Vec::with_capacity(len);
        for i in 0..len {
            let att_id = self.doc.get(&attachments_obj, i).ok()??.1;
            let hash = self.doc.get(&att_id, "hash").ok()??.0.into_bytes().ok()?;
            let hash: [u8; 32] = hash.try_into().ok()?;
            let size = self.doc.get(&att_id, "size").ok()??.0.to_u64()?;
            let mime = self.doc.get(&att_id, "mime").ok()??.0.into_string().ok()?;
            let wrapped_key = self
                .doc
                .get(&att_id, "wrapped_key")
                .ok()??
                .0
                .into_bytes()
                .ok()?;
            attachments.push(crate::domain::AttachmentRef {
                hash,
                size,
                mime,
                wrapped_key,
            });
        }

        Some(Message {
            sender: crate::domain::DeviceId(sender),
            content,
            attachments,
        })
    }

    /// Reconstructs the [`Reaction`] stored at `key` on `target`, or `None`
    /// if `key` doesn't hold a well-formed reaction entry, or a field on it
    /// is missing/mistyped/malformed. Never panics, mirroring
    /// [`Segment::read_message`]. The returned `Reaction`'s `target` field
    /// is `target` itself, re-serialized via [`objid_to_target_string`] --
    /// see the doc comment on `Reaction::target` for why that field exists
    /// alongside the `target: &ObjId` parameter here.
    pub fn read_reaction(&self, target: &ObjId, key: &str) -> Option<Reaction> {
        let id = self.reaction(target, key)?;

        let actor = self.doc.get(&id, "actor").ok()??.0.into_bytes().ok()?;
        let actor: [u8; 32] = actor.try_into().ok()?;

        let emoji = self.doc.get(&id, "emoji").ok()??.0.into_string().ok()?;

        Some(Reaction {
            target: objid_to_target_string(target),
            actor: crate::domain::DeviceId(actor),
            emoji,
        })
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

    /// Attaches `reaction` to `target` (a message's `ObjId`, obtained via
    /// [`Segment::message`]) and returns the Automerge object ID of the
    /// newly created reaction entry.
    ///
    /// Each reaction is stored as its own map at a fresh unique
    /// `"reaction:<uuid>"` key directly on `target`, not inside a shared
    /// list -- the same fix applied to messages (see the `Segment` doc
    /// comment) applied one level deeper: two peers who both already have
    /// `target` and independently react to it before syncing with each
    /// other would otherwise each create a "reactions" list at the same
    /// `(target, "reactions")` key with no causal link between the two
    /// creation events, which Automerge resolves via last-writer-wins,
    /// silently discarding one side's reaction.
    ///
    /// Returns `Err` rather than panicking if either `target` is foreign to
    /// this document (e.g. it came from a `Segment` that has never synced
    /// with this one -- its actor isn't in this document's actor cache) or
    /// `reaction.target` doesn't match `target` once parsed back into an
    /// `ObjId` (see `Reaction::target`'s doc comment for why that check
    /// exists). `target` is wire-facing: once a later milestone parses it
    /// out of a network message, a bad value must not be able to crash the
    /// process.
    pub fn append_reaction(
        &mut self,
        target: &ObjId,
        reaction: &Reaction,
    ) -> Result<ObjId, AutomergeError> {
        if target_string_to_objid(&reaction.target).as_ref() != Some(target) {
            return Err(AutomergeError::InvalidObjId(reaction.target.clone()));
        }

        let key = Self::new_reaction_key();
        let entry = self.doc.put_object(target, &key, ObjType::Map)?;
        self.doc.put(&entry, "actor", reaction.actor.0.to_vec())?;
        self.doc.put(&entry, "emoji", reaction.emoji.clone())?;

        self.cursor += 1;
        Ok(entry)
    }

    /// Number of well-formed reaction entries attached to `target`.
    ///
    /// Counts `"reaction:"`-prefixed keys on `target` that pass the same
    /// shape check as [`Segment::reaction`], mirroring how
    /// [`Segment::message_count`] filters through [`Segment::message`].
    /// This must be computed by counting keys, not by reading the length of
    /// a shared list, precisely because there is no shared list --
    /// concurrently-created reactions land at distinct keys, and counting
    /// keys is what makes `reaction_count` reflect all of them after sync,
    /// not just whichever side's container won a conflict.
    pub fn reaction_count(&self, target: &ObjId) -> usize {
        self.doc
            .keys(target)
            .filter(|key| key.starts_with(REACTION_KEY_PREFIX))
            .filter(|key| self.reaction(target, key).is_some())
            .count()
    }

    /// Marks `target` deleted by setting a `"deleted"` tombstone field to
    /// `true`, without removing the structural entry -- per the protocol
    /// spec, other peers still need the entry present to know to hide it.
    ///
    /// This is a plain scalar `put` at a fixed key, not a unique-key
    /// scheme: `target` is an object both peers can only have obtained via
    /// prior sync (there's no way to call `apply_delete` on an `ObjId` you
    /// don't already hold), so both sides already share causal history at
    /// that key. Concurrent writes of the same scalar value (`true`) to an
    /// already-shared key are safe -- whichever way Automerge's
    /// last-writer-wins resolves the conflict, the surviving value is
    /// still `true` -- unlike creating a brand-new object at a key with no
    /// shared ancestor, which is the hazard `append_reaction` and
    /// `append_message` avoid.
    ///
    /// Returns `Err` rather than panicking if `target` is foreign to this
    /// document, or if `delete.target` doesn't match `target` once parsed
    /// back into an `ObjId` -- see [`Segment::append_reaction`]'s doc
    /// comment for the identical reasoning, which applies here unchanged.
    pub fn apply_delete(&mut self, target: &ObjId, delete: &Delete) -> Result<(), AutomergeError> {
        if target_string_to_objid(&delete.target).as_ref() != Some(target) {
            return Err(AutomergeError::InvalidObjId(delete.target.clone()));
        }

        self.doc.put(target, "deleted", true)?;

        self.cursor += 1;
        Ok(())
    }

    /// Whether `target` has been marked deleted via [`Segment::apply_delete`].
    pub fn is_deleted(&self, target: &ObjId) -> bool {
        matches!(
            self.doc.get(target, "deleted"),
            Ok(Some((Value::Scalar(s), _))) if matches!(*s, ScalarValue::Boolean(true))
        )
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
    ///
    /// The loaded segment gets `space_id`/`epoch`/`cursor` fresh from
    /// `space_id`/`epoch` arguments and a zeroed cursor -- `bytes` alone
    /// (an Automerge document snapshot) doesn't carry that identity or
    /// cursor bookkeeping; callers that need to preserve it across a
    /// save/load round trip must track and re-supply it themselves.
    pub fn load(
        bytes: &[u8],
        space_id: impl Into<String>,
        epoch: u64,
    ) -> Result<Self, automerge::AutomergeError> {
        let doc = AutoCommit::load(bytes)?;
        Ok(Self {
            doc,
            space_id: space_id.into(),
            epoch,
            cursor: 0,
        })
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

    /// The underlying Automerge document's current heads (its
    /// content-addressed change-hash frontier). Exposed so external callers
    /// (tests, and eventually Milestone 2's storage layer) can assert exact
    /// document-state equality between peers -- a stronger convergence
    /// proof than comparing counts or key sets, since it's sensitive to
    /// *any* difference in causal history, not just the visible entries.
    pub fn heads(&mut self) -> Vec<automerge::ChangeHash> {
        self.doc.get_heads()
    }

    /// Builds a [`SegmentChange`] snapshotting this segment's current state
    /// for a [`crate::projection::Projection`] to `apply`. Callable after
    /// any mutation (`append_message`, a successful `append_reaction`, a
    /// successful `apply_delete`), each of which advances `cursor` by one.
    ///
    /// Deliberate simplification for this milestone: `bytes` is a full
    /// document snapshot (`self.save()`), not an incremental diff since the
    /// last change. `SegmentChange`/`Projection`'s design doesn't mandate
    /// incremental diffs, and getting incremental Automerge change
    /// extraction right (e.g. via `save_after_heads` / change hashes) is
    /// exactly the kind of decision Milestone 2's actual storage design
    /// should make deliberately, not something to improvise here.
    pub fn latest_change(&mut self) -> SegmentChange {
        SegmentChange {
            space_id: self.space_id.clone(),
            epoch: self.epoch,
            cursor: SegmentCursor(self.cursor),
            bytes: self.save(),
        }
    }
}

/// Creates a fresh sync-protocol state for tracking one peer relationship.
/// A `Segment` needs one `sync::State` per remote peer it exchanges sync
/// messages with (see `Segment::generate_sync_message` /
/// `receive_sync_message`).
pub fn sync_state() -> sync::State {
    sync::State::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{DeviceId, Message};
    use crate::projection::{Projection, ProjectionError, SegmentChange, SegmentCursor};

    #[test]
    fn appended_message_survives_save_and_load() {
        let mut segment = Segment::new("space-1", 0);
        let msg = Message {
            sender: DeviceId([7u8; 32]),
            content: "hello segment".to_string(),
            attachments: vec![],
        };
        segment.append_message(&msg);
        let bytes = segment.save();

        let loaded = Segment::load(&bytes, "space-1", 0).unwrap();
        assert_eq!(loaded.message_count(), 1);
    }

    #[test]
    fn appended_message_attachments_survive_save_and_load() {
        use crate::domain::AttachmentRef;

        let mut segment = Segment::new("space-1", 0);
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

        let loaded = Segment::load(&bytes, "space-1", 0).unwrap();
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

    #[test]
    fn read_message_reconstructs_sender_content_and_attachments() {
        use crate::domain::AttachmentRef;

        let mut segment = Segment::new("space-1", 0);
        let attachment = AttachmentRef {
            hash: [9u8; 32],
            size: 1234,
            mime: "image/png".to_string(),
            wrapped_key: vec![1, 2, 3, 4],
        };
        let msg = Message {
            sender: DeviceId([7u8; 32]),
            content: "look at this".to_string(),
            attachments: vec![attachment],
        };
        segment.append_message(&msg);

        let key = segment
            .message_keys()
            .next()
            .expect("expected exactly one message key");
        let read_back = segment
            .read_message(&key)
            .expect("a freshly-appended message should read back");
        assert_eq!(read_back, msg);
    }

    #[test]
    fn read_message_returns_none_for_malformed_or_missing_keys() {
        let mut segment = Segment::new("space-1", 0);
        segment
            .doc
            .put(ROOT, "msg:not-a-map", "surprise")
            .expect("put on ROOT cannot fail");

        assert!(segment.read_message("msg:not-a-map").is_none());
        assert!(segment.read_message("msg:does-not-exist").is_none());
    }

    #[test]
    fn read_reaction_reconstructs_actor_emoji_and_target() {
        let mut segment = Segment::new("space-1", 0);
        let msg_id = segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "react to me".to_string(),
            attachments: vec![],
        });

        let reaction = Reaction {
            target: objid_to_target_string(&msg_id),
            actor: DeviceId([2u8; 32]),
            emoji: "\u{1F44D}".to_string(),
        };
        segment.append_reaction(&msg_id, &reaction).unwrap();

        let key = segment
            .doc
            .keys(&msg_id)
            .find(|k| k.starts_with(REACTION_KEY_PREFIX))
            .expect("expected exactly one reaction key");
        let read_back = segment
            .read_reaction(&msg_id, &key)
            .expect("a freshly-appended reaction should read back");
        assert_eq!(read_back, reaction);
    }

    #[test]
    fn read_reaction_returns_none_for_malformed_or_missing_keys() {
        let mut segment = Segment::new("space-1", 0);
        let msg_id = segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "react to me".to_string(),
            attachments: vec![],
        });
        segment
            .doc
            .put(&msg_id, "reaction:not-a-map", "surprise")
            .expect("put on target cannot fail");

        assert!(segment
            .read_reaction(&msg_id, "reaction:not-a-map")
            .is_none());
        assert!(segment
            .read_reaction(&msg_id, "reaction:does-not-exist")
            .is_none());
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
        let mut alice = Segment::new("space-1", 0);
        let mut bob = Segment::new("space-1", 0);

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

        let mut alice_heads = alice.heads();
        let mut bob_heads = bob.heads();
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
        let mut alice = Segment::new("space-1", 0);
        let mut bob = Segment::new("space-1", 0);

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

        let loaded =
            Segment::load(&bytes, "space-1", 0).expect("a plain Automerge doc should load fine");
        assert_eq!(loaded.message_count(), 0);
    }

    #[test]
    fn load_rejects_undecodable_bytes() {
        let result = Segment::load(b"not an automerge document", "space-1", 0);
        assert!(
            result.is_err(),
            "expected Segment::load to reject bytes that aren't a valid Automerge document"
        );
    }

    #[test]
    fn message_count_ignores_non_message_keys_at_root() {
        let mut segment = Segment::new("space-1", 0);
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
        let mut segment = Segment::new("space-1", 0);

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
        let mut segment = Segment::new("space-1", 0);
        segment
            .doc
            .put(ROOT, "msg:not-a-map", "surprise")
            .expect("put on ROOT cannot fail");

        assert!(segment.message("msg:not-a-map").is_none());
        assert!(segment.message("msg:does-not-exist").is_none());
    }

    #[test]
    fn reaction_count_skips_malformed_reaction_entries_without_panicking() {
        let mut segment = Segment::new("space-1", 0);
        let msg_id = segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "react to me".to_string(),
            attachments: vec![],
        });

        // A "reaction:"-prefixed key whose value is a plain scalar, not a
        // map at all.
        segment
            .doc
            .put(&msg_id, "reaction:not-a-map", "surprise")
            .expect("put on target cannot fail");

        // A "reaction:"-prefixed key whose value is a map, but missing the
        // required "emoji" field.
        segment
            .doc
            .put_object(&msg_id, "reaction:missing-emoji", ObjType::Map)
            .expect("put_object on target cannot fail");

        assert_eq!(segment.reaction_count(&msg_id), 0);

        segment
            .append_reaction(
                &msg_id,
                &Reaction {
                    target: objid_to_target_string(&msg_id),
                    actor: DeviceId([2u8; 32]),
                    emoji: "\u{1F44D}".to_string(),
                },
            )
            .unwrap();
        assert_eq!(segment.reaction_count(&msg_id), 1);
    }

    #[test]
    fn reaction_and_delete_apply_to_a_message() {
        let mut segment = Segment::new("space-1", 0);
        let msg_id = segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "react to me".to_string(),
            attachments: vec![],
        });

        segment
            .append_reaction(
                &msg_id,
                &Reaction {
                    target: objid_to_target_string(&msg_id),
                    actor: DeviceId([2u8; 32]),
                    emoji: "\u{1F44D}".to_string(),
                },
            )
            .unwrap();
        assert_eq!(segment.reaction_count(&msg_id), 1);

        segment
            .apply_delete(
                &msg_id,
                &Delete {
                    target: objid_to_target_string(&msg_id),
                },
            )
            .unwrap();
        assert!(segment.is_deleted(&msg_id));
        // Deleting doesn't remove the structural entry -- per the protocol
        // spec, other peers still need it to know to hide the message.
        assert_eq!(segment.message_count(), 1);
    }

    /// Finding 1 of the Milestone 1 final review: `append_reaction` used to
    /// `.expect()` the `put_object` call, which panics whenever `target`'s
    /// actor isn't in this document's actor cache -- exactly what happens
    /// when `target` comes from an independently-created `Segment` that has
    /// never synced with this one. `target` is wire-facing (a later
    /// milestone will parse it out of a network message), so a bad value
    /// must produce a `Result`, not crash the process. This proves it does.
    #[test]
    fn append_reaction_returns_err_instead_of_panicking_for_a_foreign_objid() {
        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "local message".to_string(),
            attachments: vec![],
        });

        // A completely independent Segment/document -- never synced with
        // `segment` above -- so its ObjId's actor is foreign to `segment`.
        let mut foreign = Segment::new("space-1", 0);
        let foreign_msg_id = foreign.append_message(&Message {
            sender: DeviceId([9u8; 32]),
            content: "foreign message".to_string(),
            attachments: vec![],
        });

        let result = segment.append_reaction(
            &foreign_msg_id,
            &Reaction {
                target: objid_to_target_string(&foreign_msg_id),
                actor: DeviceId([2u8; 32]),
                emoji: "\u{1F44D}".to_string(),
            },
        );

        assert!(
            result.is_err(),
            "expected append_reaction to return Err for a foreign ObjId, not panic"
        );
    }

    /// Same hazard as above, for `apply_delete`. See
    /// `append_reaction_returns_err_instead_of_panicking_for_a_foreign_objid`.
    #[test]
    fn apply_delete_returns_err_instead_of_panicking_for_a_foreign_objid() {
        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "local message".to_string(),
            attachments: vec![],
        });

        let mut foreign = Segment::new("space-1", 0);
        let foreign_msg_id = foreign.append_message(&Message {
            sender: DeviceId([9u8; 32]),
            content: "foreign message".to_string(),
            attachments: vec![],
        });

        let result = segment.apply_delete(
            &foreign_msg_id,
            &Delete {
                target: objid_to_target_string(&foreign_msg_id),
            },
        );

        assert!(
            result.is_err(),
            "expected apply_delete to return Err for a foreign ObjId, not panic"
        );
    }

    #[test]
    fn append_reaction_rejects_a_target_field_that_does_not_match_the_objid_parameter() {
        let mut segment = Segment::new("space-1", 0);
        let msg_id = segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "react to me".to_string(),
            attachments: vec![],
        });
        let other_msg_id = segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "not the reaction target".to_string(),
            attachments: vec![],
        });

        let result = segment.append_reaction(
            &msg_id,
            &Reaction {
                // Mismatched on purpose: this string names `other_msg_id`,
                // not `msg_id`.
                target: objid_to_target_string(&other_msg_id),
                actor: DeviceId([2u8; 32]),
                emoji: "\u{1F44D}".to_string(),
            },
        );

        assert!(
            result.is_err(),
            "expected append_reaction to reject a target field that doesn't match the ObjId parameter"
        );
    }

    /// Two independently-created `Segment`s each append a message, sync so
    /// both sides have the same message `ObjId`, then *without any further
    /// sync in between* each independently reacts to that shared message.
    /// If reactions were stored in a shared "reactions" list container
    /// (the brief's stale example), this would be exactly the Task 4 bug
    /// one level deeper: two independent `put_object(target, "reactions",
    /// ObjType::List)` calls at the same key with no causal link, resolved
    /// by last-writer-wins, silently discarding one side's reaction. With
    /// reactions stored as their own map at a unique `"reaction:<uuid>"`
    /// key directly on `target`, both reactions survive the final sync.
    #[test]
    fn concurrent_reactions_from_independent_segments_both_survive_sync() {
        let mut alice = Segment::new("space-1", 0);
        let mut bob = Segment::new("space-1", 0);

        let msg_id = alice.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "react to me".to_string(),
            attachments: vec![],
        });

        // Sync so bob has the same message object before either side reacts.
        let mut alice_state = sync_state();
        let mut bob_state = sync_state();
        run_sync_to_completion(&mut alice, &mut alice_state, &mut bob, &mut bob_state);
        assert_eq!(bob.message_count(), 1);
        let bob_msg_id = bob
            .message_keys()
            .next()
            .and_then(|k| bob.message(&k))
            .expect("bob should have received alice's message");

        // Now both react concurrently, with no sync in between.
        alice
            .append_reaction(
                &msg_id,
                &Reaction {
                    target: objid_to_target_string(&msg_id),
                    actor: DeviceId([1u8; 32]),
                    emoji: "\u{1F44D}".to_string(),
                },
            )
            .unwrap();
        bob.append_reaction(
            &bob_msg_id,
            &Reaction {
                target: objid_to_target_string(&bob_msg_id),
                actor: DeviceId([2u8; 32]),
                emoji: "\u{2764}".to_string(),
            },
        )
        .unwrap();

        run_sync_to_completion(&mut alice, &mut alice_state, &mut bob, &mut bob_state);

        assert_eq!(
            alice.reaction_count(&msg_id),
            2,
            "both concurrently-created reactions should survive sync on alice's side"
        );
        assert_eq!(
            bob.reaction_count(&bob_msg_id),
            2,
            "both concurrently-created reactions should survive sync on bob's side"
        );
    }

    /// Same hazard, but for deletes: two independently-created `Segment`s
    /// each already have `target` via prior sync, then both concurrently
    /// call `apply_delete` before syncing again. Unlike reactions, this is
    /// safe even without a unique-key scheme: `apply_delete` is a `put` of
    /// the same scalar value (`true`) at a fixed `"deleted"` key on an
    /// object both peers already share causal history for, so even if
    /// Automerge's conflict resolution picks "the other side's" write, the
    /// result is still `true` on both sides.
    #[test]
    fn concurrent_deletes_from_independent_segments_are_preserved_after_sync() {
        let mut alice = Segment::new("space-1", 0);
        let mut bob = Segment::new("space-1", 0);

        let msg_id = alice.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "delete me".to_string(),
            attachments: vec![],
        });

        let mut alice_state = sync_state();
        let mut bob_state = sync_state();
        run_sync_to_completion(&mut alice, &mut alice_state, &mut bob, &mut bob_state);
        let bob_msg_id = bob
            .message_keys()
            .next()
            .and_then(|k| bob.message(&k))
            .expect("bob should have received alice's message");

        alice
            .apply_delete(
                &msg_id,
                &Delete {
                    target: objid_to_target_string(&msg_id),
                },
            )
            .unwrap();
        bob.apply_delete(
            &bob_msg_id,
            &Delete {
                target: objid_to_target_string(&bob_msg_id),
            },
        )
        .unwrap();

        run_sync_to_completion(&mut alice, &mut alice_state, &mut bob, &mut bob_state);

        assert!(alice.is_deleted(&msg_id));
        assert!(bob.is_deleted(&bob_msg_id));
    }

    /// First end-to-end proof that `Segment` and `Projection` actually work
    /// together, not just that each compiles in isolation: append a
    /// message, take `latest_change()`, feed it to a `Projection::apply`,
    /// and check the projection's watermark reflects it.
    #[test]
    fn latest_change_can_be_applied_by_a_projection_and_advances_its_watermark() {
        struct CountingProjection {
            watermark: SegmentCursor,
        }

        impl Projection for CountingProjection {
            fn watermark(&self) -> SegmentCursor {
                self.watermark
            }
            fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError> {
                self.watermark = change.cursor;
                Ok(())
            }
        }

        let mut segment = Segment::new("space-42", 3);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello projection".to_string(),
            attachments: vec![],
        });

        let change = segment.latest_change();
        assert_eq!(change.space_id, "space-42");
        assert_eq!(change.epoch, 3);
        assert_eq!(change.cursor, SegmentCursor(1));

        let mut projection = CountingProjection {
            watermark: SegmentCursor(0),
        };
        projection.apply(&change).unwrap();
        assert_eq!(projection.watermark(), SegmentCursor(1));
    }

    #[test]
    fn cursor_advances_on_every_successful_mutating_call() {
        let mut segment = Segment::new("space-1", 0);
        assert_eq!(segment.latest_change().cursor, SegmentCursor(0));

        let msg_id = segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "one".to_string(),
            attachments: vec![],
        });
        assert_eq!(segment.latest_change().cursor, SegmentCursor(1));

        segment
            .append_reaction(
                &msg_id,
                &Reaction {
                    target: objid_to_target_string(&msg_id),
                    actor: DeviceId([2u8; 32]),
                    emoji: "\u{1F44D}".to_string(),
                },
            )
            .unwrap();
        assert_eq!(segment.latest_change().cursor, SegmentCursor(2));

        segment
            .apply_delete(
                &msg_id,
                &Delete {
                    target: objid_to_target_string(&msg_id),
                },
            )
            .unwrap();
        assert_eq!(segment.latest_change().cursor, SegmentCursor(3));
    }

    #[test]
    fn a_failed_append_reaction_or_apply_delete_does_not_advance_the_cursor() {
        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "one".to_string(),
            attachments: vec![],
        });
        assert_eq!(segment.latest_change().cursor, SegmentCursor(1));

        let mut foreign = Segment::new("space-1", 0);
        let foreign_msg_id = foreign.append_message(&Message {
            sender: DeviceId([9u8; 32]),
            content: "foreign".to_string(),
            attachments: vec![],
        });

        assert!(segment
            .append_reaction(
                &foreign_msg_id,
                &Reaction {
                    target: objid_to_target_string(&foreign_msg_id),
                    actor: DeviceId([2u8; 32]),
                    emoji: "\u{1F44D}".to_string(),
                },
            )
            .is_err());
        assert!(segment
            .apply_delete(
                &foreign_msg_id,
                &Delete {
                    target: objid_to_target_string(&foreign_msg_id),
                },
            )
            .is_err());

        assert_eq!(
            segment.latest_change().cursor,
            SegmentCursor(1),
            "a failed append_reaction/apply_delete must not advance the cursor"
        );
    }
}

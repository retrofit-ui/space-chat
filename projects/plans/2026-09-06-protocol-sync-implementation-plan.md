# Protocol & Sync Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `space-chat-core` — the domain types, segment/epoch management, Automerge-backed sync engine, and sequencer logic — as a headless Rust library where two or more in-process instances can create a shared space, exchange messages, and converge, with no networking or storage backend yet (both are stubbed with in-memory test doubles, per the trait boundaries the storage spec already defines).

**Architecture:** `space-chat-core` owns domain types (`Message`, `Reaction`, `Delete`, `AttachmentRef`), wraps one `automerge::AutoCommit` document per epoch segment, exposes a `Projection` trait + change-feed for derived views to consume, and implements sequencer election as pure deterministic logic over group membership. MLS/OpenMLS integration (`space-chat-openmls`) is a separate follow-on plan, not part of this one — see the closing note.

**Tech Stack:** Rust, `automerge` crate for CRDT sync, `serde`/`ciborium` for the domain type wire shapes, `tokio` for the async change-feed.

## Global Constraints

- No dependency on `redb`, `tantivy`, `openmls`, or any transport crate in `space-chat-core` — per the storage spec's crate layout, this crate only defines trait boundaries for those, it doesn't implement against them.
- No message editing — `Message` is immutable once created, per the protocol spec's Non-goals.
- Messages target their `Reaction`/`Delete` relationships via Automerge's native object id, not a reinvented message-id scheme, per the protocol spec's Data model.
- **`automerge` crate method names in this plan reflect the API as best known at writing time. Before implementing any step that calls into the `automerge` crate, verify the exact method signatures against `docs.rs` for whatever version gets pinned in `Cargo.toml` — do not treat the code below as gospel over the actual crate docs if they've drifted.** This caveat applies only to external-crate calls; all `space-chat-core`-defined types/signatures in this plan are authoritative for later tasks.

---

### Task 1: Domain types

**Files:**
- Create: `space-chat-core/Cargo.toml`
- Create: `space-chat-core/src/lib.rs`
- Create: `space-chat-core/src/domain.rs`
- Test: `space-chat-core/src/domain.rs` (inline `#[cfg(test)]` module)

**Interfaces:**
- Produces: `DeviceId(pub [u8; 32])`, `Message { sender: DeviceId, content: String, attachments: Vec<AttachmentRef> }`, `Reaction { target: automerge::ObjId, actor: DeviceId, emoji: String }`, `Delete { target: automerge::ObjId }`, `AttachmentRef { hash: [u8; 32], size: u64, mime: String, wrapped_key: Vec<u8> }` — all `#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]`.

- [ ] **Step 1: Create the crate**

```bash
mkdir -p space-chat-core/src
cat > space-chat-core/Cargo.toml <<'EOF'
[package]
name = "space-chat-core"
version = "0.1.0"
edition = "2021"

[dependencies]
automerge = "0.5"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["sync", "rt"] }

[dev-dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
EOF
echo 'pub mod domain;' > space-chat-core/src/lib.rs
```

- [ ] **Step 2: Write the failing test**

```rust
// space-chat-core/src/domain.rs
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd space-chat-core && cargo test message_round_trips_through_serde`
Expected: FAIL — `Message` is not defined, and `serde_json` isn't a dependency yet.

- [ ] **Step 4: Add `serde_json` as a dev-dependency and write the domain types**

```bash
cd space-chat-core && cargo add --dev serde_json
```

```rust
// space-chat-core/src/domain.rs (above the tests module)
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd space-chat-core && cargo test message_round_trips_through_serde`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add space-chat-core/Cargo.toml space-chat-core/src/lib.rs space-chat-core/src/domain.rs space-chat-core/Cargo.lock
git commit -m "feat(core): add domain types for Message, Reaction, Delete, AttachmentRef"
```

---

### Task 2: The `Projection` trait and change feed

**Files:**
- Create: `space-chat-core/src/projection.rs`
- Modify: `space-chat-core/src/lib.rs` — add `pub mod projection;`
- Test: `space-chat-core/src/projection.rs` (inline)

**Interfaces:**
- Consumes: nothing from Task 1 directly (this task is standalone plumbing).
- Produces: `SegmentCursor(pub u64)`, `SegmentChange { space_id: String, epoch: u64, cursor: SegmentCursor, bytes: Vec<u8> }`, `trait Projection { fn watermark(&self) -> SegmentCursor; fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError>; }`, `ProjectionError` (an enum with at least `ApplyFailed(String)`). Later tasks (Task 4's change feed, and the storage/app-shell plans) consume this trait and these exact type names.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-core/src/projection.rs
#[cfg(test)]
mod tests {
    use super::*;

    struct CountingProjection { count: u64 }

    impl Projection for CountingProjection {
        fn watermark(&self) -> SegmentCursor {
            SegmentCursor(self.count)
        }
        fn apply(&mut self, _change: &SegmentChange) -> Result<(), ProjectionError> {
            self.count += 1;
            Ok(())
        }
    }

    #[test]
    fn projection_advances_watermark_on_apply() {
        let mut p = CountingProjection { count: 0 };
        let change = SegmentChange {
            space_id: "space-1".to_string(),
            epoch: 0,
            cursor: SegmentCursor(1),
            bytes: vec![1, 2, 3],
        };
        p.apply(&change).unwrap();
        assert_eq!(p.watermark(), SegmentCursor(1));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-core && cargo test projection_advances_watermark_on_apply`
Expected: FAIL — `Projection`, `SegmentChange`, `SegmentCursor`, `ProjectionError` not defined.

- [ ] **Step 3: Write the trait and types**

```rust
// space-chat-core/src/projection.rs (above the tests module)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentCursor(pub u64);

#[derive(Debug, Clone)]
pub struct SegmentChange {
    pub space_id: String,
    pub epoch: u64,
    pub cursor: SegmentCursor,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionError {
    ApplyFailed(String),
}

pub trait Projection {
    fn watermark(&self) -> SegmentCursor;
    fn apply(&mut self, change: &SegmentChange) -> Result<(), ProjectionError>;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd space-chat-core && cargo test projection_advances_watermark_on_apply`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add space-chat-core/src/projection.rs space-chat-core/src/lib.rs
git commit -m "feat(core): add Projection trait and change-feed types"
```

---

### Task 3: Epoch segment wrapping Automerge

**Files:**
- Create: `space-chat-core/src/segment.rs`
- Modify: `space-chat-core/src/lib.rs` — add `pub mod segment;`
- Test: `space-chat-core/src/segment.rs` (inline)

**Interfaces:**
- Consumes: `Message` from Task 1 (`domain.rs`).
- Produces: `struct Segment { /* wraps automerge::AutoCommit */ }` with `Segment::new() -> Self`, `fn append_message(&mut self, msg: &Message) -> automerge::ObjId`, `fn save(&mut self) -> Vec<u8>`, `fn load(bytes: &[u8]) -> Result<Self, automerge::AutomergeError>`. Task 5 consumes `append_message`, `save`, and `load` directly.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-core/src/segment.rs
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-core && cargo test appended_message_survives_save_and_load`
Expected: FAIL — `Segment` not defined.

- [ ] **Step 3: Write the segment wrapper**

```rust
// space-chat-core/src/segment.rs (above the tests module)
use automerge::{transaction::Transactable, AutoCommit, ObjId, ObjType, ROOT};
use crate::domain::Message;

pub struct Segment {
    doc: AutoCommit,
}

impl Segment {
    pub fn new() -> Self {
        let mut doc = AutoCommit::new();
        // "messages" is a list object living at the document root; each
        // appended message becomes one list element, keeping the doc
        // append-mostly per the protocol spec's no-edit decision.
        doc.put_object(ROOT, "messages", ObjType::List)
            .expect("creating the root messages list cannot fail on a fresh doc");
        Self { doc }
    }

    pub fn append_message(&mut self, msg: &Message) -> ObjId {
        let messages = self.doc.get(ROOT, "messages")
            .expect("get on a known key cannot fail")
            .expect("messages list was created in Segment::new")
            .1;
        let idx = self.doc.length(&messages);
        let entry = self.doc
            .insert_object(&messages, idx, ObjType::Map)
            .expect("inserting into the messages list cannot fail");
        self.doc.put(&entry, "sender", msg.sender.0.to_vec())
            .expect("put on a freshly-inserted map cannot fail");
        self.doc.put(&entry, "content", msg.content.clone())
            .expect("put on a freshly-inserted map cannot fail");
        entry
    }

    pub fn message_count(&self) -> usize {
        let messages = self.doc.get(ROOT, "messages")
            .expect("get on a known key cannot fail")
            .expect("messages list was created in Segment::new")
            .1;
        self.doc.length(&messages)
    }

    pub fn save(&mut self) -> Vec<u8> {
        self.doc.save()
    }

    pub fn load(bytes: &[u8]) -> Result<Self, automerge::AutomergeError> {
        Ok(Self { doc: AutoCommit::load(bytes)? })
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd space-chat-core && cargo test appended_message_survives_save_and_load`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add space-chat-core/src/segment.rs space-chat-core/src/lib.rs
git commit -m "feat(core): wrap Automerge AutoCommit as an epoch Segment"
```

---

### Task 4: Two segments converge via Automerge sync messages

**Files:**
- Modify: `space-chat-core/src/segment.rs`
- Test: `space-chat-core/src/segment.rs` (inline)

**Interfaces:**
- Consumes: `Segment` from Task 3.
- Produces: `fn sync_state() -> automerge::sync::State` (free function), `fn generate_sync_message(&mut self, state: &mut automerge::sync::State) -> Option<automerge::sync::Message>`, `fn receive_sync_message(&mut self, state: &mut automerge::sync::State, msg: automerge::sync::Message) -> Result<(), automerge::AutomergeError>` on `Segment`. Task 7's integration test consumes all three directly to drive convergence between multiple in-process `Segment`s.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-core/src/segment.rs, inside the tests module
#[test]
fn two_segments_converge_via_sync_messages() {
    use crate::domain::DeviceId;

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

    // Exchange messages until neither side has anything left to send —
    // Automerge's sync protocol may need more than one round trip.
    loop {
        let a_to_b = alice.generate_sync_message(&mut alice_state);
        let b_to_a = bob.generate_sync_message(&mut bob_state);
        let (a_msg, b_msg) = (a_to_b.is_none(), b_to_a.is_none());
        if let Some(msg) = a_to_b {
            bob.receive_sync_message(&mut bob_state, msg).unwrap();
        }
        if let Some(msg) = b_to_a {
            alice.receive_sync_message(&mut alice_state, msg).unwrap();
        }
        if a_msg && b_msg {
            break;
        }
    }

    assert_eq!(alice.message_count(), 2);
    assert_eq!(bob.message_count(), 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-core && cargo test two_segments_converge_via_sync_messages`
Expected: FAIL — `sync_state`, `generate_sync_message`, `receive_sync_message` not defined.

- [ ] **Step 3: Add sync methods to `Segment`**

```rust
// space-chat-core/src/segment.rs — add near the top, alongside the imports
use automerge::sync::{self, SyncDoc};

// add inside impl Segment, alongside append_message/save/load
pub fn generate_sync_message(&mut self, state: &mut sync::State) -> Option<sync::Message> {
    self.doc.generate_sync_message(state)
}

pub fn receive_sync_message(
    &mut self,
    state: &mut sync::State,
    msg: sync::Message,
) -> Result<(), automerge::AutomergeError> {
    self.doc.receive_sync_message(state, msg)
}

// free function, defined below the impl block
pub fn sync_state() -> sync::State {
    sync::State::new()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd space-chat-core && cargo test two_segments_converge_via_sync_messages`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add space-chat-core/src/segment.rs
git commit -m "feat(core): add Automerge sync-message exchange to Segment"
```

---

### Task 5: Sequencer election

**Files:**
- Create: `space-chat-core/src/sequencer.rs`
- Modify: `space-chat-core/src/lib.rs` — add `pub mod sequencer;`
- Test: `space-chat-core/src/sequencer.rs` (inline)

**Interfaces:**
- Consumes: `DeviceId` from Task 1.
- Produces: `fn elect_sequencer(members: &[DeviceId]) -> DeviceId`. Task 7's integration test and the future MLS-integration plan consume this directly — it must be a pure function with no side effects, since both the protocol spec and every member computes it independently and must agree.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-core/src/sequencer.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::DeviceId;

    #[test]
    fn sequencer_is_lowest_device_id() {
        let members = vec![
            DeviceId([5u8; 32]),
            DeviceId([1u8; 32]),
            DeviceId([9u8; 32]),
        ];
        assert_eq!(elect_sequencer(&members), DeviceId([1u8; 32]));
    }

    #[test]
    fn sequencer_recomputes_after_removal() {
        // Per the protocol spec: removing the current sequencer from
        // membership must deterministically elect the next-lowest id,
        // with no separate handoff message needed.
        let members = vec![DeviceId([5u8; 32]), DeviceId([9u8; 32])];
        assert_eq!(elect_sequencer(&members), DeviceId([5u8; 32]));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-core && cargo test sequencer_is_lowest_device_id`
Expected: FAIL — `elect_sequencer` not defined.

- [ ] **Step 3: Implement the pure election function**

```rust
// space-chat-core/src/sequencer.rs (above the tests module)
use crate::domain::DeviceId;

/// Deterministic sequencer election per the protocol spec: the member with
/// the lowest DeviceId (compared byte-for-byte) is the sequencer. Every
/// member computes this independently from current membership — there is
/// no election message, so removing the sequencer from membership and
/// recomputing this function is the entire "handoff."
pub fn elect_sequencer(members: &[DeviceId]) -> DeviceId {
    *members
        .iter()
        .min_by_key(|d| d.0)
        .expect("elect_sequencer requires at least one member")
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd space-chat-core && cargo test sequencer`
Expected: PASS (both tests)

- [ ] **Step 5: Commit**

```bash
git add space-chat-core/src/sequencer.rs space-chat-core/src/lib.rs
git commit -m "feat(core): add deterministic sequencer election"
```

---

### Task 6: Reaction and Delete application on `Segment`

**Files:**
- Modify: `space-chat-core/src/segment.rs`
- Test: `space-chat-core/src/segment.rs` (inline)

**Interfaces:**
- Consumes: `Reaction`, `Delete` from Task 1 (`domain.rs`); `Segment` from Task 3.
- Produces: `fn append_reaction(&mut self, target: &ObjId, reaction: &Reaction) -> ObjId` and `fn apply_delete(&mut self, target: &ObjId)` on `Segment`. Task 7's integration test does not need these directly, but the storage plan's `ListingIndex` projection (Milestone 2) will call `apply_delete`'s effect (the tombstone field) when building read-side views.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-core/src/segment.rs, inside the tests module
#[test]
fn reaction_and_delete_apply_to_a_message() {
    use crate::domain::{DeviceId, Reaction};

    let mut segment = Segment::new();
    let msg_id = segment.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "react to me".to_string(),
        attachments: vec![],
    });

    segment.append_reaction(&msg_id, &Reaction {
        target: format!("{:?}", msg_id),
        actor: DeviceId([2u8; 32]),
        emoji: "👍".to_string(),
    });
    assert_eq!(segment.reaction_count(&msg_id), 1);

    segment.apply_delete(&msg_id);
    assert!(segment.is_deleted(&msg_id));
    // Deleting doesn't remove the structural entry — per the protocol
    // spec, other peers still need it to know to hide the message.
    assert_eq!(segment.message_count(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-core && cargo test reaction_and_delete_apply_to_a_message`
Expected: FAIL — `append_reaction`, `reaction_count`, `apply_delete`, `is_deleted` not defined.

- [ ] **Step 3: Implement reaction and delete handling**

```rust
// space-chat-core/src/segment.rs — add to the imports
use crate::domain::Reaction;

// add inside impl Segment
pub fn append_reaction(&mut self, target: &ObjId, reaction: &Reaction) -> ObjId {
    let reactions = match self.doc.get(target, "reactions").expect("get cannot fail") {
        Some((_, obj)) => obj,
        None => self.doc.put_object(target, "reactions", ObjType::List)
            .expect("creating the reactions list on a valid target cannot fail"),
    };
    let idx = self.doc.length(&reactions);
    let entry = self.doc.insert_object(&reactions, idx, ObjType::Map)
        .expect("inserting into the reactions list cannot fail");
    self.doc.put(&entry, "actor", reaction.actor.0.to_vec())
        .expect("put on a freshly-inserted map cannot fail");
    self.doc.put(&entry, "emoji", reaction.emoji.clone())
        .expect("put on a freshly-inserted map cannot fail");
    entry
}

pub fn reaction_count(&self, target: &ObjId) -> usize {
    match self.doc.get(target, "reactions").expect("get cannot fail") {
        Some((_, obj)) => self.doc.length(&obj),
        None => 0,
    }
}

pub fn apply_delete(&mut self, target: &ObjId) {
    // A tombstone field, not removal — the structural entry must survive
    // so other peers know to hide it, per the protocol spec's Data model.
    self.doc.put(target, "deleted", true)
        .expect("put on a valid target cannot fail");
}

pub fn is_deleted(&self, target: &ObjId) -> bool {
    match self.doc.get(target, "deleted").expect("get cannot fail") {
        Some((automerge::Value::Scalar(s), _)) => matches!(*s, automerge::ScalarValue::Boolean(true)),
        _ => false,
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd space-chat-core && cargo test reaction_and_delete_apply_to_a_message`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add space-chat-core/src/segment.rs
git commit -m "feat(core): add Reaction and Delete application to Segment"
```

---

### Task 7: Three-member convergence integration test

**Files:**
- Create: `space-chat-core/tests/convergence.rs`

**Interfaces:**
- Consumes: `Segment` (Task 3/4), `elect_sequencer` (Task 5), `Message`/`DeviceId` (Task 1). Produces nothing new — this is the milestone's exit-criteria test, not a building block for later tasks.

- [ ] **Step 1: Write the integration test**

```rust
// space-chat-core/tests/convergence.rs
use space_chat_core::domain::{DeviceId, Message};
use space_chat_core::segment::{sync_state, Segment};
use space_chat_core::sequencer::elect_sequencer;

fn send(segment: &mut Segment, sender: DeviceId, content: &str) {
    segment.append_message(&Message {
        sender,
        content: content.to_string(),
        attachments: vec![],
    });
}

fn sync_pair(a: &mut Segment, a_state: &mut automerge::sync::State, b: &mut Segment, b_state: &mut automerge::sync::State) {
    loop {
        let a_to_b = a.generate_sync_message(a_state);
        let b_to_a = b.generate_sync_message(b_state);
        let (a_done, b_done) = (a_to_b.is_none(), b_to_a.is_none());
        if let Some(msg) = a_to_b {
            b.receive_sync_message(b_state, msg).unwrap();
        }
        if let Some(msg) = b_to_a {
            a.receive_sync_message(a_state, msg).unwrap();
        }
        if a_done && b_done {
            break;
        }
    }
}

#[test]
fn three_members_converge_after_partition_and_reconnect() {
    let alice_id = DeviceId([1u8; 32]);
    let bob_id = DeviceId([2u8; 32]);
    let carol_id = DeviceId([3u8; 32]);

    // Sanity check on Task 5's sequencer logic in a 3-member context —
    // this is what the protocol spec's membership-change routing depends on.
    assert_eq!(elect_sequencer(&[alice_id, bob_id, carol_id]), alice_id);

    let mut alice = Segment::new();
    let mut bob = Segment::new();
    let mut carol = Segment::new();
    let (mut a_state, mut b_state, mut c_state) = (sync_state(), sync_state(), sync_state());

    // All three start in sync.
    sync_pair(&mut alice, &mut a_state, &mut bob, &mut b_state);
    sync_pair(&mut bob, &mut b_state, &mut carol, &mut c_state);
    sync_pair(&mut alice, &mut a_state, &mut carol, &mut c_state);

    // Simulate a partition: Alice and Bob send messages Carol doesn't see yet.
    send(&mut alice, alice_id, "from alice during partition");
    send(&mut bob, bob_id, "from bob during partition");

    // Reconnect: sync every pair until all three match.
    sync_pair(&mut alice, &mut a_state, &mut bob, &mut b_state);
    sync_pair(&mut bob, &mut b_state, &mut carol, &mut c_state);
    sync_pair(&mut alice, &mut a_state, &mut carol, &mut c_state);
    // One more pass, since Carol's new state from Bob may need to reach Alice.
    sync_pair(&mut alice, &mut a_state, &mut carol, &mut c_state);

    assert_eq!(alice.message_count(), 2);
    assert_eq!(bob.message_count(), 2);
    assert_eq!(carol.message_count(), 2);
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `cd space-chat-core && cargo test --test convergence`
Expected: PASS — this exercises the exact scenario shape from the protocol spec's Testing section ("Both sides of a partition converge after reconnecting"), at the `space-chat-core` level rather than through the full `cucumber-rs`+`fantoccini` harness, since neither transport nor UI exist yet at this milestone. This test should be revisited and re-expressed as a Gherkin scenario in the full harness once Milestone 3/4 land, per the protocol spec's Testing section.

- [ ] **Step 3: Commit**

```bash
git add space-chat-core/tests/convergence.rs
git commit -m "test(core): add three-member partition/reconnect convergence test"
```

---

## Closing note: what this plan deliberately excludes

**MLS/OpenMLS integration (`space-chat-openmls`) is not part of this plan.** It's a separable follow-on: the sequencer and segment/Automerge logic built here don't need MLS to be testable (this plan's convergence test uses plaintext `Segment`s with no encryption layer at all), and OpenMLS's exact API surface should be verified against current `docs.rs` when that plan is written, rather than drafted from memory alongside a dozen other tasks here — the risk of subtly wrong external-crate call signatures compounding across an already-long plan isn't worth taking. Write that as its own plan once this one is merged and `space-chat-core`'s domain types (which OpenMLS integration will need to reference — e.g. `DeviceId` as the MLS credential identity) are settled by real usage, not just by this spec.

**Also excluded, and why**: the CBOR wire envelope (`(space_id, category)` tagging) and the ephemeral-message channel — both are transport-spec concerns (Milestone 3) that need real network bytes to test meaningfully, not something to stub out here.

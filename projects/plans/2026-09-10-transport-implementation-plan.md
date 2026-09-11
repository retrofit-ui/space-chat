# Transport & Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> ## ⚠️ STALE CODE EXAMPLES — READ BEFORE USING THIS PLAN AS A REFERENCE
>
> **This plan has been implemented. The code examples embedded in Tasks 6, 7,
> 9, and 10 predate numerous real bugs found during implementation and do NOT
> reflect the final, correct, shipped behavior.** They are preserved verbatim
> as a record of the original design intent, not as a description of what the
> crate does.
>
> **`space-chat-transport/src/` is authoritative. This document's embedded
> code samples are not.** Copying Task 6/7/9/10's snippets as a design
> reference would reintroduce this milestone's entire bug list — the
> falsely-"lazy" stream-opening doc comment, the lock-across-I/O stall, the
> lock-order-inversion deadlock, the cancellation-unsafe `read_frame`-in-
> `select!`, the asymmetric dialer/accepter accept loop, the
> `Connected`-fired-before-handshake race, the `addr_via_own_relay` busy-spin
> livelock, unbounded join-request forwarding, and more.
>
> See **[Post-implementation amendments](#post-implementation-amendments)** at
> the bottom of this document for what actually changed, and for the list of
> known limitations that were deliberately deferred rather than fixed.

**Goal:** Build `space-chat-transport` — a real `iroh`-based networking crate that lets two or more `space-chat-core` instances dial each other by endpoint ID, negotiate per-space activity over a connection-level control stream, exchange Automerge sync/gossip/MLS-control/ephemeral/attachment traffic over eagerly-opened, prioritized, per-`(space_id, category)` QUIC streams, pair via a link/QR-encoded invite, and route invite-based joins through the space's elected sequencer — proven by two (and three, for the multi-hop scenario) separate OS processes converging over a local `iroh` test relay.

**Architecture:** One crate, `space-chat-transport`, not several. Unlike Milestone 2's storage crates (which split along genuinely different backend dependency trees — `redb` vs `tantivy` vs plain files, each independently swappable), every piece of transport — connection setup, control-stream digest exchange, per-space stream lifecycle, invite encoding, sequencer-routed joins — depends on the same `iroh` dependency and interoperates constantly within a single connection's lifecycle. Splitting it into multiple crates would be crate-boundary ceremony with no real isolation benefit, so it stays one crate with clear internal modules.

The central design insight, worth stating up front because it explains several later tasks: **`Transport` does not own message/reaction/delete semantics at all.** The caller (eventually `space-chat-app`) owns and mutates a `space_chat_core::segment::Segment` exactly as it does today; `Transport` is handed a **shared, mutex-guarded handle to that same `Segment`** (`Arc<tokio::sync::Mutex<Segment>>`) per space it's tracking, and its only job re: that segment is calling the three sync-message methods `Segment` already exposes (`generate_sync_message`/`receive_sync_message`/`heads`) against whichever peers are currently connected for that space. **Multi-hop convergence for messages falls out of this for free, with no relay/forwarding code written anywhere**: if peer A and peer B are both syncing pairwise against their own local copy of the *same shared* segment, content A sent to B is already present in B's segment by the time B's independent sync loop with C runs next — exactly the way `git fetch` doesn't care which remote actually authored a commit, and exactly the property the transport spec's Resilience section describes as "emergent, not designed machinery." Attachment transfer deliberately does **not** share this property: it is a direct request/response between the requesting peer and a specific peer believed to hold the bytes, with no equivalent "shared state that naturally propagates" mechanism — Task 9 builds it as its own strictly-point-to-point exchange, and Task 8's test proves the absence of relaying is real, not just unexercised.

A second design note worth flagging up front: the protocol spec's "thin outer envelope tagging `(space_id, category)`" is implemented here as a **stream-open header, sent once when a per-`(space_id, category)` stream is newly opened, not repeated on every message**. Once a QUIC stream exists for a given `(space_id, category)`, every frame on it is already unambiguously scoped — repeating the tag on each frame would be redundant. The protocol spec explicitly defers this mapping decision to this spec ("See the transport spec for how `(space_id, category)` maps onto QUIC streams"), so this is this plan's call to make, not a deviation from anything already decided.

A third note: `space-chat-openmls` does not exist yet (deliberately excluded from Milestone 1's plan as a separable follow-on) — real MLS group membership, `KeyPackage`s, and Commits are not available to build against. This plan defines a `SpaceMembership` trait boundary (Task 10) that a future MLS integration implements; until then, tests use a trivial in-memory fake. Join-request *payloads* (the bytes a joiner presents, which a real implementation would fill with an MLS `KeyPackage`/proposal) are carried as opaque `Vec<u8>` throughout — this crate never inspects or interprets them, only routes them to the right device.

**Tech Stack:** Rust, `iroh` (peer-to-peer QUIC connectivity, NAT traversal, relay fallback), `tokio` (async runtime, used throughout for connection/stream tasks), `serde` + `ciborium` (CBOR wire encoding, matching the protocol spec's own choice of `ciborium`), `space-chat-core` (domain types, `Segment`, `sequencer::elect_sequencer`).

## Global Constraints

- **Endpoint identity is a separate keypair from the MLS device signing key.** `TransportIdentity` (Task 1) has no notion that an MLS key exists anywhere; it is generated independently. A future composition root persists both together as one on-disk device identity, but that persistence — and any linkage — lives outside this crate.
- **v1 relay policy is n0's public relay network, not self-hosted.** Production `Transport::bind` calls use `iroh`'s default preset (which includes n0's relay/discovery defaults); only tests override this with a local `iroh::test_utils::run_relay_server()`.
- **Attachment transfer is strictly direct-endpoint.** No multi-hop relaying through an intermediate peer that happens to already have the bytes — this is a deliberate scope limit from the transport spec, not an oversight to fix later. Task 8's test proves it.
- **Networks that block UDP outright will fail to connect.** `iroh` is QUIC/UDP-based with no TCP fallback; this is a known, accepted limitation, not something any task here designs around.
- **`iroh` shipped a stable 1.0 in June 2026; this plan pins `iroh = "1"`.** Every `iroh`-crate method signature in this plan was checked against `docs.rs/iroh/latest` at the time this plan was written (`Endpoint::builder(preset)`, `.secret_key()`, `.alpns()`, `.relay_mode()`, `.bind()`, `.connect()`, `.accept()`, `.id()`, `.network_change()`, `Connection::open_bi/open_uni/accept_bi/accept_uni/remote_id()`, `SendStream::set_priority()`, `SecretKey::generate()/from_bytes()/to_bytes()/public()`, `iroh::test_utils::run_relay_server()`) — but **verify every one against `docs.rs` for whichever exact `iroh` version gets pinned in `Cargo.toml` before implementing the step that calls it**, the same caveat the Milestone 1 plan applied to `automerge` and the Milestone 2 plan applied to `redb`/`tantivy`. Some details this plan could not fully pin down from the docs alone (the exact `RelayMode::Custom` argument shape, `EndpointAddr`'s builder methods for attaching a relay URL to a bare endpoint ID, whether `SendStream`/`RecvStream` implement `tokio::io::AsyncWrite`/`AsyncRead` directly) are flagged inline at the step that needs them — resolve those against the real docs at implementation time, not by guessing further.
- **No dependency on `space-chat-openmls`, `redb`, or `tantivy` in `space-chat-transport`.** This crate only defines the `SpaceMembership` trait boundary a future MLS integration will implement; it never implements against MLS, storage, or search itself.
- **Testing has no rendered UI to assert against.** Milestone 4 (app shell) hasn't been built, so this plan's tests assert directly against real two/three-OS-process convergence and connection-status observations rather than through the `cucumber-rs`+`fantoccini` harness the transport spec's Testing section describes — the same fallback the storage plan used for the same reason. The Gherkin scenarios in the transport spec should be revisited and re-expressed through the real harness once Milestone 4 lands.
- **QR-code pixel rendering is out of scope for this crate.** `Invite::encode_link` (Task 3) produces the canonical string a QR code would encode; turning that string into an actual scannable image is a UI-layer concern (Milestone 4), consistent with keeping this crate free of any rendering dependency.

---

### Task 1: Crate scaffold, `TransportError`, `TransportIdentity`

**Files:**
- Create: `space-chat-transport/Cargo.toml`
- Create: `space-chat-transport/src/lib.rs`
- Create: `space-chat-transport/src/error.rs`
- Create: `space-chat-transport/src/identity.rs`
- Test: `space-chat-transport/src/identity.rs` (inline `#[cfg(test)]`)
- Modify: workspace root `Cargo.toml` — add `"space-chat-transport"` to `members`

**Interfaces:**
- Consumes: nothing from other tasks (first task).
- Produces: `TransportError` (enum: `Io(String)`, `Codec(String)`, `Connection(String)`, `NotFound`, `Timeout`), `TransportIdentity` with `generate() -> Self`, `from_bytes(bytes: &[u8; 32]) -> Self`, `to_bytes(&self) -> [u8; 32]`, `endpoint_id(&self) -> iroh::EndpointId`, `pub(crate) fn secret_key(&self) -> iroh::SecretKey`. Every later task's fallible calls return `Result<_, TransportError>`; every later task that needs an identity uses `TransportIdentity` by these exact names.

- [ ] **Step 1: Create the crate and add it to the workspace**

```bash
mkdir -p space-chat-transport/src
cat > space-chat-transport/Cargo.toml <<'EOF'
[package]
name = "space-chat-transport"
version = "0.1.0"
edition = "2021"

[dependencies]
space-chat-core = { path = "../space-chat-core" }
iroh = "1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
ciborium = "0.2"

[dev-dependencies]
tempfile = "3"
EOF
echo 'pub mod error;' > space-chat-transport/src/lib.rs
echo 'pub mod identity;' >> space-chat-transport/src/lib.rs
```

Modify the workspace root `Cargo.toml`'s `members` array to include `"space-chat-transport"`.

- [ ] **Step 2: Write `TransportError`**

```rust
// space-chat-transport/src/error.rs
use std::fmt;

/// Crate-wide error type. Kept as one flat enum (mirroring
/// `space_chat_core::storage::StorageError`'s shape) rather than a
/// per-module error type per module, since almost every fallible operation
/// in this crate ultimately bottoms out in one of these four causes.
#[derive(Debug)]
pub enum TransportError {
    /// The underlying OS/network layer failed (bind, socket, connect).
    Io(String),
    /// A CBOR encode/decode of one of this crate's own wire types failed.
    Codec(String),
    /// `iroh` itself rejected or dropped a connection/stream.
    Connection(String),
    /// A requested resource (an attachment by hash, a stream for a
    /// category) isn't available from the peer asked.
    NotFound,
    /// An operation exceeded its deadline (e.g. waiting for a peer's
    /// control-stream response).
    Timeout,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Io(msg) => write!(f, "transport io error: {msg}"),
            TransportError::Codec(msg) => write!(f, "transport codec error: {msg}"),
            TransportError::Connection(msg) => write!(f, "transport connection error: {msg}"),
            TransportError::NotFound => write!(f, "not found"),
            TransportError::Timeout => write!(f, "timed out"),
        }
    }
}

impl std::error::Error for TransportError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_non_empty_and_distinct() {
        let variants = [
            TransportError::Io("x".to_string()),
            TransportError::Codec("x".to_string()),
            TransportError::Connection("x".to_string()),
            TransportError::NotFound,
            TransportError::Timeout,
        ];
        let messages: Vec<String> = variants.iter().map(|e| e.to_string()).collect();
        let mut unique = messages.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), messages.len(), "each variant should render distinctly");
    }
}
```

- [ ] **Step 3: Run test to verify it passes (no red/green cycle needed — this is a plain data type)**

Run: `cd space-chat-transport && cargo test display_messages_are_non_empty_and_distinct`
Expected: PASS

- [ ] **Step 4: Write the failing test for `TransportIdentity`**

```rust
// space-chat-transport/src/identity.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_identities_have_distinct_endpoint_ids() {
        let a = TransportIdentity::generate();
        let b = TransportIdentity::generate();
        assert_ne!(a.endpoint_id(), b.endpoint_id());
    }

    #[test]
    fn identity_round_trips_through_bytes() {
        let original = TransportIdentity::generate();
        let bytes = original.to_bytes();
        let restored = TransportIdentity::from_bytes(&bytes);
        assert_eq!(original.endpoint_id(), restored.endpoint_id());
    }
}
```

- [ ] **Step 5: Run test to verify it fails**

Run: `cd space-chat-transport && cargo test --lib identity`
Expected: FAIL — `TransportIdentity` not defined.

- [ ] **Step 6: Implement `TransportIdentity`**

```rust
// space-chat-transport/src/identity.rs (above the tests module)
/// This device's transport-layer identity: an `iroh` endpoint keypair.
///
/// Deliberately a separate keypair from the MLS device signing key (see the
/// transport spec's Identity section) — different crypto hygiene domains,
/// even though both represent "this device." This type has no idea an MLS
/// key exists; a future composition root (`space-chat-app`) is expected to
/// persist both keys together as one on-disk device identity, but that
/// pairing decision doesn't belong here.
pub struct TransportIdentity {
    secret_key: iroh::SecretKey,
}

impl TransportIdentity {
    /// Generates a fresh random identity via `iroh::SecretKey::generate()`,
    /// which uses the OS RNG internally — not derived from, or related to,
    /// any MLS key material.
    pub fn generate() -> Self {
        Self {
            secret_key: iroh::SecretKey::generate(),
        }
    }

    /// Restores a previously-generated identity from its raw 32-byte
    /// secret. Persistence itself (where these bytes live on disk) is out
    /// of scope here — a storage-layer or app-shell concern.
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self {
            secret_key: iroh::SecretKey::from_bytes(bytes),
        }
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        self.secret_key.to_bytes()
    }

    /// This device's iroh endpoint ID — the public key other devices dial
    /// to reach it, and the value encoded into invite links (Task 3).
    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.secret_key.public()
    }

    pub(crate) fn secret_key(&self) -> iroh::SecretKey {
        self.secret_key.clone()
    }
}
```

- [ ] **Step 7: Add the module to `lib.rs` and run tests**

```rust
// space-chat-transport/src/lib.rs
pub mod error;
pub mod identity;
```

Run: `cd space-chat-transport && cargo test --lib identity`
Expected: PASS (both tests)

- [ ] **Step 8: Commit**

```bash
git add space-chat-transport Cargo.toml Cargo.lock
git commit -m "feat(transport): add crate scaffold, TransportError, TransportIdentity"
```

---

### Task 2: Wire framing — `Category`, length-prefixed frames, the per-stream `Envelope` header

**Files:**
- Create: `space-chat-transport/src/envelope.rs`
- Create: `space-chat-transport/src/framing.rs`
- Modify: `space-chat-transport/src/lib.rs` — add `pub mod envelope;` and `pub mod framing;`
- Test: both files (inline)

**Interfaces:**
- Consumes: `TransportError` (Task 1).
- Produces: `Category` (enum: `MlsControl`, `AutomergeSync`, `Gossip`, `Ephemeral`, `AttachmentTransfer`, matching the protocol spec's five message categories exactly) with `fn stream_priority(self) -> i32`; `Envelope { space_id: String, category: Category }` with `fn encode(&self) -> Result<Vec<u8>, TransportError>` / `fn decode(bytes: &[u8]) -> Result<Self, TransportError>`; `async fn write_frame<W>(w: &mut W, bytes: &[u8]) -> Result<(), TransportError>` / `async fn read_frame<R>(r: &mut R) -> Result<Vec<u8>, TransportError>` generic over `tokio::io::AsyncWrite`/`AsyncRead`. Every later task that puts bytes on a QUIC stream uses `write_frame`/`read_frame`; every task that opens a per-`(space_id, category)` stream sends one `Envelope::encode()`'d frame first.

- [ ] **Step 1: Write the failing tests for `Category`/`Envelope`**

```rust
// space-chat-transport/src/envelope.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_transfer_has_the_lowest_priority() {
        let all = [
            Category::MlsControl,
            Category::AutomergeSync,
            Category::Gossip,
            Category::Ephemeral,
            Category::AttachmentTransfer,
        ];
        let attachment_priority = Category::AttachmentTransfer.stream_priority();
        for category in all {
            if category != Category::AttachmentTransfer {
                assert!(
                    category.stream_priority() > attachment_priority,
                    "{category:?} should outrank AttachmentTransfer per the transport spec's \
                     stream prioritization section"
                );
            }
        }
    }

    #[test]
    fn envelope_round_trips_through_cbor() {
        let envelope = Envelope {
            space_id: "space-1".to_string(),
            category: Category::Gossip,
        };
        let bytes = envelope.encode().unwrap();
        let decoded = Envelope::decode(&bytes).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn envelope_decode_rejects_garbage() {
        assert!(Envelope::decode(b"not cbor").is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd space-chat-transport && cargo test --lib envelope`
Expected: FAIL — `Category`, `Envelope` not defined.

- [ ] **Step 3: Implement `Category` and `Envelope`**

```rust
// space-chat-transport/src/envelope.rs (above the tests module)
use crate::error::TransportError;
use serde::{Deserialize, Serialize};

/// The protocol spec's five wire message categories, mapped one-to-one
/// onto a per-`(space_id, category)` QUIC stream (see this plan's
/// Architecture note on stream-open headers vs. per-message tagging).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Category {
    MlsControl,
    AutomergeSync,
    Gossip,
    Ephemeral,
    AttachmentTransfer,
}

impl Category {
    /// Relative QUIC send-stream priority, per the transport spec's stream
    /// prioritization section: control/sync/gossip ranked above bulk
    /// attachment transfer, consistently across every active space. Exact
    /// weights are a tuning parameter (the spec's own Open questions say
    /// so explicitly) — the only hard requirement encoded here is that
    /// `AttachmentTransfer` is strictly lowest.
    pub fn stream_priority(self) -> i32 {
        match self {
            Category::MlsControl => 30,
            Category::AutomergeSync => 20,
            Category::Gossip => 20,
            Category::Ephemeral => 10,
            Category::AttachmentTransfer => 0,
        }
    }
}

/// Sent once, as the first frame on a newly-opened per-`(space_id,
/// category)` stream, so the accepting side (which only sees "a new
/// bidirectional stream arrived" from `iroh`, with no application metadata
/// attached by QUIC itself) learns which space and category it's for.
/// **Not** repeated on every subsequent frame on that stream — once the
/// stream exists, every frame on it is already unambiguously scoped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub space_id: String,
    pub category: Category,
}

impl Envelope {
    pub fn encode(&self) -> Result<Vec<u8>, TransportError> {
        let mut buf = Vec::new();
        ciborium::into_writer(self, &mut buf)
            .map_err(|e| TransportError::Codec(e.to_string()))?;
        Ok(buf)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, TransportError> {
        ciborium::from_reader(bytes).map_err(|e| TransportError::Codec(e.to_string()))
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd space-chat-transport && cargo test --lib envelope`
Expected: PASS (3 tests)

- [ ] **Step 5: Write the failing tests for frame read/write**

```rust
// space-chat-transport/src/framing.rs
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn a_written_frame_reads_back_identical() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"hello frame").await.unwrap();

        let mut cursor = Cursor::new(buf);
        let read_back = read_frame(&mut cursor).await.unwrap();
        assert_eq!(read_back, b"hello frame".to_vec());
    }

    #[tokio::test]
    async fn read_frame_rejects_a_length_prefix_over_the_max_frame_size() {
        // A hand-crafted length prefix claiming an absurd frame size --
        // must be rejected before attempting to allocate/read that many
        // bytes, since this could be attacker-supplied.
        let mut buf: Vec<u8> = (u32::MAX).to_be_bytes().to_vec();
        let mut cursor = Cursor::new(buf.split_off(0));
        let result = read_frame(&mut cursor).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_frame_returns_err_on_truncated_input() {
        // A length prefix promising 100 bytes, but the stream ends after 3.
        let mut buf = 100u32.to_be_bytes().to_vec();
        buf.extend_from_slice(b"abc");
        let mut cursor = Cursor::new(buf);
        assert!(read_frame(&mut cursor).await.is_err());
    }
}
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `cd space-chat-transport && cargo test --lib framing`
Expected: FAIL — `write_frame`/`read_frame` not defined.

- [ ] **Step 7: Implement `write_frame`/`read_frame`**

```rust
// space-chat-transport/src/framing.rs (above the tests module)
use crate::error::TransportError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Frames larger than this are rejected outright rather than allocated —
/// a defensive bound against a corrupt or adversarial length prefix, since
/// every frame on every category (including attachment chunks, which are
/// deliberately sized well under this) is bounded far below it in practice.
const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// Writes `bytes` as one length-prefixed frame: a 4-byte big-endian length
/// followed by the bytes themselves. Generic over `AsyncWrite` so it works
/// identically against `iroh::endpoint::SendStream` (verify at
/// implementation time that `SendStream` implements `tokio::io::AsyncWrite`
/// directly, per this plan's Global Constraints caveat — if it instead
/// exposes only its own `write`/`write_all` methods, adapt this function
/// to call those instead of going through the `AsyncWrite` trait) and
/// plain in-memory buffers, as the tests above exercise.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    bytes: &[u8],
) -> Result<(), TransportError> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| TransportError::Codec("frame too large to encode a length prefix".to_string()))?;
    w.write_all(&len.to_be_bytes())
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    w.write_all(bytes)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    Ok(())
}

/// Reads back one frame written by `write_frame`. Returns
/// `Err(TransportError::Codec(_))` if the length prefix exceeds
/// `MAX_FRAME_LEN`, and `Err(TransportError::Io(_))` if the stream ends
/// before the declared length is fully read — both cases a malformed or
/// adversarial peer could trigger, so neither may panic.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Vec<u8>, TransportError> {
    let mut len_bytes = [0u8; 4];
    r.read_exact(&mut len_bytes)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    let len = u32::from_be_bytes(len_bytes);
    if len > MAX_FRAME_LEN {
        return Err(TransportError::Codec(format!(
            "frame length {len} exceeds MAX_FRAME_LEN {MAX_FRAME_LEN}"
        )));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    Ok(buf)
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `cd space-chat-transport && cargo test --lib framing`
Expected: PASS (3 tests)

- [ ] **Step 9: Add both modules to `lib.rs`**

```rust
// space-chat-transport/src/lib.rs
pub mod envelope;
pub mod error;
pub mod framing;
pub mod identity;
```

- [ ] **Step 10: Commit**

```bash
git add space-chat-transport/src/envelope.rs space-chat-transport/src/framing.rs space-chat-transport/src/lib.rs
git commit -m "feat(transport): add Category/Envelope wire types and length-prefixed framing"
```

---

### Task 3: Invite/pairing link encoding

**Files:**
- Create: `space-chat-transport/src/invite.rs`
- Modify: `space-chat-transport/src/lib.rs` — add `pub mod invite;`
- Test: `space-chat-transport/src/invite.rs` (inline)

**Interfaces:**
- Consumes: `TransportError` (Task 1).
- Produces: `Invite { endpoint_id: [u8; 32], space_id: String, join_token: String }` with `fn encode_link(&self) -> String`, `fn decode_link(link: &str) -> Result<Invite, TransportError>`. Task 10's `Transport::generate_invite`/`join_via_invite` consume this directly. `join_token` is an opaque, caller-supplied string (e.g. a short-lived random nonce) this crate never interprets beyond carrying it — validating it (expiry, single-use) is the future MLS-integration/app-shell layer's job.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-transport/src/invite.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invite_round_trips_through_its_link_encoding() {
        let invite = Invite {
            endpoint_id: [7u8; 32],
            space_id: "space-1".to_string(),
            join_token: "nonce-abc123".to_string(),
        };
        let link = invite.encode_link();
        assert!(link.starts_with("spacechat://join/"));

        let decoded = Invite::decode_link(&link).unwrap();
        assert_eq!(decoded, invite);
    }

    #[test]
    fn decode_link_rejects_a_missing_scheme() {
        assert!(Invite::decode_link("not-a-link-at-all").is_err());
    }

    #[test]
    fn decode_link_rejects_malformed_hex_after_the_scheme() {
        assert!(Invite::decode_link("spacechat://join/not-hex-zz").is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd space-chat-transport && cargo test --lib invite`
Expected: FAIL — `Invite` not defined.

- [ ] **Step 3: Implement `Invite`**

```rust
// space-chat-transport/src/invite.rs (above the tests module)
use crate::error::TransportError;
use serde::{Deserialize, Serialize};

const INVITE_SCHEME_PREFIX: &str = "spacechat://join/";

/// Everything a link or QR code needs to encode for the pairing flow: the
/// inviting device's iroh endpoint ID and enough context to request joining
/// a specific space. Per this plan's Global Constraints, turning
/// `encode_link`'s output into an actual scannable QR image is a UI-layer
/// concern (Milestone 4) — this type only produces/parses the string a QR
/// code would encode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Invite {
    pub endpoint_id: [u8; 32],
    pub space_id: String,
    /// Opaque, caller-supplied token (e.g. a short-lived random nonce).
    /// This crate never interprets it — expiry/single-use validation is a
    /// future layer's job (see this task's Interfaces note).
    pub join_token: String,
}

impl Invite {
    /// Hex-encodes a CBOR-serialized `Invite` behind a `spacechat://join/`
    /// scheme, matching the transport spec's "link or QR code encoding its
    /// iroh endpoint ID and enough context" description. Hex (not
    /// base32/base64) to avoid a new dependency, consistent with how
    /// `space_chat_core::segment::objid_to_target_string` already encodes
    /// binary data for this codebase.
    pub fn encode_link(&self) -> String {
        let mut cbor = Vec::new();
        ciborium::into_writer(self, &mut cbor).expect("Invite always CBOR-encodes");
        let hex: String = cbor.iter().map(|b| format!("{b:02x}")).collect();
        format!("{INVITE_SCHEME_PREFIX}{hex}")
    }

    pub fn decode_link(link: &str) -> Result<Self, TransportError> {
        let hex = link
            .strip_prefix(INVITE_SCHEME_PREFIX)
            .ok_or_else(|| TransportError::Codec(format!("missing {INVITE_SCHEME_PREFIX} scheme")))?;
        if !hex.len().is_multiple_of(2) {
            return Err(TransportError::Codec("odd-length hex".to_string()));
        }
        let cbor = (0..hex.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&hex[i..i + 2], 16)
                    .map_err(|e| TransportError::Codec(e.to_string()))
            })
            .collect::<Result<Vec<u8>, TransportError>>()?;
        ciborium::from_reader(cbor.as_slice()).map_err(|e| TransportError::Codec(e.to_string()))
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd space-chat-transport && cargo test --lib invite`
Expected: PASS (3 tests)

- [ ] **Step 5: Add the module to `lib.rs`**

```rust
// space-chat-transport/src/lib.rs
pub mod envelope;
pub mod error;
pub mod framing;
pub mod identity;
pub mod invite;
```

- [ ] **Step 6: Commit**

```bash
git add space-chat-transport/src/invite.rs space-chat-transport/src/lib.rs
git commit -m "feat(transport): add Invite link encoding for pairing/discovery"
```

---

### Task 4: Real `iroh::Endpoint` bootstrap over a local test relay

**Files:**
- Create: `space-chat-transport/src/bootstrap.rs`
- Modify: `space-chat-transport/src/lib.rs` — add `pub mod bootstrap;`
- Test: `space-chat-transport/src/bootstrap.rs` (inline, real `iroh` network calls — first task in this plan that touches the network for real)

**Interfaces:**
- Consumes: `TransportIdentity` (Task 1), `TransportError` (Task 1).
- Produces: `pub const ALPN: &[u8]`, `TransportConfig { relay: Option<(iroh::RelayMap, iroh::RelayUrl)> }`, `async fn bind_endpoint(identity: &TransportIdentity, config: TransportConfig) -> Result<iroh::Endpoint, TransportError>`. Task 6 onward call this directly to construct the `iroh::Endpoint` a `Transport` wraps; every test from here on that needs two peers uses `iroh::test_utils::run_relay_server()`'s `(RelayMap, RelayUrl)` to build a `TransportConfig`.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-transport/src/bootstrap.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::TransportIdentity;

    #[tokio::test]
    async fn two_endpoints_over_a_local_relay_can_connect_and_exchange_bytes() {
        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server()
            .await
            .expect("local test relay should start");

        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();

        let alice = bind_endpoint(
            &alice_identity,
            TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
        )
        .await
        .unwrap();
        let bob = bind_endpoint(
            &bob_identity,
            TransportConfig { relay: Some((relay_map, relay_url)) },
        )
        .await
        .unwrap();

        let bob_id = bob.id();
        let bob_addr = bob.addr();

        // Spawn bob's accept loop before alice dials, so the connection has
        // somewhere to land.
        let bob_task = tokio::spawn(async move {
            let incoming = bob.accept().await.expect("bob should see an incoming connection");
            let conn = incoming.await.expect("incoming connection should complete the handshake");
            assert_eq!(conn.remote_id().expect("remote id should be known post-handshake"), alice.id());
            let (mut send, mut recv) = conn.accept_bi().await.expect("bob should accept alice's stream");
            let mut buf = [0u8; 5];
            tokio::io::AsyncReadExt::read_exact(&mut recv, &mut buf).await.unwrap();
            assert_eq!(&buf, b"hello");
            tokio::io::AsyncWriteExt::write_all(&mut send, b"world").await.unwrap();
        });

        let conn = alice.connect(bob_addr, ALPN).await.expect("alice should connect to bob");
        assert_eq!(conn.remote_id().expect("remote id should be known post-handshake"), bob_id);
        let (mut send, mut recv) = conn.open_bi().await.expect("alice should open a stream");
        tokio::io::AsyncWriteExt::write_all(&mut send, b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        tokio::io::AsyncReadExt::read_exact(&mut recv, &mut buf).await.unwrap();
        assert_eq!(&buf, b"world");

        bob_task.await.unwrap();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-transport && cargo test --lib bootstrap`
Expected: FAIL — `bind_endpoint`, `TransportConfig`, `ALPN` not defined.

- [ ] **Step 3: Implement `bind_endpoint`**

```rust
// space-chat-transport/src/bootstrap.rs (above the tests module)
use crate::error::TransportError;
use crate::identity::TransportIdentity;

/// The single ALPN this crate's connections negotiate. space-chat has one
/// wire protocol (this crate's own), not several, so one fixed ALPN
/// suffices — no `iroh::protocol::Router`-style multi-protocol dispatch is
/// needed on top of it.
pub const ALPN: &[u8] = b"space-chat/1";

/// `relay: None` uses `iroh`'s production preset (n0's public relay
/// network, per this plan's Global Constraints); `relay: Some((map, url))`
/// overrides it with a specific relay — every test in this plan supplies
/// `iroh::test_utils::run_relay_server()`'s `(RelayMap, RelayUrl)` here so
/// tests never touch production infrastructure.
pub struct TransportConfig {
    pub relay: Option<(iroh::RelayMap, iroh::RelayUrl)>,
}

/// Binds a real `iroh::Endpoint` under `identity`'s keypair, ready to
/// `connect`/`accept` on `ALPN`.
///
/// Caveat (see this plan's Global Constraints): `iroh` 1.0's builder is
/// preset-based (`Endpoint::builder(presets::N0)`), not the argument-less
/// `Endpoint::builder()` older `iroh-net` versions used — verify
/// `presets::N0` (or whichever preset module path the pinned version
/// actually exposes) and `RelayMode::Custom`'s exact argument shape against
/// docs.rs before implementing this step for real.
pub async fn bind_endpoint(
    identity: &TransportIdentity,
    config: TransportConfig,
) -> Result<iroh::Endpoint, TransportError> {
    let mut builder = iroh::Endpoint::builder(iroh::presets::N0)
        .secret_key(identity.secret_key())
        .alpns(vec![ALPN.to_vec()]);

    if let Some((relay_map, _relay_url)) = config.relay {
        builder = builder.relay_mode(iroh::RelayMode::Custom(relay_map));
    }

    builder
        .bind()
        .await
        .map_err(|e| TransportError::Io(e.to_string()))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd space-chat-transport && cargo test --lib bootstrap -- --test-threads=1`
Expected: PASS. (`--test-threads=1` here and in every subsequent real-network test in this plan: each test binds real UDP sockets and starts a real relay server; serializing avoids port-exhaustion flakiness on a busy CI box, not a correctness requirement.)

- [ ] **Step 5: Add the module to `lib.rs`**

```rust
// space-chat-transport/src/lib.rs
pub mod bootstrap;
pub mod envelope;
pub mod error;
pub mod framing;
pub mod identity;
pub mod invite;
```

- [ ] **Step 6: Commit**

```bash
git add space-chat-transport/src/bootstrap.rs space-chat-transport/src/lib.rs
git commit -m "feat(transport): bind real iroh::Endpoint and prove dial/accept over a local relay"
```

---

### Task 5: Control stream + per-space digest exchange

**Files:**
- Create: `space-chat-transport/src/control.rs`
- Modify: `space-chat-transport/src/lib.rs` — add `pub mod control;`
- Test: `space-chat-transport/src/control.rs` (inline, real `iroh` connections)

**Interfaces:**
- Consumes: `bind_endpoint`, `TransportConfig`, `ALPN` (Task 4), `TransportError` (Task 1), `write_frame`/`read_frame` (Task 2).
- Produces: `SpaceDigest { space_id: String, epoch: u64, heads: Vec<[u8; 32]> }`, `ControlHello { digests: Vec<SpaceDigest> }` (CBOR encode/decode), `async fn exchange_digests(conn: &iroh::endpoint::Connection, is_dialer: bool, local: ControlHello) -> Result<ControlHello, TransportError>`. Task 7's `Transport` calls `exchange_digests` immediately after a connection is established, before opening any per-`(space_id, category)` stream — this is the "bootstrapping order" the transport spec requires.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-transport/src/control.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{bind_endpoint, TransportConfig, ALPN};
    use crate::identity::TransportIdentity;

    #[tokio::test]
    async fn both_sides_learn_the_others_digests_over_the_control_stream() {
        let (relay_map, relay_url, _relay_server) =
            iroh::test_utils::run_relay_server().await.unwrap();

        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let alice = bind_endpoint(&alice_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let bob = bind_endpoint(&bob_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();
        let bob_addr = bob.addr();

        let bob_digests = ControlHello {
            digests: vec![SpaceDigest { space_id: "space-1".to_string(), epoch: 0, heads: vec![[9u8; 32]] }],
        };
        let bob_digests_clone = bob_digests.clone();
        let bob_task = tokio::spawn(async move {
            let incoming = bob.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            let from_alice = exchange_digests(&conn, false, bob_digests_clone).await.unwrap();
            from_alice
        });

        let alice_digests = ControlHello {
            digests: vec![SpaceDigest { space_id: "space-1".to_string(), epoch: 2, heads: vec![[1u8; 32], [2u8; 32]] }],
        };
        let conn = alice.connect(bob_addr, ALPN).await.unwrap();
        let from_bob = exchange_digests(&conn, true, alice_digests.clone()).await.unwrap();
        assert_eq!(from_bob, bob_digests);

        let from_alice = bob_task.await.unwrap();
        assert_eq!(from_alice, alice_digests);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-transport && cargo test --lib control -- --test-threads=1`
Expected: FAIL — `SpaceDigest`, `ControlHello`, `exchange_digests` not defined.

- [ ] **Step 3: Implement the digest types and exchange**

```rust
// space-chat-transport/src/control.rs (above the tests module)
use crate::error::TransportError;
use crate::framing::{read_frame, write_frame};
use serde::{Deserialize, Serialize};

/// One space's sync-relevant state, per the protocol spec's sync flow step
/// 2 ("a lightweight per-space digest: epoch number + Automerge doc
/// heads"). `heads` are `automerge::ChangeHash` bytes — kept as raw
/// `[u8; 32]` here rather than depending on `automerge` directly, since
/// this crate only ever compares/forwards them, never interprets them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpaceDigest {
    pub space_id: String,
    pub epoch: u64,
    pub heads: Vec<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlHello {
    pub digests: Vec<SpaceDigest>,
}

/// Runs the connection-level control-stream handshake: the dialing side
/// opens the stream (per the transport spec's "one connection-level
/// control stream, opened first, before any per-space stream sets exist");
/// the accepting side accepts it. Both sides then exchange their
/// `ControlHello` and learn the other's. This is the only network activity
/// that happens before either side knows which shared spaces are even
/// worth opening per-`(space_id, category)` streams for.
pub async fn exchange_digests(
    conn: &iroh::endpoint::Connection,
    is_dialer: bool,
    local: ControlHello,
) -> Result<ControlHello, TransportError> {
    let local_bytes = {
        let mut buf = Vec::new();
        ciborium::into_writer(&local, &mut buf).map_err(|e| TransportError::Codec(e.to_string()))?;
        buf
    };

    let (mut send, mut recv) = if is_dialer {
        conn.open_bi()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?
    } else {
        conn.accept_bi()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?
    };

    // Both sides write, then both sides read -- a fixed, symmetric order
    // avoids a dialer/accepter-specific deadlock (each side blocking on a
    // read the other hasn't sent yet).
    write_frame(&mut send, &local_bytes).await?;
    let remote_bytes = read_frame(&mut recv).await?;

    ciborium::from_reader(remote_bytes.as_slice()).map_err(|e| TransportError::Codec(e.to_string()))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd space-chat-transport && cargo test --lib control -- --test-threads=1`
Expected: PASS

- [ ] **Step 5: Add the module to `lib.rs`**

```rust
// space-chat-transport/src/lib.rs
pub mod bootstrap;
pub mod control;
pub mod envelope;
pub mod error;
pub mod framing;
pub mod identity;
pub mod invite;
```

- [ ] **Step 6: Commit**

```bash
git add space-chat-transport/src/control.rs space-chat-transport/src/lib.rs
git commit -m "feat(transport): add connection-level control stream and per-space digest exchange"
```

---

### Task 6: Per-`(space_id, category)` stream manager, eager open + prioritization

> **⚠️ Amendment (post-implementation):** this task's title and doc comment
> below originally described streams as opened lazily — "only once a space
> becomes active between two peers, not eagerly for every shared space
> regardless of activity." That was never what shipped. `StreamManager`
> itself owns no opening policy at all; its only caller,
> `Transport::run_connection` (Task 7), opens an `AutomergeSync` stream
> **eagerly** for **every** space present in both peers' registries at a
> matching epoch, all at once, immediately after the handshake, regardless of
> activity. Idle per-space streams are also never closed, despite the
> transport spec calling for it. See
> [Post-implementation amendments](#post-implementation-amendments) (L4);
> `space-chat-transport/src/streams.rs` is authoritative.

**Files:**
- Create: `space-chat-transport/src/streams.rs`
- Modify: `space-chat-transport/src/lib.rs` — add `pub mod streams;`
- Test: `space-chat-transport/src/streams.rs` (inline, real `iroh` connections)

**Interfaces:**
- Consumes: `Envelope`, `Category` (Task 2), `write_frame`/`read_frame` (Task 2), `bind_endpoint`/`TransportConfig`/`ALPN` (Task 4), `TransportError` (Task 1).
- Produces: `StreamManager::new(conn: iroh::endpoint::Connection) -> Self`, `async fn open(&self, space_id: &str, category: Category) -> Result<StreamHandle, TransportError>` (opens a fresh stream, sends the `Envelope` header, sets `SendStream` priority per `Category::stream_priority`), `async fn accept_next(&self) -> Result<(Envelope, StreamHandle), TransportError>` (accepts the next incoming stream and reads its header), `StreamHandle { send: iroh::endpoint::SendStream, recv: iroh::endpoint::RecvStream }`. Task 7's sync loop and Task 9's attachment transfer both open/accept streams exclusively through this API, never through `iroh::endpoint::Connection` directly.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-transport/src/streams.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{bind_endpoint, TransportConfig, ALPN};
    use crate::identity::TransportIdentity;

    #[tokio::test]
    async fn opening_a_stream_sends_the_header_and_sets_priority() {
        let (relay_map, relay_url, _relay_server) =
            iroh::test_utils::run_relay_server().await.unwrap();
        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let alice = bind_endpoint(&alice_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let bob = bind_endpoint(&bob_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();
        let bob_addr = bob.addr();

        let bob_task = tokio::spawn(async move {
            let incoming = bob.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            let manager = StreamManager::new(conn);
            let (envelope, mut handle) = manager.accept_next().await.unwrap();
            assert_eq!(envelope.space_id, "space-1");
            assert_eq!(envelope.category, Category::AttachmentTransfer);
            let body = crate::framing::read_frame(&mut handle.recv).await.unwrap();
            assert_eq!(body, b"chunk-bytes".to_vec());
        });

        let conn = alice.connect(bob_addr, ALPN).await.unwrap();
        let manager = StreamManager::new(conn);
        let mut handle = manager.open("space-1", Category::AttachmentTransfer).await.unwrap();
        assert_eq!(
            handle.send.priority().expect("priority should be readable back"),
            Category::AttachmentTransfer.stream_priority(),
        );
        crate::framing::write_frame(&mut handle.send, b"chunk-bytes").await.unwrap();

        bob_task.await.unwrap();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-transport && cargo test --lib streams -- --test-threads=1`
Expected: FAIL — `StreamManager`, `StreamHandle` not defined.

- [ ] **Step 3: Implement `StreamManager`**

```rust
// space-chat-transport/src/streams.rs (above the tests module)
use crate::envelope::{Category, Envelope};
use crate::error::TransportError;
use crate::framing::{read_frame, write_frame};

pub struct StreamHandle {
    pub send: iroh::endpoint::SendStream,
    pub recv: iroh::endpoint::RecvStream,
}

/// Opens/accepts per-`(space_id, category)` streams on one already-
/// established `iroh::endpoint::Connection`. This type owns no notion of
/// *which* spaces are active — it just opens whatever it is told to; that
/// policy decision belongs to `Transport` (Task 7), which in the shipped
/// code calls `open` **eagerly** for **every** space present in both
/// peers' registries at a matching epoch, immediately after the handshake,
/// regardless of activity (not lazily/only-on-divergence, as an earlier
/// draft of this comment claimed).
pub struct StreamManager {
    conn: iroh::endpoint::Connection,
}

impl StreamManager {
    pub fn new(conn: iroh::endpoint::Connection) -> Self {
        Self { conn }
    }

    /// Opens a fresh bidirectional stream for `(space_id, category)`,
    /// writes the `Envelope` header identifying it, and sets its QUIC send
    /// priority per `Category::stream_priority` — this is the mechanism
    /// behind the transport spec's stream-prioritization requirement.
    pub async fn open(&self, space_id: &str, category: Category) -> Result<StreamHandle, TransportError> {
        let (mut send, recv) = self
            .conn
            .open_bi()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        send.set_priority(category.stream_priority())
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        let header = Envelope { space_id: space_id.to_string(), category }.encode()?;
        write_frame(&mut send, &header).await?;

        Ok(StreamHandle { send, recv })
    }

    /// Accepts the next incoming bidirectional stream and reads its header
    /// frame, telling the caller which `(space_id, category)` it's for.
    /// `Transport` (Task 7) runs this in a loop per connection, dispatching
    /// each accepted stream to the right handler by the `Envelope` it
    /// returns.
    pub async fn accept_next(&self) -> Result<(Envelope, StreamHandle), TransportError> {
        let (send, mut recv) = self
            .conn
            .accept_bi()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;
        let header_bytes = read_frame(&mut recv).await?;
        let envelope = Envelope::decode(&header_bytes)?;
        Ok((envelope, StreamHandle { send, recv }))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd space-chat-transport && cargo test --lib streams -- --test-threads=1`
Expected: PASS

- [ ] **Step 5: Add the module to `lib.rs`**

```rust
// space-chat-transport/src/lib.rs
pub mod bootstrap;
pub mod control;
pub mod envelope;
pub mod error;
pub mod framing;
pub mod identity;
pub mod invite;
pub mod streams;
```

- [ ] **Step 6: Commit**

```bash
git add space-chat-transport/src/streams.rs space-chat-transport/src/lib.rs
git commit -m "feat(transport): add per-(space_id, category) StreamManager with lazy open and priority"
```

---

### Task 7: `Transport` — connection lifecycle + Automerge sync/gossip wired to a shared `Segment`

> **⚠️ Amendment (post-implementation):** the `run_connection` /
> `run_automerge_sync` code below is STALE and contains several bugs that were
> found and fixed during implementation. See
> [Post-implementation amendments](#post-implementation-amendments) for the
> list; read `space-chat-transport/src/transport.rs` for what actually ships.

**Files:**
- Create: `space-chat-transport/src/transport.rs`
- Modify: `space-chat-transport/Cargo.toml` — add `automerge = "0.5"` (matching `space-chat-core`'s pin) as a direct dependency: this task calls `automerge::sync::Message::encode`/`decode` and `automerge::ChangeHash` directly, since `Segment::generate_sync_message`/`receive_sync_message`'s public signatures already expose `automerge::sync::{State, Message}` types (see `space-chat-core/src/segment.rs`), so any caller driving them needs to name those types too.
- Modify: `space-chat-transport/src/lib.rs` — add `pub mod transport;`
- Test: `space-chat-transport/src/transport.rs` (inline, real `iroh` connections)

**Interfaces:**
- Consumes: `bind_endpoint`/`TransportConfig`/`ALPN` (Task 4), `exchange_digests`/`ControlHello`/`SpaceDigest` (Task 5), `StreamManager`/`StreamHandle` (Task 6), `Category`/`Envelope` (Task 2), `write_frame`/`read_frame` (Task 2), `TransportIdentity` (Task 1), `space_chat_core::segment::{Segment, sync_state}`, `space_chat_core::projection::SegmentChange` (Milestone 1).
- Produces: `TransportEvent` (enum: `Connected { endpoint_id: iroh::EndpointId }`, `Disconnected { endpoint_id: iroh::EndpointId }`, `IncomingChange(SegmentChange)` — more variants added in Tasks 9/10), `Transport` with `async fn bind(identity: &TransportIdentity, config: TransportConfig) -> Result<(Transport, tokio::sync::mpsc::UnboundedReceiver<TransportEvent>), TransportError>`, `fn endpoint_id(&self) -> iroh::EndpointId`, `fn endpoint_addr(&self) -> iroh::EndpointAddr`, `async fn add_space(&self, space_id: impl Into<String>, epoch: u64, segment: std::sync::Arc<tokio::sync::Mutex<Segment>>)`, `async fn dial(&self, addr: impl Into<iroh::EndpointAddr>) -> Result<(), TransportError>`, `async fn notify_local_change(&self, space_id: &str)`. Task 8 reuses this exact API for the multi-hop test; Tasks 9–11 extend `Transport`'s internals (adding fields/methods) but do not change any signature listed here.

- [ ] **Step 1: Add the `automerge` dependency**

```bash
cd space-chat-transport && cargo add automerge@0.5
```

- [ ] **Step 2: Write the failing test**

```rust
// space-chat-transport/src/transport.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::TransportConfig;
    use crate::identity::TransportIdentity;
    use space_chat_core::domain::{DeviceId, Message};
    use space_chat_core::segment::Segment;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn two_transports_converge_over_a_real_connection() {
        let (relay_map, relay_url, _relay_server) =
            iroh::test_utils::run_relay_server().await.unwrap();

        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let (alice, _alice_events) = Transport::bind(
            &alice_identity,
            TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
        )
        .await
        .unwrap();
        let (bob, mut bob_events) = Transport::bind(
            &bob_identity,
            TransportConfig { relay: Some((relay_map, relay_url)) },
        )
        .await
        .unwrap();

        let alice_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
        let bob_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
        alice.add_space("space-1", 0, alice_segment.clone()).await;
        bob.add_space("space-1", 0, bob_segment.clone()).await;

        alice_segment.lock().await.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "from alice".to_string(),
            attachments: vec![],
        });
        bob_segment.lock().await.append_message(&Message {
            sender: DeviceId([2u8; 32]),
            content: "from bob".to_string(),
            attachments: vec![],
        });

        alice.dial(bob.endpoint_addr()).await.unwrap();
        alice.notify_local_change("space-1").await;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let a = alice_segment.lock().await.message_count();
                let b = bob_segment.lock().await.message_count();
                if a == 2 && b == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("both sides should converge to 2 messages within the timeout");

        // Prove convergence was actually observed through the public event
        // stream, not only via the shared Segment mutating invisibly.
        let saw_incoming_change = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match bob_events.recv().await {
                    Some(TransportEvent::IncomingChange(_)) => return true,
                    Some(_) => continue,
                    None => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(saw_incoming_change, "bob should have observed at least one IncomingChange event");
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd space-chat-transport && cargo test --lib transport -- --test-threads=1`
Expected: FAIL — `Transport`, `TransportEvent` not defined.

- [ ] **Step 4: Implement `Transport`**

```rust
// space-chat-transport/src/transport.rs (above the tests module)
use crate::bootstrap::{bind_endpoint, TransportConfig, ALPN};
use crate::control::{exchange_digests, ControlHello, SpaceDigest};
use crate::envelope::Category;
use crate::error::TransportError;
use crate::framing::{read_frame, write_frame};
use crate::identity::TransportIdentity;
use crate::streams::StreamManager;
use space_chat_core::projection::SegmentChange;
use space_chat_core::segment::{sync_state, Segment};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};

/// Events a consumer (eventually `space-chat-app`, Milestone 4) observes.
/// More variants are added in Tasks 9 (attachment progress isn't
/// event-based in this plan — `request_attachment` is a direct
/// request/response call, see Task 9) and 10 (`JoinRequest`).
pub enum TransportEvent {
    Connected { endpoint_id: iroh::EndpointId },
    Disconnected { endpoint_id: iroh::EndpointId },
    /// New content merged into a locally-tracked `Segment` as a result of
    /// sync with some peer. Carries a full `SegmentChange` snapshot (same
    /// shape Milestone 1's `Segment::latest_change` and Milestone 2's
    /// `Projection::apply` already use), so a consumer can feed it directly
    /// into a `ListingIndex`/`SearchIndex`/persistence layer without this
    /// crate needing to know any of those exist.
    IncomingChange(SegmentChange),
}

struct SpaceEntry {
    epoch: u64,
    segment: Arc<Mutex<Segment>>,
    /// Wakes every active per-peer sync task for this space to run another
    /// round immediately, instead of waiting for its next periodic tick —
    /// the mechanism behind the protocol spec's "gossip carries new changes
    /// live." `Transport::notify_local_change` fires this.
    notify: Arc<Notify>,
}

/// Converts an `automerge::ChangeHash` to a plain `[u8; 32]` for this
/// crate's own CBOR-serializable `SpaceDigest.heads`. Caveat (see this
/// plan's Global Constraints): verify `ChangeHash`'s exact byte-access API
/// against docs.rs for the pinned `automerge` version — this assumes
/// `AsRef<[u8]>` (true of essentially every 32-byte hash newtype in the
/// Rust ecosystem, but not confirmed against this specific crate's docs at
/// plan-writing time).
fn change_hash_to_bytes(h: &automerge::ChangeHash) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(h.as_ref());
    bytes
}

pub struct Transport {
    endpoint: iroh::Endpoint,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    events_tx: mpsc::UnboundedSender<TransportEvent>,
}

impl Transport {
    pub async fn bind(
        identity: &TransportIdentity,
        config: TransportConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<TransportEvent>), TransportError> {
        let endpoint = bind_endpoint(identity, config).await?;
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let spaces: Arc<Mutex<HashMap<String, SpaceEntry>>> = Arc::new(Mutex::new(HashMap::new()));

        let transport = Self { endpoint: endpoint.clone(), spaces: spaces.clone(), events_tx: events_tx.clone() };

        // Background accept loop: every inbound connection gets its own
        // per-connection task, mirroring what `dial` (below) does for
        // outbound connections.
        tokio::spawn(async move {
            loop {
                let Some(incoming) = endpoint.accept().await else { break };
                let Ok(conn) = incoming.await else { continue };
                tokio::spawn(run_connection(conn, false, spaces.clone(), events_tx.clone()));
            }
        });

        Ok((transport, events_rx))
    }

    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.endpoint.id()
    }

    pub fn endpoint_addr(&self) -> iroh::EndpointAddr {
        self.endpoint.addr()
    }

    /// Registers `segment` as this device's current state for `space_id`.
    /// `Segment` doesn't expose its own `space_id`/`epoch` fields (see
    /// `space_chat_core::segment::Segment`), so the caller — which already
    /// knows both, having constructed or loaded this `Segment` — supplies
    /// them alongside it. Every connection this `Transport` has (existing
    /// or future) starts syncing `space_id` against whichever peers'
    /// control-stream digests (Task 5) also name it.
    pub async fn add_space(&self, space_id: impl Into<String>, epoch: u64, segment: Arc<Mutex<Segment>>) {
        let space_id = space_id.into();
        self.spaces.lock().await.insert(
            space_id,
            SpaceEntry { epoch, segment, notify: Arc::new(Notify::new()) },
        );
    }

    /// Dials `addr` and starts syncing every space currently registered via
    /// `add_space` against it, once the control-stream digest exchange
    /// (Task 5) determines which spaces the remote peer also knows about.
    pub async fn dial(&self, addr: impl Into<iroh::EndpointAddr>) -> Result<(), TransportError> {
        let conn = self
            .endpoint
            .connect(addr, ALPN)
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;
        tokio::spawn(run_connection(conn, true, self.spaces.clone(), self.events_tx.clone()));
        Ok(())
    }

    /// Wakes every active connection's sync task for `space_id` to run
    /// another round immediately. Call this right after mutating the
    /// `Segment` registered for `space_id` via `add_space` (e.g. right
    /// after `append_message`).
    pub async fn notify_local_change(&self, space_id: &str) {
        if let Some(entry) = self.spaces.lock().await.get(space_id) {
            entry.notify.notify_waiters();
        }
    }
}

async fn run_connection(
    conn: iroh::endpoint::Connection,
    is_dialer: bool,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    events: mpsc::UnboundedSender<TransportEvent>,
) {
    let Ok(remote_id) = conn.remote_id() else { return };
    let _ = events.send(TransportEvent::Connected { endpoint_id: remote_id });

    let local_digests = {
        let guard = spaces.lock().await;
        let mut digests = Vec::with_capacity(guard.len());
        for (space_id, entry) in guard.iter() {
            let heads = entry.segment.lock().await.heads().iter().map(change_hash_to_bytes).collect();
            digests.push(SpaceDigest { space_id: space_id.clone(), epoch: entry.epoch, heads });
        }
        digests
    };

    let Ok(remote_hello) = exchange_digests(&conn, is_dialer, ControlHello { digests: local_digests }).await else {
        let _ = events.send(TransportEvent::Disconnected { endpoint_id: remote_id });
        return;
    };
    let remote_space_ids: HashSet<String> = remote_hello.digests.into_iter().map(|d| d.space_id).collect();

    let manager = Arc::new(StreamManager::new(conn));

    // Deliberate scope simplification: this plan uses the single
    // `AutomergeSync` category stream for both catch-up reconciliation and
    // live push, since Automerge's own sync-message protocol already
    // naturally serves both continuously (see `run_automerge_sync` below).
    // The protocol spec's separate `Gossip` category is kept in `Category`
    // (Task 2) for wire compatibility with its five named categories and
    // to leave room for a cheaper "just the raw new-change bytes, skip
    // bloom-filter reconciliation" fast path later, but this milestone's
    // implementation never opens a `Gossip`-category stream.
    if is_dialer {
        let guard = spaces.lock().await;
        for (space_id, entry) in guard.iter() {
            if !remote_space_ids.contains(space_id) {
                continue;
            }
            if let Ok(handle) = manager.open(space_id, Category::AutomergeSync).await {
                tokio::spawn(run_automerge_sync(handle, entry.segment.clone(), entry.notify.clone(), events.clone()));
            }
        }
    } else {
        while let Ok((envelope, handle)) = manager.accept_next().await {
            if envelope.category != Category::AutomergeSync {
                // Other categories (MlsControl, AttachmentTransfer, ...)
                // are dispatched by Tasks 9/10's revisions of this loop.
                continue;
            }
            let guard = spaces.lock().await;
            if let Some(entry) = guard.get(&envelope.space_id) {
                tokio::spawn(run_automerge_sync(handle, entry.segment.clone(), entry.notify.clone(), events.clone()));
            }
        }
    }

    let _ = events.send(TransportEvent::Disconnected { endpoint_id: remote_id });
}

/// Runs one space's sync-message exchange against one peer, for the
/// lifetime of the underlying stream. Loops: send anything new the local
/// segment has, then wait for either (a) a frame from the peer, (b) a
/// local-change notification (Task's `notify_local_change`), or (c) a
/// short timeout, whichever comes first, and repeat. This single loop
/// implements both "catch up on reconnect" and "push live changes" — see
/// `run_connection`'s comment on why `Gossip` isn't a separate stream here.
async fn run_automerge_sync(
    mut handle: crate::streams::StreamHandle,
    segment: Arc<Mutex<Segment>>,
    notify: Arc<Notify>,
    events: mpsc::UnboundedSender<TransportEvent>,
) {
    let mut state = sync_state();
    let mut last_emitted_cursor = segment.lock().await.latest_change().cursor;

    loop {
        let outgoing = {
            let mut seg = segment.lock().await;
            seg.generate_sync_message(&mut state)
        };
        if let Some(msg) = outgoing {
            if write_frame(&mut handle.send, &msg.encode()).await.is_err() {
                return;
            }
        }

        tokio::select! {
            frame = read_frame(&mut handle.recv) => {
                let Ok(bytes) = frame else { return };
                let Ok(msg) = automerge::sync::Message::decode(&bytes) else { continue };
                let mut seg = segment.lock().await;
                if seg.receive_sync_message(&mut state, msg).is_ok() {
                    let change = seg.latest_change();
                    if change.cursor > last_emitted_cursor {
                        last_emitted_cursor = change.cursor;
                        let _ = events.send(TransportEvent::IncomingChange(change));
                    }
                }
            }
            _ = notify.notified() => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {}
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd space-chat-transport && cargo test --lib transport -- --test-threads=1`
Expected: PASS

- [ ] **Step 6: Add the module to `lib.rs`**

```rust
// space-chat-transport/src/lib.rs
pub mod bootstrap;
pub mod control;
pub mod envelope;
pub mod error;
pub mod framing;
pub mod identity;
pub mod invite;
pub mod streams;
pub mod transport;
```

- [ ] **Step 7: Commit**

```bash
git add space-chat-transport/src/transport.rs space-chat-transport/src/lib.rs space-chat-transport/Cargo.toml space-chat-transport/Cargo.lock
git commit -m "feat(transport): add Transport connection lifecycle and Automerge sync/gossip loop"
```

---

### Task 8: Multi-hop message convergence (three in-process peers)

**Files:**
- Create: `space-chat-transport/tests/multi_hop_convergence.rs`

**Interfaces:**
- Consumes: everything from Task 7 (`Transport`, `TransportEvent`), `TransportIdentity` (Task 1), `TransportConfig` (Task 4), `space_chat_core::{domain::{DeviceId, Message}, segment::Segment}` (Milestone 1).
- Produces: nothing new — this is a proof, not a building block.

- [ ] **Step 1: Write the test**

```rust
// space-chat-transport/tests/multi_hop_convergence.rs
use space_chat_core::domain::{DeviceId, Message};
use space_chat_core::segment::Segment;
use space_chat_transport::bootstrap::TransportConfig;
use space_chat_transport::identity::TransportIdentity;
use space_chat_transport::transport::Transport;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Proves the transport spec's "multi-hop convergence for messages" claim:
/// Carol receives Alice's message purely because Bob independently syncs
/// with both of them against a *shared* local segment (Task 7's central
/// design insight) — Carol never dials, and is never given the address
/// of, Alice. No relay/forwarding code exists anywhere for this to work;
/// this test is the proof that none is needed.
#[tokio::test]
async fn carol_receives_alices_message_via_bob_without_ever_connecting_to_alice() {
    let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

    let alice_identity = TransportIdentity::generate();
    let bob_identity = TransportIdentity::generate();
    let carol_identity = TransportIdentity::generate();

    let (alice, _alice_events) = Transport::bind(&alice_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
    let (bob, _bob_events) = Transport::bind(&bob_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
    let (carol, _carol_events) = Transport::bind(&carol_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();

    let alice_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
    let bob_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
    let carol_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
    alice.add_space("space-1", 0, alice_segment.clone()).await;
    bob.add_space("space-1", 0, bob_segment.clone()).await;
    carol.add_space("space-1", 0, carol_segment.clone()).await;

    // Topology: alice<->bob, bob<->carol. Alice and Carol never dial each
    // other, and Carol is never handed alice's EndpointAddr at all.
    alice.dial(bob.endpoint_addr()).await.unwrap();
    carol.dial(bob.endpoint_addr()).await.unwrap();

    alice_segment.lock().await.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "hello from alice, three hops of trust but zero hops of relay code".to_string(),
        attachments: vec![],
    });
    alice.notify_local_change("space-1").await;

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if carol_segment.lock().await.message_count() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("carol should converge on alice's message via bob within the timeout");
}
```

- [ ] **Step 2: Run the test**

Run: `cd space-chat-transport && cargo test --test multi_hop_convergence -- --test-threads=1`
Expected: PASS. Note this task adds no production code — if it fails, the bug is in Task 7's implementation, not something new to build here.

- [ ] **Step 3: Commit**

```bash
git add space-chat-transport/tests/multi_hop_convergence.rs
git commit -m "test(transport): prove multi-hop message convergence needs no relay code"
```

---

### Task 9: Chunked, direct-endpoint-only attachment transfer

> **⚠️ Amendment (post-implementation):** the attachment-transfer code below is
> STALE — it has no size cap, no space scoping, no read timeout, and relies on
> an accept loop that (as written in Task 7) only ran on one side of a
> connection. See [Post-implementation amendments](#post-implementation-amendments);
> `space-chat-transport/src/transport.rs` is authoritative.

**Files:**
- Modify: `space-chat-transport/src/transport.rs`
- Modify: `space-chat-transport/Cargo.toml` — add `sha2 = "0.10"` (dev-and-runtime dependency, used to verify received attachment bytes against their requested hash — illustrative of the protocol spec's "content hash" field; the real hash algorithm `AttachmentRef.hash` (Milestone 1's `space_chat_core::domain`) ultimately uses is unspecified by that type itself, which is algorithm-agnostic `[u8; 32]`)
- Test: `space-chat-transport/src/transport.rs` (inline, extends the existing tests module)

**Interfaces:**
- Consumes: everything from Task 7, `Category::AttachmentTransfer` (Task 2).
- Produces: `Transport::serve_attachment(&self, hash: [u8; 32], bytes: Vec<u8>)`, `async fn request_attachment(&self, space_id: &str, hash: [u8; 32], from: iroh::EndpointId) -> Result<Vec<u8>, TransportError>`. Task 8's topology is reused (not modified) by Task 8's own test file remaining unchanged; this task's new test lives alongside Task 7's in `transport.rs`.

- [ ] **Step 1: Add the `sha2` dependency**

```bash
cd space-chat-transport && cargo add sha2@0.10
```

- [ ] **Step 2: Write the failing test**

```rust
// space-chat-transport/src/transport.rs, inside the existing tests module
#[tokio::test]
async fn attachment_transfer_is_direct_endpoint_only_and_hash_verified() {
    use sha2::{Digest, Sha256};

    let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();
    let alice_identity = TransportIdentity::generate();
    let bob_identity = TransportIdentity::generate();
    let (alice, _alice_events) = Transport::bind(&alice_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
    let (bob, _bob_events) = Transport::bind(&bob_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();

    alice.add_space("space-1", 0, Arc::new(Mutex::new(Segment::new("space-1", 0)))).await;
    bob.add_space("space-1", 0, Arc::new(Mutex::new(Segment::new("space-1", 0)))).await;

    let content = b"a rather large attachment, in spirit if not in this test's actual byte count".to_vec();
    let mut hasher = Sha256::new();
    hasher.update(&content);
    let hash: [u8; 32] = hasher.finalize().into();
    alice.serve_attachment(hash, content.clone()).await;

    bob.dial(alice.endpoint_addr()).await.unwrap();
    // Give the connection's control-stream handshake a moment to land
    // before requesting -- request_attachment requires an already-tracked
    // connection (see this task's Interfaces note) and does not implicitly
    // dial or wait.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let fetched = bob
        .request_attachment("space-1", hash, alice.endpoint_id())
        .await
        .expect("bob should fetch the attachment directly from alice");
    assert_eq!(fetched, content);

    // Requesting a hash alice never served must fail, not hang or panic.
    let missing_hash = [0xffu8; 32];
    let result = bob.request_attachment("space-1", missing_hash, alice.endpoint_id()).await;
    assert!(result.is_err(), "requesting an unserved hash should return an error, not succeed");
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd space-chat-transport && cargo test --lib attachment_transfer -- --test-threads=1`
Expected: FAIL — `serve_attachment`, `request_attachment` not defined.

- [ ] **Step 4: Extend `Transport` with attachment serving/requesting**

```rust
// space-chat-transport/src/transport.rs -- add to the imports at the top
use std::convert::TryInto;

// Replace the `Transport` struct definition with this (adds `conns` and
// `attachments`):
pub struct Transport {
    endpoint: iroh::Endpoint,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    events_tx: mpsc::UnboundedSender<TransportEvent>,
}

const ATTACHMENT_CHUNK_SIZE: usize = 64 * 1024;

impl Transport {
    // `bind` gains two lines constructing `conns`/`attachments` and passes
    // them into `run_connection` -- shown here as the full replacement body:
    pub async fn bind(
        identity: &TransportIdentity,
        config: TransportConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<TransportEvent>), TransportError> {
        let endpoint = bind_endpoint(identity, config).await?;
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let spaces: Arc<Mutex<HashMap<String, SpaceEntry>>> = Arc::new(Mutex::new(HashMap::new()));
        let conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>> = Arc::new(Mutex::new(HashMap::new()));
        let attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));

        let transport = Self {
            endpoint: endpoint.clone(),
            spaces: spaces.clone(),
            conns: conns.clone(),
            attachments: attachments.clone(),
            events_tx: events_tx.clone(),
        };

        tokio::spawn(async move {
            loop {
                let Some(incoming) = endpoint.accept().await else { break };
                let Ok(conn) = incoming.await else { continue };
                tokio::spawn(run_connection(conn, false, spaces.clone(), conns.clone(), attachments.clone(), events_tx.clone()));
            }
        });

        Ok((transport, events_rx))
    }

    // `dial` gains `self.conns.clone()`/`self.attachments.clone()` in its
    // call to `run_connection` -- full replacement body:
    pub async fn dial(&self, addr: impl Into<iroh::EndpointAddr>) -> Result<(), TransportError> {
        let conn = self
            .endpoint
            .connect(addr, ALPN)
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;
        tokio::spawn(run_connection(
            conn,
            true,
            self.spaces.clone(),
            self.conns.clone(),
            self.attachments.clone(),
            self.events_tx.clone(),
        ));
        Ok(())
    }

    /// Registers `bytes` as this device's copy of the attachment content-
    /// addressed by `hash`, available to serve to any peer that requests it
    /// by hash. A real integration (Milestone 4) would back this with
    /// Milestone 2's `AttachmentBlobStore` rather than an in-memory map;
    /// this crate stays free of that dependency (per Global Constraints)
    /// and exposes the minimal surface a future integration plugs into.
    pub async fn serve_attachment(&self, hash: [u8; 32], bytes: Vec<u8>) {
        self.attachments.lock().await.insert(hash, bytes);
    }

    /// Requests attachment bytes directly from `from` — no relaying
    /// through any other connected peer is attempted, even if some other
    /// peer happens to also have a connection to `from`. This method
    /// deliberately does **not** implicitly `dial` — it looks up an
    /// already-tracked connection to `from` (populated by `run_connection`
    /// below) and fails with `TransportError::NotFound` if none exists,
    /// which is exactly what makes "attachments are strictly
    /// direct-endpoint, not multi-hop" a structural property of this API
    /// rather than an unexercised code path (see Task 8's multi-hop test,
    /// which never dials the peer it fetches no attachment from).
    pub async fn request_attachment(
        &self,
        space_id: &str,
        hash: [u8; 32],
        from: iroh::EndpointId,
    ) -> Result<Vec<u8>, TransportError> {
        let conn = self.conns.lock().await.get(&from).cloned().ok_or(TransportError::NotFound)?;
        let manager = StreamManager::new(conn);
        let mut handle = manager.open(space_id, Category::AttachmentTransfer).await?;
        write_frame(&mut handle.send, &hash).await?;

        let mut bytes = Vec::new();
        loop {
            let chunk = read_frame(&mut handle.recv).await?;
            if chunk.is_empty() {
                break;
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(TransportError::NotFound);
        }

        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, &bytes);
        let actual: [u8; 32] = sha2::Digest::finalize(hasher).into();
        if actual != hash {
            return Err(TransportError::Codec(
                "received attachment content did not match its requested hash".to_string(),
            ));
        }
        Ok(bytes)
    }
}
```

- [ ] **Step 5: Extend `run_connection` to track connections and serve attachments**

```rust
// space-chat-transport/src/transport.rs -- full replacement of run_connection
async fn run_connection(
    conn: iroh::endpoint::Connection,
    is_dialer: bool,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    events: mpsc::UnboundedSender<TransportEvent>,
) {
    let Ok(remote_id) = conn.remote_id() else { return };
    conns.lock().await.insert(remote_id, conn.clone());
    let _ = events.send(TransportEvent::Connected { endpoint_id: remote_id });

    let local_digests = {
        let guard = spaces.lock().await;
        let mut digests = Vec::with_capacity(guard.len());
        for (space_id, entry) in guard.iter() {
            let heads = entry.segment.lock().await.heads().iter().map(change_hash_to_bytes).collect();
            digests.push(SpaceDigest { space_id: space_id.clone(), epoch: entry.epoch, heads });
        }
        digests
    };

    let digest_result = exchange_digests(&conn, is_dialer, ControlHello { digests: local_digests }).await;
    let Ok(remote_hello) = digest_result else {
        conns.lock().await.remove(&remote_id);
        let _ = events.send(TransportEvent::Disconnected { endpoint_id: remote_id });
        return;
    };
    let remote_space_ids: HashSet<String> = remote_hello.digests.into_iter().map(|d| d.space_id).collect();

    let manager = Arc::new(StreamManager::new(conn));

    if is_dialer {
        let guard = spaces.lock().await;
        for (space_id, entry) in guard.iter() {
            if !remote_space_ids.contains(space_id) {
                continue;
            }
            if let Ok(handle) = manager.open(space_id, Category::AutomergeSync).await {
                tokio::spawn(run_automerge_sync(handle, entry.segment.clone(), entry.notify.clone(), events.clone()));
            }
        }
    } else {
        while let Ok((envelope, handle)) = manager.accept_next().await {
            match envelope.category {
                Category::AutomergeSync => {
                    let guard = spaces.lock().await;
                    if let Some(entry) = guard.get(&envelope.space_id) {
                        tokio::spawn(run_automerge_sync(handle, entry.segment.clone(), entry.notify.clone(), events.clone()));
                    }
                }
                Category::AttachmentTransfer => {
                    let attachments = attachments.clone();
                    tokio::spawn(serve_attachment_request(handle, attachments));
                }
                // MlsControl (Task 10), Gossip, Ephemeral: not opened by
                // this milestone's implementation (see run_connection's
                // earlier comment on Gossip) or handled by a later task.
                _ => {}
            }
        }
    }

    conns.lock().await.remove(&remote_id);
    let _ = events.send(TransportEvent::Disconnected { endpoint_id: remote_id });
}

/// Serves one incoming attachment request: reads the requested hash,
/// writes back either the content in fixed-size chunks followed by an
/// empty terminator frame, or just the empty terminator if this device
/// doesn't have that hash. Never relays the request to any other peer —
/// there is no code path here that could, which is the point (see
/// `Transport::request_attachment`'s doc comment).
async fn serve_attachment_request(
    mut handle: crate::streams::StreamHandle,
    attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
) {
    let Ok(hash_bytes) = read_frame(&mut handle.recv).await else { return };
    let Ok(hash): Result<[u8; 32], _> = hash_bytes.try_into() else { return };

    let bytes = attachments.lock().await.get(&hash).cloned();
    if let Some(bytes) = bytes {
        for chunk in bytes.chunks(ATTACHMENT_CHUNK_SIZE) {
            if write_frame(&mut handle.send, chunk).await.is_err() {
                return;
            }
        }
    }
    let _ = write_frame(&mut handle.send, &[]).await;
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd space-chat-transport && cargo test --lib -- --test-threads=1`
Expected: PASS (all `transport.rs` tests, including Task 7's, since the `run_connection`/`bind`/`dial` signatures changed)

- [ ] **Step 7: Commit**

```bash
git add space-chat-transport/src/transport.rs space-chat-transport/Cargo.toml space-chat-transport/Cargo.lock
git commit -m "feat(transport): add chunked, direct-endpoint-only, hash-verified attachment transfer"
```

---

### Task 10: Sequencer-routed invite join flow

> **⚠️ Amendment (post-implementation):** the join-routing code below is STALE —
> it has no hop cap, no envelope/payload `space_id` cross-check, no
> self-dial guard, no read timeout, and silently drops a forward to a
> sequencer it isn't already connected to (the shipped code dials fresh). See
> [Post-implementation amendments](#post-implementation-amendments);
> `space-chat-transport/src/transport.rs` and `join.rs` are authoritative.

**Files:**
- Create: `space-chat-transport/src/join.rs`
- Modify: `space-chat-transport/src/transport.rs` — add `generate_invite`/`join_via_invite`, a `membership`/`own_device_id` slot, and an `MlsControl` dispatch arm
- Modify: `space-chat-transport/src/lib.rs` — add `pub mod join;`
- Test: `space-chat-transport/src/join.rs` (inline, real `iroh` connections, in-memory `SpaceMembership` fake)

**Interfaces:**
- Consumes: `Invite` (Task 3), `Category::MlsControl` (Task 2), `Transport` (Tasks 7/9), `space_chat_core::{domain::DeviceId, sequencer::elect_sequencer}` (Milestone 1).
- Produces: `SpaceMembership` (trait: `fn members(&self, space_id: &str) -> Vec<DeviceId>`, `fn endpoint_for(&self, device: DeviceId) -> Option<iroh::EndpointId>`), `JoinRequest { space_id: String, joiner_endpoint_id: [u8; 32], payload: Vec<u8> }`, `async fn Transport::configure_membership(&self, own_device_id: DeviceId, membership: Arc<dyn SpaceMembership>)`, `fn Transport::generate_invite(&self, space_id: &str, join_token: impl Into<String>) -> Invite` (synchronous — it only reads `self.endpoint_id()` and constructs a value, no lock/network access needed), `async fn Transport::join_via_invite(&self, invite: &Invite, payload: Vec<u8>) -> Result<(), TransportError>`, and a new `TransportEvent::JoinRequest(JoinRequest)` variant. A future `space-chat-openmls` integration implements `SpaceMembership` for real; this task's tests use an in-memory fake.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-transport/src/join.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::TransportConfig;
    use crate::identity::TransportIdentity;
    use crate::invite::Invite;
    use crate::transport::{Transport, TransportEvent};
    use space_chat_core::domain::DeviceId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    struct FakeMembership {
        members: Vec<DeviceId>,
        endpoints: StdMutex<HashMap<DeviceId, iroh::EndpointId>>,
    }

    impl SpaceMembership for FakeMembership {
        fn members(&self, _space_id: &str) -> Vec<DeviceId> {
            self.members.clone()
        }
        fn endpoint_for(&self, device: DeviceId) -> Option<iroh::EndpointId> {
            self.endpoints.lock().unwrap().get(&device).copied()
        }
    }

    #[tokio::test]
    async fn a_join_request_to_a_non_sequencer_inviter_is_forwarded_to_the_elected_sequencer() {
        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

        // Device A has the lowest DeviceId -- it is the elected sequencer.
        let device_a = DeviceId([1u8; 32]);
        let device_b = DeviceId([2u8; 32]);

        let a_identity = TransportIdentity::generate();
        let b_identity = TransportIdentity::generate();
        let joiner_identity = TransportIdentity::generate();

        let (a, mut a_events) = Transport::bind(&a_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let (b, _b_events) = Transport::bind(&b_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let (joiner, _joiner_events) = Transport::bind(&joiner_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();

        let endpoints = StdMutex::new(HashMap::from([
            (device_a, a.endpoint_id()),
            (device_b, b.endpoint_id()),
        ]));
        let membership: Arc<dyn SpaceMembership> = Arc::new(FakeMembership {
            members: vec![device_a, device_b],
            endpoints,
        });

        a.configure_membership(device_a, membership.clone()).await;
        b.configure_membership(device_b, membership).await;

        // B (not the sequencer) generates the invite the joiner uses.
        let invite = b.generate_invite("space-1", "nonce-1");

        joiner.join_via_invite(&invite, b"opaque key package bytes".to_vec()).await.unwrap();

        let request = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match a_events.recv().await {
                    Some(TransportEvent::JoinRequest(req)) => return req,
                    Some(_) => continue,
                    None => panic!("a's event channel closed before a JoinRequest arrived"),
                }
            }
        })
        .await
        .expect("device A, the elected sequencer, should receive the forwarded join request");

        assert_eq!(request.space_id, "space-1");
        assert_eq!(request.joiner_endpoint_id, *joiner.endpoint_id().as_bytes());
        assert_eq!(request.payload, b"opaque key package bytes".to_vec());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-transport && cargo test --lib join -- --test-threads=1`
Expected: FAIL — `SpaceMembership`, `JoinRequest`, `configure_membership`, `generate_invite`, `join_via_invite` not defined.

- [ ] **Step 3: Implement `join.rs`**

```rust
// space-chat-transport/src/join.rs (above the tests module)
use serde::{Deserialize, Serialize};
use space_chat_core::domain::DeviceId;

/// A future MLS integration (`space-chat-openmls`, not built yet — see
/// this plan's Architecture note) implements this for real, backed by
/// actual MLS group state. Until then, tests use an in-memory fake. This
/// crate never calls `members`/`endpoint_for` for any purpose other than
/// `elect_sequencer` routing (see `handle_join_request` in `transport.rs`).
pub trait SpaceMembership: Send + Sync {
    fn members(&self, space_id: &str) -> Vec<DeviceId>;
    fn endpoint_for(&self, device: DeviceId) -> Option<iroh::EndpointId>;
}

/// Carried opaquely end-to-end: this crate never inspects `payload`. A real
/// integration fills it with an MLS `KeyPackage`/proposal; here it is
/// exactly the bytes the joiner passed to `join_via_invite`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinRequest {
    pub space_id: String,
    pub joiner_endpoint_id: [u8; 32],
    pub payload: Vec<u8>,
}
```

- [ ] **Step 4: Extend `Transport` with membership configuration and invite join**

```rust
// space-chat-transport/src/transport.rs -- add to the imports
use crate::invite::Invite;
use crate::join::{JoinRequest, SpaceMembership};
use space_chat_core::domain::DeviceId;
use space_chat_core::sequencer::elect_sequencer;

// Add a new variant to TransportEvent:
pub enum TransportEvent {
    Connected { endpoint_id: iroh::EndpointId },
    Disconnected { endpoint_id: iroh::EndpointId },
    IncomingChange(SegmentChange),
    JoinRequest(JoinRequest),
}

// Add two fields to Transport (full struct shown for clarity):
pub struct Transport {
    endpoint: iroh::Endpoint,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    membership: Arc<Mutex<Option<(DeviceId, Arc<dyn SpaceMembership>)>>>,
    events_tx: mpsc::UnboundedSender<TransportEvent>,
}

impl Transport {
    // `bind` gains one more `Arc::new(Mutex::new(None))` for `membership`,
    // threaded into `run_connection` exactly like `conns`/`attachments`
    // were in Task 9 -- omitted here for brevity; apply the same pattern
    // Task 9's Step 4 established (construct it in `bind`, clone it into
    // both the accept-loop closure and `dial`, and add it as a
    // `run_connection` parameter, shown in full in Step 5 below).

    /// Registers this device's own MLS `DeviceId` and the `SpaceMembership`
    /// source used to resolve `elect_sequencer` routing for join requests.
    /// Not needed for any purpose other than Task 10's join flow.
    pub async fn configure_membership(&self, own_device_id: DeviceId, membership: Arc<dyn SpaceMembership>) {
        *self.membership.lock().await = Some((own_device_id, membership));
    }

    pub fn generate_invite(&self, space_id: &str, join_token: impl Into<String>) -> Invite {
        Invite {
            endpoint_id: *self.endpoint_id().as_bytes(),
            space_id: space_id.to_string(),
            join_token: join_token.into(),
        }
    }

    /// Dials the inviting device named in `invite` and sends a
    /// `JoinRequest` over an `MlsControl`-category stream scoped to
    /// `invite.space_id`. The inviter routes it onward per
    /// `handle_join_request` below — this method's only job is delivering
    /// the request to *a* member; routing to the actual sequencer is the
    /// receiving side's responsibility, not the joiner's.
    pub async fn join_via_invite(&self, invite: &Invite, payload: Vec<u8>) -> Result<(), TransportError> {
        let inviter_id = iroh::EndpointId::from_bytes(&invite.endpoint_id)
            .map_err(|e| TransportError::Codec(e.to_string()))?;
        self.dial(iroh::EndpointAddr::from(inviter_id)).await?;

        // `dial` spawns `run_connection` in the background; give the
        // control-stream digest handshake a moment to complete and
        // register the connection before looking it up. A production
        // implementation would await a `Connected` event instead of
        // sleeping — left as a known simplification, since this plan's
        // tests tolerate the fixed delay.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let conn = self.conns.lock().await.get(&inviter_id).cloned().ok_or(TransportError::Timeout)?;

        let manager = StreamManager::new(conn);
        let mut handle = manager.open(&invite.space_id, Category::MlsControl).await?;
        let request = JoinRequest {
            space_id: invite.space_id.clone(),
            joiner_endpoint_id: *self.endpoint_id().as_bytes(),
            payload,
        };
        let mut bytes = Vec::new();
        ciborium::into_writer(&request, &mut bytes).map_err(|e| TransportError::Codec(e.to_string()))?;
        write_frame(&mut handle.send, &bytes).await
    }
}

/// Handles one incoming `JoinRequest` on an `MlsControl` stream: if this
/// device is the space's elected sequencer, surfaces it as a
/// `TransportEvent::JoinRequest` for a higher layer (a future
/// `space-chat-openmls` integration) to actually act on; otherwise forwards
/// the same request, unmodified, to whichever device *is* the elected
/// sequencer — dialing it fresh if not already connected. This is what
/// "invite-based joins still route through the space's elected sequencer"
/// (the transport spec's Pairing/discovery section) means concretely: the
/// inviter is not required to already be the sequencer.
async fn handle_join_request(
    mut handle: crate::streams::StreamHandle,
    space_id: String,
    membership: Arc<Mutex<Option<(DeviceId, Arc<dyn SpaceMembership>)>>>,
    conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    events: mpsc::UnboundedSender<TransportEvent>,
) {
    let Ok(bytes) = read_frame(&mut handle.recv).await else { return };
    let Ok(request): Result<JoinRequest, _> = ciborium::from_reader(bytes.as_slice()) else { return };

    let Some((own_device_id, membership)) = membership.lock().await.clone() else { return };
    let members = membership.members(&space_id);
    let Some(sequencer) = elect_sequencer(&members) else { return };

    if sequencer == own_device_id {
        let _ = events.send(TransportEvent::JoinRequest(request));
        return;
    }

    let Some(sequencer_endpoint) = membership.endpoint_for(sequencer) else { return };
    let conn = conns.lock().await.get(&sequencer_endpoint).cloned();
    let Some(conn) = conn else { return }; // not connected to the sequencer -- see this plan's closing notes on this known gap
    let manager = StreamManager::new(conn);
    if let Ok(mut forward_handle) = manager.open(&space_id, Category::MlsControl).await {
        let _ = write_frame(&mut forward_handle.send, &bytes).await;
    }
}
```

- [ ] **Step 5: Wire `MlsControl` into `run_connection`'s dispatch and thread `membership` through `bind`/`dial`**

```rust
// space-chat-transport/src/transport.rs -- full replacement of bind, dial, and run_connection

impl Transport {
    pub async fn bind(
        identity: &TransportIdentity,
        config: TransportConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<TransportEvent>), TransportError> {
        let endpoint = bind_endpoint(identity, config).await?;
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let spaces: Arc<Mutex<HashMap<String, SpaceEntry>>> = Arc::new(Mutex::new(HashMap::new()));
        let conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>> = Arc::new(Mutex::new(HashMap::new()));
        let attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
        let membership: Arc<Mutex<Option<(DeviceId, Arc<dyn SpaceMembership>)>>> = Arc::new(Mutex::new(None));

        let transport = Self {
            endpoint: endpoint.clone(),
            spaces: spaces.clone(),
            conns: conns.clone(),
            attachments: attachments.clone(),
            membership: membership.clone(),
            events_tx: events_tx.clone(),
        };

        tokio::spawn(async move {
            loop {
                let Some(incoming) = endpoint.accept().await else { break };
                let Ok(conn) = incoming.await else { continue };
                tokio::spawn(run_connection(
                    conn, false, spaces.clone(), conns.clone(), attachments.clone(), membership.clone(), events_tx.clone(),
                ));
            }
        });

        Ok((transport, events_rx))
    }

    pub async fn dial(&self, addr: impl Into<iroh::EndpointAddr>) -> Result<(), TransportError> {
        let conn = self
            .endpoint
            .connect(addr, ALPN)
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;
        tokio::spawn(run_connection(
            conn, true, self.spaces.clone(), self.conns.clone(), self.attachments.clone(), self.membership.clone(), self.events_tx.clone(),
        ));
        Ok(())
    }
}

async fn run_connection(
    conn: iroh::endpoint::Connection,
    is_dialer: bool,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    membership: Arc<Mutex<Option<(DeviceId, Arc<dyn SpaceMembership>)>>>,
    events: mpsc::UnboundedSender<TransportEvent>,
) {
    let Ok(remote_id) = conn.remote_id() else { return };
    conns.lock().await.insert(remote_id, conn.clone());
    let _ = events.send(TransportEvent::Connected { endpoint_id: remote_id });

    let local_digests = {
        let guard = spaces.lock().await;
        let mut digests = Vec::with_capacity(guard.len());
        for (space_id, entry) in guard.iter() {
            let heads = entry.segment.lock().await.heads().iter().map(change_hash_to_bytes).collect();
            digests.push(SpaceDigest { space_id: space_id.clone(), epoch: entry.epoch, heads });
        }
        digests
    };

    let digest_result = exchange_digests(&conn, is_dialer, ControlHello { digests: local_digests }).await;
    let Ok(remote_hello) = digest_result else {
        conns.lock().await.remove(&remote_id);
        let _ = events.send(TransportEvent::Disconnected { endpoint_id: remote_id });
        return;
    };
    let remote_space_ids: HashSet<String> = remote_hello.digests.into_iter().map(|d| d.space_id).collect();

    let manager = Arc::new(StreamManager::new(conn));

    if is_dialer {
        let guard = spaces.lock().await;
        for (space_id, entry) in guard.iter() {
            if !remote_space_ids.contains(space_id) {
                continue;
            }
            if let Ok(handle) = manager.open(space_id, Category::AutomergeSync).await {
                tokio::spawn(run_automerge_sync(handle, entry.segment.clone(), entry.notify.clone(), events.clone()));
            }
        }
    } else {
        while let Ok((envelope, handle)) = manager.accept_next().await {
            match envelope.category {
                Category::AutomergeSync => {
                    let guard = spaces.lock().await;
                    if let Some(entry) = guard.get(&envelope.space_id) {
                        tokio::spawn(run_automerge_sync(handle, entry.segment.clone(), entry.notify.clone(), events.clone()));
                    }
                }
                Category::AttachmentTransfer => {
                    tokio::spawn(serve_attachment_request(handle, attachments.clone()));
                }
                Category::MlsControl => {
                    tokio::spawn(handle_join_request(handle, envelope.space_id, membership.clone(), conns.clone(), events.clone()));
                }
                Category::Gossip | Category::Ephemeral => {}
            }
        }
    }

    conns.lock().await.remove(&remote_id);
    let _ = events.send(TransportEvent::Disconnected { endpoint_id: remote_id });
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd space-chat-transport && cargo test --lib -- --test-threads=1`
Expected: PASS (every `transport.rs` and `join.rs` test)

- [ ] **Step 7: Add the module to `lib.rs`**

```rust
// space-chat-transport/src/lib.rs
pub mod bootstrap;
pub mod control;
pub mod envelope;
pub mod error;
pub mod framing;
pub mod identity;
pub mod invite;
pub mod join;
pub mod streams;
pub mod transport;
```

- [ ] **Step 8: Commit**

```bash
git add space-chat-transport/src/join.rs space-chat-transport/src/transport.rs space-chat-transport/src/lib.rs
git commit -m "feat(transport): route invite-based joins through the space's elected sequencer"
```

---

### Task 11: Roaming survival

**Files:**
- Modify: `space-chat-transport/src/transport.rs` — add `Transport::simulate_network_change`
- Test: `space-chat-transport/src/transport.rs` (inline, extends the existing tests module)

**Interfaces:**
- Consumes: everything from Task 7.
- Produces: `async fn Transport::simulate_network_change(&self)`, wrapping `iroh::Endpoint::network_change()`. No later task depends on this; it exists for this task's own test and for a future `space-chat-app` to call when the OS reports a network interface change (per the transport spec's Resilience section).

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-transport/src/transport.rs, inside the existing tests module
#[tokio::test]
async fn a_connection_survives_a_simulated_network_change_mid_conversation() {
    let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();
    let alice_identity = TransportIdentity::generate();
    let bob_identity = TransportIdentity::generate();
    let (alice, _alice_events) = Transport::bind(&alice_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
    let (bob, _bob_events) = Transport::bind(&bob_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();

    let alice_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
    let bob_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
    alice.add_space("space-1", 0, alice_segment.clone()).await;
    bob.add_space("space-1", 0, bob_segment.clone()).await;

    alice.dial(bob.endpoint_addr()).await.unwrap();

    alice_segment.lock().await.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "before roam".to_string(),
        attachments: vec![],
    });
    alice.notify_local_change("space-1").await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if bob_segment.lock().await.message_count() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("bob should see the before-roam message");

    // Per the transport spec's Resilience section: QUIC identifies
    // connections by connection ID, not IP:port, so a network change
    // doesn't require tearing down and re-establishing anything. This
    // simulates the OS-level notification a real app would forward (e.g.
    // Android's network-callback API) without this test needing to
    // actually toggle a network interface.
    alice.simulate_network_change().await;

    alice_segment.lock().await.append_message(&Message {
        sender: DeviceId([1u8; 32]),
        content: "after roam".to_string(),
        attachments: vec![],
    });
    alice.notify_local_change("space-1").await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if bob_segment.lock().await.message_count() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("bob should see the after-roam message over the same connection, with no re-dial");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd space-chat-transport && cargo test --lib a_connection_survives -- --test-threads=1`
Expected: FAIL — `simulate_network_change` not defined.

- [ ] **Step 3: Implement `simulate_network_change`**

```rust
// space-chat-transport/src/transport.rs -- add inside impl Transport
    /// Notifies the local `iroh::Endpoint` that the network may have
    /// changed (wifi → cellular, etc.). A real `space-chat-app` calls this
    /// from whatever OS-level network-change callback the platform
    /// provides; tests call it directly to simulate that notification.
    /// Per `iroh`'s own docs, this is harmless to call even when nothing
    /// actually changed.
    pub async fn simulate_network_change(&self) {
        self.endpoint.network_change().await;
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd space-chat-transport && cargo test --lib a_connection_survives -- --test-threads=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add space-chat-transport/src/transport.rs
git commit -m "feat(transport): expose network-change notification for roaming survival"
```

---

### Task 12: Exit criteria — two (and three) separate OS processes converge over a local relay

**Files:**
- Create: `space-chat-transport/src/bin/test_peer.rs`
- Create: `space-chat-transport/tests/two_process_convergence.rs`
- Modify: `space-chat-transport/Cargo.toml` — no change needed; a binary placed under `src/bin/` is automatically built as an additional target alongside the library, per standard Cargo convention.

**Interfaces:**
- Consumes: everything from Tasks 1–11.
- Produces: nothing new — this is the milestone's exit-criteria proof.

- [ ] **Step 1: Write `test_peer`, a small scripted binary driven entirely by CLI arguments**

```rust
// space-chat-transport/src/bin/test_peer.rs
use space_chat_core::domain::{DeviceId, Message};
use space_chat_core::segment::Segment;
use space_chat_transport::bootstrap::TransportConfig;
use space_chat_transport::identity::TransportIdentity;
use space_chat_transport::transport::Transport;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(s: &str) -> [u8; 32] {
    let bytes: Vec<u8> = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex from a peer's own ENDPOINT line"))
        .collect();
    bytes.try_into().expect("endpoint id hex should decode to exactly 32 bytes")
}

/// Usage: test_peer <relay-url> <role> [<peer-endpoint-hex> ...]
/// Roles:
///   - "alice": appends one message, notifies, then (if a second arg
///     "roam" trails the peer list) simulates a network change and sends a
///     second message, then exits 0.
///   - "bob"/"carol": dials the given peers, waits until its segment shows
///     the expected message count, prints "CONVERGED <n>", then exits 0
///     (or "TIMEOUT" and exits 1).
#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let relay_url: iroh::RelayUrl = args[1].parse().expect("valid relay url");
    let role = args[2].clone();
    let peer_hexes: Vec<String> = args[3..].iter().filter(|a| *a != "roam").cloned().collect();
    let roam = args[3..].iter().any(|a| a == "roam");

    // Caveat (see this plan's Global Constraints): verify
    // `iroh::RelayMap`'s exact single-relay convenience constructor against
    // docs.rs for the pinned version -- this assumes one exists that takes
    // just a `RelayUrl`.
    let relay_map = iroh::RelayMap::from_url(relay_url.clone());

    let identity = TransportIdentity::generate();
    let (transport, mut events) = Transport::bind(&identity, TransportConfig { relay: Some((relay_map, relay_url)) })
        .await
        .expect("test_peer should bind its endpoint");

    println!("ENDPOINT {}", hex_encode(transport.endpoint_id().as_bytes().as_slice()));
    std::io::stdout().flush().unwrap();

    let segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
    transport.add_space("space-1", 0, segment.clone()).await;

    for hex in &peer_hexes {
        let peer_id = iroh::EndpointId::from_bytes(&hex_decode(hex)).expect("valid endpoint id");
        transport
            .dial(iroh::EndpointAddr::from(peer_id))
            .await
            .expect("test_peer should be able to dial its configured peer");
    }

    // Drain events in the background so the mpsc channel never backs up;
    // this test only cares about message_count() converging, not about
    // asserting on individual events (Task 7/9's inline tests already do
    // that at the unit level).
    tokio::spawn(async move { while events.recv().await.is_some() {} });

    match role.as_str() {
        "alice" => {
            segment.lock().await.append_message(&Message {
                sender: DeviceId([1u8; 32]),
                content: "before roam".to_string(),
                attachments: vec![],
            });
            transport.notify_local_change("space-1").await;

            if roam {
                tokio::time::sleep(Duration::from_secs(2)).await;
                transport.simulate_network_change().await;
                segment.lock().await.append_message(&Message {
                    sender: DeviceId([1u8; 32]),
                    content: "after roam".to_string(),
                    attachments: vec![],
                });
                transport.notify_local_change("space-1").await;
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
            println!("DONE");
        }
        "bob" | "carol" => {
            let expected = if roam { 2 } else { 1 };
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                if segment.lock().await.message_count() >= expected {
                    println!("CONVERGED {expected}");
                    std::io::stdout().flush().unwrap();
                    return;
                }
                if tokio::time::Instant::now() > deadline {
                    println!("TIMEOUT");
                    std::process::exit(1);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        other => panic!("unknown role: {other}"),
    }
}
```

- [ ] **Step 2: Write the two/three-process integration test**

```rust
// space-chat-transport/tests/two_process_convergence.rs
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};

fn spawn_peer(relay_url: &str, role: &str, extra_args: &[String]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_test_peer"))
        .arg(relay_url)
        .arg(role)
        .args(extra_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn the test_peer binary")
}

fn read_line(child: &mut Child) -> String {
    let stdout = child.stdout.as_mut().expect("child stdout should be piped");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).expect("child should print a line");
    line.trim().to_string()
}

/// The milestone's exit criterion, part 1: two separate OS processes (not
/// in-process function calls, unlike Tasks 7–11's tests) converge over a
/// real local iroh relay.
#[tokio::test]
async fn two_separate_os_processes_converge() {
    let (_relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

    let mut bob = spawn_peer(relay_url.as_str(), "bob", &[]);
    let bob_endpoint = read_line(&mut bob).strip_prefix("ENDPOINT ").unwrap().to_string();

    let mut alice = spawn_peer(relay_url.as_str(), "alice", &[bob_endpoint]);

    assert!(alice.wait().unwrap().success(), "alice's process should exit successfully");

    let bob_output = read_line(&mut bob);
    assert_eq!(bob_output, "CONVERGED 1");
    assert!(bob.wait().unwrap().success(), "bob's process should exit successfully");
}

/// The milestone's exit criterion, part 2: three separate OS processes,
/// where Alice and Carol are never directly connected, still converge on
/// Alice's message via Bob — the same multi-hop property Task 8 proved
/// in-process, now proved across real process/OS boundaries.
#[tokio::test]
async fn three_separate_os_processes_converge_via_a_middle_hop() {
    let (_relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

    let mut bob = spawn_peer(relay_url.as_str(), "bob", &[]);
    let bob_endpoint = read_line(&mut bob).strip_prefix("ENDPOINT ").unwrap().to_string();

    let mut carol = spawn_peer(relay_url.as_str(), "carol", &[bob_endpoint.clone()]);
    let mut alice = spawn_peer(relay_url.as_str(), "alice", &[bob_endpoint]);

    assert!(alice.wait().unwrap().success());

    let carol_output = read_line(&mut carol);
    assert_eq!(carol_output, "CONVERGED 1", "carol should converge via bob without ever dialing alice");
    assert!(carol.wait().unwrap().success());

    let bob_output = read_line(&mut bob);
    assert_eq!(bob_output, "CONVERGED 1");
    assert!(bob.wait().unwrap().success());
}

/// The milestone's exit criterion, part 3: roaming survival, across real
/// OS processes rather than Task 11's in-process simulation.
#[tokio::test]
async fn a_roaming_process_still_delivers_its_second_message() {
    let (_relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

    let mut bob = spawn_peer(relay_url.as_str(), "bob", &["roam".to_string()]);
    let bob_endpoint = read_line(&mut bob).strip_prefix("ENDPOINT ").unwrap().to_string();

    let mut alice = spawn_peer(relay_url.as_str(), "alice", &[bob_endpoint, "roam".to_string()]);

    assert!(alice.wait().unwrap().success());

    let bob_output = read_line(&mut bob);
    assert_eq!(bob_output, "CONVERGED 2", "bob should see both the before- and after-roam messages");
    assert!(bob.wait().unwrap().success());
}
```

- [ ] **Step 3: Run the exit-criteria tests**

Run: `cd space-chat-transport && cargo test --test two_process_convergence -- --test-threads=1`
Expected: PASS (all three tests)

- [ ] **Step 4: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS — every test from Milestone 1, Milestone 2 (if merged by this point), and this plan's Tasks 1–12, combined.

- [ ] **Step 5: Commit**

```bash
git add space-chat-transport/src/bin/test_peer.rs space-chat-transport/tests/two_process_convergence.rs
git commit -m "test(transport): prove two/three-process convergence, multi-hop, and roaming over a real local relay"
```

---

## Closing note: what this plan deliberately excludes

- **Real MLS/OpenMLS integration.** `space-chat-openmls` doesn't exist yet (excluded from Milestone 1 as a separable follow-on). This plan defines the `SpaceMembership` trait boundary (Task 10) and treats join-request payloads as opaque bytes throughout; wiring a real `KeyPackage`/Commit flow through that boundary is a future plan's job, once `space-chat-openmls` exists.
- ~~**The "not connected to the sequencer" gap in `handle_join_request` (Task 10).** If the device forwarding a join request has no existing connection to the elected sequencer, the forward silently drops rather than dialing fresh.~~ **CORRECTED (post-implementation): this is no longer true and never shipped that way.** `handle_join_request` DOES dial the sequencer on demand, via `dial_and_spawn` (the same helper `Transport::dial` uses), when it has no existing connection to it. What remains a genuine gap is narrower: if that *dial itself fails* (sequencer offline/unreachable), or the handshake doesn't land inside the fixed 300ms settle delay, the request is dropped with no retry or queuing. That narrower liveness gap is the "sequencer-unreachable" case already covered by the "no new mechanism" bullet further down.
- **QR-code pixel rendering, and any UI for scanning one.** `Invite::encode_link` (Task 3) is as far as this crate goes; turning that string into pixels (and back) is Milestone 4's job.
- **A self-hosted relay ("peer server at a price").** Explicitly deferred by the transport spec itself, not something this plan takes a position on.
- **A TCP fallback transport for UDP-blocking networks.** Documented as a known limitation (Global Constraints), not designed around.
- **Real full-scale interop with Milestone 2's storage traits.** `Transport::serve_attachment`/`request_attachment` (Task 9) use a bare in-memory `HashMap`, not `space_chat_core::storage::AttachmentBlobStore` — this crate has no dependency on Milestone 2's storage crates (Global Constraints), and a real composition root will bridge the two (feed `AttachmentBlobStore::load_attachment` results into `serve_attachment`, and persist fetched bytes via `save_attachment`) rather than this crate reaching into storage itself.
- **Resumable attachment transfer.** The transport spec's Resilience section calls for "a dropped connection mid-transfer means re-requesting missing chunks on reconnect, not restarting." Task 9's `request_attachment` only implements the chunked part — a dropped connection mid-fetch means the next `request_attachment` call re-fetches the whole attachment from byte zero, not just the missing chunks. Adding resume support (tracking which chunk offsets were already received and requesting only the remainder) is a natural extension of Task 9's existing chunk-framing once a real caller needs it, not a redesign — left out here to keep this milestone's scope to what the exit criteria actually require.
- **The sequencer-unreachable liveness limitation and hole-punch/relay-both-fail fallback** are both explicitly "no new mechanism" cases per the transport spec's Error handling section — messaging degrades to whatever multi-hop path exists (Task 8's mechanism) or waits for reconnect, exactly as Milestone 1's segment/sync design already handles an offline peer. Nothing in this plan adds special-case code for either, which is the point: they're inherited properties of the design, not gaps this plan leaves unaddressed.
- **Gossip as its own stream/category.** As documented inline in Task 7, this plan's `AutomergeSync` stream handles both catch-up reconciliation and live push; the `Gossip` category variant exists in the wire vocabulary (Task 2) but no code path in this plan ever opens a `Gossip`-category stream. Revisit if a cheaper "just the raw new-change bytes" fast path proves necessary once real usage patterns are known.

## Integration note for the final whole-branch review

Per this project's established process, run a final review across the whole branch's diff before merging to `main`. In addition to the usual per-task review points, flag this explicitly: **this crate's public API for consuming/producing changes (`Transport::add_space`, `notify_local_change`, `dial`, and the `TransportEvent` enum) was designed independently of the Milestone 4 app-shell plan, which was written concurrently by a separate agent and will not have been reconciled with this plan before both land.** In particular, check before wiring the two together in `space-chat-app`:

- Whether `Transport::add_space` taking `Arc<tokio::sync::Mutex<Segment>>` (a shared handle the caller also mutates directly) matches whatever ownership model the app-shell plan assumed for `Segment` — it may have assumed `Transport` owns `Segment` outright, or that mutations flow through `Transport` rather than around it.
- Whether `TransportEvent::IncomingChange(SegmentChange)` is the right shape for the app-shell's `Projection`/`ListingIndex`/`SearchIndex`/live-spec-patching consumers, or whether they need a different granularity (e.g. per-message rather than per-segment-snapshot) — this plan reuses `SegmentChange` as-is from Milestone 1/2 rather than inventing a new shape, but the app-shell plan may have made a different assumption about what it receives.
- Whether the app-shell plan already assumed a `SpaceMembership` implementation shape (Task 10) different from this plan's — reconcile before `space-chat-app` provides the real one backed by `space-chat-openmls`.
- Whether `notify_local_change`'s "wake the sync loop, don't pass the change" design (as opposed to, say, an API that takes the new content directly) fits how the app-shell plan intends to drive sends after a local mutation.

---

## Post-implementation amendments

Written after the final whole-branch review of Milestone 3, before merge to
`main`. Two purposes: (1) record the real fixes made to Tasks 7, 9 and 10 so
this document's stale code examples can't mislead a future reader, and (2)
collect, in one place, every known limitation this milestone deliberately
deferred rather than fixed.

**The source of truth is `space-chat-transport/src/`.** Where this section and
an earlier code example disagree, the example is wrong.

### Task 7 (`Transport`, connection lifecycle, Automerge sync) — fixes made

- **Lock held across network I/O.** The example held the `spaces` mutex guard
  across `manager.open(...).await` (a real `open_bi()` plus a network write),
  which stalls every other public API call and every other connection's task
  behind one slow peer. Fixed by cloning out `(space_id, segment, notify)`
  under the lock and dropping the guard before opening anything.
- **Lock-order inversion deadlocking the whole `Transport`** (final-review
  Critical #1). The digest-building block held the `spaces` guard across
  `entry.segment.lock().await`, establishing `spaces → segment`. The natural
  caller pattern is the opposite (`segment → spaces`: lock your segment to
  append, then call `notify_local_change`). Run concurrently, the cycle closes
  and *nothing* that needs `spaces` ever progresses again — every future
  `add_space`/`dial`/`notify_local_change` and every other connection's
  `run_connection`. Fixed by snapshotting `(space_id, epoch, Arc<Mutex<Segment>>)`
  under `spaces` and dropping that guard before locking any segment, so the two
  locks are never held simultaneously. Regression test:
  `notify_local_change_does_not_deadlock_when_a_caller_holds_its_own_segment_lock`
  (verified to fail against the pre-fix code). The caller-side contract is now
  documented on `Transport::add_space`.
- **Cancellation-unsafe `read_frame` inside `select!`.** The example raced
  `read_frame` directly against `notify.notified()` and a timeout.
  `read_frame` is two sequential `read_exact` calls — not cancellation-safe —
  so a cancelled branch could drop already-read bytes and permanently desync
  the stream's frame alignment. Fixed by moving `recv` into a dedicated reader
  task that forwards whole frames over an `mpsc` channel; the `select!` races
  the (cancellation-safe) `mpsc::Receiver::recv()` instead.
- **Asymmetric dialer/accepter loops.** Only the accepter ran an
  `accept_next()` loop; the dialer opened its sync streams and then blocked on
  `conn.closed()`. Since `conns` is populated for both roles, asking a
  dialer-role peer for an attachment hung the requester forever. Both roles now
  run the same accept loop. Regression test:
  `accepter_can_request_an_attachment_from_the_dialer`.
- **`Disconnected` fired on a healthy connection.** The dialer branch fell
  straight through to sending `Disconnected` after opening its (possibly zero)
  sync streams. Fixed by the symmetric accept loop above. Regression test:
  `dialer_reports_disconnected_only_after_the_peer_actually_disconnects`.
- **`Connected` fired before the handshake.** It was sent at the top of
  `run_connection`, before `exchange_digests` and before `conns` was populated,
  so a consumer reacting to it could get a spurious `NotFound` from
  `request_attachment`. Now sent only after the handshake succeeds and `conns`
  is populated — and correspondingly, a failed handshake sends *neither* event,
  preserving `Connected`/`Disconnected` pairing.
- **Blind `conns` removal on teardown.** A reconnect race could mean a newer
  connection is already registered under the same `EndpointId`. Teardown now
  compares `Connection::stable_id()` and only removes the entry if it is still
  this task's own connection.
- **Whole-connection teardown on one malformed stream.** Any `accept_next`
  error used to break the loop. Only `TransportError::Connection(_)` now means
  the connection is gone; other errors skip that one stream and keep accepting.
- **Epoch gate missing on the accepter side.** The dialer checked that the
  remote shares the space at a matching epoch; the accepter didn't, so an
  inbound stream could sync mismatched-epoch segments. The check is now
  symmetric.
- **Heads-based change detection.** Deciding whether a received sync message
  actually merged anything by comparing a remembered `cursor` was wrong —
  `cursor` is a document-wide counter another peer's sync task can bump. Now
  compares `heads()` immediately before and after `receive_sync_message` under
  the same guard.
- **`addr_via_own_relay` busy-spin livelock.** `tokio::time::timeout_at(...)`'s
  outer `Result` was checked with `.is_err()` alone, missing the `Ok(Err(_))`
  "watcher disconnected" case. A disconnected `Watcher` resolves `Ready` on
  every poll, so the loop spun a tokio worker at 100% CPU instead of waiting.
  Both the outer and inner `Result` are now matched explicitly.

### Task 9 (attachment transfer) — fixes made

- **Unbounded accumulation.** `request_attachment` had no size cap; a malicious
  or buggy peer could stream chunks until the requester OOMed. Now capped by
  `MAX_ATTACHMENT_SIZE` (100 MiB), checked *before* appending each chunk.
- **No space scoping at all.** `request_attachment` took a `space_id` that
  nothing checked — any connected peer could fetch any hash.
  `serve_attachment_request` now refuses a `space_id` the requester didn't
  claim in its `ControlHello`. **This is a best-effort filter, not an
  authorization boundary**, and the code says so: the claim is self-asserted
  with no membership proof, and the `attachments` map has no space dimension
  anyway. Real enforcement waits on MLS membership.
- **No read timeout** (final-review fix) — see the shared item below.
- **Chunking was untested at scale.** The original test content was smaller
  than one chunk, so it couldn't distinguish real chunking from a single giant
  frame. Added `attachment_larger_than_one_chunk_reassembles_correctly`.

### Task 10 (sequencer-routed joins) — fixes made

- **Envelope/payload `space_id` confusion.** The routing `space_id` came from
  the stream envelope while the payload carried its own, independently
  peer-controlled one. A peer could route a space-B join request through
  space-A's sequencer. The two must now agree or the request is dropped.
- **Unbounded forwarding loops.** Two devices at different MLS epochs can each
  elect the other as sequencer and forward the same request back and forth
  forever, each hop spawning a fresh connection and task. Now capped by
  `JoinRequest::MAX_HOPS`.
- **Self-dial loop.** If membership resolved the elected sequencer to this
  device's own `EndpointId` (while not matching its own `DeviceId`), the code
  would dial itself and reprocess its own forward. Now dropped as a malformed
  mapping.
- **Redundant dial in `join_via_invite`.** It dialed the inviter
  unconditionally even when already connected; now guarded, mirroring
  `handle_join_request`.
- **The forward path dials fresh.** This plan's closing note claimed the
  forward "silently drops" when not already connected to the sequencer. That
  was corrected during implementation *and* the closing note has now been
  corrected above: the shipped code calls `dial_and_spawn`. What remains is
  only the narrower liveness gap when that dial fails or its handshake doesn't
  land inside the fixed 300 ms settle delay.
- **No read timeout** (final-review fix) — see below.

### Shared final-review fix: read timeouts on peer-driven handlers

`serve_attachment_request`, `handle_join_request`, and `request_attachment` all
called `read_frame` with no deadline, so a peer that opened a stream and sent
nothing parked the handling task indefinitely. All three now go through
`read_frame_timeout`, which applies `PEER_READ_TIMEOUT` (30 s per frame) and
maps an elapsed deadline to `TransportError::Timeout`. This was never an
unbounded DoS — QUIC's own concurrent-stream limits bound it — but a per-read
deadline is cheap hardening. The bound is deliberately per-frame, not
per-transfer, so a large-but-progressing attachment is never cut off, and it is
deliberately *not* applied to `run_automerge_sync`'s reader task, whose stream
is long-lived and legitimately idle between changes.

### Known limitations deliberately deferred (not fixed in this milestone)

These are documented in the source at the point they bite, and pinned by tests
where the behavior is observable. **A test pinning a limitation going red is
the expected signal that someone fixed it** — update the test and these notes
together, don't work around it.

- **L1 — A space registered after a connection exists never syncs over that
  connection.** `exchange_digests` runs exactly once per connection,
  immediately post-handshake, and that snapshot is frozen for the connection's
  lifetime. Both the dialer's stream-opening decision and the accepter's epoch
  gate consult it, so `add_space` for a new `space_id` after a connection is up
  yields no sync over that connection — **in either direction, ever**, for as
  long as it lives (and roaming survival deliberately makes connections
  long-lived). Attachment transfer is hit by the same mechanism:
  `serve_attachment_request`'s `remote_spaces` check is built from that same
  snapshot, so an attachment request naming a late-registered space comes back
  as not-found. Pinned by
  `a_space_added_after_a_connection_exists_does_not_sync_over_it_known_limitation`.
  **Milestone 4's composition root MUST account for this** — either register
  every space *before* dialing or accepting any connection, or build
  mid-connection digest re-negotiation before relying on dynamic space
  registration. Re-negotiation is real design work (when to re-exchange, how to
  avoid redundant streams, how to tear down streams for removed spaces) and was
  out of scope for a review-fix pass.
- **L2 — `conns` is keyed by `EndpointId` alone, breaking `Connected`/
  `Disconnected` pairing under mutual/concurrent dial.** Two peers dialing each
  other at once establish two independent connections; both insert under the
  same key and both emit their own lifecycle events. A consumer can therefore
  see `Connected` twice with no `Disconnected` between, and — in rarer timing —
  the stored connection closing first evicts the `conns` entry while the other
  connection is still live, making `request_attachment` return `NotFound`
  against a healthy peer. (The `stable_id` guard only prevents the *other*
  direction of this race: it stops a task evicting a newer connection's entry,
  but cannot restore the survivor's entry once the stored one is gone.) Pinned
  by `mutual_dial_can_emit_connected_twice_without_a_disconnected_known_limitation`.
  A real fix means tracking possibly-multiple live connections per peer with
  refcounted event emission.
- **L3 — No shutdown/close API on `Transport`.** `Transport` exposes no
  `close`/`shutdown`, and its background accept-loop task holds its own clone
  of the `Arc`-backed `iroh::Endpoint`, so dropping a `Transport` value does
  not tear down its endpoint, its accept loop, or any live connection. This is
  already noted in the source (see
  `dialer_reports_disconnected_only_after_the_peer_actually_disconnects`, which
  drives a raw endpoint rather than a `Transport` precisely because of it); it
  is recorded here so it lives alongside the other deferred gaps. Milestone 4
  will need one for orderly app shutdown and for tests that want deterministic
  teardown.
- **L4 — Per-space streams are opened eagerly, not lazily, and are never
  closed when idle.** The transport spec calls for a per-space stream set to
  be opened lazily as spaces become active and closed when idle. Neither half
  of that is what shipped: `run_connection` opens an `AutomergeSync` stream
  **eagerly** for **every** shared, same-epoch space immediately at handshake
  time, regardless of activity (not lazily, only once a space becomes
  active); and once opened, a stream lives for the whole connection with
  nothing to close it when idle. `StreamManager`'s doc comment (Task 6) used
  to claim the lazy half was true — that was never the case for the shipped
  code and has been corrected. Worth addressing both halves in a follow-up if
  per-space stream count or resource usage becomes a real concern at scale.
- **L5 — Re-registering an existing `space_id` leaks its old sync tasks.**
  Calling `add_space` again with the same key replaces the `SpaceEntry`
  (segment and notify included) but does not cancel sync tasks already running
  against the *old* segment/notify; they keep running against stale state until
  their connection ends. Needs a real per-space cancellation mechanism
  (generation counter or cancellation token). Documented on `add_space`.
- **L6 — `addr_via_own_relay` is a test-topology workaround.** It
  unconditionally attaches the *local* endpoint's own relay URL to a *remote*
  peer's `EndpointAddr`, which is only correct in this crate's
  single-shared-relay test topology. In a real multi-relay deployment the local
  home relay is frequently not the remote peer's, and attaching it can seed a
  misleading address instead of letting real discovery resolve a bare
  `EndpointId`. Documented on the function itself.
- **L7 — Fixed-delay settles instead of awaiting `Connected`.**
  `join_via_invite` and `handle_join_request`'s forward path both
  `sleep(300 ms)` after dialing rather than awaiting the `Connected` event.
  Fine for this milestone's local-relay tests; a real implementation should
  await the event.


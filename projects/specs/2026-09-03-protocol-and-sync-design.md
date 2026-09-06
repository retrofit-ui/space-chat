# space-chat: wire protocol & sync engine design

Sub-project 1 of 5 in the space-chat initiative (protocol → sync engine → storage → transport → app shell/UI). This spec covers the data that two devices exchange and how they reconcile it. It does not cover connection establishment/NAT traversal (sub-project 4) or local persistence layout (sub-project 3), though both build directly on what's defined here.

## Source issue

GitHub Projects item 76527720, "HOTW: space chat" (draft issue on user `thenomadlad`'s "Werk" project):

> Create an app that's truly p2p in chatting and storing information — 1-1 messaging with sync, n-n messaging with sync between all clients, n-n-1 peer-server-at-a-price option.

The issue's listed initiatives (Matrix client/server, Matrix spec, element.io, Freenet/Locutus) were explicitly rejected in favor of a Tailscale-style direct-connection model — see Non-goals.

## Goals

- Two or more devices can exchange messages, reactions, deletions, and attachments for a shared conversation ("space") and converge on the same state regardless of connection order or offline periods.
- Content is end-to-end encrypted: no intermediary, including any future relay, can read message content.
- Works for both 1-1 and multi-party (group) spaces from v1, including membership changes over time.
- Attachments of arbitrary size don't block or degrade message sync.

## Non-goals (v1)

- **Not a multi-implementation interop spec.** Unlike Matrix or retrofit-ui's UI contract, this protocol is not designed for independent third-party implementations in other languages. There is one canonical Rust implementation (built on `automerge` and `openmls`), embedded via Tauri on every platform. Only the *rendering* layer (retrofit-ui components) has independent per-platform implementations. Revisit if a client that can't embed the Rust core (e.g. web-only) becomes a requirement.
- **No message editing.** Messages are immutable once created; correcting a mistake means deleting and resending. This was a deliberate simplification to avoid concurrent-edit conflict resolution, which gets especially confusing when the same person's two devices edit the same message independently.
- **No cross-device identity linking.** Each device is an independent identity (see Identity model). A person owning two devices is not a protocol-level concept in v1.
- **No fully decentralized commit ordering.** See "Commit sequencing" — v1 accepts a liveness limitation here rather than solving decentralized total ordering.
- **The optional relay/server tier** mentioned in the source issue ("n-n-1 peer server at a price") is not designed here. Content stays E2EE regardless of whether a relay exists later, because encryption happens above the transport layer (see Architecture).

## Architecture overview & identity model

- Every **device** is an independent MLS client with its own signing/HPKE keypair. There is no cross-device linking primitive in v1.
- A **space** is an MLS group. Membership changes go through standard MLS Add/Remove/Update proposals and Commits, which gives forward secrecy and post-compromise security for free via MLS's epoch ratchet.
- **Multi-device-per-person falls out for free without being a special case**: a second device is just another group member. Nothing in the protocol needs to know two members belong to the same human — that grouping, if wanted, is a UI-level concern layered on top, not a protocol concern.
- Underneath the MLS group's encrypted channel, each **epoch segment** has: (a) a locally-derived index entry (epoch number, segment's Automerge doc id, pointer to the previous segment) and (b) one Automerge document holding that epoch's content (messages, reactions, deletions, attachment references). A new epoch (membership change or periodic rotation) starts a new segment, chained via the index.
- **The index is not a wire concept.** The protocol only ever carries opaque MLS application messages; each device reconstructs its own epoch→segment mapping locally by processing the group's Commit history as it arrives.
- **Space-level metadata** (name, avatar, description) lives in its own small, continuously-merged Automerge document, separate from epoch-sharded content segments — renaming a space isn't tied to key rotation, so it shouldn't be forced through the same segment boundaries.

## Data model

Event/object types inside an epoch's content Automerge doc:

| Type | Shape | Notes |
|---|---|---|
| `Message` | content, sender device id, attachment refs | Immutable once created — no mutation, no merge conflict. |
| `Reaction` | target message (Automerge object id), actor, emoji | Targets use Automerge's native object addressing, not a reinvented message-id scheme. |
| `Delete` | tombstone boolean on the message object | The structural entry survives so peers know to hide it, even if content is scrubbed. Concurrent deletes OR together — trivial merge, unlike edits would have been. |
| `AttachmentRef` | content hash, size, mime, wrapped per-attachment content-encryption key | The blob itself is encrypted with a random key; only that key travels inside the MLS-encrypted message (envelope encryption). |

**Ephemeral signals** (typing indicators, read receipts) are explicitly excluded from Automerge entirely — they're high-frequency and have no lasting value, and persisting them would bloat every segment. They travel as their own lightweight message type over the same MLS channel but are never written into document history.

**Attachments** are content-addressed and fetched separately from message sync, per Automerge's own guidance to keep large binaries out of documents. The `AttachmentRef`'s hash + wrapped key travel inline with the message; the bytes move over a dedicated blob-transfer channel on demand.

## Why Automerge instead of a hand-rolled event DAG

The original framing considered a git/Matrix-style hash-linked event DAG with a hand-rolled reconciliation algorithm. Automerge (`automerge-rs`) is not a different choice — it's a concrete, battle-tested implementation of the same idea: every Automerge change has a hash and explicit hashes of its causal-parent changes, i.e. it already is a hash-linked DAG. Using it means:

- We inherit an efficient sync protocol (`SyncState` / bloom-filter-based head diffing) instead of building and debugging set reconciliation over a DAG ourselves — this is the kind of distributed-systems code that's easy to get subtly wrong (see git's own pack negotiation for how non-trivial this problem is).
- We use it in a mostly-append-only way (no message edits, per Non-goals) — the richer concurrent-merge machinery Automerge offers is available for cases where it's genuinely useful (e.g. space metadata) without being forced onto the message log.

Costs accepted knowingly:
- The wire format for sync is "whatever Automerge's change/sync-message encoding is," not something we define independently — acceptable specifically because of the "single canonical implementation" non-goal above.
- MLS group membership and the Automerge-visible conversation state are two separate sources of truth that must not be allowed to diverge. **Rule: MLS group membership is authoritative. Any participant list visible in application data is derived from successful MLS Commits, never independently editable.**

## Wire messages & sync flow

Message categories, once two devices have an authenticated encrypted channel (connection establishment is sub-project 4's concern):

- **MLS control** — Proposals/Commits/Welcomes for membership and key rotation.
- **Automerge sync** — bloom-filter/head-diff exchange for the current epoch's content doc and the metadata doc, carried as MLS application messages.
- **Gossip** — freshly created Automerge changes pushed immediately to connected peers (the "push" half of the hybrid model below).
- **Ephemeral** — typing/read receipts, best-effort, never persisted.
- **Attachment transfer** — chunked, content-hash-keyed blob request/response, on its own channel so large files never head-of-line-block message sync.

A thin outer envelope (CBOR via `ciborium`) tags **both which space and which of these five categories** a payload belongs to — `(space_id, category)`, not category alone. Two devices are frequently members of more than one shared space at once, and the category alone can't disambiguate which space's gossip or sync traffic a given frame belongs to. The payload itself is opaque at that layer (MLS ciphertext, or MLS-application-message-wrapped Automerge/gossip/attachment bytes). See the transport spec for how `(space_id, category)` maps onto QUIC streams.

**Sync flow on connect (hybrid gossip + reconcile — chosen over pure pull or pure push):**

1. Transport handshake completes (out of scope here).
2. Peers exchange a lightweight per-space digest (epoch number + Automerge doc heads) so effort isn't spent on spaces the other side isn't in.
3. For each shared space, the behind peer catches up on any missed MLS Commits (see Commit sequencing).
4. Once on a shared or historically-accessible epoch, peers run the Automerge sync-message exchange for the content and metadata docs.
5. Missing attachments are fetched (lazily or eagerly — deferred to the app-shell layer, sub-project 5).
6. While connected, gossip carries new changes live; any reconnect re-runs step 2 onward.

Pure pull-only reconciliation was rejected because it drops the low-latency feel of live conversation; pure push/gossip was rejected because it has no answer for peers reconnecting after being apart — reconciliation is the only correctness-critical path either way, so gossip is purely a latency optimization on top of it, never required for correctness.

## Commit sequencing (open problem, v1 answer)

MLS assumes a **Delivery Service** — something that gives every group member a single agreed order for Commits, so the epoch ratchet advances linearly with no forks. Centralized deployments use a server for this; we have none. This is a known open problem for decentralized MLS generally, not something specific we're missing.

**v1 answer:** each space elects a **sequencer** device via a deterministic rule (lowest device id among current members). Only the sequencer issues Commits; other members' membership-change requests route through it. This means:

- Messaging is unaffected if the sequencer is offline — gossip/reconcile don't depend on it.
- Membership changes (add/remove a device) stall if the sequencer is unreachable. This is a documented v1 liveness limitation, not a bug to silently work around.
- **This can be a permanent dead end, not just a stall.** Only the sequencer can issue a Commit — including the one that would remove the sequencer itself and hand the role to the next-lowest-device-id member. If the sequencer's device is permanently lost (destroyed, wiped, owner unreachable forever) rather than merely temporarily offline, **that space can never change membership again** — there is no bootstrapping path out, since the fix requires a Commit only the now-gone sequencer could issue. This is a materially sharper claim than "stalls," and worth stating as such rather than letting the milder wording imply it always self-resolves once the sequencer returns. Revisit if it proves too painful in practice — likely direction would be a proper decentralized sequencing scheme, treated as its own sub-project rather than folded in here.

## Error handling

- **Device compromise/removal** doesn't retroactively protect history the device already decrypted — inherent to E2EE, stated explicitly rather than left implied.
- **Sequencer offline**: membership changes stall; messaging continues normally.
- **Malformed/invalid MLS Commits** are rejected by the receiving device and not applied; the sender is flagged locally. No group-wide consequence required for v1.
- **Attachment fetch failures** retry with backoff; the attachment is marked unavailable in the UI rather than blocking the message itself from rendering.

## Testing strategy

Correctness here lives in multi-peer eventual-consistency behavior, which example-based unit tests won't catch. Uses the shared multi-actor `cucumber-rs`+`fantoccini` harness defined in the app-shell spec's Testing section — real per-actor stacks over a local iroh test relay, asserting through rendered UI wherever the outcome is user-visible. Scenarios:

```gherkin
Feature: Convergence under partition and concurrent membership changes

  Scenario: Both sides of a partition converge after reconnecting
    Given Alice and Bob are members of a space, then become partitioned from each other
    When Alice sends "from alice" and Bob sends "from bob" while partitioned
    And Alice and Bob reconnect
    Then Alice's conversation view shows both messages
    And Bob's conversation view shows both messages in the same order

  Scenario: Concurrent membership requests both route through the sequencer, no fork
    Given Alice, Bob, and Carol are members of a space with Alice's device as sequencer
    When Bob and Carol both request adding a new device at nearly the same time
    Then exactly one add is applied first and the other follows it
    And all three members' conversation views show the same final membership
```

- Out-of-order Commit and gossip delivery — same harness, injecting deliberate reordering at the transport step rather than a separate mechanism.
- Automerge sync convergence under randomized message/attachment-ref creation (property-based) — this one has no single user-visible outcome to assert in the UI, so it stays a lower-level property test directly against `space-chat-core`, run alongside the Gherkin suite rather than folded into it.

## Open questions carried forward

- Eager vs. lazy attachment fetch policy — deferred to app-shell/UX design (sub-project 5).
- Whether the sequencer liveness limitation needs a real fix before general availability, or is acceptable long-term for typical space sizes.
- Multi-device-per-person UX (grouping devices under one visual identity) — a UI-layer concern once retrofit-ui rendering is designed (sub-project 5), not a protocol change.

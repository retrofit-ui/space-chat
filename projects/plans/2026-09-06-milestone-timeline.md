# space-chat: milestone timeline

Sequences the four sub-project specs into build order. Sizing is rough relative effort for one engineer (S/M/L/XL), not committed calendar time — there's no velocity data for this codebase yet, and giving false-precision week counts would be worse than none.

## Dependency shape

```
Milestone 1: Protocol & sync   (space-chat-core, space-chat-openmls)
        │
        ├──────────────┬──────────────┐
        ▼              ▼              │
Milestone 2:      Milestone 3:        │
Storage           Transport           │
(space-chat-      (space-chat-        │
storage-redb,     transport-*)        │
space-chat-                           │
search-tantivy)                       │
        │              │              │
        └──────┬───────┘              │
               ▼                      │
       Milestone 4: App shell & UI ◄──┘
       (space-chat-app, Tauri, retrofit-ui renderer)
```

Storage and Transport both depend only on Milestone 1's domain types and wire envelope format being fixed — not on each other — so they can proceed in parallel (different engineers/sessions) once Milestone 1 lands. App shell needs all three functioning together, since it's the layer that actually renders synced, persisted data.

## Milestone 1: Protocol & sync engine — size L

Spec: [protocol & sync design](../specs/2026-09-03-protocol-and-sync-design.md)

Produces: `space-chat-core` (domain types, segment/epoch management, Automerge integration, sequencer logic) and `space-chat-openmls` (MLS group/membership via OpenMLS). Testable as a headless library — two in-process `space-chat-core` instances can create a space, exchange messages via direct Automerge sync-message calls (no real network yet), and converge. This is the detailed plan being written now (see below).

**Exit criteria**: a Rust integration test can create a 3-member space, have each "device" (in-process instance) send messages, and assert all three converge on identical conversation state, including a simulated partition-and-reconnect.

## Milestone 2: Storage engine — size M

Spec: [storage engine design](../specs/2026-09-03-storage-engine-design.md)

Produces: `space-chat-storage-redb`, `space-chat-search-tantivy`, flat-file segment/attachment stores, the `Projection`/watermark catch-up mechanism, mark-and-sweep GC. Depends on Milestone 1's segment/epoch types being stable.

**Exit criteria**: Milestone 1's headless integration test still passes with real persistent storage substituted for in-memory state, plus a kill-and-restart test proving watermark catch-up converges, plus a GC test proving the cross-peer grace-window behavior from the spec.

## Milestone 3: Transport & discovery — size L

Spec: [transport design](../specs/2026-09-05-transport-design.md)

Produces: `iroh`-based networking, `(space_id, category)` stream mapping, invite/pairing flow, sequencer-routed join requests. Depends on Milestone 1's wire envelope format, not on Milestone 2.

**Exit criteria**: two separate OS processes (not in-process function calls) converge over a local iroh test relay, including the multi-hop-for-messages/not-for-attachments distinction and roaming survival.

## Milestone 4: App shell & UI — size XL

Spec: [app shell & UI design](../specs/2026-09-05-app-shell-ui-design.md)

Produces: `space-chat-app` (composition root), the Tauri IPC/event layer, the local retrofit-ui renderer (`conversation` kind + copied `card`/`text`/`flex`/`grid`), the adapted live-spec patching mechanism, and the Android foreground-service/iOS-limitation handling from the background-delivery section. Depends on Milestones 1–3 all being usable together.

**Exit criteria**: the full `cucumber-rs`+`fantoccini` multi-actor harness (defined in the app-shell spec's Testing section) passes against a real, rendered Tauri app — this is the first point at which the shared testing harness is fully exercisable end to end, since it needs real UI to assert against.

## Sizing rationale (why L / M / L / XL)

- **Protocol & sync is L, not XL**, because MLS (via OpenMLS) and CRDT sync (via Automerge) are both delegated to mature libraries rather than built from scratch — the actual new work is the domain model, segment/epoch chaining, and sequencer logic wrapping them, not the cryptography or CRDT algorithm itself.
- **Storage is M** — three storage backends behind already-fully-specified trait boundaries, with no novel algorithmic risk; the GC design and watermark mechanism are already fully worked out in the spec, this milestone is implementing what's already decided.
- **Transport is L** — `iroh` absorbs the hardest part (NAT traversal/relay), but stream-per-`(space, category)` lifecycle management, the sequencer-routed invite flow, and getting real two-process integration tests working (not just unit tests) is genuine new integration work.
- **App shell is XL** — it's the only milestone touching a second language/runtime (SolidJS/TypeScript) and a second framework's extension pattern (retrofit-ui's local-renderer convention), on top of needing the full three-platform Tauri build/packaging story and the Android/iOS background-delivery handling. This is where most of the genuinely unfamiliar-to-the-team work concentrates.

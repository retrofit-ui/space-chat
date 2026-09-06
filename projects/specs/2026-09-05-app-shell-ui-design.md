# space-chat: app shell & UI design

Sub-project 5 of 5 in the space-chat initiative. Covers how `space-chat-core`'s domain logic reaches a rendered UI via Tauri and retrofit-ui, on every target platform. Builds on all four prior specs: [protocol & sync](2026-09-03-protocol-and-sync-design.md), [storage](2026-09-03-storage-engine-design.md), [transport](2026-09-05-transport-design.md).

## Goals

- Every platform (desktop and mobile) is a Tauri app rendering the same retrofit-ui-based frontend — one UI implementation, not one per platform.
- The primary conversation view updates the instant new data arrives (a sent or received message appears immediately), not on a polling interval.
- `space-chat-core` stays free of any UI-shaped concept — spec generation is a separate, thin layer, not core domain logic.

## UI architecture: specs in, retrofit-ui renders

Per retrofit-ui's own model, the Rust core (by way of a thin spec-generation layer, not core itself) declares a spec; a bundled SolidJS renderer draws it. There's no backend/frontend network boundary here — both live in one Tauri process — but the contract discipline is kept anyway: the spec-generation layer never reaches into rendering concerns, and the renderer never reaches back into domain logic. "Independent implementations per platform," per the earlier framing question, means independent retrofit-ui renderer components tuned to each platform's visual conventions, delivered through Tauri's webview everywhere — not a different UI toolkit per platform.

## Two gaps found in retrofit-ui-as-it-exists, and how this project handles them

Investigating before assuming retrofit-ui's current shape would just fit surfaced two real gaps:

**1. No spec kind shaped for a chat message.** `TimelineSpec`/`TimelineEvent` (`packages/core/src/types/resource-spec.ts`) is the closest existing primitive, but its fields (`timestamp`, `title`, `description`, `variant`, `icon`) are shaped for an audit-log/activity-feed, not a message with a sender, attachments, reactions, and a reply-to relationship.

**2. No live/streaming update mechanism.** retrofit-ui's shipped model is fetch-once, fully-populated response, no second fetch — correct for a CRUD dashboard page load, not shaped for a conversation view where a new message must appear the moment it arrives via gossip.

**Resolution for both, following an existing convention already established across this workspace, not a new one invented for space-chat:** `tenju-tofu` and `chalk-app` (the other two Tauri+retrofit-ui apps in this workspace) both already hand-roll their own top-level spec renderer that supersedes retrofit-ui's default one, reimplementing retrofit-ui's own structural kinds (`card`, `text`, `flex`, `grid`) locally so their recursion can interleave custom, app-specific kinds (`product-grid`, `chalk-graph`, `chalk-sets`) that retrofit-ui's built-in `SpecRenderer` has no way to know about, falling back to the real `SpecRenderer` for anything not locally handled. **Custom kinds never touch `@retrofit-ui/core`** in either app — they're local types matched in a local `Switch`, promoted to the shared package only once a pattern proves out across multiple apps.

space-chat follows the same pattern:
- A local `conversation` spec kind (never added to `@retrofit-ui/core`), with a bespoke message-list component behind it, alongside space-chat's own copies of `card`/`text`/`flex`/`grid` (starting from chalk-app's more complete versions) and a fallback to the real `SpecRenderer`.
- The live-update mechanism is adapted from `tenju-tofu/src-tauri/src/live_spec.rs` — a real, already-working prototype explicitly tied to retrofit-ui issue #141 (incremental spec patching): a versioned spec that produces an RFC 6902 JSON Patch diff when a client quotes back the version it last saw, falling back to a full resend if the client has fallen too far behind. Two adaptations from the original: (a) **push over Tauri events instead of HTTP polling** — the prototype's 3-second poll interval fits its cart/checkout use case, not chat's need for instant delivery; (b) **a short rolling version-history window (last 5–10 versions) instead of tolerating only one version back** — the original's own comment flags this limitation honestly, and chat's higher update frequency (a burst of several gossip messages) would exceed a one-version window often enough to matter.
- **All three apps are explicitly case studies feeding back into the shared framework**, not final destinations for these patterns. The duplication (three apps each locally reimplementing `card`/`text`, and space-chat plus tenju-tofu each carrying a copy of the live-spec prototype) is intentional at this stage — it's what eventually justifies promoting a real extensibility point and real incremental-patching support into `@retrofit-ui/core` itself, once the same need has shown up independently more than once. Not attempted now.

## Composition & Tauri IPC

- **Spec generation is a thin layer between `space-chat-core` and the frontend, not part of core.** It turns core's domain data (messages, reactions, membership) into `ConversationSpec` values, applying retrofit-ui's "do the work on the server" formatting — precomputed relative timestamps, resolved sender display names, read state — the same way a real backend would, except "the server" here is this device's own process. This mirrors retrofit-ui's own `core`/`builder-zod` split and keeps `space-chat-core` free of any UI-shaped concept, consistent with its role as the ports-and-adapters domain crate.
- **This layer is the third consumer of the storage spec's `Projection` change feed**, alongside `ListingIndex` and `SearchIndex` — it subscribes to the same "new segment change" signal, but only for whichever single conversation is currently active, per the bounded-window/active-view gating below. It reads its ordered message window from `ListingIndex` directly rather than re-deriving ordering from raw segments — `ListingIndex` already owns that ordering, and duplicating it here would be pure waste.
- **Tauri commands** (frontend → Rust): `open_conversation(id)` — returns the initial full spec + version and marks the conversation as actively viewed; `close_conversation(id)`; `send_message`; `react`; `delete_message`; `create_space`; `generate_invite`; `join_via_invite`; `fetch_older_page` (using the storage spec's ordered listing keys for pagination).
- **Only actively-viewed conversations carry a live `LiveSpec` instance.** A user may belong to many spaces but is only looking at one at a time; others need just lightweight unread/listing metadata, not a continuously diffed spec — consistent with the bounded-window approach from the storage design.
- **Tauri events** (Rust → frontend): a spec-patch event per actively-viewed conversation, emitted the instant `LiveSpec::update()` produces a new version, plus connection-status/sync-progress events for UI affordances like "reconnecting."
- **Attachments bypass IPC/JSON entirely.** A `ConversationSpec`'s attachment reference carries a URL under a custom Tauri protocol (e.g. `spacechat://attachment/<hash>`) backed directly by the attachment blob store, so the webview loads bytes directly rather than them being base64-encoded through the spec payload.

## Background delivery (mobile)

Stress-testing the other four specs against a concrete offline/reconnect scenario surfaced a gap none of them addressed: on mobile, a backgrounded or killed app can't receive gossip at all, since the OS tears down its process (and any open iroh connection) after a short time. The standard fix — a silent push notification via APNs/FCM to wake the app — requires a server-side component holding platform push credentials, even in a content-blind form that never sees message content or sender identity. **Decision: space-chat stays fully server-free, including for this case.** No push relay exists. The consequence differs materially by platform, and is documented per-platform rather than as one blanket "delayed on mobile" statement:

- **Desktop (Windows/macOS/Linux): no gap.** A Tauri desktop app can run as an ordinary background/tray process holding a live iroh connection indefinitely — nothing here is mobile's OS-level suspension model, so gossip/reconcile work exactly as designed with the app simply running in the background.
- **Android: mitigated via a foreground service, at a real but bounded cost.** Android allows an app to trade a persistent, user-visible notification for guaranteed background execution, including a held network connection — an established, Play-Store-legitimate pattern (VPN apps, music players use it for the same reason). Adopting this for space-chat's Android build keeps a live iroh connection open while backgrounded, with no server component, at the cost of extra battery use and an always-visible "space-chat is running" notification. This is a real mitigation, not a workaround-in-name-only — worth building.
- **iOS: no server-free fix exists, and this is a hard platform wall, not an engineering gap to close later.** Apple does not permit arbitrary persistent background networking for non-VoIP apps. The one background mode that wakes an app on demand (`PushKit`, VoIP-only) still routes through APNs, so it doesn't avoid the server dependency either, and using it outside real VoIP calling risks App Store rejection. Given the server-free decision above, **messages will only arrive on iOS when the user has the app open**, plus whatever iOS's own short, infrequent, unreliable background-refresh windows happen to deliver — not something to design reconcile timing around. This should be surfaced to users directly (e.g. "open the app to receive new messages on iPhone") rather than left as a silent gap they discover on their own.

This mirrors the same "gossip is a latency optimization, reconcile is the correctness guarantee" pattern already established in the protocol spec: nothing here changes correctness — a message sent while a peer's iOS app is closed is still fully delivered the moment they next open it and reconcile runs. What's lost is only the "arrives instantly without opening the app" property, and only on iOS specifically.

## Error handling

- **Backend restart needs no new mechanism.** A restarted core's version counter resets to zero; any version the frontend then quotes is "unknown" to the new instance, which the existing `diff_since` logic already resolves via a full resend (`PatchResponse::Full`). The frontend's patch handler already accepts an unrequested full response, since the original prototype's fallback path already does this for its own reasons (client too far behind).
- **Spec-generation failure for one conversation** (e.g. a malformed segment) should degrade that conversation's view to an error state, not crash the whole app shell — isolate failures per-conversation the same way the sync engine isolates per-space state.

## Testing

- Golden path: open a conversation, send a message, verify a patch event arrives and the local renderer reflects it without a full resend.
- Verify the custom-kind fallback path: a spec containing only standard retrofit-ui kinds still renders correctly through the real `SpecRenderer` fallback, not just through space-chat's local kinds.
- Attachment loading via the custom protocol handler, including the not-yet-fetched case (per the protocol spec's lazy-fetch policy).
- Backend-restart recovery: kill and restart the core mid-session, verify the frontend recovers via full resend without a manual reload.

## Open questions carried forward

- Eager vs. lazy attachment fetch policy (from the protocol spec) still needs a concrete answer here, since it determines what the custom protocol handler does on a cache miss — fetch synchronously and block the image load, or return a placeholder and emit an event when the fetch completes.
- Exact rolling-window size for live-spec version history (5–10 proposed) is a tuning parameter, not a design fork.
- When and how patterns get promoted from space-chat/tenju-tofu/chalk-app's local renderers into `@retrofit-ui/core` is explicitly out of scope for this spec — a future decision made by looking across all three case studies together, not something to design in advance of having three real examples.

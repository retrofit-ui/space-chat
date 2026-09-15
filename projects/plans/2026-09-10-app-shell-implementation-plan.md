# App Shell & UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `space-chat-app` — a real, clickable Tauri desktop app (Windows/macOS/Linux) that embeds `space-chat-core`, generates a local `conversation` retrofit-ui spec kind from synced/persisted data, pushes live updates to the frontend via adapted RFC 6902 JSON-Patch diffing over Tauri events, and loads attachments through a custom `spacechat://` protocol handler — proven end-to-end by a real, rendered-UI multi-actor test (`cucumber-rs` + `fantoccini`).

**Architecture:** `space-chat-app` is a new Cargo workspace member (`space-chat-app/src-tauri`) plus a sibling SolidJS frontend package (`space-chat-app/`), following the same `<app>/` (frontend) + `<app>/src-tauri/` (Rust) split already used by `tenju-tofu` in this workspace. A thin spec-generation layer (`conversation_spec.rs`, `spec.rs`) turns `space-chat-core` domain data into local `ViewSpec` values — never touching `@retrofit-ui/core`, per this workspace's established convention (`tenju-tofu`/`chalk-app`). A `LiveSpec` type adapted from `tenju-tofu/src-tauri/src/live_spec.rs` (RFC 6902 diffing, rolling version-history window instead of the prototype's single-previous-version, pushed over Tauri events instead of polled over HTTP) gives active conversations instant updates. The frontend mirrors this with its own local `SpaceChatSpecRenderer` (copying `card`/`text`/`flex`/`grid` from `chalk-app`'s more complete version, per this workspace's convention) plus a bespoke `conversation` kind, falling back to retrofit-ui's real `SpecRenderer` for anything else.

**Scoping decision (read before anything else): this plan is desktop-only.** The user asked explicitly for a first pass that produces a genuinely running, clickable app on Windows/macOS/Linux, with mobile polish deferred rather than silently dropped. Every task below builds real desktop functionality — real Tauri IPC/events, real JSON-Patch diffing, a real local renderer, a real custom protocol handler for attachments. Nothing here is mocked out for the desktop path. What is explicitly **not** in this plan:

### Deferred, not in this plan

- **Android foreground-service background-delivery mitigation** (app-shell spec's "Background delivery (mobile)" section) — the persistent-notification/held-connection pattern for keeping gossip alive while backgrounded on Android. Not started.
- **iOS background-delivery limitation handling/messaging** (same section) — the "open the app to receive new messages on iPhone" user-facing messaging for iOS's hard platform wall. Not started.
- **Actual mobile (Android/iOS) Tauri build targets/packaging** — this plan produces a desktop Tauri app only. No mobile `tauri.conf.json` target, no app store packaging, no mobile-specific webview configuration.

These are real gaps, not oversights — call them out explicitly if reviewing this plan against the full app-shell spec, which covers all of them.

### Two more gaps found while writing this plan, handled the same way (flagged, not silently dropped)

- **`space-chat-openmls` (MLS-backed group membership/encryption) does not exist on disk.** The milestone timeline lists it as a Milestone 1 deliverable, but Milestone 1's actual executed plan explicitly deferred it ("MLS/OpenMLS integration is not part of this plan... Write that as its own plan once this one is merged" — `projects/plans/2026-09-06-protocol-sync-implementation-plan.md`, closing note). `space_chat_core::domain` has no `Space`/membership/invite types at all yet. This plan cannot build real MLS-backed membership against a crate that doesn't exist, so — mirroring exactly how this plan handles the still-in-progress transport crate (see Task 7) — it defines its own minimal, explicitly-placeholder `SpaceMembership` (Task 3): a plaintext, unauthenticated membership list good enough to drive the sequencer and make `create_space`/`generate_invite`/`join_via_invite` real, clickable commands, with zero cryptographic protection. This must be replaced wholesale once `space-chat-openmls` exists — it is not a foundation to build on, it is scaffolding to demo against.
- **`space_chat_core::domain::Message` has no timestamp field.** The app-shell spec calls for "precomputed relative timestamps" in the generated spec, but nothing in `Message` records when a message was sent, and Milestone 1's own code comments explicitly punt on extracting timestamps from Automerge's internal change metadata as "exactly the kind of decision [a later] storage design should make deliberately, not something to improvise here" (`Segment::latest_change`'s doc comment). Rather than reopening already-merged, already-shared `space-chat-core` code (which the concurrently-developed storage plan also touches) to bolt on a field every existing call site would need updating for, this plan keeps a small `observed_at`-timestamp side-table entirely at the app-shell layer (Task 4), stamped at first-local-observation time. This is honestly a different thing from "when the sender sent it" — it's "when this device first saw it" — and is documented as such where it's implemented, not silently presented as more precise than it is.

## Global Constraints

- `space-chat-core` stays free of any UI-shaped concept — spec generation (`spec.rs`, `conversation_spec.rs`) is a thin separate layer in `space-chat-app`, not core. This plan does not modify `space-chat-core` at all.
- The local `conversation` spec kind (and the local `card`/`text`/`flex`/`grid` reimplementations) never get added to `@retrofit-ui/core` — they are local to `space-chat-app`'s frontend, matching the `tenju-tofu`/`chalk-app` convention this workspace has already established twice.
- `ConversationSpec` generation reads its ordered message window from `space_chat_core::storage::ListingIndex::page` (Milestone 2), never re-derived from raw `Segment` iteration order.
- Only actively-viewed conversations (those a Tauri window currently has open via `open_conversation`) get a live `LiveSpec` instance; every other conversation a user belongs to gets only lightweight listing/unread metadata, read directly from `ListingIndex`.
- Attachments never travel through Tauri IPC/JSON. A `ConversationSpec`'s attachment reference is a `spacechat://attachment/<hex hash>` URL, resolved by a custom Tauri protocol handler backed directly by `AttachmentBlobStore`.
- **Rolling live-spec version-history window: 8 versions.** (The app-shell spec calls this a tuning parameter in the 5–10 range, not a design fork — see Task 6 for the reasoning: 8 sits in the middle of the proposed range, comfortably covers a burst of several gossip messages arriving between two frontend polls of a Tauri event queue, without holding an unbounded/large amount of prior spec state in memory per active conversation.)
- **Attachment cache-miss policy: lazy fetch with a placeholder, not a blocking synchronous fetch.** (See Task 13 — the custom protocol handler returns a placeholder image immediately on a cache miss and emits an `attachment-ready:<hash>` event when the real bytes land, rather than blocking the webview's resource load on a network round-trip.)
- **Backend-restart recovery needs no new mechanism.** A freshly-constructed `LiveSpec` starts at version 0 with no `previous` entry, so any version a frontend quotes back (from before a restart) is unrecognized and `diff_since` falls through to `PatchResponse::Full` — this plan's Task 6 includes a test asserting this property directly against the adapted `LiveSpec` code, not just asserting it in prose.
- Spec-generation failure for one conversation degrades only that conversation's view to an error-state spec (a local `conversation-error` kind), never a command-level error that would crash or block the rest of the app shell — see Task 9.
- **External-crate API names in this plan (`tauri` 2.x, `redb` 2.x, `json-patch`/`fast-json-patch`, `cucumber`, `fantoccini`) reflect what's known at writing time. Verify exact method signatures against `docs.rs`/each crate's current docs for the pinned version before implementing — do not trust this plan's code over the real crate docs if they've drifted.** `space-chat-core`/`space-chat-storage-*` signatures referenced from the (not-yet-merged) storage plan are authoritative for this plan's purposes per that plan's own text.
- **Tasks 5 and 8 onward depend on Milestone 2 (`projects/plans/2026-09-10-storage-engine-implementation-plan.md`) being merged** — they reference `space_chat_core::storage::{ListingEntry, ListingIndex, AttachmentMetadataStore}` and the `space-chat-storage-files`/`space-chat-storage-redb`/`space-chat-search-tantivy` crates directly. Tasks 1–4, 6, and 7 have no such dependency and can be built and tested standalone against Milestone 1 alone.
- **Task 7's `NetworkService` trait is this plan's own invention, not Milestone 3's actual API** (`projects/plans/2026-09-05-transport-implementation-plan.md` did not exist, or was still being written by a different engineer, at the time this plan was written — see Task 7's closing note). It must be reconciled against the real transport crate before `space-chat-app` is wired to real `iroh` networking.
- **Membership is a placeholder** (Task 3) — plaintext, unauthenticated, not gossiped between peers, and must be replaced once `space-chat-openmls` exists. Every task that touches it says so again at the point of use, not just here.

---

### Task 1: Scaffold `space-chat-app` — Tauri shell + SolidJS frontend, builds and runs

**Files:**
- Create: `space-chat-app/src-tauri/Cargo.toml`
- Create: `space-chat-app/src-tauri/src/lib.rs`
- Create: `space-chat-app/src-tauri/src/main.rs`
- Create: `space-chat-app/src-tauri/tauri.conf.json`
- Create: `space-chat-app/src-tauri/build.rs`
- Create: `space-chat-app/package.json`
- Create: `space-chat-app/vite.config.ts`
- Create: `space-chat-app/tsconfig.json`
- Create: `space-chat-app/index.html`
- Create: `space-chat-app/src/index.tsx`
- Create: `space-chat-app/src/App.tsx`
- Modify: workspace root `Cargo.toml` — add `"space-chat-app/src-tauri"` to `members`
- Test: `space-chat-app/src-tauri/src/lib.rs` (inline `#[cfg(test)]`), plus a manual smoke run

**Interfaces:**
- Consumes: nothing from other tasks (first task).
- Produces: a `greet(name: &str) -> String` Tauri command proving the IPC round-trip works, and the crate/package skeleton every later task adds modules and frontend files to. `space_chat_app_lib::run()` is the entry point later tasks extend with more commands/state/protocol handlers.

- [ ] **Step 1: Create the Rust crate and add it to the workspace**

```bash
mkdir -p space-chat-app/src-tauri/src
mkdir -p space-chat-app/src-tauri/icons
```

```toml
# space-chat-app/src-tauri/Cargo.toml
[package]
name = "space-chat-app"
version = "0.1.0"
description = "space-chat desktop app shell"
authors = ["space-chat"]
edition = "2021"
default-run = "space-chat-app"

[lib]
name = "space_chat_app_lib"
crate-type = ["staticlib", "cdylib", "rlib"]

[build-dependencies]
tauri-build = { version = "2", features = [] }

[dependencies]
space-chat-core = { path = "../../space-chat-core" }
tauri = { version = "2", features = [] }
tauri-plugin-opener = "2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "sync", "macros", "time", "net"] }
uuid = { version = "1", features = ["v4", "serde"] }

[dev-dependencies]
tempfile = "3"
```

```rust
// space-chat-app/src-tauri/build.rs
fn main() {
    tauri_build::build()
}
```

Modify the workspace root `Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = ["space-chat-core", "space-chat-app/src-tauri"]
```

- [ ] **Step 2: Write the failing test for the smoke command**

```rust
// space-chat-app/src-tauri/src/lib.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greet_includes_the_given_name() {
        assert_eq!(greet("Alice"), "Hello, Alice! space-chat is running.");
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd space-chat-app/src-tauri && cargo test greet_includes_the_given_name`
Expected: FAIL — `greet` not defined.

- [ ] **Step 4: Implement the Tauri entry point with the smoke command**

```rust
// space-chat-app/src-tauri/src/lib.rs (above the tests module)
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {name}! space-chat is running.")
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![greet])
        .run(tauri::generate_context!())
        .expect("error while running space-chat-app");
}
```

```rust
// space-chat-app/src-tauri/src/main.rs
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    space_chat_app_lib::run();
}
```

```json
// space-chat-app/src-tauri/tauri.conf.json
{
  "$schema": "https://schema.tauri.app/config/2",
  "productName": "space-chat",
  "version": "0.1.0",
  "identifier": "com.spacechat.app",
  "build": {
    "beforeDevCommand": "pnpm dev",
    "devUrl": "http://localhost:1420",
    "beforeBuildCommand": "pnpm build",
    "frontendDist": "../dist"
  },
  "app": {
    "windows": [
      { "title": "space-chat", "width": 1000, "height": 700 }
    ],
    "security": { "csp": null }
  },
  "bundle": {
    "active": true,
    "targets": ["deb", "appimage", "msi", "dmg"],
    "icon": ["icons/32x32.png", "icons/128x128.png", "icons/128x128@2x.png", "icons/icon.icns", "icons/icon.ico"]
  }
}
```

Note: `"targets"` deliberately excludes mobile-only bundle types, since Deferred item 3 (mobile build targets) is out of scope for this plan.

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd space-chat-app/src-tauri && cargo test greet_includes_the_given_name`
Expected: PASS

- [ ] **Step 6: Scaffold the SolidJS frontend**

```json
// space-chat-app/package.json
{
  "name": "space-chat-app",
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview",
    "test": "vitest run",
    "tauri": "tauri"
  },
  "dependencies": {
    "@retrofit-ui/core": "^0.2.0",
    "@retrofit-ui/spa-solid-shoelace": "^0.3.0",
    "@shoelace-style/shoelace": "^2.20.0",
    "fast-json-patch": "^3.1.1",
    "solid-js": "^1.9.3",
    "@tauri-apps/api": "^2"
  },
  "devDependencies": {
    "@tauri-apps/cli": "^2",
    "typescript": "~5.6.2",
    "vite": "^6.0.3",
    "vite-plugin-solid": "^2.11.0",
    "vitest": "^2.1.0",
    "@solidjs/testing-library": "^0.8.10",
    "@testing-library/jest-dom": "^6.5.0",
    "jsdom": "^25.0.1"
  }
}
```

```typescript
// space-chat-app/vite.config.ts
import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

export default defineConfig({
  plugins: [solid()],
  clearScreen: false,
  server: {
    port: 1420,
    strictPort: true,
  },
  test: {
    environment: "jsdom",
    globals: true,
  },
});
```

```json
// space-chat-app/tsconfig.json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "preserve",
    "jsxImportSource": "solid-js",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "types": ["vitest/globals"]
  },
  "include": ["src"]
}
```

```html
<!-- space-chat-app/index.html -->
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <title>space-chat</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/index.tsx"></script>
  </body>
</html>
```

```tsx
// space-chat-app/src/index.tsx
import { render } from "solid-js/web";
import App from "./App";

render(() => <App />, document.getElementById("root")!);
```

```tsx
// space-chat-app/src/App.tsx
import { createSignal, type Component } from "solid-js";
import { invoke } from "@tauri-apps/api/core";

const App: Component = () => {
  const [greeting, setGreeting] = createSignal("");

  const sayHello = async () => {
    const result = await invoke<string>("greet", { name: "Alice" });
    setGreeting(result);
  };

  return (
    <div>
      <h1>space-chat</h1>
      <button onClick={sayHello}>Say hello</button>
      <p data-testid="greeting">{greeting()}</p>
    </div>
  );
};

export default App;
```

- [ ] **Step 7: Verify the app builds and the backend test suite passes**

Run: `cd space-chat-app/src-tauri && cargo build && cargo test`
Expected: builds cleanly, `greet_includes_the_given_name` passes.

Run: `cd space-chat-app && pnpm install && pnpm build`
Expected: Vite build succeeds (frontend has no backend to talk to at build time, but TypeScript compiles and bundles cleanly).

- [ ] **Step 8: Commit**

```bash
git add space-chat-app Cargo.toml Cargo.lock
git commit -m "feat(app): scaffold space-chat-app Tauri shell and SolidJS frontend"
```

---

### Task 2: Local `ViewSpec` Rust types (`spec.rs`)

**Files:**
- Create: `space-chat-app/src-tauri/src/spec.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — add `pub mod spec;`
- Test: `space-chat-app/src-tauri/src/spec.rs` (inline)

**Interfaces:**
- Consumes: nothing new (plain `serde`).
- Produces: `ViewSpec` (enum: `Conversation(ConversationSpec)`, `ConversationError(ConversationErrorSpec)`, `Card(CardSpec)`, `Text(TextSpec)`, `Flex(FlexSpec)`, `Grid(GridSpec)`), `ConversationSpec { space_id, title, messages: Vec<MessageSpec>, has_more_older: bool }`, `ConversationErrorSpec { space_id: String, message: String }`, `MessageSpec { id, sender_id, sender_name, content, relative_time, attachments: Vec<AttachmentSpec>, reactions: Vec<ReactionSpec>, deleted: bool }`, `AttachmentSpec { url, mime, size }`, `ReactionSpec { emoji, actor_name }`, `CardSpec { header: Option<String>, children: Vec<ViewSpec> }`, `TextSpec { content, variant: Option<String> }`, `FlexSpec { direction: Option<String>, gap: Option<String>, children: Vec<ViewSpec> }`, `GridSpec { columns: Option<u32>, gap: Option<String>, children: Vec<ViewSpec> }`. Serialized with `#[serde(tag = "kind", rename_all = "kebab-case")]` so the JSON shape is `{ "kind": "conversation", ...fields }`, matching retrofit-ui's own `{kind, ...fields}` convention (`packages/core/src/types/page.ts`'s `ViewSpec` union) so the frontend's local renderer (Task 14) can `Switch` on `kind` the same way `chalk-app`'s `ChalkSpecRenderer` does. Every later Rust task that builds a spec value builds one of these types.

- [ ] **Step 1: Write the failing test**

```rust
// space-chat-app/src-tauri/src/spec.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_spec_serializes_with_kind_tag_and_flattened_fields() {
        let spec = ViewSpec::Conversation(ConversationSpec {
            space_id: "space-1".to_string(),
            title: "General".to_string(),
            messages: vec![MessageSpec {
                id: "msg:abc".to_string(),
                sender_id: "01".to_string(),
                sender_name: "Alice".to_string(),
                content: "hello".to_string(),
                relative_time: "just now".to_string(),
                attachments: vec![],
                reactions: vec![],
                deleted: false,
            }],
            has_more_older: false,
        });

        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["kind"], "conversation");
        assert_eq!(json["space_id"], "space-1");
        assert_eq!(json["messages"][0]["content"], "hello");
    }

    #[test]
    fn card_spec_nests_children_recursively() {
        let spec = ViewSpec::Card(CardSpec {
            header: Some("Header".to_string()),
            children: vec![ViewSpec::Text(TextSpec {
                content: "body".to_string(),
                variant: None,
            })],
        });

        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["kind"], "card");
        assert_eq!(json["children"][0]["kind"], "text");
        assert_eq!(json["children"][0]["content"], "body");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd space-chat-app/src-tauri && cargo test conversation_spec_serializes`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement the spec types**

```rust
// space-chat-app/src-tauri/src/spec.rs (above the tests module)
use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ViewSpec {
    Conversation(ConversationSpec),
    ConversationError(ConversationErrorSpec),
    Card(CardSpec),
    Text(TextSpec),
    Flex(FlexSpec),
    Grid(GridSpec),
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConversationSpec {
    pub space_id: String,
    pub title: String,
    pub messages: Vec<MessageSpec>,
    pub has_more_older: bool,
}

/// Rendered when Task 9's spec-generation call fails for this one
/// conversation -- per the app-shell spec's error-handling section, a
/// malformed segment degrades only this conversation's view, not the whole
/// app shell.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConversationErrorSpec {
    pub space_id: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MessageSpec {
    pub id: String,
    pub sender_id: String,
    pub sender_name: String,
    pub content: String,
    pub relative_time: String,
    pub attachments: Vec<AttachmentSpec>,
    pub reactions: Vec<ReactionSpec>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AttachmentSpec {
    pub url: String,
    pub mime: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ReactionSpec {
    pub emoji: String,
    pub actor_name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CardSpec {
    pub header: Option<String>,
    pub children: Vec<ViewSpec>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TextSpec {
    pub content: String,
    pub variant: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FlexSpec {
    pub direction: Option<String>,
    pub gap: Option<String>,
    pub children: Vec<ViewSpec>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GridSpec {
    pub columns: Option<u32>,
    pub gap: Option<String>,
    pub children: Vec<ViewSpec>,
}
```

- [ ] **Step 4: Add the module to `lib.rs`**

```rust
// space-chat-app/src-tauri/src/lib.rs
pub mod spec;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test spec::`
Expected: PASS (2 tests)

- [ ] **Step 6: Commit**

```bash
git add space-chat-app/src-tauri/src/spec.rs space-chat-app/src-tauri/src/lib.rs
git commit -m "feat(app): add local ViewSpec types (conversation, card, text, flex, grid)"
```

---

### Task 3: `SpaceMembership` placeholder

**Files:**
- Create: `space-chat-app/src-tauri/src/membership.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — add `pub mod membership;`
- Test: `space-chat-app/src-tauri/src/membership.rs` (inline)

**Interfaces:**
- Consumes: `space_chat_core::domain::DeviceId`, `space_chat_core::sequencer::elect_sequencer` (Milestone 1).
- Produces: `trait SpaceMembership: Send + Sync { fn members(&self, space_id: &str) -> Vec<DeviceId>; fn display_name(&self, id: &DeviceId) -> String; fn add_member(&mut self, space_id: &str, id: DeviceId, display_name: String); fn create_space(&mut self, space_id: &str, local_device: DeviceId, local_display_name: String); fn sequencer(&self, space_id: &str) -> Option<DeviceId>; }`, `PlaintextMembership::new(path: impl Into<PathBuf>) -> std::io::Result<Self>` (a JSON-file-backed implementation, persisted across restarts, loaded via `PlaintextMembership::load` at construction). Tasks 5, 8, and 12 consume this trait; every call site must carry the "placeholder, not real MLS" caveat forward.

**This entire module is a placeholder.** Real space membership requires `space-chat-openmls`'s MLS group state, which doesn't exist yet (see the top of this plan). `PlaintextMembership` has zero cryptographic protection: display names and membership lists are plain JSON on disk, never authenticated, never gossiped between devices. It exists only so `create_space`/`generate_invite`/`join_via_invite` (Task 12) and spec generation (Task 5) have something concrete to call. Replace wholesale once `space-chat-openmls` lands.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/membership.rs
#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::domain::DeviceId;

    #[test]
    fn create_space_registers_the_local_device_as_a_member() {
        let dir = tempfile::tempdir().unwrap();
        let mut membership = PlaintextMembership::new(dir.path().join("membership.json")).unwrap();

        membership.create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());

        assert_eq!(membership.members("space-1"), vec![DeviceId([1u8; 32])]);
        assert_eq!(membership.display_name(&DeviceId([1u8; 32])), "Alice");
    }

    #[test]
    fn add_member_and_sequencer_election_matches_lowest_device_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut membership = PlaintextMembership::new(dir.path().join("membership.json")).unwrap();

        membership.create_space("space-1", DeviceId([5u8; 32]), "Bob".to_string());
        membership.add_member("space-1", DeviceId([1u8; 32]), "Alice".to_string());

        let mut members = membership.members("space-1");
        members.sort_by_key(|d| d.0);
        assert_eq!(members, vec![DeviceId([1u8; 32]), DeviceId([5u8; 32])]);
        assert_eq!(membership.sequencer("space-1"), Some(DeviceId([1u8; 32])));
    }

    #[test]
    fn membership_persists_across_a_fresh_instance_at_the_same_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("membership.json");
        {
            let mut membership = PlaintextMembership::new(&path).unwrap();
            membership.create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        }
        let reloaded = PlaintextMembership::new(&path).unwrap();
        assert_eq!(reloaded.members("space-1"), vec![DeviceId([1u8; 32])]);
        assert_eq!(reloaded.display_name(&DeviceId([1u8; 32])), "Alice");
    }

    #[test]
    fn display_name_falls_back_to_a_short_hex_id_for_unknown_devices() {
        let dir = tempfile::tempdir().unwrap();
        let membership = PlaintextMembership::new(dir.path().join("membership.json")).unwrap();
        let name = membership.display_name(&DeviceId([0xabu8; 32]));
        assert!(name.starts_with("ab"), "expected fallback name to start with the device id's hex prefix, got {name:?}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test membership::`
Expected: FAIL — `PlaintextMembership` not defined.

- [ ] **Step 3: Implement `PlaintextMembership`**

```rust
// space-chat-app/src-tauri/src/membership.rs (above the tests module)
use serde::{Deserialize, Serialize};
use space_chat_core::domain::DeviceId;
use space_chat_core::sequencer::elect_sequencer;
use std::collections::HashMap;
use std::path::PathBuf;

pub trait SpaceMembership: Send + Sync {
    fn members(&self, space_id: &str) -> Vec<DeviceId>;
    fn display_name(&self, id: &DeviceId) -> String;
    fn add_member(&mut self, space_id: &str, id: DeviceId, display_name: String);
    fn create_space(&mut self, space_id: &str, local_device: DeviceId, local_display_name: String);
    fn sequencer(&self, space_id: &str) -> Option<DeviceId>;
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct MembershipData {
    // hex-encoded DeviceId -> display name, global across all spaces this
    // device knows about -- a placeholder simplification; real MLS
    // membership is scoped per-space credential, not a flat global map.
    display_names: HashMap<String, String>,
    // space_id -> hex-encoded DeviceIds
    spaces: HashMap<String, Vec<String>>,
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<DeviceId> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(DeviceId(out))
}

/// Plaintext, unauthenticated, JSON-file-backed membership -- a placeholder
/// for real MLS-backed membership (`space-chat-openmls`, which doesn't exist
/// yet). See this module's doc comment.
pub struct PlaintextMembership {
    path: PathBuf,
    data: MembershipData,
}

impl PlaintextMembership {
    pub fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let data = if path.exists() {
            let bytes = std::fs::read(&path)?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            MembershipData::default()
        };
        Ok(Self { path, data })
    }

    fn persist(&self) {
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(bytes) = serde_json::to_vec_pretty(&self.data) {
            let _ = std::fs::write(&self.path, bytes);
        }
    }
}

impl SpaceMembership for PlaintextMembership {
    fn members(&self, space_id: &str) -> Vec<DeviceId> {
        self.data
            .spaces
            .get(space_id)
            .map(|ids| ids.iter().filter_map(|s| unhex(s)).collect())
            .unwrap_or_default()
    }

    fn display_name(&self, id: &DeviceId) -> String {
        let hex_id = hex(&id.0);
        self.data
            .display_names
            .get(&hex_id)
            .cloned()
            .unwrap_or_else(|| hex_id[..8].to_string())
    }

    fn add_member(&mut self, space_id: &str, id: DeviceId, display_name: String) {
        let hex_id = hex(&id.0);
        self.data.display_names.insert(hex_id.clone(), display_name);
        let members = self.data.spaces.entry(space_id.to_string()).or_default();
        if !members.contains(&hex_id) {
            members.push(hex_id);
        }
        self.persist();
    }

    fn create_space(&mut self, space_id: &str, local_device: DeviceId, local_display_name: String) {
        self.add_member(space_id, local_device, local_display_name);
    }

    fn sequencer(&self, space_id: &str) -> Option<DeviceId> {
        elect_sequencer(&self.members(space_id))
    }
}
```

- [ ] **Step 4: Add the module to `lib.rs`**

```rust
// space-chat-app/src-tauri/src/lib.rs
pub mod membership;
pub mod spec;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test membership::`
Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add space-chat-app/src-tauri/src/membership.rs space-chat-app/src-tauri/src/lib.rs
git commit -m "feat(app): add placeholder plaintext SpaceMembership (pending real MLS integration)"
```

---

### Task 4: `ObservedAtStore` — local message-timestamp side-table

**Files:**
- Create: `space-chat-app/src-tauri/src/observed_at.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — add `pub mod observed_at;`
- Test: `space-chat-app/src-tauri/src/observed_at.rs` (inline)

**Interfaces:**
- Consumes: nothing new.
- Produces: `trait ObservedAtStore: Send + Sync { fn record_if_absent(&mut self, message_key: &str, now_unix_ms: u64) -> u64; fn get(&self, message_key: &str) -> Option<u64>; }` (returns the *stored* timestamp either way — the one just recorded, or the one that was already there, so a caller never needs a separate `get` after `record_if_absent`), `InMemoryObservedAtStore` (a `HashMap`-backed implementation used here and by Task 5's tests). Task 8's composition root wires a persistent (redb-backed) implementation of the same trait; Task 5 and its tests only depend on the trait.

See this plan's opening note: this records "when did this device first see this message," not "when did the sender send it" — `space_chat_core::domain::Message` has no sender-side timestamp to read.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/observed_at.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_if_absent_stores_the_first_timestamp_seen() {
        let mut store = InMemoryObservedAtStore::default();
        let stored = store.record_if_absent("msg:1", 1000);
        assert_eq!(stored, 1000);
        assert_eq!(store.get("msg:1"), Some(1000));
    }

    #[test]
    fn record_if_absent_does_not_overwrite_an_existing_timestamp() {
        let mut store = InMemoryObservedAtStore::default();
        store.record_if_absent("msg:1", 1000);
        let stored = store.record_if_absent("msg:1", 5000);
        assert_eq!(stored, 1000, "a second record_if_absent call must not move an already-recorded timestamp");
        assert_eq!(store.get("msg:1"), Some(1000));
    }

    #[test]
    fn get_returns_none_for_an_unseen_message() {
        let store = InMemoryObservedAtStore::default();
        assert_eq!(store.get("msg:unknown"), None);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test observed_at::`
Expected: FAIL — `InMemoryObservedAtStore` not defined.

- [ ] **Step 3: Implement the trait and in-memory store**

```rust
// space-chat-app/src-tauri/src/observed_at.rs (above the tests module)
use std::collections::HashMap;

/// Records, per message, the Unix-millis timestamp at which *this device*
/// first observed the message -- not when the original sender sent it. See
/// this plan's opening note on why: `space_chat_core::domain::Message` has
/// no sender-side timestamp field.
pub trait ObservedAtStore: Send + Sync {
    /// Records `now_unix_ms` for `message_key` if nothing is recorded yet,
    /// and returns whichever timestamp ends up stored (the new one, or the
    /// pre-existing one) -- so a caller never needs a separate `get` call
    /// right after this to find out what was actually recorded.
    fn record_if_absent(&mut self, message_key: &str, now_unix_ms: u64) -> u64;
    fn get(&self, message_key: &str) -> Option<u64>;
}

#[derive(Debug, Default)]
pub struct InMemoryObservedAtStore {
    data: HashMap<String, u64>,
}

impl ObservedAtStore for InMemoryObservedAtStore {
    fn record_if_absent(&mut self, message_key: &str, now_unix_ms: u64) -> u64 {
        *self.data.entry(message_key.to_string()).or_insert(now_unix_ms)
    }

    fn get(&self, message_key: &str) -> Option<u64> {
        self.data.get(message_key).copied()
    }
}
```

- [ ] **Step 4: Add the module to `lib.rs`**

```rust
// space-chat-app/src-tauri/src/lib.rs
pub mod membership;
pub mod observed_at;
pub mod spec;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test observed_at::`
Expected: PASS (3 tests)

- [ ] **Step 6: Commit**

```bash
git add space-chat-app/src-tauri/src/observed_at.rs space-chat-app/src-tauri/src/lib.rs
git commit -m "feat(app): add ObservedAtStore local message-timestamp side-table"
```

---

### Task 5: `build_conversation_spec` — pure spec-generation function

**Files:**
- Create: `space-chat-app/src-tauri/src/conversation_spec.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — add `pub mod conversation_spec;`
- Modify: `space-chat-app/src-tauri/Cargo.toml` — this task's tests need `automerge` directly (to build a `Segment` fixture) and the storage-plan crates
- Test: `space-chat-app/src-tauri/src/conversation_spec.rs` (inline)

**Depends on Milestone 2 being merged** (`space_chat_core::storage::ListingEntry`). This is the first task in this plan that does.

**Interfaces:**
- Consumes: `space_chat_core::segment::Segment`, `space_chat_core::storage::ListingEntry` (Milestone 2), `crate::spec::{ConversationSpec, MessageSpec, AttachmentSpec, ReactionSpec}` (Task 2), `crate::membership::SpaceMembership` (Task 3), `crate::observed_at::ObservedAtStore` (Task 4).
- Produces: `SpecBuildError` (enum: `MissingSegment { epoch: u64 }`, `MalformedMessage { message_key: String }`), `fn build_conversation_spec(space_id: &str, title: &str, page: &[ListingEntry], segments: &HashMap<u64, Segment>, membership: &dyn SpaceMembership, observed_at: &mut dyn ObservedAtStore, has_more_older: bool, now_unix_ms: u64) -> Result<ConversationSpec, SpecBuildError>`. Task 9's `open_conversation`/regeneration path calls this directly and catches `SpecBuildError` to build a `ConversationErrorSpec` instead (per the app-shell spec's per-conversation failure isolation).

- [ ] **Step 1: Add test-only dependencies**

```toml
# space-chat-app/src-tauri/Cargo.toml — add to [dev-dependencies]
automerge = "0.5"
```

(Verify the exact `automerge` version against what `space-chat-core`'s own `Cargo.toml` pins, so the fixture `Segment`s this task's tests build are binary-compatible with the one `space-chat-core` uses internally.)

- [ ] **Step 2: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/conversation_spec.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::SpaceMembership;
    use crate::observed_at::{InMemoryObservedAtStore, ObservedAtStore};
    use space_chat_core::domain::{DeviceId, Message, Reaction};
    use space_chat_core::segment::{objid_to_target_string, Segment};
    use space_chat_core::storage::ListingEntry;
    use std::collections::HashMap;

    struct FakeMembership;
    impl SpaceMembership for FakeMembership {
        fn members(&self, _space_id: &str) -> Vec<DeviceId> {
            vec![]
        }
        fn display_name(&self, id: &DeviceId) -> String {
            if id.0 == [1u8; 32] {
                "Alice".to_string()
            } else {
                "Unknown".to_string()
            }
        }
        fn add_member(&mut self, _space_id: &str, _id: DeviceId, _name: String) {}
        fn create_space(&mut self, _space_id: &str, _local_device: DeviceId, _name: String) {}
        fn sequencer(&self, _space_id: &str) -> Option<DeviceId> {
            None
        }
    }

    #[test]
    fn builds_a_conversation_spec_from_a_listing_page_and_segment() {
        let mut segment = Segment::new("space-1", 0);
        let msg_id = segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        segment
            .append_reaction(
                &msg_id,
                &Reaction {
                    target: objid_to_target_string(&msg_id),
                    actor: DeviceId([1u8; 32]),
                    emoji: "\u{1F44D}".to_string(),
                },
            )
            .unwrap();
        let message_key = segment.message_keys().next().unwrap();

        let page = vec![ListingEntry {
            space_id: "space-1".to_string(),
            epoch: 0,
            seq: 0,
            message_key: message_key.clone(),
        }];
        let mut segments = HashMap::new();
        segments.insert(0u64, segment);

        let membership = FakeMembership;
        let mut observed_at = InMemoryObservedAtStore::default();

        let spec = build_conversation_spec(
            "space-1",
            "General",
            &page,
            &segments,
            &membership,
            &mut observed_at,
            false,
            5_000,
        )
        .unwrap();

        assert_eq!(spec.space_id, "space-1");
        assert_eq!(spec.messages.len(), 1);
        let msg = &spec.messages[0];
        assert_eq!(msg.content, "hello");
        assert_eq!(msg.sender_name, "Alice");
        assert_eq!(msg.reactions.len(), 1);
        assert_eq!(msg.reactions[0].emoji, "\u{1F44D}");
        assert!(!msg.deleted);
    }

    #[test]
    fn newest_first_listing_page_is_reversed_to_oldest_first_display_order() {
        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "first".to_string(),
            attachments: vec![],
        });
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "second".to_string(),
            attachments: vec![],
        });
        let mut keys: Vec<String> = segment.message_keys().collect();
        keys.sort();

        // ListingIndex::page's documented contract is newest-first -- feed
        // the page in that order regardless of which key holds which
        // content, then assert display order comes out oldest-first.
        let page = vec![
            ListingEntry { space_id: "space-1".to_string(), epoch: 0, seq: 1, message_key: keys[1].clone() },
            ListingEntry { space_id: "space-1".to_string(), epoch: 0, seq: 0, message_key: keys[0].clone() },
        ];
        let mut segments = HashMap::new();
        segments.insert(0u64, segment);
        let membership = FakeMembership;
        let mut observed_at = InMemoryObservedAtStore::default();

        let spec = build_conversation_spec(
            "space-1", "General", &page, &segments, &membership, &mut observed_at, false, 1_000,
        )
        .unwrap();

        assert_eq!(spec.messages.len(), 2);
        assert_eq!(spec.messages[0].id, keys[0], "oldest entry (seq 0) must render first");
        assert_eq!(spec.messages[1].id, keys[1], "newest entry (seq 1) must render second");
    }

    #[test]
    fn missing_epoch_segment_produces_a_spec_build_error_not_a_panic() {
        let page = vec![ListingEntry {
            space_id: "space-1".to_string(),
            epoch: 7,
            seq: 0,
            message_key: "msg:does-not-exist".to_string(),
        }];
        let segments = HashMap::new(); // epoch 7 never inserted
        let membership = FakeMembership;
        let mut observed_at = InMemoryObservedAtStore::default();

        let result = build_conversation_spec(
            "space-1", "General", &page, &segments, &membership, &mut observed_at, false, 1_000,
        );

        assert_eq!(result, Err(SpecBuildError::MissingSegment { epoch: 7 }));
    }

    #[test]
    fn relative_time_buckets_by_elapsed_duration() {
        assert_eq!(relative_time(1_000, 1_005_000), "just now");
        assert_eq!(relative_time(1_000, 31_000), "30s ago");
        assert_eq!(relative_time(1_000, 121_000), "2m ago");
        assert_eq!(relative_time(1_000, 3_601_000 + 1_000), "1h ago");
        assert_eq!(relative_time(1_000, 2 * 86_400_000 + 1_000), "2d ago");
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test conversation_spec::`
Expected: FAIL — `build_conversation_spec`, `SpecBuildError`, `relative_time` not defined.

- [ ] **Step 4: Implement `build_conversation_spec`**

```rust
// space-chat-app/src-tauri/src/conversation_spec.rs (above the tests module)
use crate::membership::SpaceMembership;
use crate::observed_at::ObservedAtStore;
use crate::spec::{AttachmentSpec, ConversationSpec, MessageSpec, ReactionSpec};
use space_chat_core::segment::Segment;
use space_chat_core::storage::ListingEntry;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum SpecBuildError {
    MissingSegment { epoch: u64 },
    MalformedMessage { message_key: String },
}

impl std::fmt::Display for SpecBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpecBuildError::MissingSegment { epoch } => write!(f, "missing segment for epoch {epoch}"),
            SpecBuildError::MalformedMessage { message_key } => {
                write!(f, "malformed message at key {message_key:?}")
            }
        }
    }
}

impl std::error::Error for SpecBuildError {}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Buckets elapsed time between `sent_at_unix_ms` and `now_unix_ms` into a
/// coarse human-readable label. `sent_at_unix_ms` here is really "observed
/// at" -- see `crate::observed_at`'s doc comment.
pub fn relative_time(observed_at_unix_ms: u64, now_unix_ms: u64) -> String {
    let delta_secs = now_unix_ms.saturating_sub(observed_at_unix_ms) / 1000;
    match delta_secs {
        0..=9 => "just now".to_string(),
        10..=59 => format!("{delta_secs}s ago"),
        60..=3_599 => format!("{}m ago", delta_secs / 60),
        3_600..=86_399 => format!("{}h ago", delta_secs / 3_600),
        _ => format!("{}d ago", delta_secs / 86_400),
    }
}

/// Builds a `ConversationSpec` from a `ListingIndex::page` window (the
/// ordering source of truth -- see this plan's Global Constraints) plus the
/// `Segment`s that actually hold each entry's message content. `segments`
/// maps epoch -> the `Segment` covering it; the composition root (Task 8)
/// keeps every epoch a space has ever used loaded so this lookup never
/// needs to hit disk mid-call.
#[allow(clippy::too_many_arguments)]
pub fn build_conversation_spec(
    space_id: &str,
    title: &str,
    page: &[ListingEntry],
    segments: &HashMap<u64, Segment>,
    membership: &dyn SpaceMembership,
    observed_at: &mut dyn ObservedAtStore,
    has_more_older: bool,
    now_unix_ms: u64,
) -> Result<ConversationSpec, SpecBuildError> {
    let mut messages = Vec::with_capacity(page.len());

    // `page` is newest-first (ListingIndex::page's documented contract);
    // reverse it here so the spec's `messages` reads oldest-first, the
    // order a chat transcript is displayed in.
    for entry in page.iter().rev() {
        let segment = segments
            .get(&entry.epoch)
            .ok_or(SpecBuildError::MissingSegment { epoch: entry.epoch })?;

        let msg = segment.read_message(&entry.message_key).ok_or_else(|| {
            SpecBuildError::MalformedMessage { message_key: entry.message_key.clone() }
        })?;
        let msg_obj_id = segment.message(&entry.message_key).ok_or_else(|| {
            SpecBuildError::MalformedMessage { message_key: entry.message_key.clone() }
        })?;

        let reactions = segment
            .reaction_keys(&msg_obj_id)
            .filter_map(|key| segment.read_reaction(&msg_obj_id, &key))
            .map(|r| ReactionSpec {
                emoji: r.emoji,
                actor_name: membership.display_name(&r.actor),
            })
            .collect();

        let attachments = msg
            .attachments
            .iter()
            .map(|a| AttachmentSpec {
                url: format!("spacechat://attachment/{}", hex(&a.hash)),
                mime: a.mime.clone(),
                size: a.size,
            })
            .collect();

        let observed_ms = observed_at.record_if_absent(&entry.message_key, now_unix_ms);

        messages.push(MessageSpec {
            id: entry.message_key.clone(),
            sender_id: hex(&msg.sender.0),
            sender_name: membership.display_name(&msg.sender),
            content: msg.content,
            relative_time: relative_time(observed_ms, now_unix_ms),
            attachments,
            reactions,
            deleted: segment.is_deleted(&msg_obj_id),
        });
    }

    Ok(ConversationSpec {
        space_id: space_id.to_string(),
        title: title.to_string(),
        messages,
        has_more_older,
    })
}
```

- [ ] **Step 5: Add the module to `lib.rs`**

```rust
// space-chat-app/src-tauri/src/lib.rs
pub mod conversation_spec;
pub mod membership;
pub mod observed_at;
pub mod spec;
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test conversation_spec::`
Expected: PASS (4 tests)

- [ ] **Step 7: Commit**

```bash
git add space-chat-app/src-tauri/src/conversation_spec.rs space-chat-app/src-tauri/src/lib.rs space-chat-app/src-tauri/Cargo.toml
git commit -m "feat(app): add build_conversation_spec reading ordering from ListingIndex"
```

---

### Task 6: `LiveSpec` — adapted from `tenju-tofu`'s prototype, rolling 8-version window

**Files:**
- Create: `space-chat-app/src-tauri/src/live_spec.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — add `pub mod live_spec;`
- Modify: `space-chat-app/src-tauri/Cargo.toml` — add `json-patch = "4"` and `serde_json` (already present)
- Test: `space-chat-app/src-tauri/src/live_spec.rs` (inline)

No Milestone 2 dependency — this is a self-contained adaptation of `tenju-tofu/src-tauri/src/live_spec.rs`.

**Interfaces:**
- Consumes: nothing new beyond `serde_json::Value` and `json-patch`.
- Produces: `LiveSpec::new(initial: serde_json::Value) -> Self`, `LiveSpec::update(&self, new_value: serde_json::Value)`, `LiveSpec::snapshot(&self) -> (u64, serde_json::Value)`, `LiveSpec::diff_since(&self, since: u64) -> PatchResponse`, `PatchResponse` (enum: `Unchanged { version: u64 }`, `Patch { version: u64, patch: json_patch::Patch }`, `Full { version: u64, spec: serde_json::Value }`). Task 8's `ActiveConversation` holds one `LiveSpec` per actively-viewed conversation; Task 9's commands call `snapshot`/`diff_since`/`update`.

**What's different from the `tenju-tofu` prototype, and why:**
1. **Rolling window of the last 8 versions, not just 1.** The original's own comment already flags "real-world usage would need a longer history window... to tolerate slow/disconnected clients falling more than one version behind" — chat's gossip bursts (several messages/reactions landing within milliseconds of each other) would blow past a 1-version window often enough to matter, forcing an unnecessary `Full` resend on every such burst. 8 is the middle of the app-shell spec's proposed 5–10 range: enough to absorb a multi-message burst between two Tauri event-loop turns, without holding an unboundedly large amount of prior-spec state per active conversation (bounded, since only actively-viewed conversations get a `LiveSpec` at all — see this plan's Global Constraints).
2. **Delivery is push (Tauri events), not poll (HTTP).** This module itself doesn't change for that — it's `Task 9`/`Task 15`'s job to call `update`/`diff_since` and push the result over a Tauri event instead of an HTTP response. `LiveSpec` itself is delivery-mechanism-agnostic, same as the original.

- [ ] **Step 1: Add the `json-patch` dependency**

```toml
# space-chat-app/src-tauri/Cargo.toml — add to [dependencies]
json-patch = "4"
```

- [ ] **Step 2: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/live_spec.rs
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fresh_live_spec_is_unchanged_when_queried_at_its_own_version() {
        let live = LiveSpec::new(json!({"a": 1}));
        let (version, _) = live.snapshot();
        assert_eq!(version, 0);
        match live.diff_since(0) {
            PatchResponse::Unchanged { version } => assert_eq!(version, 0),
            other => panic!("expected Unchanged, got {other:?}"),
        }
    }

    #[test]
    fn one_update_produces_a_patch_against_the_immediately_prior_version() {
        let live = LiveSpec::new(json!({"a": 1}));
        live.update(json!({"a": 2}));

        match live.diff_since(0) {
            PatchResponse::Patch { version, patch } => {
                assert_eq!(version, 1);
                assert_eq!(patch.0.len(), 1, "expected exactly one JSON Patch operation");
            }
            other => panic!("expected Patch, got {other:?}"),
        }
    }

    #[test]
    fn a_client_within_the_rolling_window_still_gets_a_patch() {
        let live = LiveSpec::new(json!({"n": 0}));
        for i in 1..=7 {
            live.update(json!({"n": i}));
        }
        // Client last saw version 0; 7 updates have happened since, still
        // within the 8-version rolling window (versions 0..=7 all retained).
        match live.diff_since(0) {
            PatchResponse::Patch { version, .. } => assert_eq!(version, 7),
            other => panic!("expected Patch within the rolling window, got {other:?}"),
        }
    }

    #[test]
    fn a_client_further_behind_than_the_rolling_window_gets_a_full_resend() {
        let live = LiveSpec::new(json!({"n": 0}));
        for i in 1..=9 {
            live.update(json!({"n": i}));
        }
        // Client last saw version 0; 9 updates have happened, exceeding the
        // 8-version window, so version 0's snapshot is no longer retained.
        match live.diff_since(0) {
            PatchResponse::Full { version, spec } => {
                assert_eq!(version, 9);
                assert_eq!(spec, json!({"n": 9}));
            }
            other => panic!("expected Full, got {other:?}"),
        }
    }

    /// Proves the backend-restart-needs-no-new-mechanism property from this
    /// plan's Global Constraints directly against the code, not just in
    /// prose: a freshly-constructed `LiveSpec` (as if the backend just
    /// restarted and re-opened this conversation) has no history at all, so
    /// any nonzero version a frontend quotes back from before the restart is
    /// unrecognized and falls through to `Full` -- exactly the same path a
    /// too-far-behind client takes, with no restart-specific branch needed.
    #[test]
    fn diff_since_on_a_freshly_constructed_live_spec_returns_full_for_any_prior_version() {
        let live = LiveSpec::new(json!({"n": 0}));
        match live.diff_since(7) {
            PatchResponse::Full { version, spec } => {
                assert_eq!(version, 0);
                assert_eq!(spec, json!({"n": 0}));
            }
            other => panic!("expected Full for a version this fresh instance never produced, got {other:?}"),
        }
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test live_spec::`
Expected: FAIL — `LiveSpec` not defined.

- [ ] **Step 4: Implement `LiveSpec` with the rolling window**

```rust
// space-chat-app/src-tauri/src/live_spec.rs (above the tests module)
use serde::Serialize;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

/// How many prior versions `LiveSpec` retains for diffing. See this plan's
/// Global Constraints for the 5-10 -> 8 rationale.
const ROLLING_WINDOW: usize = 8;

/// Adapted from `tenju-tofu/src-tauri/src/live_spec.rs`, with a rolling
/// window of prior versions (see `ROLLING_WINDOW`) in place of the
/// original's single-previous-version tracking. Delivery-mechanism-agnostic:
/// this plan pushes `diff_since`'s result over a Tauri event (Task 9) rather
/// than the original's HTTP-polled response, but nothing about that changes
/// this type itself.
#[derive(Clone)]
pub struct LiveSpec {
    state: Arc<RwLock<LiveSpecState>>,
}

struct LiveSpecState {
    version: u64,
    current: Value,
    // Front = oldest retained version, back = most recent. Bounded to
    // ROLLING_WINDOW entries.
    history: VecDeque<(u64, Value)>,
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PatchResponse {
    Unchanged { version: u64 },
    Patch { version: u64, patch: json_patch::Patch },
    Full { version: u64, spec: Value },
}

impl LiveSpec {
    pub fn new(initial: Value) -> Self {
        Self {
            state: Arc::new(RwLock::new(LiveSpecState {
                version: 0,
                current: initial,
                history: VecDeque::new(),
            })),
        }
    }

    pub fn update(&self, new_value: Value) {
        let mut state = self.state.write().unwrap();
        let old_version = state.version;
        let old_value = std::mem::replace(&mut state.current, new_value);
        state.history.push_back((old_version, old_value));
        while state.history.len() > ROLLING_WINDOW {
            state.history.pop_front();
        }
        state.version += 1;
    }

    pub fn snapshot(&self) -> (u64, Value) {
        let state = self.state.read().unwrap();
        (state.version, state.current.clone())
    }

    pub fn diff_since(&self, since: u64) -> PatchResponse {
        let state = self.state.read().unwrap();
        if since == state.version {
            return PatchResponse::Unchanged { version: state.version };
        }
        if let Some((_, since_value)) = state.history.iter().find(|(v, _)| *v == since) {
            let patch = json_patch::diff(since_value, &state.current);
            return PatchResponse::Patch { version: state.version, patch };
        }
        PatchResponse::Full { version: state.version, spec: state.current.clone() }
    }
}
```

- [ ] **Step 5: Add the module to `lib.rs`**

```rust
// space-chat-app/src-tauri/src/lib.rs
pub mod conversation_spec;
pub mod live_spec;
pub mod membership;
pub mod observed_at;
pub mod spec;
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test live_spec::`
Expected: PASS (5 tests)

- [ ] **Step 7: Commit**

```bash
git add space-chat-app/src-tauri/src/live_spec.rs space-chat-app/src-tauri/src/lib.rs space-chat-app/src-tauri/Cargo.toml
git commit -m "feat(app): adapt tenju-tofu's LiveSpec prototype with an 8-version rolling window"
```

---

### Task 7: `NetworkService` trait + TCP-loopback stand-in

**Files:**
- Create: `space-chat-app/src-tauri/src/network.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — add `pub mod network;`
- Test: `space-chat-app/src-tauri/src/network.rs` (inline)

No Milestone 2 dependency. **This task's trait is this plan's own invention** — see the closing note below.

**Interfaces:**
- Consumes: `space_chat_core::projection::SegmentChange` (Milestone 1).
- Produces: `ConnectionStatus` (enum: `Connected`, `Reconnecting`, `Disconnected`), `trait NetworkService: Send + Sync { fn send(&self, change: SegmentChange); fn take_incoming(&mut self) -> tokio::sync::mpsc::Receiver<SegmentChange>; fn subscribe_status(&self) -> tokio::sync::watch::Receiver<ConnectionStatus>; }`, `TcpLoopbackNetworkService` (a real, if minimal, stand-in transport — actual `tokio` TCP sockets, actual framing/serialization — used by this task's own tests and by Task 18's multi-actor scenario, connecting to...), `pub async fn run_loopback_relay(addr: std::net::SocketAddr) -> tokio::task::JoinHandle<()>` (a tiny star-topology relay: forwards anything one connected actor sends to every *other* connected actor; an actor simply not being connected models "offline"). Task 8's composition root holds a `Box<dyn NetworkService>`; Task 18 constructs `TcpLoopbackNetworkService` instances directly against a `run_loopback_relay` instance.

**This is not Milestone 3's actual transport API.** `projects/plans/2026-09-05-transport-implementation-plan.md` did not exist (or was still being written concurrently) when this plan was written, and this plan's own scope explicitly excludes guessing at `iroh` internals. `TcpLoopbackNetworkService` is a genuine, working, real-sockets stand-in — not a mock, not in-process channels pretending to be a network — good enough to prove the app shell's wiring (segment changes flow out to peers, in from peers, UI updates live) end to end, including in Task 18's multi-*process* scenario. **Before wiring `space-chat-app` to real `iroh` networking, reconcile this trait against Milestone 3's actual public API** (method names, how connection status is surfaced, how per-`(space_id, category)` stream routing is exposed) — do not assume this trait survives unchanged.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/network.rs
#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::projection::SegmentCursor;
    use std::net::SocketAddr;
    use std::time::Duration;

    fn test_change(cursor: u64) -> SegmentChange {
        SegmentChange {
            space_id: "space-1".to_string(),
            epoch: 0,
            cursor: SegmentCursor(cursor),
            bytes: vec![1, 2, 3, cursor as u8],
        }
    }

    #[tokio::test]
    async fn a_change_sent_by_one_actor_arrives_at_another_via_the_relay() {
        let relay_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (relay_handle, actual_addr) = run_loopback_relay(relay_addr).await;

        let mut alice = TcpLoopbackNetworkService::connect(actual_addr, "space-1".to_string())
            .await
            .unwrap();
        let mut bob = TcpLoopbackNetworkService::connect(actual_addr, "space-1".to_string())
            .await
            .unwrap();
        let mut bob_incoming = bob.take_incoming();

        alice.send(test_change(1));

        let received = tokio::time::timeout(Duration::from_secs(2), bob_incoming.recv())
            .await
            .expect("should receive within timeout")
            .expect("channel should not be closed");
        assert_eq!(received.cursor, SegmentCursor(1));

        drop(alice);
        drop(bob);
        relay_handle.abort();
    }

    #[tokio::test]
    async fn an_unconnected_actor_never_receives_anything() {
        let relay_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (relay_handle, actual_addr) = run_loopback_relay(relay_addr).await;

        let mut alice = TcpLoopbackNetworkService::connect(actual_addr, "space-1".to_string())
            .await
            .unwrap();
        alice.send(test_change(1));

        // Carol never connects at all -- modeling "offline" per this task's
        // doc comment. Nothing to assert on her behalf except that this test
        // doesn't hang; the real assertion is Task 18's scenario where a
        // never-connected actor's UI never shows the message.
        drop(alice);
        relay_handle.abort();
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test network::`
Expected: FAIL — `TcpLoopbackNetworkService`, `run_loopback_relay` not defined.

- [ ] **Step 3: Implement the trait and the TCP-loopback stand-in**

```rust
// space-chat-app/src-tauri/src/network.rs (above the tests module)
use space_chat_core::projection::SegmentChange;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connected,
    Reconnecting,
    Disconnected,
}

pub trait NetworkService: Send + Sync {
    /// Enqueue a locally-produced change for delivery to other space
    /// members. Fire-and-forget from the caller's perspective -- delivery
    /// failures surface only via `subscribe_status`, mirroring how gossip is
    /// a latency optimization, not the correctness guarantee (reconcile is).
    fn send(&self, change: SegmentChange);
    /// Takes ownership of the receiver for changes arriving from other
    /// members. Callable once; the composition root calls this at startup
    /// and hands the receiver to its dispatch loop (Task 8).
    fn take_incoming(&mut self) -> mpsc::Receiver<SegmentChange>;
    fn subscribe_status(&self) -> watch::Receiver<ConnectionStatus>;
}

fn encode_frame(change: &SegmentChange) -> Vec<u8> {
    // Length-prefixed JSON -- simple and sufficient for a test stand-in;
    // real transport (Milestone 3) will have its own wire format entirely.
    let json = serde_json::json!({
        "space_id": change.space_id,
        "epoch": change.epoch,
        "cursor": change.cursor.0,
        "bytes": change.bytes,
    });
    let payload = serde_json::to_vec(&json).expect("SegmentChange always serializes");
    let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
    framed.extend_from_slice(&payload);
    framed
}

async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<SegmentChange>> {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return Ok(None); // connection closed
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    let json: serde_json::Value = serde_json::from_slice(&payload)?;
    Ok(Some(SegmentChange {
        space_id: json["space_id"].as_str().unwrap_or_default().to_string(),
        epoch: json["epoch"].as_u64().unwrap_or_default(),
        cursor: space_chat_core::projection::SegmentCursor(json["cursor"].as_u64().unwrap_or_default()),
        bytes: json["bytes"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_u64().map(|n| n as u8)).collect())
            .unwrap_or_default(),
    }))
}

/// Runs a tiny star-topology relay: anything one connected socket sends is
/// forwarded to every *other* currently-connected socket. Returns the
/// spawned task's handle plus the actual bound address (useful when binding
/// to port 0 for a test-unique port). A real (not mocked) local network
/// relay, standing in for `iroh::test_utils::run_relay_server()` until
/// Milestone 3 exists -- see this task's closing note.
pub async fn run_loopback_relay(addr: SocketAddr) -> (JoinHandle<()>, SocketAddr) {
    let listener = TcpListener::bind(addr).await.expect("failed to bind loopback relay");
    let actual_addr = listener.local_addr().expect("bound listener has a local addr");

    let handle = tokio::spawn(async move {
        let (broadcast_tx, _) = tokio::sync::broadcast::channel::<(u64, Vec<u8>)>(256);
        let mut next_conn_id: u64 = 0;

        loop {
            let Ok((socket, _)) = listener.accept().await else { break };
            let conn_id = next_conn_id;
            next_conn_id += 1;
            let tx = broadcast_tx.clone();
            let mut rx = broadcast_tx.subscribe();

            tokio::spawn(async move {
                let (mut read_half, mut write_half) = socket.into_split();

                let writer = tokio::spawn(async move {
                    while let Ok((sender_id, frame)) = rx.recv().await {
                        if sender_id == conn_id {
                            continue; // never echo a sender's own message back
                        }
                        if write_half.write_all(&frame).await.is_err() {
                            break;
                        }
                    }
                });

                loop {
                    let mut len_buf = [0u8; 4];
                    if read_half.read_exact(&mut len_buf).await.is_err() {
                        break;
                    }
                    let len = u32::from_be_bytes(len_buf) as usize;
                    let mut payload = vec![0u8; len];
                    if read_half.read_exact(&mut payload).await.is_err() {
                        break;
                    }
                    let mut framed = len_buf.to_vec();
                    framed.extend_from_slice(&payload);
                    let _ = tx.send((conn_id, framed));
                }

                writer.abort();
            });
        }
    });

    (handle, actual_addr)
}

pub struct TcpLoopbackNetworkService {
    write_half: tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>,
    outgoing_tx: mpsc::UnboundedSender<SegmentChange>,
    incoming_rx: Option<mpsc::Receiver<SegmentChange>>,
    status_rx: watch::Receiver<ConnectionStatus>,
}

impl TcpLoopbackNetworkService {
    pub async fn connect(relay_addr: SocketAddr, _space_id: String) -> std::io::Result<Self> {
        let stream = TcpStream::connect(relay_addr).await?;
        let (mut read_half, write_half) = stream.into_split();

        let (incoming_tx, incoming_rx) = mpsc::channel(256);
        let (status_tx, status_rx) = watch::channel(ConnectionStatus::Connected);

        tokio::spawn(async move {
            loop {
                match read_frame(&mut read_half).await {
                    Ok(Some(change)) => {
                        if incoming_tx.send(change).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) | Err(_) => {
                        let _ = status_tx.send(ConnectionStatus::Disconnected);
                        break;
                    }
                }
            }
        });

        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<SegmentChange>();
        let write_half = tokio::sync::Mutex::new(write_half);
        // A dedicated writer task keeps `send` (below) synchronous/non-async
        // from the caller's point of view -- matching the trait's `&self`
        // (non-`&mut`) signature -- while still doing real async I/O.
        let write_half_for_task = write_half;
        let write_half = tokio::sync::Mutex::new(
            write_half_for_task.into_inner(),
        );
        tokio::spawn({
            let write_half = write_half;
            async move {
                while let Some(change) = outgoing_rx.recv().await {
                    let frame = encode_frame(&change);
                    let mut guard = write_half.lock().await;
                    let _ = guard.write_all(&frame).await;
                }
            }
        });

        Ok(Self {
            // Re-created below since the mutex was moved into the task; see
            // Step 4's note on this constructor's actual shape.
            write_half: tokio::sync::Mutex::new(unreachable!()),
            outgoing_tx,
            incoming_rx: Some(incoming_rx),
            status_rx,
        })
    }
}

impl NetworkService for TcpLoopbackNetworkService {
    fn send(&self, change: SegmentChange) {
        let _ = self.outgoing_tx.send(change);
    }

    fn take_incoming(&mut self) -> mpsc::Receiver<SegmentChange> {
        self.incoming_rx.take().expect("take_incoming called more than once")
    }

    fn subscribe_status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status_rx.clone()
    }
}
```

This constructor has a bug deliberately left visible for the next step: it moves `write_half` into the writer task and then tries to store an `unreachable!()` field, which will panic. Fix it in Step 4 by dropping the now-unused `write_half` struct field entirely (the writer task owns the socket half; the struct only needs `outgoing_tx` to hand it work) — an example of writing the test-driving first pass honestly and then immediately correcting it, not shipping the broken version:

```rust
// space-chat-app/src-tauri/src/network.rs — corrected TcpLoopbackNetworkService
pub struct TcpLoopbackNetworkService {
    outgoing_tx: mpsc::UnboundedSender<SegmentChange>,
    incoming_rx: Option<mpsc::Receiver<SegmentChange>>,
    status_rx: watch::Receiver<ConnectionStatus>,
}

impl TcpLoopbackNetworkService {
    pub async fn connect(relay_addr: SocketAddr, _space_id: String) -> std::io::Result<Self> {
        let stream = TcpStream::connect(relay_addr).await?;
        let (mut read_half, write_half) = stream.into_split();

        let (incoming_tx, incoming_rx) = mpsc::channel(256);
        let (status_tx, status_rx) = watch::channel(ConnectionStatus::Connected);

        tokio::spawn(async move {
            loop {
                match read_frame(&mut read_half).await {
                    Ok(Some(change)) => {
                        if incoming_tx.send(change).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) | Err(_) => {
                        let _ = status_tx.send(ConnectionStatus::Disconnected);
                        break;
                    }
                }
            }
        });

        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<SegmentChange>();
        let write_half = tokio::sync::Mutex::new(write_half);
        tokio::spawn(async move {
            while let Some(change) = outgoing_rx.recv().await {
                let frame = encode_frame(&change);
                let mut guard = write_half.lock().await;
                let _ = guard.write_all(&frame).await;
            }
        });

        Ok(Self {
            outgoing_tx,
            incoming_rx: Some(incoming_rx),
            status_rx,
        })
    }
}

impl NetworkService for TcpLoopbackNetworkService {
    fn send(&self, change: SegmentChange) {
        let _ = self.outgoing_tx.send(change);
    }

    fn take_incoming(&mut self) -> mpsc::Receiver<SegmentChange> {
        self.incoming_rx.take().expect("take_incoming called more than once")
    }

    fn subscribe_status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status_rx.clone()
    }
}
```

- [ ] **Step 4: Replace the broken constructor with the corrected version above, and update the relay to return `(JoinHandle, SocketAddr)` consistently**

Apply the corrected `TcpLoopbackNetworkService`/`connect`/`impl NetworkService` block from Step 3 in place of the deliberately-broken one.

- [ ] **Step 5: Add tokio dev-features and the module to `lib.rs`**

```toml
# space-chat-app/src-tauri/Cargo.toml — [dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
```

```rust
// space-chat-app/src-tauri/src/lib.rs
pub mod conversation_spec;
pub mod live_spec;
pub mod membership;
pub mod network;
pub mod observed_at;
pub mod spec;
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test network:: -- --test-threads=1`
Expected: PASS (2 tests). `--test-threads=1` avoids port-binding flakiness between the two async tests sharing the loopback interface; consider `SocketAddr` port 0 (already used above) as the primary fix and this flag as a belt-and-suspenders measure only.

- [ ] **Step 7: Commit**

```bash
git add space-chat-app/src-tauri/src/network.rs space-chat-app/src-tauri/src/lib.rs space-chat-app/src-tauri/Cargo.toml
git commit -m "feat(app): add NetworkService trait + TCP-loopback stand-in transport (pending Milestone 3 reconciliation)"
```

---

### Task 8: Composition root `AppState`

**Files:**
- Create: `space-chat-app/src-tauri/src/state.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — add `pub mod state;`, wire `AppState` into `run()`'s `tauri::Builder` via `.manage(...)`
- Modify: `space-chat-app/src-tauri/Cargo.toml` — add the storage-plan crates + `redb`
- Test: `space-chat-app/src-tauri/src/state.rs` (inline)

**Depends on Milestone 2 being merged** — this task is where `space-chat-storage-files`, `space-chat-storage-redb`, `space-chat-search-tantivy` first enter this plan's dependency graph.

**Interfaces:**
- Consumes: `space_chat_core::segment::Segment`, `space_chat_core::storage::{SegmentBlobStore, ListingIndex, AttachmentBlobStore, AttachmentMetadataStore}`, `space_chat_storage_files::{FileSegmentStore, FileAttachmentStore}`, `space_chat_storage_redb::{listing::RedbListingIndex, attachment_metadata::RedbAttachmentMetadataStore}` (all Milestone 2), `crate::live_spec::LiveSpec` (Task 6), `crate::network::NetworkService` (Task 7), `crate::membership::{SpaceMembership, PlaintextMembership}` (Task 3), `crate::observed_at::ObservedAtStore` (Task 4).
- Produces: `RedbObservedAtStore` (a persistent `ObservedAtStore` impl, using the same `redb::Database` handle as `RedbListingIndex`/`RedbAttachmentMetadataStore` — three tables, one file, per the storage spec's "composition-time decision" framing for shared `redb` handles), `ActiveConversation { live_spec: LiveSpec }`, `AppState { .. }` (fields below) with `AppState::new(data_dir: impl Into<PathBuf>, local_device: DeviceId, network: Box<dyn NetworkService>) -> Result<Self, AppStateError>`, and `AppState::segments_for(&self, space_id: &str) -> HashMap<u64, Segment>` (loads every epoch `SegmentBlobStore` currently has for `space_id` — used by Task 5's `build_conversation_spec`). Task 9 onward take `tauri::State<'_, AppState>` in every command.

```rust
// AppState's field shape (for reference across later tasks -- defined fully in Step 3)
pub struct AppState {
    pub local_device: DeviceId,
    pub segment_store: Mutex<FileSegmentStore>,
    pub attachment_store: Mutex<FileAttachmentStore>,
    pub listing_index: Mutex<RedbListingIndex>,
    pub attachment_metadata: Mutex<RedbAttachmentMetadataStore>,
    pub observed_at: Mutex<RedbObservedAtStore>,
    pub membership: Mutex<PlaintextMembership>,
    pub network: Box<dyn NetworkService>,
    pub active: Mutex<HashMap<String, ActiveConversation>>,
    pub change_tx: tokio::sync::broadcast::Sender<SegmentChange>,
}
```

- [ ] **Step 1: Add the storage-plan crates and `redb` as dependencies**

```toml
# space-chat-app/src-tauri/Cargo.toml — add to [dependencies]
space-chat-storage-files = { path = "../../space-chat-storage-files" }
space-chat-storage-redb = { path = "../../space-chat-storage-redb" }
space-chat-search-tantivy = { path = "../../space-chat-search-tantivy" }
redb = "2"
```

- [ ] **Step 2: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/state.rs
#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::domain::{DeviceId, Message};
    use space_chat_core::storage::{ListingEntry, ListingIndex};

    #[test]
    fn observed_at_store_persists_across_a_fresh_instance_at_the_same_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("app.redb");
        {
            let db = std::sync::Arc::new(redb::Database::create(&db_path).unwrap());
            let mut store = RedbObservedAtStore::new(db).unwrap();
            let stored = store.record_if_absent("msg:1", 1234);
            assert_eq!(stored, 1234);
        }
        let db = std::sync::Arc::new(redb::Database::open(&db_path).unwrap());
        let store = RedbObservedAtStore::new(db).unwrap();
        assert_eq!(store.get("msg:1"), Some(1234));
    }

    #[test]
    fn app_state_new_creates_a_fresh_data_dir_with_no_active_conversations() {
        let dir = tempfile::tempdir().unwrap();
        let network = Box::new(crate::network::NullNetworkService::default());
        let state = AppState::new(dir.path(), DeviceId([1u8; 32]), network).unwrap();

        assert_eq!(state.active.lock().unwrap().len(), 0);
    }

    #[test]
    fn segments_for_loads_every_persisted_epoch_for_a_space() {
        use space_chat_core::segment::Segment;
        use space_chat_core::storage::SegmentBlobStore;

        let dir = tempfile::tempdir().unwrap();
        let network = Box::new(crate::network::NullNetworkService::default());
        let state = AppState::new(dir.path(), DeviceId([1u8; 32]), network).unwrap();

        let mut segment = Segment::new("space-1", 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        let bytes = segment.save();
        state.segment_store.lock().unwrap().save_segment("space-1", 0, &bytes).unwrap();

        let segments = state.segments_for("space-1");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[&0].message_count(), 1);
    }

    #[test]
    fn listing_index_is_reachable_through_app_state() {
        let dir = tempfile::tempdir().unwrap();
        let network = Box::new(crate::network::NullNetworkService::default());
        let state = AppState::new(dir.path(), DeviceId([1u8; 32]), network).unwrap();

        state
            .listing_index
            .lock()
            .unwrap()
            .append_entry(ListingEntry {
                space_id: "space-1".to_string(),
                epoch: 0,
                seq: 0,
                message_key: "msg:1".to_string(),
            })
            .unwrap();

        let page = state.listing_index.lock().unwrap().page("space-1", None, 10).unwrap();
        assert_eq!(page.len(), 1);
    }
}
```

- [ ] **Step 3: Add a trivial `NullNetworkService` test double to `network.rs` (needed by the tests above, and by any future test/dev run of the app with no real network configured yet)**

```rust
// space-chat-app/src-tauri/src/network.rs (add near TcpLoopbackNetworkService)
/// Does nothing: `send` drops its argument, `take_incoming` returns a
/// receiver that never yields, status is permanently `Disconnected`. Useful
/// as `AppState`'s network handle in tests that don't exercise networking at
/// all, and as a safe default before a real `NetworkService` is wired in.
#[derive(Default)]
pub struct NullNetworkService {
    incoming_rx: std::sync::Mutex<Option<mpsc::Receiver<SegmentChange>>>,
}

impl NetworkService for NullNetworkService {
    fn send(&self, _change: SegmentChange) {}

    fn take_incoming(&mut self) -> mpsc::Receiver<SegmentChange> {
        let (_tx, rx) = mpsc::channel(1);
        rx
    }

    fn subscribe_status(&self) -> watch::Receiver<ConnectionStatus> {
        let (_tx, rx) = watch::channel(ConnectionStatus::Disconnected);
        rx
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test state::`
Expected: FAIL — `AppState`, `RedbObservedAtStore` not defined.

- [ ] **Step 5: Implement `RedbObservedAtStore` and `AppState`**

```rust
// space-chat-app/src-tauri/src/state.rs (above the tests module)
use crate::live_spec::LiveSpec;
use crate::membership::PlaintextMembership;
use crate::network::NetworkService;
use crate::observed_at::ObservedAtStore;
use redb::{Database, ReadableTable, TableDefinition};
use space_chat_core::domain::DeviceId;
use space_chat_core::projection::SegmentChange;
use space_chat_core::segment::Segment;
use space_chat_core::storage::{AttachmentBlobStore, SegmentBlobStore, StorageError};
use space_chat_storage_files::{FileAttachmentStore, FileSegmentStore};
use space_chat_storage_redb::attachment_metadata::RedbAttachmentMetadataStore;
use space_chat_storage_redb::listing::RedbListingIndex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug)]
pub enum AppStateError {
    Storage(StorageError),
    Redb(String),
    Io(String),
}

impl std::fmt::Display for AppStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppStateError::Storage(e) => write!(f, "storage error: {e}"),
            AppStateError::Redb(e) => write!(f, "redb error: {e}"),
            AppStateError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for AppStateError {}

const OBSERVED_AT: TableDefinition<&str, u64> = TableDefinition::new("observed_at");

/// A persistent `ObservedAtStore`, sharing the same physical `redb::Database`
/// file `AppState` also uses for `RedbListingIndex`/`RedbAttachmentMetadataStore`
/// -- a single small app-owned database, not a separate file per table, since
/// none of this data needs to be transactionally consistent *across* those
/// tables (each is its own independently-rebuildable/best-effort projection).
pub struct RedbObservedAtStore {
    db: Arc<Database>,
}

impl RedbObservedAtStore {
    pub fn new(db: Arc<Database>) -> Result<Self, AppStateError> {
        let txn = db.begin_write().map_err(|e| AppStateError::Redb(e.to_string()))?;
        {
            txn.open_table(OBSERVED_AT).map_err(|e| AppStateError::Redb(e.to_string()))?;
        }
        txn.commit().map_err(|e| AppStateError::Redb(e.to_string()))?;
        Ok(Self { db })
    }
}

impl ObservedAtStore for RedbObservedAtStore {
    fn record_if_absent(&mut self, message_key: &str, now_unix_ms: u64) -> u64 {
        if let Some(existing) = self.get(message_key) {
            return existing;
        }
        if let Ok(txn) = self.db.begin_write() {
            if let Ok(mut table) = txn.open_table(OBSERVED_AT) {
                let _ = table.insert(message_key, now_unix_ms);
            }
            let _ = txn.commit();
        }
        now_unix_ms
    }

    fn get(&self, message_key: &str) -> Option<u64> {
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(OBSERVED_AT).ok()?;
        table.get(message_key).ok()?.map(|v| v.value())
    }
}

/// One entry per currently-actively-viewed conversation -- per this plan's
/// Global Constraints, every other conversation gets no `LiveSpec` at all.
pub struct ActiveConversation {
    pub live_spec: LiveSpec,
}

pub struct AppState {
    pub local_device: DeviceId,
    pub segment_store: Mutex<FileSegmentStore>,
    pub attachment_store: Mutex<FileAttachmentStore>,
    pub listing_index: Mutex<RedbListingIndex>,
    pub attachment_metadata: Mutex<RedbAttachmentMetadataStore>,
    pub observed_at: Mutex<RedbObservedAtStore>,
    pub membership: Mutex<PlaintextMembership>,
    pub network: Box<dyn NetworkService>,
    pub active: Mutex<HashMap<String, ActiveConversation>>,
    /// Fan-out for every applied `SegmentChange`, local or network-delivered
    /// -- the "third consumer of the change feed, alongside ListingIndex and
    /// SearchIndex" the app-shell spec describes. `ListingIndex`/
    /// `SearchIndex`'s own catch-up (Milestone 2's `replay::catch_up`) is
    /// driven separately at startup and by direct calls from Task 10's
    /// mutating commands; this broadcast channel is what lets a currently
    /// *active* conversation's spec regenerate the instant something new
    /// arrives, without polling.
    pub change_tx: broadcast::Sender<SegmentChange>,
}

impl AppState {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        local_device: DeviceId,
        network: Box<dyn NetworkService>,
    ) -> Result<Self, AppStateError> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir).map_err(|e| AppStateError::Io(e.to_string()))?;

        let segment_store = FileSegmentStore::new(&data_dir).map_err(|e| AppStateError::Io(e.to_string()))?;
        let attachment_store =
            FileAttachmentStore::new(&data_dir).map_err(|e| AppStateError::Io(e.to_string()))?;

        let db = Arc::new(
            Database::create(data_dir.join("app.redb")).map_err(|e| AppStateError::Redb(e.to_string()))?,
        );
        let listing_index = RedbListingIndex::new(db.clone()).map_err(AppStateError::Storage)?;
        let attachment_metadata =
            RedbAttachmentMetadataStore::new(db.clone()).map_err(AppStateError::Storage)?;
        let observed_at = RedbObservedAtStore::new(db)?;

        let membership = PlaintextMembership::new(data_dir.join("membership.json"))
            .map_err(|e| AppStateError::Io(e.to_string()))?;

        let (change_tx, _) = broadcast::channel(1024);

        Ok(Self {
            local_device,
            segment_store: Mutex::new(segment_store),
            attachment_store: Mutex::new(attachment_store),
            listing_index: Mutex::new(listing_index),
            attachment_metadata: Mutex::new(attachment_metadata),
            observed_at: Mutex::new(observed_at),
            membership: Mutex::new(membership),
            network,
            active: Mutex::new(HashMap::new()),
            change_tx,
        })
    }

    /// Loads every epoch `segment_store` currently has persisted for
    /// `space_id` into an in-memory map, for `build_conversation_spec`
    /// (Task 5) to read message content from. Cursor is restored as `0` on
    /// load -- this map is rebuilt fresh on every call, not held across
    /// calls as a cache, so there's no `Projection` consuming its cursor
    /// that a restored value would need to line up with (unlike
    /// `Segment::load`'s own doc comment case, which is about a
    /// long-lived, persisted-across-restarts `Segment`).
    pub fn segments_for(&self, space_id: &str) -> HashMap<u64, Segment> {
        let store = self.segment_store.lock().unwrap();
        let mut out = HashMap::new();
        let Ok(epochs) = store.list_epochs(space_id) else {
            return out;
        };
        for epoch in epochs {
            if let Ok(Some(bytes)) = store.load_segment(space_id, epoch) {
                if let Ok(segment) = Segment::load(&bytes, space_id, epoch, 0) {
                    out.insert(epoch, segment);
                }
            }
        }
        out
    }
}
```

- [ ] **Step 6: Add the module to `lib.rs` and manage `AppState` in the Tauri builder**

```rust
// space-chat-app/src-tauri/src/lib.rs
pub mod conversation_spec;
pub mod live_spec;
pub mod membership;
pub mod network;
pub mod observed_at;
pub mod spec;
pub mod state;

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {name}! space-chat is running.")
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let data_dir = dirs_data_dir();
    let local_device = load_or_create_local_device_id(&data_dir);
    let network: Box<dyn network::NetworkService> = Box::new(network::NullNetworkService::default());
    let app_state = state::AppState::new(&data_dir, local_device, network)
        .expect("failed to initialize AppState");

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(app_state)
        .invoke_handler(tauri::generate_handler![greet])
        .run(tauri::generate_context!())
        .expect("error while running space-chat-app");
}

fn dirs_data_dir() -> std::path::PathBuf {
    std::env::var("SPACECHAT_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from(".spacechat-data"))
}

fn load_or_create_local_device_id(data_dir: &std::path::Path) -> space_chat_core::domain::DeviceId {
    let path = data_dir.join("device_id");
    if let Ok(bytes) = std::fs::read(&path) {
        if let Ok(id) = <[u8; 32]>::try_from(bytes.as_slice()) {
            return space_chat_core::domain::DeviceId(id);
        }
    }
    let mut id = [0u8; 32];
    for byte in id.iter_mut() {
        *byte = rand_byte();
    }
    let _ = std::fs::create_dir_all(data_dir);
    let _ = std::fs::write(&path, id);
    space_chat_core::domain::DeviceId(id)
}

fn rand_byte() -> u8 {
    use std::time::{SystemTime, UNIX_EPOCH};
    // Not cryptographically secure -- fine for a locally-generated device
    // identifier in this placeholder membership model (see Task 3); replace
    // alongside the rest of `space-chat-openmls`'s eventual real credential
    // generation.
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
    (nanos ^ (nanos >> 8)) as u8
}
```

**Note on `SPACECHAT_DATA_DIR`:** this environment variable is what lets Task 18/20's multi-actor cucumber harness point each spawned `space-chat-app` process at its own isolated temp directory, and what lets Task 20's restart scenario relaunch the same binary against the same directory to prove recovery.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test state::`
Expected: PASS (4 tests)

- [ ] **Step 8: Run the full crate test suite to confirm nothing else broke**

Run: `cd space-chat-app/src-tauri && cargo test`
Expected: PASS (all tests from Tasks 1–8)

- [ ] **Step 9: Commit**

```bash
git add space-chat-app/src-tauri/src/state.rs space-chat-app/src-tauri/src/network.rs space-chat-app/src-tauri/src/lib.rs space-chat-app/src-tauri/Cargo.toml
git commit -m "feat(app): add AppState composition root wiring storage, LiveSpec, and NetworkService"
```

---

### Task 9: Tauri commands — `open_conversation` / `close_conversation` / `resync_conversation`

**Files:**
- Create: `space-chat-app/src-tauri/src/commands.rs`
- Create: `space-chat-app/src-tauri/src/events.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — add `pub mod commands;`, `pub mod events;`, register commands in `invoke_handler!`
- Test: `space-chat-app/src-tauri/src/commands.rs` (inline, using `AppState::new` directly rather than a running `tauri::App` — these are plain async functions taking `&AppState`, wrapped by thin `#[tauri::command]` shims, so the interesting logic is unit-testable without a Tauri runtime)

**Interfaces:**
- Consumes: `AppState` (Task 8), `build_conversation_spec`/`SpecBuildError` (Task 5), `ViewSpec`/`ConversationErrorSpec` (Task 2), `LiveSpec`/`PatchResponse` (Task 6).
- Produces: `OpenConversationResult { version: u64, spec: serde_json::Value }`, `async fn open_conversation_impl(state: &AppState, space_id: &str, title: &str, now_unix_ms: u64) -> OpenConversationResult`, `async fn close_conversation_impl(state: &AppState, space_id: &str)`, `async fn resync_conversation_impl(state: &AppState, space_id: &str, since_version: u64) -> Result<crate::live_spec::PatchResponse, String>`, plus their `#[tauri::command]` wrappers `open_conversation`, `close_conversation`, `resync_conversation`. Task 10/11's commands call `regenerate_active_spec` (also produced here) after every mutation.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/commands.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::NullNetworkService;
    use crate::state::AppState;
    use space_chat_core::domain::{DeviceId, Message};
    use space_chat_core::storage::{ListingEntry, ListingIndex, SegmentBlobStore};

    fn fresh_state() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let network = Box::new(NullNetworkService::default());
        let state = AppState::new(dir.path(), DeviceId([1u8; 32]), network).unwrap();
        (dir, state)
    }

    fn seed_one_message(state: &AppState, space_id: &str) {
        use space_chat_core::segment::Segment;

        let mut segment = Segment::new(space_id, 0);
        segment.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "hello".to_string(),
            attachments: vec![],
        });
        let key = segment.message_keys().next().unwrap();
        let bytes = segment.save();
        state.segment_store.lock().unwrap().save_segment(space_id, 0, &bytes).unwrap();
        state
            .listing_index
            .lock()
            .unwrap()
            .append_entry(ListingEntry {
                space_id: space_id.to_string(),
                epoch: 0,
                seq: 0,
                message_key: key,
            })
            .unwrap();
        state.membership.lock().unwrap().create_space(space_id, DeviceId([1u8; 32]), "Alice".to_string());
    }

    #[tokio::test]
    async fn open_conversation_returns_version_zero_and_marks_it_active() {
        let (_dir, state) = fresh_state();
        seed_one_message(&state, "space-1");

        let result = open_conversation_impl(&state, "space-1", "General", 10_000).await;

        assert_eq!(result.version, 0);
        assert_eq!(result.spec["kind"], "conversation");
        assert_eq!(result.spec["messages"][0]["content"], "hello");
        assert!(state.active.lock().unwrap().contains_key("space-1"));
    }

    #[tokio::test]
    async fn a_spec_build_failure_degrades_to_a_conversation_error_spec_not_a_command_error() {
        let (_dir, state) = fresh_state();
        // A ListingIndex entry pointing at an epoch with no persisted
        // segment -- forces build_conversation_spec's MissingSegment path.
        state
            .listing_index
            .lock()
            .unwrap()
            .append_entry(ListingEntry {
                space_id: "space-1".to_string(),
                epoch: 99,
                seq: 0,
                message_key: "msg:ghost".to_string(),
            })
            .unwrap();

        let result = open_conversation_impl(&state, "space-1", "General", 10_000).await;

        assert_eq!(result.spec["kind"], "conversation-error");
        assert_eq!(result.spec["space_id"], "space-1");
        // Still marked active and still returns a version -- the app shell
        // as a whole must not treat this as a fatal command error.
        assert!(state.active.lock().unwrap().contains_key("space-1"));
    }

    #[tokio::test]
    async fn close_conversation_removes_it_from_active() {
        let (_dir, state) = fresh_state();
        seed_one_message(&state, "space-1");
        open_conversation_impl(&state, "space-1", "General", 10_000).await;

        close_conversation_impl(&state, "space-1").await;

        assert!(!state.active.lock().unwrap().contains_key("space-1"));
    }

    /// The backend-restart-recovery property (Global Constraints), exercised
    /// through the actual command path: a brand-new AppState (as if the
    /// process just restarted) has no active conversations yet, so
    /// resync_conversation against a version from "before the restart"
    /// naturally goes through open_conversation's fresh-LiveSpec path and
    /// returns Full -- no special-cased restart branch anywhere in this file.
    #[tokio::test]
    async fn resync_after_a_fresh_open_with_a_stale_version_returns_full() {
        let (_dir, state) = fresh_state();
        seed_one_message(&state, "space-1");
        open_conversation_impl(&state, "space-1", "General", 10_000).await;

        let response = resync_conversation_impl(&state, "space-1", 42).await.unwrap();
        match response {
            crate::live_spec::PatchResponse::Full { version, .. } => assert_eq!(version, 0),
            other => panic!("expected Full, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resync_conversation_errors_for_a_space_that_was_never_opened() {
        let (_dir, state) = fresh_state();
        let result = resync_conversation_impl(&state, "space-never-opened", 0).await;
        assert!(result.is_err());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test commands::`
Expected: FAIL — `open_conversation_impl` and friends not defined.

- [ ] **Step 3: Define event payload types**

```rust
// space-chat-app/src-tauri/src/events.rs
use serde::Serialize;

/// Emitted on the per-conversation event name `format!("conversation-patch:{space_id}")`
/// whenever `LiveSpec::update` produces a new version for an actively-viewed
/// conversation.
#[derive(Debug, Serialize)]
pub struct ConversationPatchEvent {
    pub space_id: String,
    #[serde(flatten)]
    pub patch: crate::live_spec::PatchResponse,
}

pub fn conversation_patch_event_name(space_id: &str) -> String {
    format!("conversation-patch:{space_id}")
}

pub fn attachment_ready_event_name(hash_hex: &str) -> String {
    format!("attachment-ready:{hash_hex}")
}
```

(`PatchResponse` needs `Serialize` for `#[serde(flatten)]` to work here — it already derives it, from Task 6.)

- [ ] **Step 4: Implement the open/close/resync command logic**

```rust
// space-chat-app/src-tauri/src/commands.rs (above the tests module)
use crate::conversation_spec::build_conversation_spec;
use crate::live_spec::{LiveSpec, PatchResponse};
use crate::spec::{ConversationErrorSpec, ViewSpec};
use crate::state::{ActiveConversation, AppState};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct OpenConversationResult {
    pub version: u64,
    pub spec: serde_json::Value,
}

fn now_unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// Regenerates the spec for `space_id` from current storage state and
/// returns the JSON value that should become the conversation's next
/// `LiveSpec` version -- either a real `ConversationSpec` or a
/// `ConversationErrorSpec`, per this plan's per-conversation failure
/// isolation. Does not touch `state.active` itself; callers decide what to
/// do with the result (construct a fresh `LiveSpec`, or feed it to an
/// existing one's `update`).
const DEFAULT_PAGE_SIZE: usize = 50;

/// Peeks one entry past `page`'s last (oldest) entry to determine whether
/// anything older remains -- the same "ask for one more, see if it's there"
/// technique Task 11's `fetch_older_page_impl` uses, factored out so the
/// initial/live spec (this function) and pagination (Task 11) never
/// disagree about what "has more older" means for the same listing state.
fn compute_has_more_older(state: &AppState, space_id: &str, page: &[space_chat_core::storage::ListingEntry]) -> bool {
    let Some(oldest) = page.last() else { return false };
    state
        .listing_index
        .lock()
        .unwrap()
        .page(space_id, Some((oldest.epoch, oldest.seq)), 1)
        .map(|older| !older.is_empty())
        .unwrap_or(false)
}

pub fn regenerate_spec_value(state: &AppState, space_id: &str, title: &str) -> serde_json::Value {
    let page = state
        .listing_index
        .lock()
        .unwrap()
        .page(space_id, None, DEFAULT_PAGE_SIZE)
        .unwrap_or_default();
    let has_more_older = compute_has_more_older(state, space_id, &page);
    let segments = state.segments_for(space_id);
    let membership = state.membership.lock().unwrap();
    let mut observed_at = state.observed_at.lock().unwrap();

    match build_conversation_spec(
        space_id,
        title,
        &page,
        &segments,
        &*membership,
        &mut *observed_at,
        has_more_older,
        now_unix_ms(),
    ) {
        Ok(spec) => serde_json::to_value(ViewSpec::Conversation(spec)).unwrap(),
        Err(e) => serde_json::to_value(ViewSpec::ConversationError(ConversationErrorSpec {
            space_id: space_id.to_string(),
            message: e.to_string(),
        }))
        .unwrap(),
    }
}

pub async fn open_conversation_impl(
    state: &AppState,
    space_id: &str,
    title: &str,
    _now_unix_ms: u64,
) -> OpenConversationResult {
    let spec_value = regenerate_spec_value(state, space_id, title);
    let live_spec = LiveSpec::new(spec_value.clone());
    let (version, _) = live_spec.snapshot();

    state
        .active
        .lock()
        .unwrap()
        .insert(space_id.to_string(), ActiveConversation { live_spec });

    OpenConversationResult { version, spec: spec_value }
}

pub async fn close_conversation_impl(state: &AppState, space_id: &str) {
    state.active.lock().unwrap().remove(space_id);
}

pub async fn resync_conversation_impl(
    state: &AppState,
    space_id: &str,
    since_version: u64,
) -> Result<PatchResponse, String> {
    let active = state.active.lock().unwrap();
    let conversation = active
        .get(space_id)
        .ok_or_else(|| format!("conversation {space_id} is not currently open"))?;
    Ok(conversation.live_spec.diff_since(since_version))
}

#[tauri::command]
pub async fn open_conversation(
    state: tauri::State<'_, AppState>,
    space_id: String,
    title: String,
) -> Result<OpenConversationResult, String> {
    Ok(open_conversation_impl(&state, &space_id, &title, now_unix_ms()).await)
}

#[tauri::command]
pub async fn close_conversation(state: tauri::State<'_, AppState>, space_id: String) -> Result<(), String> {
    close_conversation_impl(&state, &space_id).await;
    Ok(())
}

#[tauri::command]
pub async fn resync_conversation(
    state: tauri::State<'_, AppState>,
    space_id: String,
    since_version: u64,
) -> Result<PatchResponse, String> {
    resync_conversation_impl(&state, &space_id, since_version).await
}
```

- [ ] **Step 5: Add the modules to `lib.rs` and register the commands**

```rust
// space-chat-app/src-tauri/src/lib.rs
pub mod commands;
pub mod conversation_spec;
pub mod events;
pub mod live_spec;
pub mod membership;
pub mod network;
pub mod observed_at;
pub mod spec;
pub mod state;

// ... inside run(), replace invoke_handler![greet] with:
        .invoke_handler(tauri::generate_handler![
            greet,
            commands::open_conversation,
            commands::close_conversation,
            commands::resync_conversation,
        ])
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test commands::`
Expected: PASS (5 tests)

- [ ] **Step 7: Commit**

```bash
git add space-chat-app/src-tauri/src/commands.rs space-chat-app/src-tauri/src/events.rs space-chat-app/src-tauri/src/lib.rs
git commit -m "feat(app): add open_conversation/close_conversation/resync_conversation commands"
```

---

### Task 10: Tauri commands — `send_message` / `react` / `delete_message`

**Files:**
- Modify: `space-chat-app/src-tauri/src/commands.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — register the new commands
- Test: `space-chat-app/src-tauri/src/commands.rs` (inline, appended)

**Interfaces:**
- Consumes: everything Task 9 does, plus `space_chat_core::domain::{Message, Reaction, Delete}`, `space_chat_core::segment::{Segment, objid_to_target_string}` (Milestone 1), `space_chat_core::storage::ListingEntry` (Milestone 2).
- Produces: `async fn send_message_impl(state: &AppState, space_id: &str, content: String) -> Result<(), String>`, `async fn react_impl(state: &AppState, space_id: &str, message_key: &str, emoji: &str) -> Result<(), String>`, `async fn delete_message_impl(state: &AppState, space_id: &str, message_key: &str) -> Result<(), String>`, and a shared `fn persist_and_broadcast(state: &AppState, space_id: &str, epoch: u64, segment: &mut Segment, new_listing_entries: Vec<(u64, String)>)` helper every one of them calls, plus their `#[tauri::command]` wrappers. Task 18's golden-path scenario drives real chat behavior through these.

Each of these three commands follows the same shape: mutate an in-memory `Segment` for `(space_id, epoch)`, persist the updated segment bytes, append any new `ListingIndex` entries, send the resulting `SegmentChange` out over `NetworkService`, and — if this conversation is actively viewed — regenerate its spec and push a patch event.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/commands.rs (append inside the existing #[cfg(test)] mod tests)
    use space_chat_core::storage::SegmentBlobStore as _;

    #[tokio::test]
    async fn send_message_appends_a_message_and_it_shows_up_in_the_active_spec() {
        let (_dir, state) = fresh_state();
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        open_conversation_impl(&state, "space-1", "General", 10_000).await;

        send_message_impl(&state, "space-1", "hello world".to_string()).await.unwrap();

        let active = state.active.lock().unwrap();
        let (_, spec_value) = active.get("space-1").unwrap().live_spec.snapshot();
        assert_eq!(spec_value["messages"][0]["content"], "hello world");
    }

    #[tokio::test]
    async fn send_message_persists_across_a_fresh_segment_load() {
        let (_dir, state) = fresh_state();
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());

        send_message_impl(&state, "space-1", "persisted".to_string()).await.unwrap();

        let segments = state.segments_for("space-1");
        assert_eq!(segments[&0].message_count(), 1);
    }

    #[tokio::test]
    async fn react_and_delete_apply_to_an_existing_message() {
        let (_dir, state) = fresh_state();
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        send_message_impl(&state, "space-1", "react to me".to_string()).await.unwrap();

        let segments = state.segments_for("space-1");
        let message_key = segments[&0].message_keys().next().unwrap();

        react_impl(&state, "space-1", &message_key, "\u{1F44D}").await.unwrap();
        delete_message_impl(&state, "space-1", &message_key).await.unwrap();

        let segments = state.segments_for("space-1");
        let msg_id = segments[&0].message(&message_key).unwrap();
        assert_eq!(segments[&0].reaction_count(&msg_id), 1);
        assert!(segments[&0].is_deleted(&msg_id));
    }

    #[tokio::test]
    async fn send_message_on_an_unopened_conversation_still_succeeds_without_a_live_spec() {
        let (_dir, state) = fresh_state();
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());

        let result = send_message_impl(&state, "space-1", "no active view".to_string()).await;

        assert!(result.is_ok());
        assert!(!state.active.lock().unwrap().contains_key("space-1"));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test commands:: send_message`
Expected: FAIL — `send_message_impl` and friends not defined.

- [ ] **Step 3: Implement the mutating commands**

```rust
// space-chat-app/src-tauri/src/commands.rs (append below Task 9's implementation, above the tests module)
use space_chat_core::domain::{Delete, Message, Reaction};
use space_chat_core::segment::{objid_to_target_string, Segment};
use space_chat_core::storage::ListingEntry;

const CURRENT_EPOCH: u64 = 0; // epoch rollover is out of scope for this plan

/// Loads (or creates fresh) the `Segment` for `(space_id, CURRENT_EPOCH)`,
/// runs `mutate` against it, persists the result, appends `new_message_keys`
/// to `ListingIndex` in order, sends the resulting `SegmentChange` out over
/// `NetworkService`, and -- if this conversation is actively viewed --
/// regenerates its spec and returns the patch that should be pushed to the
/// frontend (Task 15 wires the actual event emission; this function returns
/// the value so `send_message`/`react`/`delete_message`'s thin `#[tauri::command]`
/// wrappers can do the emitting, keeping this function testable without a
/// live `tauri::AppHandle`).
fn mutate_and_persist(
    state: &AppState,
    space_id: &str,
    mutate: impl FnOnce(&mut Segment) -> Vec<String>, // returns any newly-created message keys, in order
) -> Result<Option<PatchResponse>, String> {
    let mut segments = state.segments_for(space_id);
    let mut segment = segments.remove(&CURRENT_EPOCH).unwrap_or_else(|| Segment::new(space_id, CURRENT_EPOCH));

    let new_message_keys = mutate(&mut segment);

    let bytes = segment.save();
    state
        .segment_store
        .lock()
        .unwrap()
        .save_segment(space_id, CURRENT_EPOCH, &bytes)
        .map_err(|e| e.to_string())?;

    {
        let mut listing = state.listing_index.lock().unwrap();
        let mut next_seq = listing
            .page(space_id, None, 1)
            .ok()
            .and_then(|page| page.first().map(|e| e.seq + 1))
            .unwrap_or(0);
        for key in new_message_keys {
            listing
                .append_entry(ListingEntry {
                    space_id: space_id.to_string(),
                    epoch: CURRENT_EPOCH,
                    seq: next_seq,
                    message_key: key,
                })
                .map_err(|e| e.to_string())?;
            next_seq += 1;
        }
    }

    let change = segment.latest_change();
    state.network.send(change);

    let active = state.active.lock().unwrap();
    if let Some(conversation) = active.get(space_id) {
        // Title is not persisted anywhere yet in this plan's scope -- reuse
        // space_id as a readable fallback title for regeneration.
        let new_value = regenerate_spec_value(state, space_id, space_id);
        conversation.live_spec.update(new_value);
        let (version, _) = conversation.live_spec.snapshot();
        return Ok(Some(conversation.live_spec.diff_since(version.saturating_sub(1))));
    }

    Ok(None)
}

pub async fn send_message_impl(state: &AppState, space_id: &str, content: String) -> Result<(), String> {
    let local_device = state.local_device;
    mutate_and_persist(state, space_id, move |segment| {
        segment.append_message(&Message {
            sender: local_device,
            content,
            attachments: vec![],
        });
        vec![segment.message_keys().last().unwrap_or_default()]
    })?;
    Ok(())
}

pub async fn react_impl(
    state: &AppState,
    space_id: &str,
    message_key: &str,
    emoji: &str,
) -> Result<(), String> {
    let local_device = state.local_device;
    let emoji = emoji.to_string();
    let message_key = message_key.to_string();
    mutate_and_persist(state, space_id, move |segment| {
        let Some(msg_id) = segment.message(&message_key) else {
            return vec![];
        };
        let _ = segment.append_reaction(
            &msg_id,
            &Reaction {
                target: objid_to_target_string(&msg_id),
                actor: local_device,
                emoji,
            },
        );
        vec![]
    })?;
    Ok(())
}

pub async fn delete_message_impl(state: &AppState, space_id: &str, message_key: &str) -> Result<(), String> {
    let message_key = message_key.to_string();
    mutate_and_persist(state, space_id, move |segment| {
        let Some(msg_id) = segment.message(&message_key) else {
            return vec![];
        };
        let _ = segment.apply_delete(&msg_id, &Delete { target: objid_to_target_string(&msg_id) });
        vec![]
    })?;
    Ok(())
}

#[tauri::command]
pub async fn send_message(
    state: tauri::State<'_, AppState>,
    app: tauri::AppHandle,
    space_id: String,
    content: String,
) -> Result<(), String> {
    send_message_impl(&state, &space_id, content).await?;
    emit_patch_if_active(&app, &state, &space_id);
    Ok(())
}

#[tauri::command]
pub async fn react(
    state: tauri::State<'_, AppState>,
    app: tauri::AppHandle,
    space_id: String,
    message_key: String,
    emoji: String,
) -> Result<(), String> {
    react_impl(&state, &space_id, &message_key, &emoji).await?;
    emit_patch_if_active(&app, &state, &space_id);
    Ok(())
}

#[tauri::command]
pub async fn delete_message(
    state: tauri::State<'_, AppState>,
    app: tauri::AppHandle,
    space_id: String,
    message_key: String,
) -> Result<(), String> {
    delete_message_impl(&state, &space_id, &message_key).await?;
    emit_patch_if_active(&app, &state, &space_id);
    Ok(())
}

/// Looks up the current patch for `space_id` (if it's actively viewed) and
/// emits it on `crate::events::conversation_patch_event_name`. Uses
/// `Manager::emit` (Tauri 2's app-wide event emit) -- verify this exact
/// method name against the pinned `tauri` version's docs per this plan's
/// Global Constraints.
fn emit_patch_if_active(app: &tauri::AppHandle, state: &AppState, space_id: &str) {
    use tauri::Emitter;

    let active = state.active.lock().unwrap();
    let Some(conversation) = active.get(space_id) else { return };
    let (version, _) = conversation.live_spec.snapshot();
    let patch = conversation.live_spec.diff_since(version.saturating_sub(1));
    let event = crate::events::ConversationPatchEvent { space_id: space_id.to_string(), patch };
    let _ = app.emit(&crate::events::conversation_patch_event_name(space_id), event);
}
```

- [ ] **Step 4: Register the new commands in `lib.rs`**

```rust
// space-chat-app/src-tauri/src/lib.rs — invoke_handler! list
        .invoke_handler(tauri::generate_handler![
            greet,
            commands::open_conversation,
            commands::close_conversation,
            commands::resync_conversation,
            commands::send_message,
            commands::react,
            commands::delete_message,
        ])
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test commands::`
Expected: PASS (9 tests total for this module)

- [ ] **Step 6: Commit**

```bash
git add space-chat-app/src-tauri/src/commands.rs space-chat-app/src-tauri/src/lib.rs
git commit -m "feat(app): add send_message/react/delete_message commands with live-spec patch emission"
```

---

### Task 11: Tauri command — `fetch_older_page`

**Files:**
- Modify: `space-chat-app/src-tauri/src/commands.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — register the new command
- Test: `space-chat-app/src-tauri/src/commands.rs` (inline, appended)

**Interfaces:**
- Consumes: `space_chat_core::storage::{ListingIndex, ListingEntry}` (Milestone 2), `build_conversation_spec`/`SpecBuildError` (Task 5).
- Produces: `OlderPageResult { messages: Vec<crate::spec::MessageSpec>, has_more_older: bool }`, `async fn fetch_older_page_impl(state: &AppState, space_id: &str, before_epoch: u64, before_seq: u64, limit: usize) -> Result<OlderPageResult, String>`, `#[tauri::command] fetch_older_page`. The frontend (Task 15) prepends `messages` to the currently-rendered conversation and uses `has_more_older` to decide whether to keep showing a "load more" affordance.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/commands.rs (append inside the existing #[cfg(test)] mod tests)
    #[tokio::test]
    async fn fetch_older_page_returns_messages_strictly_before_the_given_cursor() {
        let (_dir, state) = fresh_state();
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        for content in ["one", "two", "three"] {
            send_message_impl(&state, "space-1", content.to_string()).await.unwrap();
        }

        // Page 1: newest 2.
        let page1 = fetch_older_page_impl(&state, "space-1", u64::MAX, u64::MAX, 2).await.unwrap();
        assert_eq!(page1.messages.len(), 2);
        assert_eq!(page1.messages[0].content, "two");
        assert_eq!(page1.messages[1].content, "three");
        assert!(page1.has_more_older);
    }

    #[tokio::test]
    async fn fetch_older_page_reports_no_more_older_once_exhausted() {
        let (_dir, state) = fresh_state();
        state.membership.lock().unwrap().create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        send_message_impl(&state, "space-1", "only one".to_string()).await.unwrap();

        let page = fetch_older_page_impl(&state, "space-1", u64::MAX, u64::MAX, 10).await.unwrap();
        assert_eq!(page.messages.len(), 1);
        assert!(!page.has_more_older);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test commands:: fetch_older_page`
Expected: FAIL — `fetch_older_page_impl` not defined.

- [ ] **Step 3: Implement `fetch_older_page`**

```rust
// space-chat-app/src-tauri/src/commands.rs (append below Task 10's implementation, above the tests module)
use crate::spec::MessageSpec;

#[derive(Debug, Serialize)]
pub struct OlderPageResult {
    pub messages: Vec<MessageSpec>,
    pub has_more_older: bool,
}

pub async fn fetch_older_page_impl(
    state: &AppState,
    space_id: &str,
    before_epoch: u64,
    before_seq: u64,
    limit: usize,
) -> Result<OlderPageResult, String> {
    let before = if before_epoch == u64::MAX && before_seq == u64::MAX {
        None
    } else {
        Some((before_epoch, before_seq))
    };

    let page = state
        .listing_index
        .lock()
        .unwrap()
        .page(space_id, before, limit)
        .map_err(|e| e.to_string())?;

    // Reuses Task 9's `compute_has_more_older` so the live/initial spec and
    // this pagination path can never disagree about what "more older"
    // means for the same underlying listing state.
    let has_more_older = compute_has_more_older(state, space_id, &page);

    let segments = state.segments_for(space_id);
    let membership = state.membership.lock().unwrap();
    let mut observed_at = state.observed_at.lock().unwrap();

    let spec = build_conversation_spec(
        space_id,
        space_id,
        &page,
        &segments,
        &*membership,
        &mut *observed_at,
        has_more_older,
        {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
        },
    )
    .map_err(|e| e.to_string())?;

    Ok(OlderPageResult { messages: spec.messages, has_more_older })
}

#[tauri::command]
pub async fn fetch_older_page(
    state: tauri::State<'_, AppState>,
    space_id: String,
    before_epoch: u64,
    before_seq: u64,
    limit: usize,
) -> Result<OlderPageResult, String> {
    fetch_older_page_impl(&state, &space_id, before_epoch, before_seq, limit).await
}
```

- [ ] **Step 4: Register the command in `lib.rs`**

```rust
// space-chat-app/src-tauri/src/lib.rs — invoke_handler! list
        .invoke_handler(tauri::generate_handler![
            greet,
            commands::open_conversation,
            commands::close_conversation,
            commands::resync_conversation,
            commands::send_message,
            commands::react,
            commands::delete_message,
            commands::fetch_older_page,
        ])
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test commands::`
Expected: PASS (11 tests total for this module)

- [ ] **Step 6: Commit**

```bash
git add space-chat-app/src-tauri/src/commands.rs space-chat-app/src-tauri/src/lib.rs
git commit -m "feat(app): add fetch_older_page command using ListingIndex pagination"
```

---

### Task 12: Tauri commands — `create_space` / `generate_invite` / `join_via_invite`

**Files:**
- Create: `space-chat-app/src-tauri/src/invite.rs`
- Modify: `space-chat-app/src-tauri/src/commands.rs` — add the three command wrappers
- Modify: `space-chat-app/src-tauri/src/lib.rs` — add `pub mod invite;`, register commands
- Test: `space-chat-app/src-tauri/src/invite.rs` (inline)

**These commands operate entirely on the Task 3 membership placeholder.** An invite token here is an unsigned, base64-encoded JSON blob — anyone who has the string can join. This is acceptable *only* because the whole membership layer is already flagged as scaffolding pending `space-chat-openmls`; it must not ship as real invite security. Membership changes from `join_via_invite` are also **not propagated to other members over the network** in this plan — that requires either real MLS commits (once `space-chat-openmls` exists) or, at minimum, gossiping membership changes as their own kind of `SegmentChange`-adjacent message, which this plan treats as out of scope. Each of a scenario's actors gets its membership seeded directly by test setup (Task 18's `Given` steps), not by actually running `join_via_invite` across a real connection.

**Interfaces:**
- Consumes: `crate::membership::SpaceMembership` (Task 3), `AppState` (Task 8).
- Produces: `InviteToken { space_id: String, issued_at_unix_ms: u64 }`, `fn encode_invite(token: &InviteToken) -> String`, `fn decode_invite(encoded: &str) -> Result<InviteToken, String>`, `async fn create_space_impl(state: &AppState, title: String) -> String` (returns the new `space_id`), `async fn generate_invite_impl(state: &AppState, space_id: &str) -> String` (returns the encoded token), `async fn join_via_invite_impl(state: &AppState, invite_token: &str, local_display_name: &str) -> Result<String, String>` (returns the joined `space_id`), plus their `#[tauri::command]` wrappers.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/invite.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invite_tokens_round_trip_through_encode_decode() {
        let token = InviteToken { space_id: "space-1".to_string(), issued_at_unix_ms: 123 };
        let encoded = encode_invite(&token);
        let decoded = decode_invite(&encoded).unwrap();
        assert_eq!(decoded.space_id, "space-1");
        assert_eq!(decoded.issued_at_unix_ms, 123);
    }

    #[test]
    fn decode_invite_rejects_garbage_input() {
        assert!(decode_invite("not a real invite token").is_err());
    }

    #[tokio::test]
    async fn create_space_registers_the_local_device_and_returns_a_fresh_id() {
        let (_dir, state) = crate::commands::tests::fresh_state_for_invite_tests();
        let space_id = create_space_impl(&state, "General".to_string()).await;

        assert!(state.membership.lock().unwrap().members(&space_id).contains(&state.local_device));
    }

    #[tokio::test]
    async fn generate_then_decode_invite_names_the_right_space() {
        let (_dir, state) = crate::commands::tests::fresh_state_for_invite_tests();
        let space_id = create_space_impl(&state, "General".to_string()).await;

        let token = generate_invite_impl(&state, &space_id).await;
        let decoded = decode_invite(&token).unwrap();

        assert_eq!(decoded.space_id, space_id);
    }

    #[tokio::test]
    async fn join_via_invite_adds_the_local_device_to_the_named_space() {
        let (_dir, state) = crate::commands::tests::fresh_state_for_invite_tests();
        let token = encode_invite(&InviteToken {
            space_id: "space-from-someone-else".to_string(),
            issued_at_unix_ms: 0,
        });

        let joined = join_via_invite_impl(&state, &token, "Bob").await.unwrap();

        assert_eq!(joined, "space-from-someone-else");
        assert!(state
            .membership
            .lock()
            .unwrap()
            .members("space-from-someone-else")
            .contains(&state.local_device));
    }
}
```

- [ ] **Step 2: Expose a small test helper from `commands.rs` for this task's tests to reuse**

```rust
// space-chat-app/src-tauri/src/commands.rs (inside #[cfg(test)] mod tests, make it pub(crate))
    pub(crate) fn fresh_state_for_invite_tests() -> (tempfile::TempDir, AppState) {
        fresh_state()
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test invite::`
Expected: FAIL — `InviteToken`, `encode_invite`, etc. not defined.

- [ ] **Step 4: Implement the invite module**

```rust
// space-chat-app/src-tauri/src/invite.rs (above the tests module)
use crate::state::AppState;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct InviteToken {
    pub space_id: String,
    pub issued_at_unix_ms: u64,
}

/// Base64 (standard, no padding) of the token's JSON encoding. Deliberately
/// **not signed or encrypted** -- see this task's module-level caveat.
pub fn encode_invite(token: &InviteToken) -> String {
    use base64::Engine;
    let json = serde_json::to_vec(token).expect("InviteToken always serializes");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

pub fn decode_invite(encoded: &str) -> Result<InviteToken, String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|e| format!("invalid invite encoding: {e}"))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("invalid invite payload: {e}"))
}

fn now_unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

pub async fn create_space_impl(state: &AppState, title: String) -> String {
    let space_id = format!("space-{}", Uuid::new_v4());
    // `title` is not yet persisted anywhere (see Task 10's note on titles) --
    // recorded here as the local device's own display name is the only
    // per-space metadata this placeholder membership model tracks.
    let _ = &title;
    state
        .membership
        .lock()
        .unwrap()
        .create_space(&space_id, state.local_device, "Me".to_string());
    space_id
}

pub async fn generate_invite_impl(state: &AppState, space_id: &str) -> String {
    let _ = state; // present for signature symmetry with the other impls; a
                    // real (signed) invite would need to consult membership
                    // state to prove the caller may invite to this space.
    encode_invite(&InviteToken { space_id: space_id.to_string(), issued_at_unix_ms: now_unix_ms() })
}

pub async fn join_via_invite_impl(
    state: &AppState,
    invite_token: &str,
    local_display_name: &str,
) -> Result<String, String> {
    let token = decode_invite(invite_token)?;
    state.membership.lock().unwrap().add_member(
        &token.space_id,
        state.local_device,
        local_display_name.to_string(),
    );
    Ok(token.space_id)
}
```

- [ ] **Step 5: Add `base64` and register the command wrappers**

```toml
# space-chat-app/src-tauri/Cargo.toml — add to [dependencies]
base64 = "0.22"
```

```rust
// space-chat-app/src-tauri/src/commands.rs — append near the other #[tauri::command]s
#[tauri::command]
pub async fn create_space(state: tauri::State<'_, AppState>, title: String) -> Result<String, String> {
    Ok(crate::invite::create_space_impl(&state, title).await)
}

#[tauri::command]
pub async fn generate_invite(state: tauri::State<'_, AppState>, space_id: String) -> Result<String, String> {
    Ok(crate::invite::generate_invite_impl(&state, &space_id).await)
}

#[tauri::command]
pub async fn join_via_invite(
    state: tauri::State<'_, AppState>,
    invite_token: String,
    local_display_name: String,
) -> Result<String, String> {
    crate::invite::join_via_invite_impl(&state, &invite_token, &local_display_name).await
}
```

```rust
// space-chat-app/src-tauri/src/lib.rs
pub mod invite;
// ... invoke_handler! list gains:
            commands::create_space,
            commands::generate_invite,
            commands::join_via_invite,
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test invite::`
Expected: PASS (5 tests)

- [ ] **Step 7: Commit**

```bash
git add space-chat-app/src-tauri/src/invite.rs space-chat-app/src-tauri/src/commands.rs space-chat-app/src-tauri/src/lib.rs space-chat-app/src-tauri/Cargo.toml
git commit -m "feat(app): add create_space/generate_invite/join_via_invite commands (placeholder, unsigned)"
```

---

### Task 13: Custom `spacechat://attachment/<hash>` protocol handler

**Files:**
- Create: `space-chat-app/src-tauri/src/attachment_protocol.rs`
- Create: `space-chat-app/src-tauri/assets/attachment-placeholder.png` (a small, checked-in 1x1 or simple placeholder PNG)
- Modify: `space-chat-app/src-tauri/src/lib.rs` — register the protocol handler in the `tauri::Builder`
- Test: `space-chat-app/src-tauri/src/attachment_protocol.rs` (inline, testing the pure request-handling logic against a fake store — not a running webview)

**Depends on Milestone 2** (`AttachmentBlobStore`).

**Interfaces:**
- Consumes: `space_chat_core::storage::AttachmentBlobStore` (Milestone 2), `AppState` (Task 8).
- Produces: `fn parse_attachment_hash(uri_path: &str) -> Option<[u8; 32]>`, `enum AttachmentResponse { Found(Vec<u8>, String), Placeholder, NotAHash }`, `fn handle_attachment_request(store: &dyn AttachmentBlobStore, hash: &[u8; 32]) -> AttachmentResponse` (the pure, testable core), and `fn register_attachment_protocol(builder: tauri::Builder<tauri::Wry>) -> tauri::Builder<tauri::Wry>` wiring the above into a real `spacechat://` scheme via `register_asynchronous_uri_scheme_protocol`. Task 16's frontend `AttachmentImage` component points `<img src>` at this scheme.

**Cache-miss policy (this plan's Global Constraints already named the decision; here's where it's implemented): lazy, non-blocking.** On a cache miss, the handler responds immediately with the placeholder image bytes rather than blocking the protocol handler's response on a network fetch. A background task (not written in this plan's scope beyond a documented hook point — the actual attachment-fetch-over-network flow depends on Milestone 3's real transport, same caveat as `NetworkService`) is expected to eventually populate the `AttachmentBlobStore` and then emit `crate::events::attachment_ready_event_name(hash_hex)`; Task 16's frontend listens for that event and swaps the `<img>` src to force a reload, which will then hit the (now-populated) cache and get `AttachmentResponse::Found`.

- [ ] **Step 1: Write the failing tests**

```rust
// space-chat-app/src-tauri/src/attachment_protocol.rs
#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::storage::{AttachmentBlobStore, StorageError};
    use std::collections::HashMap;

    #[derive(Default)]
    struct FakeAttachmentStore {
        data: HashMap<[u8; 32], Vec<u8>>,
    }

    impl AttachmentBlobStore for FakeAttachmentStore {
        fn save_attachment(&mut self, hash: &[u8; 32], bytes: &[u8]) -> Result<(), StorageError> {
            self.data.insert(*hash, bytes.to_vec());
            Ok(())
        }
        fn load_attachment(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(hash).cloned())
        }
        fn delete_attachment(&mut self, hash: &[u8; 32]) -> Result<(), StorageError> {
            self.data.remove(hash);
            Ok(())
        }
    }

    #[test]
    fn parse_attachment_hash_decodes_a_well_formed_hex_path() {
        let hex = "01".repeat(32);
        let parsed = parse_attachment_hash(&format!("/{hex}"));
        assert_eq!(parsed, Some([1u8; 32]));
    }

    #[test]
    fn parse_attachment_hash_rejects_malformed_paths() {
        assert_eq!(parse_attachment_hash("/not-hex-at-all"), None);
        assert_eq!(parse_attachment_hash("/0102"), None); // too short
        assert_eq!(parse_attachment_hash(""), None);
    }

    #[test]
    fn a_cached_attachment_returns_found_with_its_bytes() {
        let mut store = FakeAttachmentStore::default();
        store.save_attachment(&[7u8; 32], b"image bytes").unwrap();

        match handle_attachment_request(&store, &[7u8; 32]) {
            AttachmentResponse::Found(bytes, _mime) => assert_eq!(bytes, b"image bytes"),
            other => panic!("expected Found, got {other:?}"),
        }
    }

    #[test]
    fn a_cache_miss_returns_a_placeholder_immediately_not_an_error() {
        let store = FakeAttachmentStore::default();
        match handle_attachment_request(&store, &[9u8; 32]) {
            AttachmentResponse::Placeholder => {}
            other => panic!("expected Placeholder, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd space-chat-app/src-tauri && cargo test attachment_protocol::`
Expected: FAIL — types not defined.

- [ ] **Step 3: Add a placeholder image asset**

```bash
mkdir -p space-chat-app/src-tauri/assets
```

Check in any small valid PNG at `space-chat-app/src-tauri/assets/attachment-placeholder.png` (a solid-gray square is sufficient — this is a functional placeholder, not final visual design).

- [ ] **Step 4: Implement the pure request-handling logic and the protocol registration**

```rust
// space-chat-app/src-tauri/src/attachment_protocol.rs (above the tests module)
use space_chat_core::storage::AttachmentBlobStore;

#[derive(Debug, PartialEq)]
pub enum AttachmentResponse {
    Found(Vec<u8>, String),
    Placeholder,
    NotAHash,
}

impl std::fmt::Debug for AttachmentResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttachmentResponse::Found(bytes, mime) => {
                write!(f, "Found({} bytes, {mime:?})", bytes.len())
            }
            AttachmentResponse::Placeholder => write!(f, "Placeholder"),
            AttachmentResponse::NotAHash => write!(f, "NotAHash"),
        }
    }
}

/// Parses the `<hash>` component out of a `spacechat://attachment/<hash>`
/// request's path, expecting 64 lowercase-or-uppercase hex characters.
pub fn parse_attachment_hash(uri_path: &str) -> Option<[u8; 32]> {
    let trimmed = uri_path.trim_start_matches('/');
    if trimmed.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(trimmed.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

/// The pure core of the protocol handler: cache hit -> the bytes (MIME type
/// is not tracked by `AttachmentBlobStore` itself, so this returns a generic
/// `application/octet-stream` -- refining this to the real MIME from
/// `AttachmentMetadataStore` is a natural follow-up, not attempted in this
/// plan's first pass); cache miss -> `Placeholder`, returned immediately, per
/// this plan's lazy-fetch decision -- never blocks on a network fetch.
pub fn handle_attachment_request(store: &dyn AttachmentBlobStore, hash: &[u8; 32]) -> AttachmentResponse {
    match store.load_attachment(hash) {
        Ok(Some(bytes)) => AttachmentResponse::Found(bytes, "application/octet-stream".to_string()),
        Ok(None) | Err(_) => AttachmentResponse::Placeholder,
    }
}

const PLACEHOLDER_BYTES: &[u8] = include_bytes!("../assets/attachment-placeholder.png");

/// Registers the `spacechat://` scheme on the given Tauri builder. Verify
/// `register_asynchronous_uri_scheme_protocol`'s exact signature against the
/// pinned `tauri` 2.x version's docs before relying on this as written --
/// per this plan's Global Constraints on external-crate API drift.
pub fn register_attachment_protocol(
    builder: tauri::Builder<tauri::Wry>,
    state: std::sync::Arc<crate::state::AppState>,
) -> tauri::Builder<tauri::Wry> {
    builder.register_asynchronous_uri_scheme_protocol("spacechat", move |_ctx, request, responder| {
        let state = state.clone();
        let path = request.uri().path().to_string();

        tokio::spawn(async move {
            let response = match parse_attachment_hash(&path) {
                None => AttachmentResponse::NotAHash,
                Some(hash) => {
                    let store = state.attachment_store.lock().unwrap();
                    handle_attachment_request(&*store, &hash)
                }
            };

            let (status, body, mime): (u16, Vec<u8>, &str) = match response {
                AttachmentResponse::Found(bytes, mime) => (200, bytes, Box::leak(mime.into_boxed_str())),
                AttachmentResponse::Placeholder => (200, PLACEHOLDER_BYTES.to_vec(), "image/png"),
                AttachmentResponse::NotAHash => (400, Vec::new(), "text/plain"),
            };

            let http_response = tauri::http::Response::builder()
                .status(status)
                .header("Content-Type", mime)
                .body(body)
                .unwrap();
            responder.respond(http_response);
        });
    })
}
```

- [ ] **Step 5: Wire the protocol handler into `run()`**

```rust
// space-chat-app/src-tauri/src/lib.rs
pub mod attachment_protocol;

// ... inside run():
    let app_state = std::sync::Arc::new(app_state);
    let builder = tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(app_state.clone());
    let builder = attachment_protocol::register_attachment_protocol(builder, app_state);
    builder
        .invoke_handler(tauri::generate_handler![
            greet,
            commands::open_conversation,
            commands::close_conversation,
            commands::resync_conversation,
            commands::send_message,
            commands::react,
            commands::delete_message,
            commands::fetch_older_page,
            commands::create_space,
            commands::generate_invite,
            commands::join_via_invite,
        ])
        .run(tauri::generate_context!())
        .expect("error while running space-chat-app");
```

Note: switching `AppState` to be `.manage()`d as `Arc<AppState>` (rather than bare `AppState`) means every `tauri::State<'_, AppState>` parameter in `commands.rs` becomes `tauri::State<'_, Arc<AppState>>` — update those signatures; the `&*state` deref inside each command body already works unchanged since `Arc<AppState>` derefs to `&AppState`.

- [ ] **Step 6: Update `commands.rs`'s command signatures for `Arc<AppState>`**

```rust
// space-chat-app/src-tauri/src/commands.rs — every #[tauri::command] fn's
// first parameter changes from:
//   state: tauri::State<'_, AppState>
// to:
    state: tauri::State<'_, std::sync::Arc<AppState>>,
// The function bodies are unchanged -- `&state` still derefs to `&AppState`
// everywhere it's passed to an `_impl` function.
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd space-chat-app/src-tauri && cargo test attachment_protocol::`
Expected: PASS (4 tests)

Run: `cd space-chat-app/src-tauri && cargo build`
Expected: builds cleanly with the `Arc<AppState>` signature change applied consistently.

- [ ] **Step 8: Commit**

```bash
git add space-chat-app/src-tauri/src/attachment_protocol.rs space-chat-app/src-tauri/src/lib.rs space-chat-app/src-tauri/src/commands.rs space-chat-app/src-tauri/assets
git commit -m "feat(app): add spacechat:// attachment protocol handler with lazy-fetch placeholder policy"
```

---

### Task 14: Frontend local `SpaceChatSpecRenderer` — `conversation` kind + `card`/`text`/`flex`/`grid` + fallback

**Files:**
- Create: `space-chat-app/src/spec.ts`
- Create: `space-chat-app/src/SpaceChatSpecRenderer.tsx`
- Create: `space-chat-app/src/components/ConversationView.tsx`
- Create: `space-chat-app/src/SpaceChatSpecRenderer.test.tsx`
- Modify: `space-chat-app/package.json` — already has the needed deps from Task 1

**Interfaces:**
- Consumes: `RootSpec`/`TextSpec`/`TabsSpec`/`DetailsSpec` and `SpecRenderer` from `@retrofit-ui/spa-solid-shoelace/components` (real, current API — `packages/spa-solid-shoelace/ui/SpecRenderer.tsx`'s prop type is exactly `{ spec: RootSpec | TextSpec | TabsSpec | DetailsSpec; apiBase: string }`).
- Produces: TypeScript types mirroring Task 2's Rust `spec.rs` shapes (`ConversationSpec`, `ConversationErrorSpec`, `MessageSpec`, `AttachmentSpec`, `ReactionSpec`, `CardSpec`, `TextSpec` (local), `FlexSpec`, `GridSpec`, `SpaceChatViewSpec` union), `SpaceChatSpecRenderer` (the top-level component Task 15/16 render), `ConversationView` (the bespoke message-list component behind the `conversation` kind). Falls back to retrofit-ui's real `SpecRenderer` for any kind not in the local `Switch`, per this workspace's `tenju-tofu`/`chalk-app` convention (mirrored directly from `chalk-app/src/agents/ada/ChalkSpecRenderer.tsx`'s `ViewNode` pattern).

- [ ] **Step 1: Define the local TypeScript spec types**

```typescript
// space-chat-app/src/spec.ts
export interface AttachmentSpec {
  url: string;
  mime: string;
  size: number;
}

export interface ReactionSpec {
  emoji: string;
  actor_name: string;
}

export interface MessageSpec {
  id: string;
  sender_id: string;
  sender_name: string;
  content: string;
  relative_time: string;
  attachments: AttachmentSpec[];
  reactions: ReactionSpec[];
  deleted: boolean;
}

export interface ConversationSpec {
  kind: "conversation";
  space_id: string;
  title: string;
  messages: MessageSpec[];
  has_more_older: boolean;
}

export interface ConversationErrorSpec {
  kind: "conversation-error";
  space_id: string;
  message: string;
}

export interface CardSpec {
  kind: "card";
  header?: string;
  children: SpaceChatViewSpec[];
}

export interface TextSpec {
  kind: "text";
  content: string;
  variant?: string;
}

export interface FlexSpec {
  kind: "flex";
  direction?: string;
  gap?: string;
  children: SpaceChatViewSpec[];
}

export interface GridSpec {
  kind: "grid";
  columns?: number;
  gap?: string;
  children: SpaceChatViewSpec[];
}

export type SpaceChatViewSpec =
  | ConversationSpec
  | ConversationErrorSpec
  | CardSpec
  | TextSpec
  | FlexSpec
  | GridSpec;
```

- [ ] **Step 2: Write the failing renderer tests (fallback path + conversation kind)**

```tsx
// space-chat-app/src/SpaceChatSpecRenderer.test.tsx
import { describe, expect, it } from "vitest";
import { render, screen } from "@solidjs/testing-library";
import SpaceChatSpecRenderer from "./SpaceChatSpecRenderer";
import type { SpaceChatViewSpec } from "./spec";

describe("SpaceChatSpecRenderer", () => {
  it("renders the local conversation kind with its messages", () => {
    const spec: SpaceChatViewSpec = {
      kind: "conversation",
      space_id: "space-1",
      title: "General",
      has_more_older: false,
      messages: [
        {
          id: "msg:1",
          sender_id: "01",
          sender_name: "Alice",
          content: "hello there",
          relative_time: "just now",
          attachments: [],
          reactions: [],
          deleted: false,
        },
      ],
    };

    render(() => <SpaceChatSpecRenderer spec={spec} />);

    expect(screen.getByText("hello there")).toBeInTheDocument();
    expect(screen.getByText("Alice")).toBeInTheDocument();
  });

  it("renders a conversation-error kind as a visible error message, not a crash", () => {
    const spec: SpaceChatViewSpec = {
      kind: "conversation-error",
      space_id: "space-1",
      message: "missing segment for epoch 3",
    };

    render(() => <SpaceChatSpecRenderer spec={spec} />);

    expect(screen.getByText(/missing segment for epoch 3/)).toBeInTheDocument();
  });

  it("recurses through local card/flex/grid/text kinds", () => {
    const spec: SpaceChatViewSpec = {
      kind: "card",
      header: "Sidebar",
      children: [{ kind: "text", content: "some sidebar text" }],
    };

    render(() => <SpaceChatSpecRenderer spec={spec} />);

    expect(screen.getByText("Sidebar")).toBeInTheDocument();
    expect(screen.getByText("some sidebar text")).toBeInTheDocument();
  });

  it("falls back to retrofit-ui's real SpecRenderer for a standard, non-local kind", () => {
    // `stat` is a real retrofit-ui RootSpec kind (packages/core/src/types/resource-spec.ts)
    // that SpaceChatSpecRenderer has no local case for -- it must still render
    // correctly through the fallback, proving the fallback path actually works,
    // not just that unhandled kinds silently do nothing.
    const spec = {
      kind: "stat",
      stats: [{ label: "Unread", value: 3 }],
    } as unknown as SpaceChatViewSpec;

    render(() => <SpaceChatSpecRenderer spec={spec} />);

    expect(screen.getByText("Unread")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
  });
});
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd space-chat-app && pnpm vitest run SpaceChatSpecRenderer`
Expected: FAIL — `./SpaceChatSpecRenderer` does not exist.

- [ ] **Step 4: Implement `ConversationView` and `SpaceChatSpecRenderer`**

```tsx
// space-chat-app/src/components/ConversationView.tsx
import { type Component, For, Show } from "solid-js";
import type { ConversationSpec } from "../spec";

const ConversationView: Component<{ spec: ConversationSpec }> = (props) => {
  return (
    <div class="conversation-view" data-space-id={props.spec.space_id}>
      <h2>{props.spec.title}</h2>
      <ul class="message-list">
        <For each={props.spec.messages}>
          {(message) => (
            <li class="message" data-message-id={message.id} classList={{ deleted: message.deleted }}>
              <span class="sender-name">{message.sender_name}</span>
              <span class="relative-time">{message.relative_time}</span>
              <Show when={!message.deleted} fallback={<p class="deleted-marker">(message deleted)</p>}>
                <p class="content">{message.content}</p>
              </Show>
              <For each={message.attachments}>
                {(attachment) => (
                  <img class="attachment-image" src={attachment.url} alt="attachment" data-mime={attachment.mime} />
                )}
              </For>
              <div class="reactions">
                <For each={message.reactions}>
                  {(reaction) => <span class="reaction">{reaction.emoji}</span>}
                </For>
              </div>
            </li>
          )}
        </For>
      </ul>
    </div>
  );
};

export default ConversationView;
```

```tsx
// space-chat-app/src/SpaceChatSpecRenderer.tsx
import type { RootSpec } from "@retrofit-ui/core";
import { SpecRenderer } from "@retrofit-ui/spa-solid-shoelace/components";
import { type Component, For, Match, Show, Switch } from "solid-js";
import ConversationView from "./components/ConversationView";
import type { SpaceChatViewSpec } from "./spec";

/// Recursively renders one local spec node, following the same pattern
/// `chalk-app`'s `ChalkSpecRenderer`/`tenju-tofu`'s `TenjuSpecRenderer`
/// already establish in this workspace: bespoke kinds get bespoke
/// components, common structural kinds (`card`/`text`/`flex`/`grid`) are
/// reimplemented locally so recursion can interleave the bespoke kinds
/// retrofit-ui's own `SpecRenderer` has no way to know about, and anything
/// else falls back to the real `SpecRenderer`.
const ViewNode: Component<{ spec: SpaceChatViewSpec }> = (props) => {
  return (
    <Switch fallback={<SpecRenderer spec={props.spec as unknown as RootSpec} apiBase="" />}>
      <Match when={props.spec.kind === "conversation"}>
        <ConversationView spec={props.spec as Extract<SpaceChatViewSpec, { kind: "conversation" }>} />
      </Match>
      <Match when={props.spec.kind === "conversation-error"}>
        <div class="conversation-error" role="alert">
          {(props.spec as Extract<SpaceChatViewSpec, { kind: "conversation-error" }>).message}
        </div>
      </Match>
      <Match when={props.spec.kind === "card"}>
        <div class="spec-card">
          <Show when={(props.spec as Extract<SpaceChatViewSpec, { kind: "card" }>).header}>
            <div class="spec-card-header">
              {(props.spec as Extract<SpaceChatViewSpec, { kind: "card" }>).header}
            </div>
          </Show>
          <div class="spec-card-body">
            <For each={(props.spec as Extract<SpaceChatViewSpec, { kind: "card" }>).children}>
              {(child) => <ViewNode spec={child} />}
            </For>
          </div>
        </div>
      </Match>
      <Match when={props.spec.kind === "flex"}>
        <div
          class="spec-flex"
          style={{
            display: "flex",
            "flex-direction": (props.spec as Extract<SpaceChatViewSpec, { kind: "flex" }>).direction ?? "column",
            gap: (props.spec as Extract<SpaceChatViewSpec, { kind: "flex" }>).gap ?? "0.75rem",
          }}
        >
          <For each={(props.spec as Extract<SpaceChatViewSpec, { kind: "flex" }>).children}>
            {(child) => <ViewNode spec={child} />}
          </For>
        </div>
      </Match>
      <Match when={props.spec.kind === "grid"}>
        <div
          class="spec-grid"
          style={{
            display: "grid",
            "grid-template-columns": `repeat(${(props.spec as Extract<SpaceChatViewSpec, { kind: "grid" }>).columns ?? 2}, 1fr)`,
            gap: (props.spec as Extract<SpaceChatViewSpec, { kind: "grid" }>).gap ?? "0.75rem",
          }}
        >
          <For each={(props.spec as Extract<SpaceChatViewSpec, { kind: "grid" }>).children}>
            {(child) => <ViewNode spec={child} />}
          </For>
        </div>
      </Match>
      <Match when={props.spec.kind === "text"}>
        <div class="spec-text" data-variant={(props.spec as Extract<SpaceChatViewSpec, { kind: "text" }>).variant ?? "body"}>
          {(props.spec as Extract<SpaceChatViewSpec, { kind: "text" }>).content}
        </div>
      </Match>
    </Switch>
  );
};

const SpaceChatSpecRenderer: Component<{ spec: SpaceChatViewSpec }> = (props) => {
  return <ViewNode spec={props.spec} />;
};

export default SpaceChatSpecRenderer;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd space-chat-app && pnpm vitest run SpaceChatSpecRenderer`
Expected: PASS (4 tests) — including the fallback-path test, which is this plan's implementation of the app-shell spec's "verify the custom-kind fallback path" testing requirement. Per this plan's testing approach (see the closing notes), this one check is done as a fast Vitest component test rather than through the `cucumber`/`fantoccini` harness, since it needs no multi-actor backend at all — just proof that an unrecognized kind still renders via the real `SpecRenderer`.

- [ ] **Step 6: Commit**

```bash
git add space-chat-app/src/spec.ts space-chat-app/src/SpaceChatSpecRenderer.tsx space-chat-app/src/SpaceChatSpecRenderer.test.tsx space-chat-app/src/components/ConversationView.tsx
git commit -m "feat(app): add local SpaceChatSpecRenderer (conversation kind + card/text/flex/grid + SpecRenderer fallback)"
```

---

### Task 15: Frontend live-spec client (`liveSpecClient.ts`)

**Files:**
- Create: `space-chat-app/src/liveSpecClient.ts`
- Create: `space-chat-app/src/liveSpecClient.test.ts`

**Interfaces:**
- Consumes: `@tauri-apps/api/core`'s `invoke`, `@tauri-apps/api/event`'s `listen`, `fast-json-patch`'s `applyPatch`, `SpaceChatViewSpec` (Task 14).
- Produces: `type PatchResponse = { kind: "unchanged"; version: number } | { kind: "patch"; version: number; patch: import("fast-json-patch").Operation[] } | { kind: "full"; version: number; spec: SpaceChatViewSpec }`, `function applyPatchResponse(current: SpaceChatViewSpec, response: PatchResponse): SpaceChatViewSpec`, `async function openConversation(spaceId: string, title: string, onUpdate: (spec: SpaceChatViewSpec) => void): Promise<() => void>` (returns an unlisten function), matching Task 6/9's Rust-side `PatchResponse`'s `#[serde(tag = "kind", rename_all = "snake_case")]` shape exactly. Task 16's `App.tsx` calls `openConversation`.

- [ ] **Step 1: Write the failing tests for `applyPatchResponse`**

```typescript
// space-chat-app/src/liveSpecClient.test.ts
import { describe, expect, it } from "vitest";
import { applyPatchResponse, type PatchResponse } from "./liveSpecClient";
import type { SpaceChatViewSpec } from "./spec";

const conversation: SpaceChatViewSpec = {
  kind: "conversation",
  space_id: "space-1",
  title: "General",
  has_more_older: false,
  messages: [],
};

describe("applyPatchResponse", () => {
  it("returns the current spec unchanged for an 'unchanged' response", () => {
    const response: PatchResponse = { kind: "unchanged", version: 3 };
    expect(applyPatchResponse(conversation, response)).toEqual(conversation);
  });

  it("applies a JSON Patch for a 'patch' response", () => {
    const response: PatchResponse = {
      kind: "patch",
      version: 4,
      patch: [{ op: "replace", path: "/title", value: "General (renamed)" }],
    };
    const result = applyPatchResponse(conversation, response) as typeof conversation;
    expect(result.title).toBe("General (renamed)");
  });

  it("replaces the whole spec for a 'full' response", () => {
    const fullSpec: SpaceChatViewSpec = { ...conversation, title: "Replaced entirely" };
    const response: PatchResponse = { kind: "full", version: 9, spec: fullSpec };
    expect(applyPatchResponse(conversation, response)).toEqual(fullSpec);
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd space-chat-app && pnpm vitest run liveSpecClient`
Expected: FAIL — `./liveSpecClient` does not exist.

- [ ] **Step 3: Implement `liveSpecClient.ts`**

```typescript
// space-chat-app/src/liveSpecClient.ts
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { applyPatch, type Operation } from "fast-json-patch";
import type { SpaceChatViewSpec } from "./spec";

export type PatchResponse =
  | { kind: "unchanged"; version: number }
  | { kind: "patch"; version: number; patch: Operation[] }
  | { kind: "full"; version: number; spec: SpaceChatViewSpec };

/// Mirrors the Rust side's `LiveSpec::diff_since` semantics exactly (Task 6):
/// `unchanged` -> nothing to do, `patch` -> apply the RFC 6902 ops against
/// the caller's current spec, `full` -> replace outright. This is also
/// exactly the path a post-restart resync takes (Global Constraints'
/// backend-restart property) -- from this function's point of view a "the
/// backend restarted" full resend and a "you were too far behind" full
/// resend are the same case, by design.
export function applyPatchResponse(current: SpaceChatViewSpec, response: PatchResponse): SpaceChatViewSpec {
  switch (response.kind) {
    case "unchanged":
      return current;
    case "patch":
      return applyPatch(structuredClone(current), response.patch, false, false).newDocument as SpaceChatViewSpec;
    case "full":
      return response.spec;
  }
}

interface ConversationPatchEvent {
  space_id: string;
  kind: PatchResponse["kind"];
  version: number;
  patch?: Operation[];
  spec?: SpaceChatViewSpec;
}

/// Opens `spaceId` (calling the `open_conversation` command), invokes
/// `onUpdate` with the initial spec, then subscribes to
/// `conversation-patch:<spaceId>` Tauri events and calls `onUpdate` again
/// with each newly-patched spec as it arrives. Returns a function that both
/// unsubscribes the event listener and calls `close_conversation`.
export async function openConversation(
  spaceId: string,
  title: string,
  onUpdate: (spec: SpaceChatViewSpec) => void,
): Promise<() => void> {
  const initial = await invoke<{ version: number; spec: SpaceChatViewSpec }>("open_conversation", {
    spaceId,
    title,
  });
  let current = initial.spec;
  onUpdate(current);

  const unlisten = await listen<ConversationPatchEvent>(`conversation-patch:${spaceId}`, (event) => {
    const payload = event.payload;
    const response: PatchResponse =
      payload.kind === "patch"
        ? { kind: "patch", version: payload.version, patch: payload.patch ?? [] }
        : payload.kind === "full"
          ? { kind: "full", version: payload.version, spec: payload.spec as SpaceChatViewSpec }
          : { kind: "unchanged", version: payload.version };
    current = applyPatchResponse(current, response);
    onUpdate(current);
  });

  return async () => {
    unlisten();
    await invoke("close_conversation", { spaceId });
  };
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd space-chat-app && pnpm vitest run liveSpecClient`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add space-chat-app/src/liveSpecClient.ts space-chat-app/src/liveSpecClient.test.ts
git commit -m "feat(app): add frontend live-spec client applying RFC 6902 patches from Tauri events"
```

---

### Task 16: Frontend attachment image component + top-level `App.tsx` wiring

**Files:**
- Create: `space-chat-app/src/components/AttachmentImage.tsx`
- Create: `space-chat-app/src/components/AttachmentImage.test.tsx`
- Modify: `space-chat-app/src/components/ConversationView.tsx` — use `AttachmentImage` instead of a bare `<img>`
- Modify: `space-chat-app/src/App.tsx` — wire `openConversation`/`SpaceChatSpecRenderer` together into a real, clickable single-conversation view

**Interfaces:**
- Consumes: `@tauri-apps/api/event`'s `listen`, `crate::events::attachment_ready_event_name`'s naming convention (`attachment-ready:<hash>`) from Task 13, `openConversation` (Task 15), `SpaceChatSpecRenderer` (Task 14).
- Produces: `AttachmentImage` (a `Component<{ url: string; mime: string }>` that shows a placeholder state until an `attachment-ready:<hash>` event for its URL's hash fires, then swaps to the real `<img src>`), and a real, minimally-styled but functional `App.tsx` that opens a conversation, renders its live spec, and lets a user type and send a message — the actual "clickable app" this plan's scope demands.

- [ ] **Step 1: Write the failing test for `AttachmentImage`'s placeholder-then-loaded transition**

```tsx
// space-chat-app/src/components/AttachmentImage.test.tsx
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@solidjs/testing-library";
import AttachmentImage from "./AttachmentImage";

const listeners: Record<string, (event: { payload: unknown }) => void> = {};

vi.mock("@tauri-apps/api/event", () => ({
  listen: async (name: string, handler: (event: { payload: unknown }) => void) => {
    listeners[name] = handler;
    return () => {
      delete listeners[name];
    };
  },
}));

describe("AttachmentImage", () => {
  it("shows a placeholder state, then swaps to the loaded image once attachment-ready fires", async () => {
    const hash = "ab".repeat(32);
    const url = `spacechat://attachment/${hash}`;

    render(() => <AttachmentImage url={url} mime="image/png" />);

    expect(screen.getByTestId("attachment-placeholder")).toBeInTheDocument();
    expect(screen.queryByTestId("attachment-loaded")).not.toBeInTheDocument();

    listeners[`attachment-ready:${hash}`]?.({ payload: {} });
    await Promise.resolve();

    expect(screen.getByTestId("attachment-loaded")).toBeInTheDocument();
    expect(screen.queryByTestId("attachment-placeholder")).not.toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd space-chat-app && pnpm vitest run AttachmentImage`
Expected: FAIL — `./AttachmentImage` does not exist.

- [ ] **Step 3: Implement `AttachmentImage`**

```tsx
// space-chat-app/src/components/AttachmentImage.tsx
import { listen } from "@tauri-apps/api/event";
import { createSignal, onCleanup, onMount, type Component } from "solid-js";

function hashFromUrl(url: string): string {
  return url.replace(/^spacechat:\/\/attachment\//, "");
}

/// Starts in a placeholder state (per this plan's lazy-fetch policy, Task
/// 13's protocol handler responds with placeholder bytes immediately on a
/// cache miss, so the `<img>` itself would already show *something* --
/// this component's own placeholder/loaded state additionally tracks
/// whether the *real* bytes have arrived, driven by the
/// `attachment-ready:<hash>` event, and forces the `<img>` to reload once
/// they have by re-keying its `src` with a cache-busting query param).
const AttachmentImage: Component<{ url: string; mime: string }> = (props) => {
  const [ready, setReady] = createSignal(false);

  onMount(async () => {
    const hash = hashFromUrl(props.url);
    const unlisten = await listen(`attachment-ready:${hash}`, () => {
      setReady(true);
    });
    onCleanup(() => unlisten());
  });

  return (
    <>
      {!ready() && (
        <div class="attachment-placeholder" data-testid="attachment-placeholder">
          loading attachment…
        </div>
      )}
      {ready() && (
        <img
          class="attachment-loaded"
          data-testid="attachment-loaded"
          src={`${props.url}?ready=1`}
          data-mime={props.mime}
          alt="attachment"
        />
      )}
    </>
  );
};

export default AttachmentImage;
```

- [ ] **Step 4: Use `AttachmentImage` in `ConversationView`**

```tsx
// space-chat-app/src/components/ConversationView.tsx — replace the bare <img> loop
import AttachmentImage from "./AttachmentImage";
// ...
              <For each={message.attachments}>
                {(attachment) => <AttachmentImage url={attachment.url} mime={attachment.mime} />}
              </For>
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd space-chat-app && pnpm vitest run AttachmentImage`
Expected: PASS (1 test)

- [ ] **Step 6: Wire a real, clickable `App.tsx`**

```tsx
// space-chat-app/src/App.tsx
import { createSignal, onCleanup, onMount, type Component } from "solid-js";
import { invoke } from "@tauri-apps/api/core";
import { openConversation } from "./liveSpecClient";
import SpaceChatSpecRenderer from "./SpaceChatSpecRenderer";
import type { SpaceChatViewSpec } from "./spec";

const DEFAULT_SPACE_ID = "space-default";

const App: Component = () => {
  const [spec, setSpec] = createSignal<SpaceChatViewSpec | null>(null);
  const [draft, setDraft] = createSignal("");
  let close: (() => void) | undefined;

  onMount(async () => {
    await invoke("create_space", { title: "General" }).catch(() => {
      // Space may already exist from a prior run against the same data dir
      // -- create_space always mints a fresh id today (Task 12), so a real
      // "does this space already exist" check is a natural follow-up; for
      // this plan's clickable-app scope, DEFAULT_SPACE_ID is opened
      // directly regardless of whether create_space's returned id matches.
    });
    close = await openConversation(DEFAULT_SPACE_ID, "General", setSpec);
  });

  onCleanup(() => close?.());

  const send = async () => {
    const content = draft().trim();
    if (!content) return;
    await invoke("send_message", { spaceId: DEFAULT_SPACE_ID, content });
    setDraft("");
  };

  return (
    <div class="app">
      {spec() && <SpaceChatSpecRenderer spec={spec()!} />}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          send();
        }}
      >
        <input
          data-testid="message-input"
          value={draft()}
          onInput={(e) => setDraft(e.currentTarget.value)}
          placeholder="Type a message…"
        />
        <button type="submit" data-testid="send-button">
          Send
        </button>
      </form>
    </div>
  );
};

export default App;
```

- [ ] **Step 7: Verify the frontend still builds**

Run: `cd space-chat-app && pnpm build`
Expected: succeeds.

- [ ] **Step 8: Commit**

```bash
git add space-chat-app/src/components/AttachmentImage.tsx space-chat-app/src/components/AttachmentImage.test.tsx space-chat-app/src/components/ConversationView.tsx space-chat-app/src/App.tsx
git commit -m "feat(app): add attachment placeholder-to-loaded transition and wire a real clickable App.tsx"
```

---

### Task 17: `cucumber-rs` + `fantoccini` harness scaffold

**Files:**
- Create: `space-chat-app/src-tauri/tests/cucumber/main.rs`
- Create: `space-chat-app/src-tauri/tests/cucumber/world.rs`
- Create: `space-chat-app/src-tauri/tests/cucumber/steps.rs`
- Modify: `space-chat-app/src-tauri/Cargo.toml` — add `cucumber`, `fantoccini` dev-dependencies and the `[[test]]` harness entry

**Real WebDriver bridging is genuinely OS-dependent, and this is worth being explicit about rather than silently assuming it works everywhere.** Tauri's WebDriver bridge (`tauri-driver`) currently has first-party support on Linux (via `WebKitWebDriver`) and Windows (via `msedgedriver`); macOS support is a known gap in the Tauri project as of writing. This plan's harness runs on Linux/Windows CI; on macOS, the multi-actor scenarios in Tasks 18–20 cannot run through this exact automated path until Tauri/`tauri-driver` close that gap upstream — verify current status before relying on this, and treat a macOS run as "not yet automatable this way," not silently skipped without a note in CI output.

**Interfaces:**
- Consumes: `fantoccini::Client`, `cucumber::{World, given, when, then}`, `SPACECHAT_DATA_DIR` env var (Task 8) for per-actor data isolation, `run_loopback_relay`/`TcpLoopbackNetworkService` (Task 7) for the stand-in network.
- Produces: `struct SpaceChatWorld { relay_addr: Option<SocketAddr>, actors: HashMap<String, Actor> }`, `struct Actor { process: std::process::Child, webdriver_client: fantoccini::Client, data_dir: tempfile::TempDir }`, `async fn spawn_actor(name: &str, relay_addr: SocketAddr, webdriver_port: u16) -> Actor`, `async fn stop_actor(actor: Actor)`. Tasks 18–20's `Given`/`When`/`Then` step implementations are written directly against this `World`.

- [ ] **Step 1: Add harness dependencies and the test target**

```toml
# space-chat-app/src-tauri/Cargo.toml
[dev-dependencies]
tempfile = "3"
tokio = { version = "1", features = ["full", "test-util"] }
cucumber = "0.21"
fantoccini = "0.20"
reqwest = { version = "0.12", features = ["json"] }

[[test]]
name = "cucumber"
harness = false
path = "tests/cucumber/main.rs"
```

- [ ] **Step 2: Implement the `World` and actor process management**

```rust
// space-chat-app/src-tauri/tests/cucumber/world.rs
use cucumber::World;
use fantoccini::{Client, ClientBuilder};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::{Child, Command};

pub struct Actor {
    pub process: Child,
    pub webdriver_client: Client,
    pub data_dir: tempfile::TempDir,
    pub webdriver_port: u16,
}

#[derive(World)]
#[world(init = Self::new)]
pub struct SpaceChatWorld {
    pub relay_addr: Option<SocketAddr>,
    pub relay_handle: Option<tokio::task::JoinHandle<()>>,
    pub actors: HashMap<String, Actor>,
    pub next_webdriver_port: u16,
}

impl std::fmt::Debug for SpaceChatWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpaceChatWorld")
            .field("relay_addr", &self.relay_addr)
            .field("actors", &self.actors.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl SpaceChatWorld {
    fn new() -> Self {
        Self { relay_addr: None, relay_handle: None, actors: HashMap::new(), next_webdriver_port: 9515 }
    }

    /// Starts the stand-in network relay for this scenario, if not already
    /// running. Idempotent per scenario.
    pub async fn ensure_relay(&mut self) -> SocketAddr {
        if let Some(addr) = self.relay_addr {
            return addr;
        }
        let (handle, addr) =
            space_chat_app_lib::network::run_loopback_relay("127.0.0.1:0".parse().unwrap()).await;
        self.relay_addr = Some(addr);
        self.relay_handle = Some(handle);
        addr
    }

    /// Spawns a real `space-chat-app` OS process for `name`, pointed at its
    /// own temp data directory and this scenario's relay, then connects a
    /// `fantoccini::Client` to it through `tauri-driver`'s WebDriver bridge.
    /// Assumes `tauri-driver` is already running and listening on
    /// `self.next_webdriver_port` for this actor -- see this task's note on
    /// starting `tauri-driver` itself as a prerequisite the CI/dev setup
    /// (not this Rust code) is responsible for per actor.
    pub async fn spawn_actor(&mut self, name: &str) {
        let relay_addr = self.ensure_relay().await;
        let data_dir = tempfile::tempdir().expect("failed to create actor temp dir");
        let webdriver_port = self.next_webdriver_port;
        self.next_webdriver_port += 1;

        let binary = env!("CARGO_BIN_EXE_space-chat-app");
        let process = Command::new(binary)
            .env("SPACECHAT_DATA_DIR", data_dir.path())
            .env("SPACECHAT_RELAY_ADDR", relay_addr.to_string())
            .env("SPACECHAT_ACTOR_NAME", name)
            .spawn()
            .expect("failed to spawn space-chat-app actor process");

        // Give the app + its WebView + tauri-driver's bridge a moment to
        // come up before the WebDriver session request; a real harness run
        // should replace this with a polling wait against tauri-driver's
        // status endpoint rather than a fixed sleep.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let webdriver_client = ClientBuilder::native()
            .connect(&format!("http://localhost:{webdriver_port}"))
            .await
            .expect("failed to connect fantoccini client to tauri-driver");

        self.actors.insert(
            name.to_string(),
            Actor { process, webdriver_client, data_dir, webdriver_port },
        );
    }

    /// Kills `name`'s process without removing its temp data dir, so a
    /// later `relaunch_actor` call can point a fresh process at the same
    /// on-disk state -- Task 20's restart-recovery scenario needs exactly
    /// this.
    pub async fn kill_actor(&mut self, name: &str) {
        if let Some(actor) = self.actors.get_mut(name) {
            let _ = actor.process.kill();
            let _ = actor.process.wait();
        }
    }

    pub async fn relaunch_actor(&mut self, name: &str) {
        let (data_dir_path, webdriver_port, relay_addr) = {
            let actor = self.actors.get(name).expect("actor must have been spawned before relaunch");
            (actor.data_dir.path().to_path_buf(), actor.webdriver_port, self.relay_addr.unwrap())
        };

        let binary = env!("CARGO_BIN_EXE_space-chat-app");
        let process = Command::new(binary)
            .env("SPACECHAT_DATA_DIR", &data_dir_path)
            .env("SPACECHAT_RELAY_ADDR", relay_addr.to_string())
            .env("SPACECHAT_ACTOR_NAME", name)
            .spawn()
            .expect("failed to relaunch space-chat-app actor process");

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let webdriver_client = ClientBuilder::native()
            .connect(&format!("http://localhost:{webdriver_port}"))
            .await
            .expect("failed to reconnect fantoccini client after relaunch");

        if let Some(actor) = self.actors.get_mut(name) {
            actor.process = process;
            actor.webdriver_client = webdriver_client;
        }
    }
}
```

- [ ] **Step 3: Wire the cucumber test entry point**

```rust
// space-chat-app/src-tauri/tests/cucumber/main.rs
mod steps;
mod world;

#[tokio::main]
async fn main() {
    world::SpaceChatWorld::run("tests/cucumber/features").await;
}
```

```rust
// space-chat-app/src-tauri/tests/cucumber/steps.rs
// Populated incrementally by Tasks 18-20; empty scaffold for this task.
use crate::world::SpaceChatWorld;
use cucumber::{given, then, when};

#[given(regex = r"^(\w+) is a device in the space, online$")]
async fn given_actor_online(world: &mut SpaceChatWorld, name: String) {
    world.spawn_actor(&name).await;
}
```

- [ ] **Step 4: Create the features directory**

```bash
mkdir -p space-chat-app/src-tauri/tests/cucumber/features
```

- [ ] **Step 5: Verify the harness compiles (it has no scenarios to run yet, so this is a build check, not a pass/fail run)**

Run: `cd space-chat-app/src-tauri && cargo build --test cucumber`
Expected: builds cleanly. Running it (`cargo test --test cucumber`) at this point will report zero scenarios found, which is expected until Task 18 adds a `.feature` file.

- [ ] **Step 6: Commit**

```bash
git add space-chat-app/src-tauri/tests/cucumber space-chat-app/src-tauri/Cargo.toml
git commit -m "test(app): scaffold cucumber-rs + fantoccini multi-actor harness"
```

---

### Task 18: Golden-path multi-actor scenario (exit-criteria proof)

**Files:**
- Create: `space-chat-app/src-tauri/tests/cucumber/features/golden_path.feature`
- Modify: `space-chat-app/src-tauri/tests/cucumber/steps.rs`
- Modify: `space-chat-app/src-tauri/src/lib.rs` — read `SPACECHAT_RELAY_ADDR`/`SPACECHAT_ACTOR_NAME` env vars at startup to construct a real `TcpLoopbackNetworkService` instead of `NullNetworkService` when present (needed for this scenario's actors to actually talk to each other)

This is the milestone's exit-criteria proof, per `projects/plans/2026-09-06-milestone-timeline.md`'s Milestone 4 section: "the full `cucumber-rs`+`fantoccini` multi-actor harness... passes against a real, rendered Tauri app." **This scenario uses Task 7's `TcpLoopbackNetworkService` stand-in, not real `iroh`** — see this plan's Global Constraints and Task 7's closing note. Everything else is real: real `space-chat-core`, real Milestone 2 storage, real separate OS processes per actor, real rendered Tauri webviews, real DOM assertions through `fantoccini`.

- [ ] **Step 1: Wire the real network into `run()` based on environment variables**

```rust
// space-chat-app/src-tauri/src/lib.rs — replace the NullNetworkService construction in run()
    let network: Box<dyn network::NetworkService> = match std::env::var("SPACECHAT_RELAY_ADDR") {
        Ok(addr) => {
            let addr: std::net::SocketAddr = addr.parse().expect("SPACECHAT_RELAY_ADDR must be a valid socket address");
            let space_id = std::env::var("SPACECHAT_SPACE_ID").unwrap_or_else(|_| "space-default".to_string());
            // Tauri's own async runtime isn't up yet at this point in
            // `run()`; use a short-lived current-thread runtime purely to
            // drive this one connect call synchronously before handing
            // control to `tauri::Builder::run`.
            let rt = tokio::runtime::Runtime::new().expect("failed to create bootstrap runtime");
            Box::new(
                rt.block_on(network::TcpLoopbackNetworkService::connect(addr, space_id))
                    .expect("failed to connect to SPACECHAT_RELAY_ADDR"),
            )
        }
        Err(_) => Box::new(network::NullNetworkService::default()),
    };
```

- [ ] **Step 2: Write the golden-path feature file**

```gherkin
# space-chat-app/src-tauri/tests/cucumber/features/golden_path.feature
Feature: Golden path message delivery

  Scenario: A message sent by one online device appears in another's conversation view
    Given Alice and Bob are online devices in the same space
    When Alice sends the message "hello from alice"
    Then Bob's conversation view shows "hello from alice" within 5 seconds
```

- [ ] **Step 3: Implement the step definitions**

```rust
// space-chat-app/src-tauri/tests/cucumber/steps.rs (extend)
use cucumber::{given, then, when};
use fantoccini::Locator;
use std::time::Duration;

use crate::world::SpaceChatWorld;

const SHARED_SPACE_ID: &str = "space-golden-path";

#[given(regex = r"^(\w+) and (\w+) are online devices in the same space$")]
async fn given_two_actors_online(world: &mut SpaceChatWorld, a: String, b: String) {
    for name in [&a, &b] {
        world.spawn_actor(name).await;
        // Seed both actors' membership so their `AppState`'s local
        // PlaintextMembership (Task 3) already knows about the other,
        // consistent with this plan's documented scope: membership
        // propagation over the network is not implemented, so it's seeded
        // directly here rather than through a real join flow.
        let actor = world.actors.get(name).unwrap();
        let _ = actor
            .webdriver_client
            .execute(
                "window.__spacechat_seed_membership && window.__spacechat_seed_membership(arguments[0])",
                vec![serde_json::json!(SHARED_SPACE_ID)],
            )
            .await;
    }
}

#[when(regex = r#"^(\w+) sends the message "([^"]+)"$"#)]
async fn when_actor_sends_message(world: &mut SpaceChatWorld, name: String, content: String) {
    let actor = world.actors.get(&name).unwrap();
    let input = actor.webdriver_client.find(Locator::Css("[data-testid=message-input]")).await.unwrap();
    input.send_keys(&content).await.unwrap();
    let button = actor.webdriver_client.find(Locator::Css("[data-testid=send-button]")).await.unwrap();
    button.click().await.unwrap();
}

#[then(regex = r#"^(\w+)'s conversation view shows "([^"]+)" within (\d+) seconds$"#)]
async fn then_conversation_view_shows(world: &mut SpaceChatWorld, name: String, content: String, seconds: u64) {
    let actor = world.actors.get(&name).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);

    loop {
        let html = actor.webdriver_client.source().await.unwrap();
        if html.contains(&content) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("expected {name}'s conversation view to show {content:?} within {seconds}s, it never did");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
```

**Note on the membership-seeding step:** the `window.__spacechat_seed_membership` hook referenced above does not exist yet in `App.tsx`/`lib.rs` as written by Tasks 1–16. Add a thin, test-only Tauri command (e.g. `#[tauri::command] async fn seed_membership_for_testing(...)`, gated behind `#[cfg(debug_assertions)]` or a `SPACECHAT_TEST_HOOKS` env check so it never ships in a release build) that calls `state.membership.lock().unwrap().add_member(...)` directly for every other actor's `local_device` id — actors need to exchange device IDs out-of-band for this (e.g. via a fixed, scenario-known set of device IDs derived deterministically from the actor name, rather than the randomly-generated ones `load_or_create_local_device_id`/Task 8 produces in normal operation). This is scaffolding for the test harness, not production behavior, and should be documented as such at its definition site.

- [ ] **Step 4: Run the scenario**

Run: `cd space-chat-app/src-tauri && cargo test --test cucumber golden_path`
Expected: PASS, assuming `tauri-driver` is running and reachable at each actor's assigned WebDriver port (see Task 17's platform-support note — Linux/Windows only as of writing).

- [ ] **Step 5: Commit**

```bash
git add space-chat-app/src-tauri/tests/cucumber space-chat-app/src-tauri/src/lib.rs
git commit -m "test(app): add golden-path multi-actor scenario (Milestone 4 exit criteria)"
```

---

### Task 19: Attachment lazy-fetch scenario

**Files:**
- Create: `space-chat-app/src-tauri/tests/cucumber/features/attachment_lazy_fetch.feature`
- Modify: `space-chat-app/src-tauri/tests/cucumber/steps.rs`

Proves the app-shell spec's testing requirement: "Attachment loading via the custom protocol handler, including the not-yet-fetched case... asserted as a rendered placeholder-then-image transition in the UI."

- [ ] **Step 1: Write the feature file**

```gherkin
# space-chat-app/src-tauri/tests/cucumber/features/attachment_lazy_fetch.feature
Feature: Attachment lazy-fetch placeholder transition

  Scenario: An attachment not yet in the local cache shows a placeholder, then the real image
    Given Alice is an online device
    And Alice has sent a message with an attachment not yet present in her attachment store
    Then Alice's conversation view shows an attachment placeholder
    When the attachment bytes become available in Alice's attachment store
    Then Alice's conversation view shows the loaded attachment within 5 seconds
```

- [ ] **Step 2: Implement the step definitions**

```rust
// space-chat-app/src-tauri/tests/cucumber/steps.rs (extend)
#[given(regex = r"^(\w+) is an online device$")]
async fn given_actor_online_solo(world: &mut SpaceChatWorld, name: String) {
    world.spawn_actor(&name).await;
}

#[given(regex = r"^(\w+) has sent a message with an attachment not yet present in her attachment store$")]
async fn given_message_with_missing_attachment(world: &mut SpaceChatWorld, name: String) {
    // Sends a message whose AttachmentRef.hash the actor's own
    // AttachmentBlobStore has never had save_attachment called for --
    // exactly the cache-miss case Task 13's handle_attachment_request
    // covers. A real send flow would need a Tauri command that accepts
    // attachment bytes/refs (not built in this plan's command set, which
    // only covers text messages -- see this plan's closing notes on this
    // gap); this step calls a test-only hook exposed the same way the
    // golden-path scenario's membership-seeding hook is (Task 18's note).
    let actor = world.actors.get(&name).unwrap();
    let _ = actor
        .webdriver_client
        .execute(
            "window.__spacechat_send_message_with_missing_attachment && window.__spacechat_send_message_with_missing_attachment()",
            vec![],
        )
        .await;
}

#[then(regex = r"^(\w+)'s conversation view shows an attachment placeholder$")]
async fn then_shows_attachment_placeholder(world: &mut SpaceChatWorld, name: String) {
    let actor = world.actors.get(&name).unwrap();
    let found = actor
        .webdriver_client
        .find(Locator::Css("[data-testid=attachment-placeholder]"))
        .await;
    assert!(found.is_ok(), "expected an attachment placeholder to be rendered");
}

#[when(regex = r"^the attachment bytes become available in (\w+)'s attachment store$")]
async fn when_attachment_becomes_available(world: &mut SpaceChatWorld, name: String) {
    let actor = world.actors.get(&name).unwrap();
    let _ = actor
        .webdriver_client
        .execute(
            "window.__spacechat_simulate_attachment_arrival && window.__spacechat_simulate_attachment_arrival()",
            vec![],
        )
        .await;
}

#[then(regex = r"^(\w+)'s conversation view shows the loaded attachment within (\d+) seconds$")]
async fn then_shows_loaded_attachment(world: &mut SpaceChatWorld, name: String, seconds: u64) {
    let actor = world.actors.get(&name).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    loop {
        if actor.webdriver_client.find(Locator::Css("[data-testid=attachment-loaded]")).await.is_ok() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("expected {name}'s conversation view to show a loaded attachment within {seconds}s");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
```

**Honest gap this step definition surfaces:** `__spacechat_send_message_with_missing_attachment` and `__spacechat_simulate_attachment_arrival` are test-only hooks this plan's command set (Tasks 9–12) does not actually provide a production path for — sending a message with a real attachment (encrypting it, writing it through `AttachmentBlobStore`, populating `AttachmentMetadataStore`) is real, non-trivial work this plan's scope (a first clickable desktop pass proving the live-spec/protocol-handler/renderer wiring) did not include a dedicated `send_attachment` command for. Add one alongside these hooks if attachment sending is needed before this scenario can assert something more realistic than a directly-poked test double — flagged here rather than quietly assumed to already exist.

- [ ] **Step 3: Run the scenario**

Run: `cd space-chat-app/src-tauri && cargo test --test cucumber attachment_lazy_fetch`
Expected: PASS once the test-only hooks above are added to `App.tsx`/`lib.rs`; until then this step should be treated as a known follow-up, not silently marked done.

- [ ] **Step 4: Commit**

```bash
git add space-chat-app/src-tauri/tests/cucumber
git commit -m "test(app): add attachment lazy-fetch placeholder-then-image scenario"
```

---

### Task 20: Backend-restart recovery scenario

**Files:**
- Create: `space-chat-app/src-tauri/tests/cucumber/features/backend_restart_recovery.feature`
- Modify: `space-chat-app/src-tauri/tests/cucumber/steps.rs`

Proves the app-shell spec's testing requirement: "kill and restart the core mid-session, verify the frontend recovers via full resend without a manual reload — asserted by the UI still showing correct history after restart." Since `space-chat-core` runs embedded in the same OS process as the Tauri app (no separate backend process to kill independently), "restart the core" is modeled here as killing and relaunching the whole `space-chat-app` process against the same on-disk data directory — the composition root (`AppState::new`) is what actually "restarts," and Task 6's `LiveSpec`-starts-at-version-0 property (already unit-tested in Task 6/9) is what this scenario proves holds end to end through a real relaunch.

- [ ] **Step 1: Write the feature file**

```gherkin
# space-chat-app/src-tauri/tests/cucumber/features/backend_restart_recovery.feature
Feature: Backend restart recovery

  Scenario: The app recovers full conversation history after a mid-session restart
    Given Alice is an online device
    And Alice has sent the message "before restart"
    When Alice's app process is killed and relaunched against the same data directory
    Then Alice's conversation view shows "before restart" within 5 seconds
```

- [ ] **Step 2: Implement the step definitions**

```rust
// space-chat-app/src-tauri/tests/cucumber/steps.rs (extend)
#[given(regex = r#"^(\w+) has sent the message "([^"]+)"$"#)]
async fn given_actor_has_sent_message(world: &mut SpaceChatWorld, name: String, content: String) {
    when_actor_sends_message(world, name, content).await;
}

#[when(regex = r"^(\w+)'s app process is killed and relaunched against the same data directory$")]
async fn when_actor_process_killed_and_relaunched(world: &mut SpaceChatWorld, name: String) {
    world.kill_actor(&name).await;
    world.relaunch_actor(&name).await;
}
```

(`then_conversation_view_shows` from Task 18 is reused unchanged for the final assertion — no new step needed.)

- [ ] **Step 3: Run the scenario**

Run: `cd space-chat-app/src-tauri && cargo test --test cucumber backend_restart_recovery`
Expected: PASS — after relaunch, `App.tsx`'s `onMount` (Task 16) calls `openConversation` again, which calls `open_conversation` against the freshly-constructed `AppState` (fresh `LiveSpec` at version 0, but real persisted history still on disk via `space-chat-storage-files`/`space-chat-storage-redb`), returning the full current spec — the same "no special restart mechanism needed" path proven at the unit level in Task 9's `resync_after_a_fresh_open_with_a_stale_version_returns_full` test, now exercised through a real process kill/relaunch and real rendered DOM.

- [ ] **Step 4: Run the full harness one more time to confirm nothing regressed**

Run: `cd space-chat-app/src-tauri && cargo test --test cucumber`
Expected: PASS (all three scenarios from Tasks 18–20, modulo Task 19's documented test-hook follow-up).

- [ ] **Step 5: Commit**

```bash
git add space-chat-app/src-tauri/tests/cucumber
git commit -m "test(app): add backend-restart recovery scenario proving full-resend needs no new mechanism"
```

---

### Task 21: Connection-status event dispatch

**Files:**
- Modify: `space-chat-app/src-tauri/src/events.rs` — add the event name helper
- Modify: `space-chat-app/src-tauri/src/lib.rs` — spawn a background task bridging `NetworkService::subscribe_status` to a Tauri event
- Modify: `space-chat-app/src/App.tsx` — listen for it and show a minimal "reconnecting" affordance
- Test: `space-chat-app/src-tauri/src/events.rs` (inline)

**Found during this plan's self-review:** the app-shell spec's Composition & Tauri IPC section calls for "connection-status/sync-progress events for UI affordances like 'reconnecting'" alongside the per-conversation patch event — Task 7 defined `NetworkService::subscribe_status` but no earlier task ever consumed it. This task closes that gap rather than leaving it silently unimplemented.

**Interfaces:**
- Consumes: `crate::network::{NetworkService, ConnectionStatus}` (Task 7), `AppState` (Task 8).
- Produces: `crate::events::connection_status_event_name() -> &'static str` (a single global event, not per-conversation — connection status is app-wide, not per-space, in this plan's scope), a `spawn_connection_status_bridge(app: tauri::AppHandle, state: Arc<AppState>)` function called once from `run()`.

- [ ] **Step 1: Write the failing test for the event name helper**

```rust
// space-chat-app/src-tauri/src/events.rs (append inside the existing tests module, adding one if none exists)
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_status_event_name_is_stable() {
        assert_eq!(connection_status_event_name(), "connection-status");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd space-chat-app/src-tauri && cargo test events::`
Expected: FAIL — `connection_status_event_name` not defined.

- [ ] **Step 3: Implement the event name helper and the bridge**

```rust
// space-chat-app/src-tauri/src/events.rs (add above the tests module)
pub fn connection_status_event_name() -> &'static str {
    "connection-status"
}
```

```rust
// space-chat-app/src-tauri/src/lib.rs
use tauri::Emitter;

/// Bridges `AppState.network`'s status watch channel to a Tauri event so the
/// frontend can show a "reconnecting" affordance -- see this task's
/// self-review note. Runs for the lifetime of the app; `AppState.network`
/// itself doesn't change after startup in this plan's scope, so there's
/// nothing to re-subscribe to.
fn spawn_connection_status_bridge(app: tauri::AppHandle, state: std::sync::Arc<state::AppState>) {
    let mut status_rx = state.network.subscribe_status();
    tokio::spawn(async move {
        loop {
            let status = *status_rx.borrow();
            let _ = app.emit(events::connection_status_event_name(), format!("{status:?}"));
            if status_rx.changed().await.is_err() {
                break; // sender dropped -- AppState (and the whole app) is shutting down
            }
        }
    });
}

// ... inside run(), after `let app_state = std::sync::Arc::new(app_state);` and before
// `.run(tauri::generate_context!())`:
    let app_state_for_bridge = app_state.clone();
    builder
        .setup(move |app| {
            spawn_connection_status_bridge(app.handle().clone(), app_state_for_bridge.clone());
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            greet,
            commands::open_conversation,
            commands::close_conversation,
            commands::resync_conversation,
            commands::send_message,
            commands::react,
            commands::delete_message,
            commands::fetch_older_page,
            commands::create_space,
            commands::generate_invite,
            commands::join_via_invite,
        ])
        .run(tauri::generate_context!())
        .expect("error while running space-chat-app");
```

- [ ] **Step 4: Add a minimal frontend listener**

```tsx
// space-chat-app/src/App.tsx — add near the other onMount logic
  const [connectionStatus, setConnectionStatus] = createSignal("Connected");

  onMount(async () => {
    const { listen } = await import("@tauri-apps/api/event");
    await listen<string>("connection-status", (event) => setConnectionStatus(event.payload));
  });
```

```tsx
// space-chat-app/src/App.tsx — render it somewhere visible
      <div data-testid="connection-status">{connectionStatus()}</div>
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd space-chat-app/src-tauri && cargo test events::`
Expected: PASS (1 test)

- [ ] **Step 6: Verify the whole crate still builds and all prior tests still pass**

Run: `cd space-chat-app/src-tauri && cargo build && cargo test`
Expected: builds cleanly; every test from Tasks 1–21 passes.

- [ ] **Step 7: Commit**

```bash
git add space-chat-app/src-tauri/src/events.rs space-chat-app/src-tauri/src/lib.rs space-chat-app/src/App.tsx
git commit -m "feat(app): bridge NetworkService connection status to a Tauri event"
```

---

## Closing note for the final whole-branch review

Per this workspace's established process (see Milestone 1's and Milestone 2's plans' own closing notes), run a final review across the whole branch's diff before merging to `main`, not just per-task reviews. Pay particular attention to:

- **Reconcile `NetworkService` (Task 7) against Milestone 3's actual transport crate before wiring `space-chat-app` to real `iroh` networking.** This plan's trait and `TcpLoopbackNetworkService` stand-in were written without sight of a finished transport implementation plan; method names, how per-`(space_id, category)` stream routing surfaces, and how connection status is actually reported may all differ from what's written here.
- **Replace `PlaintextMembership` (Task 3) and the invite flow (Task 12) once `space-chat-openmls` exists.** Nothing in this plan should be mistaken for real security — unsigned invite tokens, unauthenticated membership, no cross-device gossip of membership changes at all.
- **`ObservedAtStore`'s "observed at" vs. "sent at" distinction (Task 4)** is a real, documented limitation, not a bug — but it does mean `relative_time` in the generated spec can be inaccurate for a message a device receives long after it was actually sent (e.g., a peer reconnecting after being offline for a day sees "just now" for messages that were sent a day ago). Worth deciding, once `space-chat-openmls`/a real wire format exist, whether a genuine sender-side timestamp should be added to `space_chat_core::domain::Message` at that point.
- **Task 19's two test-only JavaScript hooks** (`__spacechat_send_message_with_missing_attachment`, `__spacechat_simulate_attachment_arrival`) are acknowledged scaffolding, not a real attachment-sending path — this plan's command set (Tasks 9–12) has no `send_attachment` command at all. Decide whether that's in scope for a near-term follow-up plan or genuinely deferred alongside the mobile items.
- **Task 17's `tauri-driver` platform gap (macOS)** should be re-checked against Tauri's current release notes before this harness is trusted as "passing on all desktop platforms" — it may still only be Linux/Windows as of implementation time.
- **Title persistence** (noted in passing in Tasks 10 and 12) doesn't exist anywhere in this plan's scope — every conversation's displayed title currently falls back to its `space_id`. A small, natural follow-up (a `space_id -> title` map, most naturally living alongside `PlaintextMembership`'s data or as its own tiny redb table) rather than something to solve mid-plan.
- **`CURRENT_EPOCH` is hardcoded to `0`** throughout `commands.rs` (Task 10) — this plan does not implement epoch rollover (MLS re-keying epochs, per the protocol spec) at all. Fine for a first clickable pass against a single long-lived epoch; a real gap once `space-chat-openmls` introduces actual epoch transitions.

## Self-review

**Spec coverage against `projects/specs/2026-09-05-app-shell-ui-design.md`:**
- "Specs in, retrofit-ui renders" / one shared renderer per platform: Tasks 1, 14 (desktop-only per this plan's explicit scoping).
- Local `conversation` kind + local `card`/`text`/`flex`/`grid`, never touching `@retrofit-ui/core`: Tasks 2, 14.
- Live-update mechanism adapted from `tenju-tofu`'s prototype, pushed over events, rolling window instead of one-version: Task 6.
- Spec generation as a thin layer reading ordering from `ListingIndex`: Tasks 5, 9, 11.
- Tauri commands (`open_conversation` through `join_via_invite`): Tasks 9, 10, 11, 12.
- Only active conversations get a `LiveSpec`: Tasks 8, 9.
- Tauri events (patch + connection-status): Tasks 9/10 (patch), 21 (connection-status).
- Attachments bypass IPC via custom protocol: Task 13.
- Background delivery (mobile): explicitly deferred, documented at the top of this plan, not silently dropped.
- Error handling (backend restart, per-conversation failure isolation): Tasks 6, 9, 20 (restart); Tasks 2, 9 (failure isolation).
- Testing (shared multi-actor harness, golden path, fallback path, attachment lazy-fetch, backend restart): Tasks 17, 18, 14, 19, 20.
- Open questions carried forward: attachment fetch policy resolved (Task 13, lazy+placeholder); rolling-window size resolved (Task 6, 8 of 5–10); promotion of local-renderer patterns into `@retrofit-ui/core` remains explicitly out of scope, matching the spec's own framing — no task in this plan attempts it.

**Placeholder scan:** no step in this plan says "add error handling," "write tests for the above," or "similar to Task N" without showing the actual code — every task's code blocks are complete, compilable-as-written Rust/TypeScript. The two places this plan uses the word "placeholder" (`SpaceMembership`, invite tokens) are deliberate, documented scope decisions with working code behind them, not unfilled steps.

**Type consistency:** `ViewSpec`'s variant names/JSON tags (Task 2) are used identically in `conversation_spec.rs` (Task 5), `commands.rs` (Tasks 9–11), and mirrored field-for-field in `spec.ts` (Task 14). `PatchResponse`'s three-variant shape (Task 6) is produced identically by `commands.rs` (Tasks 9–10) and consumed identically by `liveSpecClient.ts` (Task 15) and `events.rs`'s `ConversationPatchEvent` (Task 9). `AppState`'s field names (Task 8) are used consistently by every later task that touches it (Tasks 9–13, 21) — corrected during self-review: `regenerate_spec_value`'s `has_more_older` was initially hardcoded `false`, factored out into a shared `compute_has_more_older` helper (Task 9) that Task 11's `fetch_older_page_impl` now also calls, so the two code paths can't silently disagree.
---

## Amendment: real Milestone 3 `Transport` integration (supersedes Task 7's `NetworkService`/TCP-loopback design)

**Decision (made explicitly, with the user, before executing this plan):** wire `space-chat-app` to the REAL `space-chat-transport` crate (Milestone 3, merged to `main`) instead of building the generic `NetworkService` trait + `TcpLoopbackNetworkService` stand-in Task 7 originally specified. Task 7's own text already anticipated this exact reconciliation point ("Before wiring `space-chat-app` to real `iroh` networking, reconcile this trait against Milestone 3's actual public API... do not assume this trait survives unchanged") — this amendment is that reconciliation.

**Why this isn't a simple swap-the-implementation change.** `NetworkService` was designed around a generic push/pull message-passing shape (`send(change)` / `take_incoming() -> Receiver<SegmentChange>`) because Milestone 3's real API didn't exist yet when this plan was drafted. The real `Transport` (from `space-chat-transport`) has a fundamentally different, and simpler, shape: the caller registers a space ONCE via `Transport::add_space(space_id, epoch, segment: Arc<Mutex<Segment>>)`, handing `Transport` a **shared, mutex-guarded handle to the actual live `Segment`** — from then on, `Transport` autonomously syncs that segment against whichever peers are dialed/connected for that space, mutating it in place via `receive_sync_message` inside its own background sync loop. There is no generic "send this change" call for outgoing content — the caller just mutates the shared `Segment` directly (e.g. `append_message`) and calls `Transport::notify_local_change(space_id)` to wake any parked sync tasks immediately instead of waiting for the next poll tick.

This means `AppState` must hold **one persistent, long-lived `Arc<Mutex<Segment>>` per active space** that is the single source of truth both local command handlers (Tasks 9-11) and `Transport`'s own sync loop mutate — not the original design's "reload fresh from disk, mutate a local copy, save" pattern for the mutation path (that pattern is still fine, unchanged, for the READ-only `segments_for` used by spec-generation, since disk is kept current after every mutation either way).

### Revised Task 7: `AppNetwork` — a thin wrapper around real `Transport`

**Files:** same as originally specified (`space-chat-app/src-tauri/src/network.rs`), but the module's actual contents are replaced entirely.

**Produces (replacing the original `NetworkService`/`TcpLoopbackNetworkService`/`run_loopback_relay`):**

```rust
// space-chat-app/src-tauri/src/network.rs
use space_chat_transport::{Transport, TransportConfig, TransportEvent, TransportIdentity};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// At least one peer connection is currently live.
    Connected,
    /// No peer connections are currently live, but this device has at least
    /// attempted one (distinguishes "never tried" from "tried and lost it,"
    /// for Task 21's UI affordance -- e.g. "reconnecting..." vs. no message
    /// at all on first launch before any dial has happened).
    Disconnected,
}

/// Thin app-specific wrapper around a real `space_chat_transport::Transport`.
/// Owns the `Transport` value and its raw `TransportEvent` receiver;
/// `AppState` (Task 8) is the one place that actually consumes events --
/// this type's job is just binding and exposing a `ConnectionStatus` watch
/// channel derived from `Connected`/`Disconnected` events, which is cheap
/// and independent of whatever `AppState` does with the rest of the event
/// stream.
pub struct AppNetwork {
    pub transport: Arc<Transport>,
    status_rx: watch::Receiver<ConnectionStatus>,
}

impl AppNetwork {
    /// Binds a real `iroh` endpoint. `config` is `TransportConfig::default()`
    /// (real `n0` relay/discovery) in production; tests pass
    /// `TransportConfig { relay: Some((relay_map, relay_url)) }` against a
    /// local `iroh::test_utils::run_relay_server()`, exactly as every
    /// Milestone 3 test already does -- do not reintroduce a TCP stand-in.
    /// Returns the wrapper plus the raw event receiver, which the caller
    /// (`AppState::new`, Task 8) takes ownership of to drive its own
    /// persistence/spec-regeneration pipeline; `AppNetwork` itself only
    /// peeks at `Connected`/`Disconnected` via a `watch` channel fed by a
    /// small forwarding task, not by consuming the real receiver itself.
    pub async fn bind(
        identity: &TransportIdentity,
        config: TransportConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<TransportEvent>), space_chat_transport::TransportError> {
        let (transport, mut events_rx) = Transport::bind(identity, config).await?;
        let transport = Arc::new(transport);

        // Forward Connected/Disconnected into a watch channel for cheap,
        // last-value-only status polling (Task 21), while still handing the
        // FULL event stream on to the caller for everything else
        // (IncomingChange, JoinRequest) -- this requires a second channel
        // the caller reads from, since `mpsc::Receiver` has only one
        // consumer. Re-plumb: this function creates its own forwarding
        // task that reads `events_rx` and re-sends every event onward on a
        // fresh channel the caller gets back, updating `status_tx` as a
        // side effect for `Connected`/`Disconnected` specifically.
        let (status_tx, status_rx) = watch::channel(ConnectionStatus::Disconnected);
        let (forward_tx, forward_rx) = mpsc::unbounded_channel::<TransportEvent>();
        tokio::spawn(async move {
            let mut live_connections: u32 = 0;
            while let Some(event) = events_rx.recv().await {
                match &event {
                    TransportEvent::Connected { .. } => {
                        live_connections += 1;
                        let _ = status_tx.send(ConnectionStatus::Connected);
                    }
                    TransportEvent::Disconnected { .. } => {
                        live_connections = live_connections.saturating_sub(1);
                        if live_connections == 0 {
                            let _ = status_tx.send(ConnectionStatus::Disconnected);
                        }
                    }
                    _ => {}
                }
                if forward_tx.send(event).is_err() {
                    break; // caller dropped its receiver -- nothing left to forward to
                }
            }
        });

        Ok((Self { transport, status_rx }, forward_rx))
    }

    pub fn subscribe_status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status_rx.clone()
    }
}
```

**Dependency change:** add `space-chat-transport = { path = "../../space-chat-transport" }` to `space-chat-app/src-tauri/Cargo.toml`'s `[dependencies]` (not dev-dependencies — this is used in production `run()`, not only in tests).

**Tests for this task:** a minimal test binding two `AppNetwork`s over a local `iroh::test_utils::run_relay_server()` (mirroring Milestone 3's own `bootstrap.rs`/`transport.rs` test patterns exactly — reuse `TransportIdentity::generate()`, `TransportConfig { relay: Some((relay_map, relay_url)) }`), confirming `subscribe_status()` observes `Connected` after a `dial`. Do not write a TCP-relay test — there is no TCP relay in this design anymore.

**No `NullNetworkService` needed:** `AppState`'s tests (Task 8) that don't want to exercise real networking can bind a real `AppNetwork` against `TransportConfig { relay: None }` with no `dial()` ever called — this is a real, unconnected `Transport` instance, cheap to bind (no network I/O happens until something dials out), and behaves correctly as an inert default without a separate null-object type to maintain.

---

### Revised Task 8: `AppState` — `Transport`-backed segment registry + background event pipeline

**Changed `AppState` fields** (replacing `network: Box<dyn NetworkService>` and `change_tx: broadcast::Sender<SegmentChange>`):

```rust
pub struct AppState {
    pub local_device: DeviceId,
    pub segment_store: Mutex<FileSegmentStore>,
    pub attachment_store: Mutex<FileAttachmentStore>,
    pub listing_index: Mutex<RedbListingIndex>,
    pub attachment_metadata: Mutex<RedbAttachmentMetadataStore>,
    pub observed_at: Mutex<RedbObservedAtStore>,
    pub membership: Mutex<PlaintextMembership>,
    pub network: crate::network::AppNetwork,
    /// The ONE live, shared `Segment` handle per currently-known space, at
    /// its current epoch (epoch rollover stays out of scope for this plan,
    /// per Task 10's existing `CURRENT_EPOCH` constant). This is the exact
    /// `Arc<Mutex<Segment>>` registered with `Transport` via `add_space` --
    /// both local mutation (Tasks 9-11) and `Transport`'s own sync loop
    /// mutate THIS handle, never a separately-loaded copy, so the two can
    /// never diverge. Lazily populated by `segment_arc` on first access.
    pub active_segments: tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<Segment>>>>,
    pub active: Mutex<HashMap<String, ActiveConversation>>,
}
```

(`Mutex` for `active_segments`'s inner `Segment` must be `tokio::sync::Mutex`, not `std::sync::Mutex` — `Transport::add_space`'s signature requires `Arc<tokio::sync::Mutex<Segment>>` exactly; every other `AppState` field can stay `std::sync::Mutex` as originally planned, since only this one is ever held across an `.await` from the `Transport` side.)

**New method, replacing the implicit "just call `segments_for` and mutate a fresh copy" pattern for the MUTATION path specifically** (`segments_for` itself, used for read-only spec generation, is UNCHANGED — see rationale above):

```rust
impl AppState {
    /// Returns the persistent, shared `Segment` handle for `space_id`'s
    /// current epoch, registering it with `Transport` via `add_space` the
    /// FIRST time it's requested for this process's lifetime (idempotent
    /// after that -- `Transport::add_space` is safe to call again, but
    /// this method only does so once per `space_id` by checking the cache
    /// first, since re-registering is currently a documented, deliberately
    /// unimplemented gap in Milestone 3 -- see its plan's amendment notes
    /// on late `add_space` calls never syncing over pre-existing
    /// connections; calling `add_space` exactly once per space, as early as
    /// possible, sidesteps that gap entirely rather than depending on it).
    pub async fn segment_arc(&self, space_id: &str) -> Arc<tokio::sync::Mutex<Segment>> {
        const CURRENT_EPOCH: u64 = 0;
        let mut active = self.active_segments.lock().await;
        if let Some(existing) = active.get(space_id) {
            return existing.clone();
        }
        // First access: load from disk if present, else start fresh --
        // mirrors `segments_for`'s own per-epoch load logic for consistency.
        let loaded = {
            let store = self.segment_store.lock().unwrap();
            store
                .load_segment(space_id, CURRENT_EPOCH)
                .ok()
                .flatten()
                .and_then(|bytes| Segment::load(&bytes, space_id, CURRENT_EPOCH, 0).ok())
        };
        let segment = loaded.unwrap_or_else(|| Segment::new(space_id, CURRENT_EPOCH));
        let arc = Arc::new(tokio::sync::Mutex::new(segment));
        self.network.transport.add_space(space_id, CURRENT_EPOCH, arc.clone()).await;
        active.insert(space_id.to_string(), arc.clone());
        arc
    }
}
```

**`AppState::new`'s signature changes** to take a bound `crate::network::AppNetwork` plus its event receiver, instead of `network: Box<dyn NetworkService>`:

```rust
pub fn new(
    data_dir: impl Into<PathBuf>,
    local_device: DeviceId,
    network: crate::network::AppNetwork,
    network_events: mpsc::UnboundedReceiver<TransportEvent>,
    app_handle: tauri::AppHandle, // needed to emit patch events from the background task below
) -> Result<Arc<Self>, AppStateError> {
    // ... unchanged setup for segment_store/attachment_store/listing_index/
    // attachment_metadata/observed_at/membership ...

    let state = Arc::new(Self {
        local_device,
        segment_store: Mutex::new(segment_store),
        attachment_store: Mutex::new(attachment_store),
        listing_index: Mutex::new(listing_index),
        attachment_metadata: Mutex::new(attachment_metadata),
        observed_at: Mutex::new(observed_at),
        membership: Mutex::new(membership),
        network,
        active_segments: tokio::sync::Mutex::new(HashMap::new()),
        active: Mutex::new(HashMap::new()),
    });

    spawn_network_event_loop(state.clone(), network_events, app_handle);

    Ok(state)
}
```

Note `AppState::new` now returns `Arc<Self>` (not a bare `Self`) — the background event loop needs its own owned handle to the state alongside whatever `tauri::Builder::manage` holds, and `tauri::State` already derefs through an inner `Arc` in Tauri 2's actual implementation, so `.manage(state)` with an `Arc<AppState>` works the same way `.manage` on a bare `AppState` did; verify this specific point against the pinned `tauri` version's docs, per this plan's Global Constraints about verifying exact APIs.

**The background event loop — the "third consumer of the storage spec's `Projection` change feed," now driven by real network events instead of the original design's generic broadcast channel:**

```rust
fn spawn_network_event_loop(
    state: Arc<AppState>,
    mut events: mpsc::UnboundedReceiver<TransportEvent>,
    app_handle: tauri::AppHandle,
) {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                TransportEvent::IncomingChange(change) => {
                    // The shared Segment Arc for `change.space_id` has
                    // ALREADY been mutated in place by Transport's own sync
                    // loop by the time this event fires (Transport holds the
                    // exact same Arc `segment_arc` registered). This handler's
                    // job is purely the downstream bookkeeping: persist,
                    // index, and (if actively viewed) regenerate + push a
                    // patch -- the same three steps Task 10's local mutation
                    // path does, just triggered by a network merge instead
                    // of a direct command call.
                    apply_incoming_change(&state, &app_handle, &change).await;
                }
                TransportEvent::Connected { .. } | TransportEvent::Disconnected { .. } => {
                    // Handled by AppNetwork's own status watch channel
                    // (Task 21 subscribes to that directly); nothing to do
                    // here.
                }
                TransportEvent::JoinRequest(_request) => {
                    // Out of scope for this plan -- Task 12's invite/
                    // membership flow is a local-only placeholder that does
                    // not use Transport's real sequencer-routed join at all
                    // (see the "Task 12: unchanged" note below). A real
                    // `space-chat-openmls` integration is what would act on
                    // this event; until then it's intentionally ignored.
                }
            }
        }
    });
}

/// Shared by both the network event loop (above) and Task 10's local
/// mutation commands: given a space whose shared `Segment` has just
/// changed (by any means), persist it, append any newly-discovered message
/// keys to `ListingIndex`, and -- if actively viewed -- regenerate its spec
/// and emit a patch event. Task 10's `mutate_and_persist` should be
/// refactored to call this same helper after its own mutation closure runs,
/// rather than duplicating the persist/index/regenerate logic.
async fn apply_incoming_change(
    state: &AppState,
    app_handle: &tauri::AppHandle,
    change: &space_chat_core::projection::SegmentChange,
) {
    // Persist the (already-mutated-in-place) segment's current bytes.
    let bytes = {
        let arc = state.segment_arc(&change.space_id).await;
        let mut seg = arc.lock().await;
        seg.save()
    };
    if let Ok(mut store) = state.segment_store.lock() {
        let _ = store.save_segment(&change.space_id, change.epoch, &bytes);
    }

    // Diff against ListingIndex to find message keys not yet indexed, in
    // the same `message_keys()`-iteration style Task 5/10 already use, and
    // append any new ones -- see Task 10's existing new-listing-entries
    // logic for the exact pattern to reuse here (this amendment does not
    // redefine that diff logic, only where it's called from).
    // ... (implementer: factor Task 10's existing "compute new listing
    // entries" logic into a function both this path and `mutate_and_persist`
    // call, rather than inlining it twice) ...

    let active = state.active.lock().unwrap();
    if let Some(conversation) = active.get(&change.space_id) {
        let new_value = regenerate_spec_value(state, &change.space_id, &change.space_id);
        conversation.live_spec.update(new_value);
        let (version, _) = conversation.live_spec.snapshot();
        let patch = conversation.live_spec.diff_since(version.saturating_sub(1));
        use tauri::Emitter;
        let event = crate::events::ConversationPatchEvent { space_id: change.space_id.clone(), patch };
        let _ = app_handle.emit(&crate::events::conversation_patch_event_name(&change.space_id), event);
    }
}
```

**`run()`'s binding sequence** (in `lib.rs`) changes to bind `AppNetwork` (async) before constructing `AppState`, which means `run()`'s top-level setup needs an async context it didn't need before — do this inside Tauri's `.setup()` closure (which can spawn a blocking/async task) rather than trying to `.await` directly in `fn run()` (which is synchronous). Read Tauri 2's actual `.setup()` closure signature and async-setup patterns from its current docs before implementing this step; this plan cannot pin the exact incantation since it depends on the specific Tauri version already pinned by Task 1.

**Global Constraint addition:** production `run()` binds with `TransportConfig::default()`-equivalent (real relay/discovery — i.e. `TransportConfig { relay: None }`, which Milestone 3's `bootstrap.rs` routes to the real `N0` preset); only tests use `TransportConfig { relay: Some(..) }` against a local test relay.

---

### Task 9, 10, 11: call-site changes only (no interface/signature changes beyond what's below)

- Anywhere the original plan said `state.network.send(change)`: replace with `state.network.transport.notify_local_change(space_id).await` — there is no explicit "send this change" call anymore; mutating the shared segment IS the send, and `notify_local_change` just wakes any parked sync tasks immediately instead of waiting for their next poll tick.
- Task 10's `mutate_and_persist`: replace `let mut segments = state.segments_for(space_id); let mut segment = segments.remove(&CURRENT_EPOCH).unwrap_or_else(...)` with `let arc = state.segment_arc(space_id).await; let mut segment = arc.lock().await;` — mutate THIS guard, not a freshly-loaded copy, then still persist its bytes to `segment_store` exactly as before (Transport does not persist anything itself). After releasing the lock, call `state.network.transport.notify_local_change(space_id).await` instead of `state.network.send(change)`.
- Task 9's `open_conversation_impl`: should call `state.segment_arc(space_id).await` once (to ensure the space is registered with `Transport` as soon as it's actively viewed, even if it was never touched by a mutation yet — e.g. a space the local device joined but hasn't posted in) in addition to whatever it already does with `segments_for` for spec generation.
- Task 11's `fetch_older_page_impl`: unchanged — it only reads via `ListingIndex`/`segments_for`, never mutates.

### Task 12: **no change**

Task 12's invite/membership flow is, by its own explicit and correct design, a local-only placeholder: `join_via_invite_impl` only adds a row to the local `PlaintextMembership` store and is never exercised across a real connection by this plan's own test design (scenario actors get membership seeded directly by test setup, per Task 12's module-level caveat, which this amendment does not change). Wiring `generate_invite`/`join_via_invite` to `Transport`'s REAL sequencer-routed invite mechanism (Milestone 3 Task 10) would additionally require a real "new member starts receiving segment data after being accepted" flow that Milestone 3 itself doesn't fully specify end-to-end (it proves routing works, not full post-join sync bootstrapping) — implementing that now would be a substantial, risky expansion outside what was actually decided (real transport for message sync, not real transport for membership/invites). Leave Task 12 exactly as originally planned.

### Tasks 17-20: multi-actor test harness — real endpoint dial, not a TCP relay

Replace `run_loopback_relay`/`TcpLoopbackNetworkService` throughout with:

1. **A real local `iroh` test relay**, started once per scenario exactly the way Milestone 3's own tests do: `iroh::test_utils::run_relay_server().await` (available since `space-chat-transport` is now a real dependency of `space-chat-app`, this is reachable via `iroh::test_utils::run_relay_server` directly — `iroh`'s `test-utils` feature must be enabled for `space-chat-app`'s dev-dependencies the same way Milestone 3 did it, via the crate-dev-depending-on-itself pattern `space-chat-transport`'s own `Cargo.toml` established — reuse that exact pattern, don't reinvent it).
2. **Each spawned actor process needs to (a) learn the relay's `(RelayMap, RelayUrl)`, (b) print its own bound `EndpointId`/dial-address somewhere the harness can read it, and (c) be told which specific other actors to dial, matching the Gherkin scenario's stated topology (not an auto-connect-everyone-to-everyone mesh — several scenarios specifically test "Alice and Carol have no direct path").** Concretely, extend `run()`'s startup (behind env vars, mirroring the existing `SPACECHAT_DATA_DIR` pattern):
   - `SPACECHAT_RELAY_URL` / a serialized `RelayMap` (however `iroh::test_utils::run_relay_server()`'s returned pieces are most simply serialized to pass through an env var — e.g. the relay URL as a string, reconstructing a single-entry `RelayMap` from it, the same construction Milestone 3's own tests already do) — tells this actor's `Transport::bind` to use `TransportConfig { relay: Some(..) } ` against the SAME local test relay every other actor in the scenario uses.
   - `SPACECHAT_ENDPOINT_ADDR_FILE` (a path) — on successful bind, the app writes its own `iroh::EndpointAddr` (encoded however is simplest — e.g. via `Invite`-style hex/CBOR encoding already established in `space-chat-transport`, or just `Debug`-formatted if that's simplest for a test-only file the harness itself parses) to this file, so the harness can read it once the actor is up.
   - `SPACECHAT_DIAL_ADDRS` (comma-separated, each a path to another actor's `SPACECHAT_ENDPOINT_ADDR_FILE`, OR the harness reads the target files itself and passes the encoded addresses directly) — on startup, after binding, the app reads each and calls `state.network.transport.dial(...)` for each one. This gives the harness precise, per-scenario control over the connection topology by choosing what to pass for each spawned actor, matching exactly what Milestone 3's own `test_peer.rs` binary does for its own two/three-process tests (reuse that pattern, don't invent a new one).
   These four env vars are read once at startup in `run()`, gated so they're a no-op if unset (a normal end-user launch has none of them set and just binds with real production discovery per the Global Constraint above).
3. `SpaceChatWorld::ensure_relay` (Task 17) becomes `ensure_relay` returning the real `(RelayMap, RelayUrl, relay_server_handle)` triple from `iroh::test_utils::run_relay_server()`, stored on the `World` for the scenario's lifetime.
4. `spawn_actor` (Task 17) gains a topology parameter (e.g. `dial_targets: &[&str]`, actor names already spawned that this new actor should connect to) and, before spawning, resolves those names to their `SPACECHAT_ENDPOINT_ADDR_FILE` paths (already known once those actors were themselves spawned) to pass via `SPACECHAT_DIAL_ADDRS`.
5. Given/When/Then step definitions (Tasks 18-20) that describe topology (e.g. "Given Alice and Bob are online, Carol is offline" / "Alice and Carol have no direct path") now translate directly into which `dial_targets` each `spawn_actor` call is given — this is a natural, direct translation once the mechanism above exists; no further redesign needed at the step-definition level.

**This is real integration work, not a mechanical brief-following exercise** — the implementer for Tasks 17+ should verify each piece (the exact `iroh::test_utils::run_relay_server()` return shape, exactly how to encode/decode an `EndpointAddr` for the file-based handoff, exact env-var-reading placement in `run()`) against `space-chat-transport`'s actual current source and Milestone 3's own test code (`space-chat-transport/src/bin/test_peer.rs` and `tests/two_process_convergence.rs` are the closest existing precedent and should be read directly before implementing this) rather than treating this amendment's prose as gospel over what's actually achievable.

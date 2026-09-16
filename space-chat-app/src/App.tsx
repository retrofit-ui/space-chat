import { createSignal, onCleanup, onMount, type Component } from "solid-js";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { openConversation } from "./liveSpecClient";
import SpaceChatSpecRenderer from "./SpaceChatSpecRenderer";
import type { SpaceChatViewSpec } from "./spec";

const DEFAULT_SPACE_ID = "space-default";

const App: Component = () => {
  const [spec, setSpec] = createSignal<SpaceChatViewSpec | null>(null);
  const [draft, setDraft] = createSignal("");
  // Seeded with the same value `network.rs` seeds its `watch::channel` with,
  // because the backend bridge emits the current status once at startup and
  // this listener may well attach after that first emit -- in which case the
  // next event won't arrive until status actually changes.
  const [connectionStatus, setConnectionStatus] = createSignal("Disconnected");
  let close: (() => void) | undefined;
  let unlistenStatus: (() => void) | undefined;

  onMount(async () => {
    // First, and before any other await: the backend's bridge starts emitting
    // as soon as `run()`'s setup closure finishes, so every await before this
    // is a window in which a status change is missed. Assigned to a variable
    // the component-body-level `onCleanup` below closes over rather than
    // calling `onCleanup` here -- registering a cleanup after an await inside
    // an async `onMount` silently never runs (see AttachmentImage.tsx, where
    // exactly that bug was found and fixed).
    unlistenStatus = await listen<string>("connection-status", (event) =>
      setConnectionStatus(event.payload),
    );
    await invoke("create_space", { title: "General" }).catch(() => {
      // create_space_impl can't actually fail -- it always mints a fresh,
      // non-deterministic space id and succeeds. We call it anyway (and
      // discard its result) purely for its side effect: it registers
      // "Me" as the local device's display name in PlaintextMembership's
      // globally-keyed (not per-space) display_names map (Task 3), which
      // every space this app opens depends on. The space id it was called
      // with is irrelevant to that effect, and the .catch here is just
      // defensive belt-and-suspenders -- not a guard against a real
      // conflict. DEFAULT_SPACE_ID below is opened separately, and is
      // fixed (not create_space's fresh id) precisely so conversation
      // history persists and reopens the same space across restarts.
    });
    close = await openConversation(DEFAULT_SPACE_ID, "General", setSpec);
  });

  onCleanup(() => {
    close?.();
    unlistenStatus?.();
  });

  const send = async () => {
    const content = draft().trim();
    if (!content) return;
    await invoke("send_message", { spaceId: DEFAULT_SPACE_ID, content });
    setDraft("");
  };

  return (
    <div class="app">
      {/*
        Renders the raw backend status ("Connected" / "Disconnected") rather
        than a friendlier "Reconnecting…". That wording would overclaim:
        `network.rs`'s ConnectionStatus doc is explicit that `Disconnected`
        is ALSO the initial value before any dial has ever been attempted, so
        it cannot distinguish "never tried" from "tried and lost it" -- saying
        "reconnecting" on a cold start would be a lie. Telling the two apart
        needs a separate signal (e.g. whether `dial` has ever been called),
        which doesn't exist yet.
      */}
      <div data-testid="connection-status" data-status={connectionStatus()}>
        {connectionStatus()}
      </div>
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

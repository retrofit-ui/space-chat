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

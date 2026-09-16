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

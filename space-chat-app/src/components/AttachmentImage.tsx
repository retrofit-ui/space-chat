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

  // `onCleanup` must be registered SYNCHRONOUSLY within `onMount`'s callback,
  // before any `await` -- SolidJS tracks the current reactive "owner" via
  // the synchronous call stack, and that context is gone once execution
  // resumes after an async gap. Calling `onCleanup` post-await (as an
  // earlier version of this code did, awaiting `listen(...)` first) logs
  // "cleanups created outside a `createRoot` or `render` will never be
  // run" and, worse, the registered cleanup silently never fires on
  // unmount -- confirmed by a scratch test that mounted/unmounted the
  // component and found `unlisten` was never called. Fixed by registering
  // `onCleanup` up front, with a `disposed` guard so the listener is torn
  // down immediately if `listen(...)`'s promise resolves after the
  // component has already unmounted.
  onMount(() => {
    const hash = hashFromUrl(props.url);
    let disposed = false;
    let unlisten: (() => void) | undefined;

    listen(`attachment-ready:${hash}`, () => setReady(true)).then((fn) => {
      if (disposed) {
        fn();
      } else {
        unlisten = fn;
      }
    });

    onCleanup(() => {
      disposed = true;
      unlisten?.();
    });
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

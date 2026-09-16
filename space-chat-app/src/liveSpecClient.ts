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

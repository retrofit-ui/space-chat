import { describe, expect, it, vi } from "vitest";
import type { SpaceChatViewSpec } from "./spec";

const conversation: SpaceChatViewSpec = {
  kind: "conversation",
  space_id: "space-1",
  title: "General",
  has_more_older: false,
  messages: [],
};

describe("applyPatchResponse", () => {
  it("returns the current spec unchanged for an 'unchanged' response", async () => {
    const { applyPatchResponse } = await import("./liveSpecClient");
    const response = { kind: "unchanged" as const, version: 3 };
    expect(applyPatchResponse(conversation, response)).toEqual(conversation);
  });

  it("applies a JSON Patch for a 'patch' response", async () => {
    const { applyPatchResponse } = await import("./liveSpecClient");
    const response = {
      kind: "patch" as const,
      version: 4,
      patch: [{ op: "replace" as const, path: "/title", value: "General (renamed)" }],
    };
    const result = applyPatchResponse(conversation, response) as typeof conversation;
    expect(result.title).toBe("General (renamed)");
  });

  it("replaces the whole spec for a 'full' response", async () => {
    const { applyPatchResponse } = await import("./liveSpecClient");
    const fullSpec: SpaceChatViewSpec = { ...conversation, title: "Replaced entirely" };
    const response = { kind: "full" as const, version: 9, spec: fullSpec };
    expect(applyPatchResponse(conversation, response)).toEqual(fullSpec);
  });
});

describe("openConversation", () => {
  /// Proves the fix for the dead-`resync_conversation` bug: a patch event
  /// whose version isn't exactly `currentVersion + 1` (e.g. an event that
  /// fired in the gap between `open_conversation` resolving and `listen`
  /// being registered, which Tauri events have no replay for) must NOT be
  /// blindly applied against the wrong base -- it must trigger a real
  /// `resync_conversation` call instead, and the resulting spec update must
  /// come from that resync response, not from the skipped-over patch.
  it("resyncs instead of blindly applying a patch when a version is skipped", async () => {
    vi.resetModules();

    const invokeCalls: Array<{ cmd: string; args: unknown }> = [];
    const listeners: Record<string, (event: { payload: unknown }) => void> = {};

    const initialSpec: SpaceChatViewSpec = { ...conversation, title: "General" };
    const resyncedSpec: SpaceChatViewSpec = { ...conversation, title: "Resynced from backend" };

    vi.doMock("@tauri-apps/api/core", () => ({
      invoke: vi.fn(async (cmd: string, args: unknown) => {
        invokeCalls.push({ cmd, args });
        if (cmd === "open_conversation") {
          return { version: 1, spec: initialSpec };
        }
        if (cmd === "resync_conversation") {
          return { kind: "full", version: 5, spec: resyncedSpec };
        }
        if (cmd === "close_conversation") {
          return undefined;
        }
        throw new Error(`unexpected invoke: ${cmd}`);
      }),
    }));

    vi.doMock("@tauri-apps/api/event", () => ({
      listen: async (name: string, handler: (event: { payload: unknown }) => void) => {
        listeners[name] = handler;
        return () => {
          delete listeners[name];
        };
      },
    }));

    const { openConversation } = await import("./liveSpecClient");

    const updates: SpaceChatViewSpec[] = [];
    await openConversation("space-1", "General", (spec) => updates.push(spec));

    expect(updates).toEqual([initialSpec]);

    // currentVersion is 1 after open_conversation; fire a patch event that
    // jumps straight to version 5, skipping 2/3/4 entirely -- exactly the
    // dropped-event scenario the fix must catch.
    listeners["conversation-patch:space-1"]?.({
      payload: {
        space_id: "space-1",
        kind: "patch",
        version: 5,
        patch: [{ op: "replace", path: "/title", value: "Blindly applied -- WRONG" }],
      },
    });

    // Let the resync's promise chain resolve.
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    const resyncCall = invokeCalls.find((c) => c.cmd === "resync_conversation");
    expect(resyncCall).toBeDefined();
    expect(resyncCall?.args).toEqual({ spaceId: "space-1", sinceVersion: 1 });

    // The final update must be the resync response's spec, not a patch
    // blindly applied against the stale base.
    expect(updates[updates.length - 1]).toEqual(resyncedSpec);
    expect(updates.some((s) => (s as typeof conversation).title === "Blindly applied -- WRONG")).toBe(false);
  });

  it("applies patches directly when the version is exactly current + 1", async () => {
    vi.resetModules();

    const invokeCalls: Array<{ cmd: string; args: unknown }> = [];
    const listeners: Record<string, (event: { payload: unknown }) => void> = {};

    const initialSpec: SpaceChatViewSpec = { ...conversation, title: "General" };

    vi.doMock("@tauri-apps/api/core", () => ({
      invoke: vi.fn(async (cmd: string, args: unknown) => {
        invokeCalls.push({ cmd, args });
        if (cmd === "open_conversation") {
          return { version: 1, spec: initialSpec };
        }
        if (cmd === "resync_conversation") {
          throw new Error("resync_conversation should not be called for a contiguous version");
        }
        if (cmd === "close_conversation") {
          return undefined;
        }
        throw new Error(`unexpected invoke: ${cmd}`);
      }),
    }));

    vi.doMock("@tauri-apps/api/event", () => ({
      listen: async (name: string, handler: (event: { payload: unknown }) => void) => {
        listeners[name] = handler;
        return () => {
          delete listeners[name];
        };
      },
    }));

    const { openConversation } = await import("./liveSpecClient");

    const updates: SpaceChatViewSpec[] = [];
    await openConversation("space-1", "General", (spec) => updates.push(spec));

    listeners["conversation-patch:space-1"]?.({
      payload: {
        space_id: "space-1",
        kind: "patch",
        version: 2,
        patch: [{ op: "replace", path: "/title", value: "Directly patched" }],
      },
    });

    await Promise.resolve();
    await Promise.resolve();

    expect(invokeCalls.some((c) => c.cmd === "resync_conversation")).toBe(false);
    expect((updates[updates.length - 1] as typeof conversation).title).toBe("Directly patched");
  });
});

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

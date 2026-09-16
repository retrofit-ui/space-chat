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
    // `stat` is a real retrofit-ui RootSpec kind (packages/core/src/types --
    // verified directly against the current retrofit-ui monorepo checkout,
    // both that RootSpec includes StatSpec and that SpecRenderer's Switch
    // has a `stat` Match rendering StatViewComponent, which reads directly
    // from the `spec` prop's `stats` array with no network fetch involved)
    // that SpaceChatSpecRenderer has no local case for -- it must still
    // render correctly through the fallback, proving the fallback path
    // actually works, not just that unhandled kinds silently do nothing.
    const spec = {
      kind: "stat",
      stats: [{ label: "Unread", value: 3 }],
    } as unknown as SpaceChatViewSpec;

    render(() => <SpaceChatSpecRenderer spec={spec} />);

    expect(screen.getByText("Unread")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
  });
});

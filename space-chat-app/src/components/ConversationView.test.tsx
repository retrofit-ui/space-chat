import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@solidjs/testing-library";
import ConversationView from "./ConversationView";
import type { ConversationSpec, MessageSpec } from "../spec";

// AttachmentImage subscribes to `attachment-ready:<hash>` on mount; without a
// Tauri runtime `listen` would throw. Nothing here fires the event -- the
// placeholder state is all these tests need.
vi.mock("@tauri-apps/api/event", () => ({
  listen: async () => () => {},
}));

function message(overrides: Partial<MessageSpec> & { id: string }): MessageSpec {
  return {
    sender_id: "01",
    sender_name: "Alice",
    content: "",
    relative_time: "just now",
    attachments: [],
    reactions: [],
    deleted: false,
    ...overrides,
  };
}

function conversation(messages: MessageSpec[], overrides: Partial<ConversationSpec> = {}): ConversationSpec {
  return {
    kind: "conversation",
    space_id: "space-1",
    title: "General",
    has_more_older: false,
    messages,
    ...overrides,
  };
}

describe("ConversationView", () => {
  it("renders the title, one row per message, and the space id on its root", () => {
    const spec = conversation([
      message({ id: "msg:1", content: "first", sender_name: "Alice", relative_time: "2m ago" }),
      message({ id: "msg:2", content: "second", sender_name: "Bob", sender_id: "02" }),
    ]);
    const { container } = render(() => <ConversationView spec={spec} />);

    const root = container.querySelector(".conversation-view");
    expect(root).toHaveAttribute("data-space-id", "space-1");
    expect(screen.getByRole("heading", { name: "General" })).toBeInTheDocument();

    const rows = container.querySelectorAll("li.message");
    expect(rows).toHaveLength(2);
    expect(rows[0]).toHaveAttribute("data-message-id", "msg:1");
    expect(rows[0].querySelector(".sender-name")).toHaveTextContent("Alice");
    expect(rows[0].querySelector(".relative-time")).toHaveTextContent("2m ago");
    expect(rows[0].querySelector(".content")).toHaveTextContent("first");
    expect(rows[1].querySelector(".sender-name")).toHaveTextContent("Bob");
    expect(rows[1].querySelector(".content")).toHaveTextContent("second");
  });

  it("renders an empty conversation as the bare root with no message rows", () => {
    const { container } = render(() => <ConversationView spec={conversation([])} />);

    expect(container.querySelector(".conversation-view")).toBeInTheDocument();
    expect(container.querySelectorAll("li.message")).toHaveLength(0);
    expect(container.querySelector(".message-list")).toBeInTheDocument();
  });

  it("replaces a deleted message's content with a marker and flags the row", () => {
    const spec = conversation([message({ id: "msg:1", content: "should not be shown", deleted: true })]);
    const { container } = render(() => <ConversationView spec={spec} />);

    const row = container.querySelector("li.message");
    expect(row).toHaveClass("deleted");
    expect(screen.getByText("(message deleted)")).toBeInTheDocument();
    expect(screen.queryByText("should not be shown")).not.toBeInTheDocument();
    // The sender is still attributed; only the body is redacted.
    expect(row?.querySelector(".sender-name")).toHaveTextContent("Alice");
  });

  it("renders one attachment node per attachment, in the placeholder state", () => {
    const spec = conversation([
      message({
        id: "msg:1",
        content: "two pictures",
        attachments: [
          { url: `spacechat://attachment/${"aa".repeat(32)}`, mime: "image/png", size: 10 },
          { url: `spacechat://attachment/${"bb".repeat(32)}`, mime: "image/jpeg", size: 20 },
        ],
      }),
    ]);
    render(() => <ConversationView spec={spec} />);

    expect(screen.getAllByTestId("attachment-placeholder")).toHaveLength(2);
    expect(screen.queryByTestId("attachment-loaded")).not.toBeInTheDocument();
  });

  it("renders each reaction's emoji under its message", () => {
    const spec = conversation([
      message({
        id: "msg:1",
        content: "reacted",
        reactions: [
          { emoji: "👍", actor_name: "Bob" },
          { emoji: "🎉", actor_name: "Carol" },
        ],
      }),
      message({ id: "msg:2", content: "plain" }),
    ]);
    const { container } = render(() => <ConversationView spec={spec} />);

    const rows = container.querySelectorAll("li.message");
    const first = Array.from(rows[0].querySelectorAll(".reaction")).map((r) => r.textContent);
    expect(first).toEqual(["👍", "🎉"]);
    expect(rows[1].querySelectorAll(".reaction")).toHaveLength(0);
  });
});

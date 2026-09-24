import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@solidjs/testing-library";
import type { SpaceChatViewSpec } from "./spec";

// One ordered log for BOTH mocked Tauri modules, because the property under
// test in the first case is the relative order of a `listen` and an
// `invoke` -- two separate call lists couldn't express that.
const calls: string[] = [];
const invokeArgs: Record<string, unknown[]> = {};
const listeners: Record<string, (event: { payload: unknown }) => void> = {};

const initialSpec: SpaceChatViewSpec = {
  kind: "conversation",
  space_id: "space-default",
  title: "General",
  has_more_older: false,
  messages: [],
};

vi.mock("@tauri-apps/api/core", () => ({
  invoke: vi.fn(async (cmd: string, args?: unknown) => {
    calls.push(`invoke:${cmd}`);
    (invokeArgs[cmd] ??= []).push(args);
    switch (cmd) {
      case "create_space":
        return "space-fresh";
      case "open_conversation":
        return { version: 1, spec: initialSpec };
      case "send_message":
      case "close_conversation":
        return undefined;
      default:
        throw new Error(`unexpected invoke: ${cmd}`);
    }
  }),
}));

vi.mock("@tauri-apps/api/event", () => ({
  listen: vi.fn(async (name: string, handler: (event: { payload: unknown }) => void) => {
    calls.push(`listen:${name}`);
    listeners[name] = handler;
    return () => {
      delete listeners[name];
    };
  }),
}));

async function renderApp() {
  const { default: App } = await import("./App");
  const result = render(() => <App />);
  // `onMount` is async; the conversation view only appears once
  // `open_conversation` has resolved, so this is the "mounted" signal.
  await waitFor(() => expect(result.container.querySelector(".conversation-view")).toBeInTheDocument());
  return result;
}

describe("App", () => {
  beforeEach(() => {
    calls.length = 0;
    for (const key of Object.keys(invokeArgs)) delete invokeArgs[key];
    for (const key of Object.keys(listeners)) delete listeners[key];
  });

  it("subscribes to connection-status before any command that could change it", async () => {
    await renderApp();

    const statusListen = calls.indexOf("listen:connection-status");
    const createSpace = calls.indexOf("invoke:create_space");
    const openConversation = calls.indexOf("invoke:open_conversation");
    expect(statusListen).toBeGreaterThanOrEqual(0);
    expect(createSpace).toBeGreaterThan(statusListen);
    expect(openConversation).toBeGreaterThan(createSpace);
    expect(invokeArgs.open_conversation).toEqual([{ spaceId: "space-default", title: "General" }]);
  });

  it("starts Disconnected and reflects connection-status events in the indicator", async () => {
    await renderApp();
    const indicator = screen.getByTestId("connection-status");
    expect(indicator).toHaveTextContent("Disconnected");
    expect(indicator).toHaveAttribute("data-status", "Disconnected");

    listeners["connection-status"]?.({ payload: "Connected" });
    await waitFor(() => expect(indicator).toHaveTextContent("Connected"));
    expect(indicator).toHaveAttribute("data-status", "Connected");

    listeners["connection-status"]?.({ payload: "Disconnected" });
    await waitFor(() => expect(indicator).toHaveAttribute("data-status", "Disconnected"));
  });

  it("sends the trimmed composer text via send_message and clears the input", async () => {
    await renderApp();
    const input = screen.getByTestId<HTMLInputElement>("message-input");

    fireEvent.input(input, { target: { value: "  hello there  " } });
    fireEvent.click(screen.getByTestId("send-button"));

    await waitFor(() => expect(invokeArgs.send_message).toEqual([{ spaceId: "space-default", content: "hello there" }]));
    await waitFor(() => expect(input.value).toBe(""));
  });

  it("does not send a blank or whitespace-only message", async () => {
    await renderApp();
    const input = screen.getByTestId<HTMLInputElement>("message-input");

    fireEvent.click(screen.getByTestId("send-button"));
    fireEvent.input(input, { target: { value: "   " } });
    fireEvent.click(screen.getByTestId("send-button"));
    // Give any (wrong) async send a chance to land before asserting absence.
    await Promise.resolve();
    await Promise.resolve();

    expect(invokeArgs.send_message).toBeUndefined();
  });

  it("renders a conversation-patch event's new message without any further invoke", async () => {
    const { container } = await renderApp();
    expect(container.querySelectorAll("li.message")).toHaveLength(0);
    const invokesBefore = calls.filter((c) => c.startsWith("invoke:")).length;

    listeners["conversation-patch:space-default"]?.({
      payload: {
        space_id: "space-default",
        kind: "patch",
        version: 2,
        patch: [
          {
            op: "add",
            path: "/messages/0",
            value: {
              id: "msg:1",
              sender_id: "02",
              sender_name: "Bob",
              content: "pushed from the backend",
              relative_time: "just now",
              attachments: [],
              reactions: [],
              deleted: false,
            },
          },
        ],
      },
    });

    await waitFor(() => expect(screen.getByText("pushed from the backend")).toBeInTheDocument());
    expect(screen.getByText("Bob")).toBeInTheDocument();
    // Version 2 is exactly current + 1, so no resync round trip was needed.
    expect(calls.filter((c) => c.startsWith("invoke:")).length).toBe(invokesBefore);
  });
});

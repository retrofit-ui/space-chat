import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@solidjs/testing-library";
import AttachmentImage from "./AttachmentImage";

const listeners: Record<string, (event: { payload: unknown }) => void> = {};

vi.mock("@tauri-apps/api/event", () => ({
  listen: async (name: string, handler: (event: { payload: unknown }) => void) => {
    listeners[name] = handler;
    return () => {
      delete listeners[name];
    };
  },
}));

describe("AttachmentImage", () => {
  it("shows a placeholder state, then swaps to the loaded image once attachment-ready fires", async () => {
    const hash = "ab".repeat(32);
    const url = `spacechat://attachment/${hash}`;

    render(() => <AttachmentImage url={url} mime="image/png" />);

    expect(screen.getByTestId("attachment-placeholder")).toBeInTheDocument();
    expect(screen.queryByTestId("attachment-loaded")).not.toBeInTheDocument();

    listeners[`attachment-ready:${hash}`]?.({ payload: {} });
    await Promise.resolve();

    expect(screen.getByTestId("attachment-loaded")).toBeInTheDocument();
    expect(screen.queryByTestId("attachment-placeholder")).not.toBeInTheDocument();
  });
});

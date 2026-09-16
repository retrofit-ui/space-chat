import type { RootSpec } from "@retrofit-ui/core";
import { SpecRenderer } from "@retrofit-ui/spa-solid-shoelace/components";
import { type Component, For, Match, Show, Switch, type JSX } from "solid-js";
import ConversationView from "./components/ConversationView";
import type { SpaceChatViewSpec } from "./spec";

/// Recursively renders one local spec node, following the same pattern
/// `chalk-app`'s `ChalkSpecRenderer`/`tenju-tofu`'s `TenjuSpecRenderer`
/// already establish in this workspace: bespoke kinds get bespoke
/// components, common structural kinds (`card`/`text`/`flex`/`grid`) are
/// reimplemented locally so recursion can interleave the bespoke kinds
/// retrofit-ui's own `SpecRenderer` has no way to know about, and anything
/// else falls back to the real `SpecRenderer`.
const ViewNode: Component<{ spec: SpaceChatViewSpec }> = (props) => {
  return (
    <Switch fallback={<SpecRenderer spec={props.spec as unknown as RootSpec} apiBase="" />}>
      <Match when={props.spec.kind === "conversation"}>
        <ConversationView spec={props.spec as Extract<SpaceChatViewSpec, { kind: "conversation" }>} />
      </Match>
      <Match when={props.spec.kind === "conversation-error"}>
        <div class="conversation-error" role="alert">
          {(props.spec as Extract<SpaceChatViewSpec, { kind: "conversation-error" }>).message}
        </div>
      </Match>
      <Match when={props.spec.kind === "card"}>
        <div class="spec-card">
          <Show when={(props.spec as Extract<SpaceChatViewSpec, { kind: "card" }>).header}>
            <div class="spec-card-header">
              {(props.spec as Extract<SpaceChatViewSpec, { kind: "card" }>).header}
            </div>
          </Show>
          <div class="spec-card-body">
            <For each={(props.spec as Extract<SpaceChatViewSpec, { kind: "card" }>).children}>
              {(child) => <ViewNode spec={child} />}
            </For>
          </div>
        </div>
      </Match>
      <Match when={props.spec.kind === "flex"}>
        <div
          class="spec-flex"
          style={{
            display: "flex",
            "flex-direction": ((props.spec as Extract<SpaceChatViewSpec, { kind: "flex" }>).direction ??
              "column") as JSX.CSSProperties["flex-direction"],
            gap: (props.spec as Extract<SpaceChatViewSpec, { kind: "flex" }>).gap ?? "0.75rem",
          }}
        >
          <For each={(props.spec as Extract<SpaceChatViewSpec, { kind: "flex" }>).children}>
            {(child) => <ViewNode spec={child} />}
          </For>
        </div>
      </Match>
      <Match when={props.spec.kind === "grid"}>
        <div
          class="spec-grid"
          style={{
            display: "grid",
            "grid-template-columns": `repeat(${(props.spec as Extract<SpaceChatViewSpec, { kind: "grid" }>).columns ?? 2}, 1fr)`,
            gap: (props.spec as Extract<SpaceChatViewSpec, { kind: "grid" }>).gap ?? "0.75rem",
          }}
        >
          <For each={(props.spec as Extract<SpaceChatViewSpec, { kind: "grid" }>).children}>
            {(child) => <ViewNode spec={child} />}
          </For>
        </div>
      </Match>
      <Match when={props.spec.kind === "text"}>
        <div class="spec-text" data-variant={(props.spec as Extract<SpaceChatViewSpec, { kind: "text" }>).variant ?? "body"}>
          {(props.spec as Extract<SpaceChatViewSpec, { kind: "text" }>).content}
        </div>
      </Match>
    </Switch>
  );
};

const SpaceChatSpecRenderer: Component<{ spec: SpaceChatViewSpec }> = (props) => {
  return <ViewNode spec={props.spec} />;
};

export default SpaceChatSpecRenderer;

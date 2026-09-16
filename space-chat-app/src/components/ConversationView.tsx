import { type Component, For, Show } from "solid-js";
import type { ConversationSpec } from "../spec";

const ConversationView: Component<{ spec: ConversationSpec }> = (props) => {
  return (
    <div class="conversation-view" data-space-id={props.spec.space_id}>
      <h2>{props.spec.title}</h2>
      <ul class="message-list">
        <For each={props.spec.messages}>
          {(message) => (
            <li class="message" data-message-id={message.id} classList={{ deleted: message.deleted }}>
              <span class="sender-name">{message.sender_name}</span>
              <span class="relative-time">{message.relative_time}</span>
              <Show when={!message.deleted} fallback={<p class="deleted-marker">(message deleted)</p>}>
                <p class="content">{message.content}</p>
              </Show>
              <For each={message.attachments}>
                {(attachment) => (
                  <img class="attachment-image" src={attachment.url} alt="attachment" data-mime={attachment.mime} />
                )}
              </For>
              <div class="reactions">
                <For each={message.reactions}>
                  {(reaction) => <span class="reaction">{reaction.emoji}</span>}
                </For>
              </div>
            </li>
          )}
        </For>
      </ul>
    </div>
  );
};

export default ConversationView;

export interface AttachmentSpec {
  url: string;
  mime: string;
  size: number;
}

export interface ReactionSpec {
  emoji: string;
  actor_name: string;
}

export interface MessageSpec {
  id: string;
  sender_id: string;
  sender_name: string;
  content: string;
  relative_time: string;
  attachments: AttachmentSpec[];
  reactions: ReactionSpec[];
  deleted: boolean;
}

export interface ConversationSpec {
  kind: "conversation";
  space_id: string;
  title: string;
  messages: MessageSpec[];
  has_more_older: boolean;
}

export interface ConversationErrorSpec {
  kind: "conversation-error";
  space_id: string;
  message: string;
}

export interface CardSpec {
  kind: "card";
  header?: string;
  children: SpaceChatViewSpec[];
}

export interface TextSpec {
  kind: "text";
  content: string;
  variant?: string;
}

export interface FlexSpec {
  kind: "flex";
  direction?: string;
  gap?: string;
  children: SpaceChatViewSpec[];
}

export interface GridSpec {
  kind: "grid";
  columns?: number;
  gap?: string;
  children: SpaceChatViewSpec[];
}

export type SpaceChatViewSpec =
  | ConversationSpec
  | ConversationErrorSpec
  | CardSpec
  | TextSpec
  | FlexSpec
  | GridSpec;

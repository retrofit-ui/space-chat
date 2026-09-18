use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ViewSpec {
    Conversation(ConversationSpec),
    ConversationError(ConversationErrorSpec),
    Card(CardSpec),
    Text(TextSpec),
    Flex(FlexSpec),
    Grid(GridSpec),
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConversationSpec {
    pub space_id: String,
    pub title: String,
    pub messages: Vec<MessageSpec>,
    pub has_more_older: bool,
}

/// Rendered when Task 9's spec-generation call fails for this one
/// conversation -- per the app-shell spec's error-handling section, a
/// malformed segment degrades only this conversation's view, not the whole
/// app shell.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConversationErrorSpec {
    pub space_id: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MessageSpec {
    pub id: String,
    pub sender_id: String,
    pub sender_name: String,
    pub content: String,
    pub relative_time: String,
    pub attachments: Vec<AttachmentSpec>,
    pub reactions: Vec<ReactionSpec>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AttachmentSpec {
    pub url: String,
    pub mime: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ReactionSpec {
    pub emoji: String,
    pub actor_name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CardSpec {
    pub header: Option<String>,
    pub children: Vec<ViewSpec>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TextSpec {
    pub content: String,
    pub variant: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FlexSpec {
    pub direction: Option<String>,
    pub gap: Option<String>,
    pub children: Vec<ViewSpec>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GridSpec {
    pub columns: Option<u32>,
    pub gap: Option<String>,
    pub children: Vec<ViewSpec>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_spec_serializes_with_kind_tag_and_flattened_fields() {
        let spec = ViewSpec::Conversation(ConversationSpec {
            space_id: "space-1".to_string(),
            title: "General".to_string(),
            messages: vec![MessageSpec {
                id: "msg:abc".to_string(),
                sender_id: "01".to_string(),
                sender_name: "Alice".to_string(),
                content: "hello".to_string(),
                relative_time: "just now".to_string(),
                attachments: vec![],
                reactions: vec![],
                deleted: false,
            }],
            has_more_older: false,
        });

        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["kind"], "conversation");
        assert_eq!(json["space_id"], "space-1");
        assert_eq!(json["messages"][0]["content"], "hello");
    }

    #[test]
    fn card_spec_nests_children_recursively() {
        let spec = ViewSpec::Card(CardSpec {
            header: Some("Header".to_string()),
            children: vec![ViewSpec::Text(TextSpec {
                content: "body".to_string(),
                variant: None,
            })],
        });

        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["kind"], "card");
        assert_eq!(json["children"][0]["kind"], "text");
        assert_eq!(json["children"][0]["content"], "body");
    }
}

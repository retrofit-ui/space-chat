use crate::error::TransportError;
use serde::{Deserialize, Serialize};

/// The protocol spec's five wire message categories, mapped one-to-one
/// onto a per-`(space_id, category)` QUIC stream (see this plan's
/// Architecture note on stream-open headers vs. per-message tagging).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Category {
    MlsControl,
    AutomergeSync,
    Gossip,
    Ephemeral,
    AttachmentTransfer,
}

impl Category {
    /// Relative QUIC send-stream priority, per the transport spec's stream
    /// prioritization section: control/sync/gossip ranked above bulk
    /// attachment transfer, consistently across every active space. Exact
    /// weights are a tuning parameter (the spec's own Open questions say
    /// so explicitly) — the only hard requirement encoded here is that
    /// `AttachmentTransfer` is strictly lowest.
    pub fn stream_priority(self) -> i32 {
        match self {
            Category::MlsControl => 30,
            Category::AutomergeSync => 20,
            Category::Gossip => 20,
            Category::Ephemeral => 10,
            Category::AttachmentTransfer => 0,
        }
    }
}

/// Sent once, as the first frame on a newly-opened per-`(space_id,
/// category)` stream, so the accepting side (which only sees "a new
/// bidirectional stream arrived" from `iroh`, with no application metadata
/// attached by QUIC itself) learns which space and category it's for.
/// **Not** repeated on every subsequent frame on that stream — once the
/// stream exists, every frame on it is already unambiguously scoped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub space_id: String,
    pub category: Category,
}

impl Envelope {
    pub fn encode(&self) -> Result<Vec<u8>, TransportError> {
        let mut buf = Vec::new();
        ciborium::into_writer(self, &mut buf).map_err(|e| TransportError::Codec(e.to_string()))?;
        Ok(buf)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, TransportError> {
        ciborium::from_reader(bytes).map_err(|e| TransportError::Codec(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_transfer_has_the_lowest_priority() {
        let all = [
            Category::MlsControl,
            Category::AutomergeSync,
            Category::Gossip,
            Category::Ephemeral,
            Category::AttachmentTransfer,
        ];
        let attachment_priority = Category::AttachmentTransfer.stream_priority();
        for category in all {
            if category != Category::AttachmentTransfer {
                assert!(
                    category.stream_priority() > attachment_priority,
                    "{category:?} should outrank AttachmentTransfer per the transport spec's \
                     stream prioritization section"
                );
            }
        }
    }

    #[test]
    fn envelope_round_trips_through_cbor() {
        let envelope = Envelope {
            space_id: "space-1".to_string(),
            category: Category::Gossip,
        };
        let bytes = envelope.encode().unwrap();
        let decoded = Envelope::decode(&bytes).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn envelope_decode_rejects_garbage() {
        assert!(Envelope::decode(b"not cbor").is_err());
    }

    #[test]
    fn envelope_decode_rejects_truncated_cbor_without_panicking() {
        let envelope = Envelope {
            space_id: "space-1".to_string(),
            category: Category::MlsControl,
        };
        let bytes = envelope.encode().unwrap();
        // Chop the encoding off partway through -- must return an Err, not
        // panic, since a peer could send a truncated frame.
        let truncated = &bytes[..bytes.len() / 2];
        assert!(Envelope::decode(truncated).is_err());
    }

    #[test]
    fn envelope_decode_rejects_empty_input() {
        assert!(Envelope::decode(b"").is_err());
    }
}

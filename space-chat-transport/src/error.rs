use std::fmt;

/// Crate-wide error type. Kept as one flat enum (mirroring
/// `space_chat_core::storage::StorageError`'s shape) rather than a
/// per-module error type per module, since almost every fallible operation
/// in this crate ultimately bottoms out in one of these four causes.
#[derive(Debug)]
pub enum TransportError {
    /// The underlying OS/network layer failed (bind, socket, connect).
    Io(String),
    /// A CBOR encode/decode of one of this crate's own wire types failed.
    Codec(String),
    /// `iroh` itself rejected or dropped a connection/stream.
    Connection(String),
    /// A requested resource (an attachment by hash, a stream for a
    /// category) isn't available from the peer asked.
    NotFound,
    /// An operation exceeded its deadline (e.g. waiting for a peer's
    /// control-stream response).
    Timeout,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Io(msg) => write!(f, "transport io error: {msg}"),
            TransportError::Codec(msg) => write!(f, "transport codec error: {msg}"),
            TransportError::Connection(msg) => write!(f, "transport connection error: {msg}"),
            TransportError::NotFound => write!(f, "not found"),
            TransportError::Timeout => write!(f, "timed out"),
        }
    }
}

impl std::error::Error for TransportError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_non_empty_and_distinct() {
        let variants = [
            TransportError::Io("x".to_string()),
            TransportError::Codec("x".to_string()),
            TransportError::Connection("x".to_string()),
            TransportError::NotFound,
            TransportError::Timeout,
        ];
        let messages: Vec<String> = variants.iter().map(|e| e.to_string()).collect();
        let mut unique = messages.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), messages.len(), "each variant should render distinctly");
    }
}

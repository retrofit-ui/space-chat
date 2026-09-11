use crate::envelope::{Category, Envelope};
use crate::error::TransportError;
use crate::framing::{read_frame, write_frame};

/// One `(space_id, category)` stream's two halves. `Transport` (Task 7) and
/// attachment transfer (Task 9) read/write through these directly; neither
/// ever holds an `iroh::endpoint::Connection` itself.
pub struct StreamHandle {
    pub send: iroh::endpoint::SendStream,
    pub recv: iroh::endpoint::RecvStream,
}

/// Opens/accepts per-`(space_id, category)` streams on one already-
/// established `iroh::endpoint::Connection`, lazily — per the transport
/// spec, a stream set is opened only once a space becomes active between
/// two peers, not eagerly for every shared space regardless of activity.
/// This type owns no notion of *which* spaces are active; that policy
/// decision belongs to `Transport` (Task 7), which calls `open` only for
/// spaces present in both its local registry and the remote peer's
/// `exchange_digests` (Task 5) result, at a *matching epoch* -- not based
/// on comparing `heads` (skipping the open when `heads` already match
/// would be a further efficiency optimization, not implemented by this
/// crate).
///
/// Each call to `open` produces a brand-new QUIC stream — this type does
/// not cache or reuse streams by `(space_id, category)`. The brief's
/// interface contract (`open` returning a fresh `StreamHandle` each call,
/// with no `&mut self` needed to record one) reflects that: this manager is
/// a thin, stateless-beyond-`conn` façade over `Connection::open_bi`, not a
/// stream pool. Any dedup/reuse policy (e.g. "don't open a second stream
/// for a space that already has one") is left to the caller.
pub struct StreamManager {
    conn: iroh::endpoint::Connection,
}

impl StreamManager {
    pub fn new(conn: iroh::endpoint::Connection) -> Self {
        Self { conn }
    }

    /// Opens a fresh bidirectional stream for `(space_id, category)`,
    /// writes the `Envelope` header identifying it, and sets its QUIC send
    /// priority per `Category::stream_priority` — this is the mechanism
    /// behind the transport spec's stream-prioritization requirement. The
    /// header is written exactly once, as the very first frame on the new
    /// stream; every subsequent write the caller makes through the returned
    /// `StreamHandle` is plain frame data, not re-tagged.
    pub async fn open(&self, space_id: &str, category: Category) -> Result<StreamHandle, TransportError> {
        let (mut send, recv) = self
            .conn
            .open_bi()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        send.set_priority(category.stream_priority())
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        let header = Envelope { space_id: space_id.to_string(), category }.encode()?;
        write_frame(&mut send, &header).await?;

        Ok(StreamHandle { send, recv })
    }

    /// Accepts the next incoming bidirectional stream and reads its header
    /// frame, telling the caller which `(space_id, category)` it's for.
    /// `Transport` (Task 7) runs this in a loop per connection, dispatching
    /// each accepted stream to the right handler by the `Envelope` it
    /// returns.
    pub async fn accept_next(&self) -> Result<(Envelope, StreamHandle), TransportError> {
        let (send, mut recv) = self
            .conn
            .accept_bi()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;
        let header_bytes = read_frame(&mut recv).await?;
        let envelope = Envelope::decode(&header_bytes)?;
        Ok((envelope, StreamHandle { send, recv }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{bind_endpoint, TransportConfig, ALPN};
    use crate::identity::TransportIdentity;

    #[tokio::test]
    async fn opening_a_stream_sends_the_header_and_sets_priority() {
        let (relay_map, relay_url, _relay_server) =
            iroh::test_utils::run_relay_server().await.unwrap();
        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let alice = bind_endpoint(
            &alice_identity,
            TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
        )
        .await
        .unwrap();
        let bob = bind_endpoint(
            &bob_identity,
            TransportConfig { relay: Some((relay_map, relay_url.clone())) },
        )
        .await
        .unwrap();
        // Built from the relay URL directly rather than `bob.addr()` -- see
        // bootstrap.rs's own test for why: right after `bind_endpoint`
        // returns, `bob`'s `EndpointAddr` watcher may not yet reflect relay
        // info, which empirically hangs the dial until QUIC's idle timeout.
        let bob_addr = iroh::EndpointAddr::new(bob.id()).with_relay_url(relay_url);

        let bob_task = tokio::spawn(async move {
            let incoming = bob.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            let manager = StreamManager::new(conn);
            let (envelope, mut handle) = manager.accept_next().await.unwrap();
            assert_eq!(envelope.space_id, "space-1");
            assert_eq!(envelope.category, Category::AttachmentTransfer);
            let body = crate::framing::read_frame(&mut handle.recv).await.unwrap();
            assert_eq!(body, b"chunk-bytes".to_vec());
            handle.recv
        });

        let conn = alice.connect(bob_addr, ALPN).await.unwrap();
        let manager = StreamManager::new(conn);
        let mut handle = manager.open("space-1", Category::AttachmentTransfer).await.unwrap();
        assert_eq!(
            handle.send.priority().expect("priority should be readable back"),
            Category::AttachmentTransfer.stream_priority(),
        );
        crate::framing::write_frame(&mut handle.send, b"chunk-bytes").await.unwrap();

        bob_task.await.unwrap();
    }
}

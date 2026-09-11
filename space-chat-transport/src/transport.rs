use crate::bootstrap::{bind_endpoint, TransportConfig, ALPN};
use crate::control::{exchange_digests, ControlHello, SpaceDigest};
use crate::envelope::Category;
use crate::error::TransportError;
use crate::framing::{read_frame, write_frame};
use crate::identity::TransportIdentity;
use crate::streams::StreamManager;
use space_chat_core::projection::SegmentChange;
use space_chat_core::segment::{sync_state, Segment};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};

/// Events a consumer (eventually `space-chat-app`, Milestone 4) observes.
/// More variants are added in Tasks 9 (attachment progress isn't
/// event-based in this plan — `request_attachment` is a direct
/// request/response call, see Task 9) and 10 (`JoinRequest`).
pub enum TransportEvent {
    Connected { endpoint_id: iroh::EndpointId },
    Disconnected { endpoint_id: iroh::EndpointId },
    /// New content merged into a locally-tracked `Segment` as a result of
    /// sync with some peer. Carries a full `SegmentChange` snapshot (same
    /// shape Milestone 1's `Segment::latest_change` and Milestone 2's
    /// `Projection::apply` already use), so a consumer can feed it directly
    /// into a `ListingIndex`/`SearchIndex`/persistence layer without this
    /// crate needing to know any of those exist.
    IncomingChange(SegmentChange),
}

struct SpaceEntry {
    epoch: u64,
    segment: Arc<Mutex<Segment>>,
    /// Wakes every active per-peer sync task for this space to run another
    /// round immediately, instead of waiting for its next periodic tick —
    /// the mechanism behind the protocol spec's "gossip carries new changes
    /// live." `Transport::notify_local_change` fires this.
    notify: Arc<Notify>,
}

/// Converts an `automerge::ChangeHash` to a plain `[u8; 32]` for this
/// crate's own CBOR-serializable `SpaceDigest.heads`. Verified against
/// automerge 0.5.12's source (`src/types.rs`): `ChangeHash` wraps a plain
/// `[u8; 32]` (`HASH_SIZE == 32`) and implements `AsRef<[u8]>` over it, so
/// the brief's assumption holds exactly.
fn change_hash_to_bytes(h: &automerge::ChangeHash) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(h.as_ref());
    bytes
}

pub struct Transport {
    endpoint: iroh::Endpoint,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    events_tx: mpsc::UnboundedSender<TransportEvent>,
}

impl Transport {
    pub async fn bind(
        identity: &TransportIdentity,
        config: TransportConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<TransportEvent>), TransportError> {
        let endpoint = bind_endpoint(identity, config).await?;
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let spaces: Arc<Mutex<HashMap<String, SpaceEntry>>> = Arc::new(Mutex::new(HashMap::new()));

        let transport = Self { endpoint: endpoint.clone(), spaces: spaces.clone(), events_tx: events_tx.clone() };

        // Background accept loop: every inbound connection gets its own
        // per-connection task, mirroring what `dial` (below) does for
        // outbound connections.
        tokio::spawn(async move {
            loop {
                let Some(incoming) = endpoint.accept().await else { break };
                let Ok(conn) = incoming.await else { continue };
                tokio::spawn(run_connection(conn, false, spaces.clone(), events_tx.clone()));
            }
        });

        Ok((transport, events_rx))
    }

    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.endpoint.id()
    }

    pub fn endpoint_addr(&self) -> iroh::EndpointAddr {
        self.endpoint.addr()
    }

    /// Registers `segment` as this device's current state for `space_id`.
    /// `Segment` doesn't expose its own `space_id`/`epoch` fields (see
    /// `space_chat_core::segment::Segment`), so the caller — which already
    /// knows both, having constructed or loaded this `Segment` — supplies
    /// them alongside it.
    ///
    /// This does NOT retroactively push `space_id` onto connections that
    /// are already established: only the dialer side of `run_connection`
    /// opens sync streams for registered spaces, and only once, immediately
    /// after that connection's digest exchange (Task 5) completes. So a
    /// space registered here is picked up by (a) any *future* `dial()`
    /// call's digest exchange, and (b) the accepter side of any connection
    /// whenever the remote peer opens a stream naming this `space_id` — but
    /// a connection that was already established before this call won't
    /// get a stream for it unless the remote peer initiates one.
    ///
    /// TODO(Milestone 4 or a follow-up): re-registering an already-tracked
    /// `space_id` (calling this again with the same key) replaces the
    /// `SpaceEntry` — including its `segment` and `notify` — but does not
    /// tear down any sync tasks already spawned against the *old*
    /// segment/notify for that `space_id`; those tasks keep running against
    /// the stale `Segment`/`Notify` until their connection ends. Fixing
    /// this needs a real per-space cancellation mechanism (e.g. a
    /// generation counter or cancellation token torn down and re-created),
    /// which is a bigger design change than this fix pass takes on.
    pub async fn add_space(&self, space_id: impl Into<String>, epoch: u64, segment: Arc<Mutex<Segment>>) {
        let space_id = space_id.into();
        self.spaces.lock().await.insert(
            space_id,
            SpaceEntry { epoch, segment, notify: Arc::new(Notify::new()) },
        );
    }

    /// Dials `addr` and starts syncing every space currently registered via
    /// `add_space` against it, once the control-stream digest exchange
    /// (Task 5) determines which spaces the remote peer also knows about.
    pub async fn dial(&self, addr: impl Into<iroh::EndpointAddr>) -> Result<(), TransportError> {
        let conn = self
            .endpoint
            .connect(addr, ALPN)
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;
        tokio::spawn(run_connection(conn, true, self.spaces.clone(), self.events_tx.clone()));
        Ok(())
    }

    /// Wakes every active connection's sync task for `space_id` to run
    /// another round immediately. Call this right after mutating the
    /// `Segment` registered for `space_id` via `add_space` (e.g. right
    /// after `append_message`).
    pub async fn notify_local_change(&self, space_id: &str) {
        if let Some(entry) = self.spaces.lock().await.get(space_id) {
            entry.notify.notify_waiters();
        }
    }
}

async fn run_connection(
    conn: iroh::endpoint::Connection,
    is_dialer: bool,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    events: mpsc::UnboundedSender<TransportEvent>,
) {
    // Adaptation vs. the brief: verified against `iroh` 1.2.0's source
    // (`src/endpoint/connection.rs`) — `Connection::remote_id(&self)`
    // returns a plain `EndpointId`, not `Result<EndpointId, _>`, for a
    // normal (non-0-RTT) connection, exactly as `bootstrap.rs`'s own test
    // already documented for this same type. The brief's `let Ok(remote_id)
    // = conn.remote_id() else { return }` doesn't compile against the real
    // API, so this calls it directly.
    let remote_id = conn.remote_id();
    let _ = events.send(TransportEvent::Connected { endpoint_id: remote_id });

    let local_digests = {
        let guard = spaces.lock().await;
        let mut digests = Vec::with_capacity(guard.len());
        for (space_id, entry) in guard.iter() {
            let heads = entry.segment.lock().await.heads().iter().map(change_hash_to_bytes).collect();
            digests.push(SpaceDigest { space_id: space_id.clone(), epoch: entry.epoch, heads });
        }
        digests
    };

    let Ok(remote_hello) = exchange_digests(&conn, is_dialer, ControlHello { digests: local_digests }).await else {
        let _ = events.send(TransportEvent::Disconnected { endpoint_id: remote_id });
        return;
    };
    // Keyed by `space_id` (not a `HashSet<String>` of names) so the dialer
    // branch below can check `epoch` too, not just "the remote also knows
    // this space_id" (Important #4 fix).
    let remote_digests: HashMap<String, SpaceDigest> =
        remote_hello.digests.into_iter().map(|d| (d.space_id.clone(), d)).collect();

    // Bug fix vs. the brief's example: `StreamManager` takes ownership of
    // `conn` and exposes no way to observe the underlying connection's
    // lifetime, so a clone is kept here purely to `.closed().await` on
    // below. Without this, the dialer branch (which, unlike the accepter
    // branch's `accept_next` loop, returns immediately after opening its
    // handful of sync streams) would fall straight through to the
    // `Disconnected` send at the bottom of this function while the
    // connection -- and the sync tasks just spawned against it -- were
    // still very much alive, firing a false "disconnected" event a few
    // hundred microseconds after connecting. `iroh::endpoint::Connection`
    // is a cheap `Clone` (documented as "may be cloned to obtain another
    // handle to the same connection" in `iroh` 1.2.0's source), so this
    // costs nothing.
    let conn_for_close = conn.clone();
    let manager = Arc::new(StreamManager::new(conn));

    // Deliberate scope simplification: this plan uses the single
    // `AutomergeSync` category stream for both catch-up reconciliation and
    // live push, since Automerge's own sync-message protocol already
    // naturally serves both continuously (see `run_automerge_sync` below).
    // The protocol spec's separate `Gossip` category is kept in `Category`
    // (Task 2) for wire compatibility with its five named categories and
    // to leave room for a cheaper "just the raw new-change bytes, skip
    // bloom-filter reconciliation" fast path later, but this milestone's
    // implementation never opens a `Gossip`-category stream.
    if is_dialer {
        // Critical fix vs. the brief's example: the lock guard must NOT be
        // held across `manager.open(...).await` below -- that call does a
        // real `conn.open_bi().await` (can block on QUIC flow control
        // against an unresponsive peer) plus a network write. Holding
        // `spaces`'s lock across it would stall every other public API call
        // (`add_space`, `notify_local_change`) and every other connection's
        // `run_connection` task (which also locks `spaces`) for as long as
        // this one peer is slow to respond. So: clone out exactly what's
        // needed (space_id, the segment/notify `Arc`s) while holding the
        // lock, in a scoped block that drops the guard before the loop
        // below ever calls `.open`.
        let to_sync: Vec<(String, Arc<Mutex<Segment>>, Arc<Notify>)> = {
            let guard = spaces.lock().await;
            guard
                .iter()
                .filter_map(|(space_id, entry)| {
                    // Important #4 fix: only sync a space the remote peer
                    // also has at the SAME epoch -- syncing the same
                    // `space_id` across two different epochs (e.g. one side
                    // hasn't caught up to a membership change yet) would
                    // merge mismatched-epoch segments against each other,
                    // which is wrong. Comparing `heads` too (skip opening a
                    // stream if they already match) would be a further
                    // efficiency optimization, not required here -- left as
                    // a TODO.
                    let same_epoch = remote_digests.get(space_id).map(|d| d.epoch) == Some(entry.epoch);
                    // TODO: also compare `heads` and skip opening a stream
                    // when they already match, as an efficiency
                    // optimization (not required by this fix).
                    same_epoch.then(|| (space_id.clone(), entry.segment.clone(), entry.notify.clone()))
                })
                .collect()
        };
        for (space_id, segment, notify) in to_sync {
            if let Ok(handle) = manager.open(&space_id, Category::AutomergeSync).await {
                tokio::spawn(run_automerge_sync(handle, segment, notify, events.clone()));
            }
        }
        // Block here for the connection's actual lifetime -- see the
        // `conn_for_close` comment above for why this is necessary on the
        // dialer side specifically.
        conn_for_close.closed().await;
    } else {
        loop {
            match manager.accept_next().await {
                Ok((envelope, handle)) => {
                    if envelope.category != Category::AutomergeSync {
                        // Other categories (MlsControl, AttachmentTransfer, ...)
                        // are dispatched by Tasks 9/10's revisions of this loop.
                        continue;
                    }
                    let guard = spaces.lock().await;
                    if let Some(entry) = guard.get(&envelope.space_id) {
                        tokio::spawn(run_automerge_sync(handle, entry.segment.clone(), entry.notify.clone(), events.clone()));
                    }
                }
                // Important #3 fix: only a `Connection`-variant error means
                // the connection itself is gone (see `streams.rs`'s
                // `accept_next` -- `accept_bi()` failures map to this
                // variant). Any other error (`Io`/`Codec`, from a bad
                // stream header) means just ONE stream from a misbehaving
                // peer was malformed; the connection is still healthy, so
                // skip that stream attempt and keep accepting instead of
                // tearing down the whole connection and firing a false
                // `Disconnected`.
                Err(TransportError::Connection(_)) => break,
                Err(_) => continue,
            }
        }
    }

    let _ = events.send(TransportEvent::Disconnected { endpoint_id: remote_id });
}

/// Runs one space's sync-message exchange against one peer, for the
/// lifetime of the underlying stream. Loops: send anything new the local
/// segment has, then wait for either (a) a frame from the peer, (b) a
/// local-change notification (Task's `notify_local_change`), or (c) a
/// short timeout, whichever comes first, and repeat. This single loop
/// implements both "catch up on reconnect" and "push live changes" — see
/// `run_connection`'s comment on why `Gossip` isn't a separate stream here.
async fn run_automerge_sync(
    handle: crate::streams::StreamHandle,
    segment: Arc<Mutex<Segment>>,
    notify: Arc<Notify>,
    events: mpsc::UnboundedSender<TransportEvent>,
) {
    // Critical #1 fix: `handle` is destructured into separate `send`/`recv`
    // locals up front (avoids partial-move awkwardness) so `recv` can be
    // handed exclusively to the reader task spawned just below, while
    // `send` stays here for the main loop's writes.
    let crate::streams::StreamHandle { mut send, mut recv } = handle;
    let mut state = sync_state();
    let mut last_emitted_cursor = segment.lock().await.latest_change().cursor;

    // Cancellation-safety fix (Critical #1): `read_frame` is built on two
    // sequential `read_exact` calls, which tokio's own docs document as NOT
    // cancellation-safe. Using it directly as a `tokio::select!` branch (as
    // the brief's example did) risks silently dropping already-read bytes
    // if another branch (`notify.notified()`, the periodic timeout)
    // completes first while a read is partway through -- permanently
    // desyncing this stream's frame alignment (the sync task then either
    // dies silently or spins misinterpreting stream garbage as frame
    // lengths). The fix: give `recv` to a dedicated background task that
    // reads whole frames in a loop and forwards them over an `mpsc`
    // channel. `mpsc::Receiver::recv()` IS cancellation-safe -- a
    // cancelled/unpolled `recv()` call never loses an already-received
    // item -- so the main loop's `select!` can safely race it against
    // `notify`/the timeout instead.
    let (frame_tx, mut frame_rx) = mpsc::channel::<Vec<u8>>(1);
    tokio::spawn(async move {
        loop {
            match read_frame(&mut recv).await {
                Ok(bytes) => {
                    if frame_tx.send(bytes).await.is_err() {
                        // Main loop (the only receiver) has gone away.
                        return;
                    }
                }
                // Stream ended or produced a malformed frame -- either way,
                // there's nothing more this reader task can do; dropping
                // `frame_tx` here makes the main loop's `frame_rx.recv()`
                // observe `None` and return, the same way it used to treat
                // a `read_frame` `Err` directly.
                Err(_) => return,
            }
        }
    });

    loop {
        let outgoing = {
            let mut seg = segment.lock().await;
            seg.generate_sync_message(&mut state)
        };
        if let Some(msg) = outgoing {
            if write_frame(&mut send, &msg.encode()).await.is_err() {
                return;
            }
        }

        tokio::select! {
            frame = frame_rx.recv() => {
                let Some(bytes) = frame else { return };
                let Ok(msg) = automerge::sync::Message::decode(&bytes) else { continue };
                let mut seg = segment.lock().await;
                if seg.receive_sync_message(&mut state, msg).is_ok() {
                    // Important #9 (known, not fixed here): `latest_change`
                    // does a full Automerge document `save()` on every
                    // call, while `seg`'s lock is held -- a "deliberate
                    // simplification" per its own doc comment in
                    // `space-chat-core/src/segment.rs`, inherited from
                    // Milestone 1's design. A real scaling concern for
                    // long-lived spaces, not something to fix in this pass.
                    let change = seg.latest_change();
                    if change.cursor > last_emitted_cursor {
                        last_emitted_cursor = change.cursor;
                        let _ = events.send(TransportEvent::IncomingChange(change));
                        // Important #5 fix: wake any OTHER sync task for
                        // this space (talking to a different peer) right
                        // away instead of leaving it to its own up-to-200ms
                        // poll -- makes multi-hop gossip propagation
                        // actually push-driven, not purely polling.
                        notify.notify_waiters();
                    }
                }
            }
            _ = notify.notified() => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::TransportConfig;
    use crate::identity::TransportIdentity;
    use space_chat_core::domain::{DeviceId, Message};
    use space_chat_core::segment::Segment;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn two_transports_converge_over_a_real_connection() {
        let (relay_map, relay_url, _relay_server) =
            iroh::test_utils::run_relay_server().await.unwrap();

        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let (alice, _alice_events) = Transport::bind(
            &alice_identity,
            TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
        )
        .await
        .unwrap();
        let (bob, mut bob_events) = Transport::bind(
            &bob_identity,
            TransportConfig { relay: Some((relay_map, relay_url)) },
        )
        .await
        .unwrap();

        let alice_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
        let bob_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
        alice.add_space("space-1", 0, alice_segment.clone()).await;
        bob.add_space("space-1", 0, bob_segment.clone()).await;

        alice_segment.lock().await.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "from alice".to_string(),
            attachments: vec![],
        });
        bob_segment.lock().await.append_message(&Message {
            sender: DeviceId([2u8; 32]),
            content: "from bob".to_string(),
            attachments: vec![],
        });

        alice.dial(bob.endpoint_addr()).await.unwrap();
        alice.notify_local_change("space-1").await;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let a = alice_segment.lock().await.message_count();
                let b = bob_segment.lock().await.message_count();
                if a == 2 && b == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("both sides should converge to 2 messages within the timeout");

        // Prove convergence was actually observed through the public event
        // stream, not only via the shared Segment mutating invisibly.
        let saw_incoming_change = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match bob_events.recv().await {
                    Some(TransportEvent::IncomingChange(_)) => return true,
                    Some(_) => continue,
                    None => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(saw_incoming_change, "bob should have observed at least one IncomingChange event");
    }

    /// Important #6 / Critical #1 & #5 regression test: the test above
    /// appends BOTH sides' messages *before* `dial()`, so it only proves
    /// initial catch-up sync (`generate_sync_message`'s very first
    /// non-empty message on the first loop iteration) actually converges.
    /// It never proves the *live-push* path -- the actual subject of the
    /// Critical #1 cancellation-safety fix and the Important #5
    /// `notify_waiters()` fix -- because by the time `notify_local_change`
    /// fires, there's nothing left in flight for a `select!` to race
    /// against.
    ///
    /// Here, both sides register EMPTY segments and connect first, and only
    /// once the connection is confirmed up (both sides observed
    /// `Connected`) does alice append a message and call
    /// `notify_local_change`. This leaves a real gap during which the
    /// per-space sync streams are alive and idle-polling (exchanging empty
    /// sync-protocol handshake frames) before there's any real content to
    /// push -- exactly the situation where a `select!` cancellation of a
    /// partially-read frame (Critical #1, if unfixed) has a real chance to
    /// fire and desync the stream, and where the multi-hop
    /// `notify_waiters()` wake (Important #5) actually matters instead of
    /// being masked by the initial catch-up sync.
    #[tokio::test]
    async fn live_push_after_connection_is_established_converges() {
        let (relay_map, relay_url, _relay_server) =
            iroh::test_utils::run_relay_server().await.unwrap();

        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let (alice, mut alice_events) = Transport::bind(
            &alice_identity,
            TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
        )
        .await
        .unwrap();
        let (bob, mut bob_events) = Transport::bind(
            &bob_identity,
            TransportConfig { relay: Some((relay_map, relay_url)) },
        )
        .await
        .unwrap();

        let alice_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
        let bob_segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
        alice.add_space("space-1", 0, alice_segment.clone()).await;
        bob.add_space("space-1", 0, bob_segment.clone()).await;

        // Both segments start empty -- no message is appended on either
        // side before dialing.
        alice.dial(bob.endpoint_addr()).await.unwrap();

        // Wait until BOTH sides have confirmed the connection is up before
        // touching either segment, so there's a real window where the sync
        // streams are alive and idle (no content queued) before the
        // live-push path gets exercised below.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match alice_events.recv().await {
                    Some(TransportEvent::Connected { .. }) => break,
                    Some(_) => continue,
                    None => panic!("alice's event channel closed before Connected"),
                }
            }
        })
        .await
        .expect("alice should observe Connected within the timeout");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match bob_events.recv().await {
                    Some(TransportEvent::Connected { .. }) => break,
                    Some(_) => continue,
                    None => panic!("bob's event channel closed before Connected"),
                }
            }
        })
        .await
        .expect("bob should observe Connected within the timeout");

        // Give the freshly-opened sync streams a brief moment to run their
        // first (empty-to-empty) catch-up round before introducing real
        // content -- widening the window in which a `select!` cancellation
        // of an in-flight frame read (Critical #1) would have a real chance
        // to fire, rather than racing the very first frame ever sent.
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Only NOW does alice produce content, live, over an
        // already-established connection.
        alice_segment.lock().await.append_message(&Message {
            sender: DeviceId([1u8; 32]),
            content: "live from alice".to_string(),
            attachments: vec![],
        });
        alice.notify_local_change("space-1").await;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if bob_segment.lock().await.message_count() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("bob should converge to alice's live-pushed message within the timeout");

        let saw_incoming_change = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match bob_events.recv().await {
                    Some(TransportEvent::IncomingChange(_)) => return true,
                    Some(_) => continue,
                    None => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            saw_incoming_change,
            "bob should have observed an IncomingChange event for alice's live-pushed message"
        );

        assert_eq!(
            alice_segment.lock().await.heads(),
            bob_segment.lock().await.heads(),
            "both sides should have converged to identical document state"
        );
    }

    /// Self-review-driven addition (not in the task brief's own test list):
    /// guards against a real bug found while implementing this task. The
    /// brief's example `run_connection` had the dialer branch fall straight
    /// through to sending `Disconnected` immediately after opening its
    /// (possibly zero) sync streams -- which return right away -- rather
    /// than waiting for the connection to actually end, the way the
    /// accepter branch's blocking `accept_next` loop naturally does. That
    /// would fire a false "disconnected" event on a still-healthy
    /// connection a moment after dialing. This proves: (a) `Connected`
    /// fires before any `Disconnected`, (b) `Disconnected` only fires once
    /// bob genuinely goes away, and (c) nothing panics along the way.
    ///
    /// Bob here is a raw `bind_endpoint` endpoint, not a `Transport` --
    /// `Transport` exposes no `close`/`shutdown` method (not part of this
    /// task's API surface), and its background accept-loop task holds its
    /// own clone of the underlying `iroh::Endpoint` (an `Arc`-backed
    /// handle) for as long as it runs, so simply dropping a `Transport`
    /// value would not actually tear down a connection -- this test needs
    /// a real, controllable disconnect, so bob is driven directly instead.
    #[tokio::test]
    async fn dialer_reports_disconnected_only_after_the_peer_actually_disconnects() {
        let (relay_map, relay_url, _relay_server) =
            iroh::test_utils::run_relay_server().await.unwrap();

        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let (alice, mut alice_events) = Transport::bind(
            &alice_identity,
            TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) },
        )
        .await
        .unwrap();

        let bob = crate::bootstrap::bind_endpoint(
            &bob_identity,
            TransportConfig { relay: Some((relay_map, relay_url.clone())) },
        )
        .await
        .unwrap();
        // Built from the relay URL directly rather than `bob.addr()` -- same
        // reason as bootstrap.rs's own test: right after bind, `addr()` may
        // not yet reflect relay info.
        let bob_addr = iroh::EndpointAddr::new(bob.id()).with_relay_url(relay_url);

        let bob_task = tokio::spawn(async move {
            let incoming = bob.accept().await.expect("bob should see alice's connection");
            let conn = incoming.await.expect("handshake should complete");
            // Speak just enough of the wire protocol (Task 5's control-stream
            // digest exchange) to unblock alice's dialer-side
            // `run_connection` past `exchange_digests`, then close --
            // simulating a peer that disconnects right after connecting.
            crate::control::exchange_digests(
                &conn,
                false,
                crate::control::ControlHello { digests: vec![] },
            )
            .await
            .expect("digest exchange should succeed");
            // Deliberate delay before closing, timed from the *digest
            // exchange completing* (the same event that unblocks alice's
            // dialer-side `run_connection` past `exchange_digests`) --
            // giving a large, unambiguous gap between "the connection is
            // fully set up and healthy" and "bob actually closes it." The
            // premature-disconnect bug this test guards against fired
            // `Disconnected` right around the former instant, not the
            // latter; measuring the gap between `Connected` and
            // `Disconnected` on alice's side (below) against this delay is
            // what makes the two cases distinguishable, regardless of how
            // long the surrounding relay/QUIC handshake itself happens to
            // take on a given run (confirmed empirically to vary well past
            // a few hundred milliseconds by itself in this environment, so
            // timing from `dial()`'s start rather than from `Connected`
            // would not reliably discriminate the two cases).
            tokio::time::sleep(Duration::from_secs(2)).await;
            conn.close(0u32.into(), b"bye");
            bob.close().await;
        });

        alice.dial(bob_addr).await.unwrap();

        let mut connected_at: Option<std::time::Instant> = None;
        let mut disconnected_at: Option<std::time::Instant> = None;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match alice_events.recv().await {
                    Some(TransportEvent::Connected { .. }) => connected_at = Some(std::time::Instant::now()),
                    Some(TransportEvent::Disconnected { .. }) => {
                        disconnected_at = Some(std::time::Instant::now());
                        return;
                    }
                    Some(_) => continue,
                    None => return,
                }
            }
        })
        .await
        .expect("alice should observe both Connected and Disconnected within the timeout");

        bob_task.await.unwrap();

        let connected_at = connected_at.expect("alice should have observed Connected before Disconnected");
        let disconnected_at = disconnected_at.expect("alice should observe Disconnected once bob actually disconnects");
        let gap = disconnected_at.duration_since(connected_at);
        assert!(
            gap >= Duration::from_millis(1500),
            "Disconnected fired only {gap:?} after Connected, well before bob's deliberate 2s \
             post-handshake delay elapsed -- this is exactly the premature-disconnect bug this \
             test guards against (the dialer branch reporting Disconnected right after opening \
             sync streams instead of waiting for the connection to actually end)"
        );
    }
}

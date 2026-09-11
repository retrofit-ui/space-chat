use crate::bootstrap::{bind_endpoint, TransportConfig, ALPN};
use crate::control::{exchange_digests, ControlHello, SpaceDigest};
use crate::envelope::Category;
use crate::error::TransportError;
use crate::framing::{read_frame, write_frame};
use crate::identity::TransportIdentity;
use crate::invite::Invite;
use crate::join::{JoinRequest, SpaceMembership};
use crate::streams::StreamManager;
use space_chat_core::domain::DeviceId;
use space_chat_core::projection::SegmentChange;
use space_chat_core::segment::{sync_state, Segment};
use space_chat_core::sequencer::elect_sequencer;
use std::collections::{HashMap, HashSet};
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
    /// A `JoinRequest` (Task 10) this device -- as the space's elected
    /// sequencer -- must act on. Only ever fired on the device
    /// `elect_sequencer` currently names for the request's `space_id`; any
    /// other member's `run_connection` task forwards the same request on
    /// instead of surfacing it (see `handle_join_request` below).
    JoinRequest(JoinRequest),
}

/// This device's own `DeviceId` plus the `SpaceMembership` source used to
/// resolve `elect_sequencer` routing for Task 10's join flow, or `None`
/// before `Transport::configure_membership` is ever called. Aliased purely
/// to keep the many function signatures that thread this through
/// (`run_connection`, `handle_join_request`, `dial_and_spawn`) readable --
/// no behavior attaches to the alias itself.
type MembershipState = Arc<Mutex<Option<(DeviceId, Arc<dyn SpaceMembership>)>>>;

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

/// Chunk size used when streaming attachment bytes back to a requester
/// (`serve_attachment_request`). Keeps any single attachment-transfer frame
/// well under `framing::MAX_FRAME_LEN`, and means a large attachment is
/// streamed incrementally rather than allocated/sent as one giant frame.
const ATTACHMENT_CHUNK_SIZE: usize = 64 * 1024;

/// Upper bound on the total size of an attachment `request_attachment` will
/// accumulate before giving up. Review finding (Important #2): without a
/// cap, a malicious or buggy peer could stream chunks indefinitely and OOM
/// the requester. 100 MiB is a tunable constant sized for this milestone's
/// expected attachment sizes, not a hard architectural limit -- a future
/// milestone that needs larger attachments (or a negotiated per-request
/// size bound) can raise this or make it configurable.
const MAX_ATTACHMENT_SIZE: usize = 100 * 1024 * 1024;

pub struct Transport {
    endpoint: iroh::Endpoint,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    /// Live connections by remote `EndpointId`, populated/removed by
    /// `run_connection`. `request_attachment` looks up an already-tracked
    /// connection here rather than dialing implicitly -- this is what makes
    /// attachment transfer structurally direct-endpoint-only (see its doc
    /// comment below): there is no path from a hash lookup to "try some
    /// other peer instead."
    conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    /// This device's own copies of attachment bytes, keyed by content hash,
    /// available to serve to any peer that asks for them directly. See
    /// `serve_attachment`'s doc comment for why this stays an in-memory map
    /// rather than depending on Milestone 2's `AttachmentBlobStore`.
    attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    /// This device's own `DeviceId` and the `SpaceMembership` source used to
    /// resolve `elect_sequencer` routing for Task 10's join flow. `None`
    /// until `configure_membership` is called -- a `Transport` that never
    /// calls it simply can't participate in join routing (any incoming
    /// `MlsControl` stream is silently dropped by `handle_join_request`
    /// below), which is fine for any test/use that doesn't touch invites.
    membership: MembershipState,
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
        let conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
        let membership: MembershipState = Arc::new(Mutex::new(None));

        let transport = Self {
            endpoint: endpoint.clone(),
            spaces: spaces.clone(),
            conns: conns.clone(),
            attachments: attachments.clone(),
            membership: membership.clone(),
            events_tx: events_tx.clone(),
        };

        // Background accept loop: every inbound connection gets its own
        // per-connection task, mirroring what `dial` (below) does for
        // outbound connections.
        tokio::spawn(async move {
            loop {
                let Some(incoming) = endpoint.accept().await else { break };
                let Ok(conn) = incoming.await else { continue };
                tokio::spawn(run_connection(
                    conn,
                    false,
                    endpoint.clone(),
                    spaces.clone(),
                    conns.clone(),
                    attachments.clone(),
                    membership.clone(),
                    events_tx.clone(),
                ));
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
        dial_and_spawn(
            &self.endpoint,
            addr,
            self.spaces.clone(),
            self.conns.clone(),
            self.attachments.clone(),
            self.membership.clone(),
            self.events_tx.clone(),
        )
        .await
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

    /// Registers `bytes` as this device's copy of the attachment content-
    /// addressed by `hash`, available to serve to any peer that requests it
    /// by hash over an already-established connection. A real integration
    /// (Milestone 4) would back this with Milestone 2's
    /// `AttachmentBlobStore` rather than an in-memory map; this crate stays
    /// free of that dependency (per Global Constraints) and exposes the
    /// minimal surface a future integration plugs into.
    pub async fn serve_attachment(&self, hash: [u8; 32], bytes: Vec<u8>) {
        self.attachments.lock().await.insert(hash, bytes);
    }

    /// Requests attachment bytes directly from `from` -- no relaying
    /// through any other connected peer is attempted, even if some other
    /// peer happens to also have a connection to `from`. This method
    /// deliberately does **not** implicitly `dial` -- it looks up an
    /// already-tracked connection to `from` (populated by `run_connection`
    /// below) and fails with `TransportError::NotFound` if none exists,
    /// which is exactly what makes "attachments are strictly
    /// direct-endpoint, not multi-hop" a structural property of this API
    /// rather than an unexercised code path (see Task 8's multi-hop test,
    /// which never dials the peer it fetches no attachment from). Verifies
    /// the received bytes against `hash` before returning them, so a peer
    /// that returns wrong/corrupt content is caught here rather than
    /// silently handed to the caller.
    pub async fn request_attachment(
        &self,
        space_id: &str,
        hash: [u8; 32],
        from: iroh::EndpointId,
    ) -> Result<Vec<u8>, TransportError> {
        let conn = self.conns.lock().await.get(&from).cloned().ok_or(TransportError::NotFound)?;
        let manager = StreamManager::new(conn);
        let mut handle = manager.open(space_id, Category::AttachmentTransfer).await?;
        write_frame(&mut handle.send, &hash).await?;

        // Straightforward request/response over one dedicated stream: no
        // `select!` needed here (nothing else this call must race against),
        // so `read_frame`'s non-cancellation-safety (see
        // `run_automerge_sync`'s comment on the same function) simply
        // doesn't apply -- this call either runs a `read_frame` to
        // completion or the whole `request_attachment` future is dropped,
        // in which case there's no partial state left for anything else to
        // observe.
        let mut bytes = Vec::new();
        loop {
            let chunk = read_frame(&mut handle.recv).await?;
            if chunk.is_empty() {
                break;
            }
            // Important #2 fix: check the cap BEFORE accumulating further,
            // so a peer that keeps streaming chunks past `MAX_ATTACHMENT_SIZE`
            // can't grow `bytes` without bound -- this bails out as soon as
            // the next chunk would push the total over the cap, rather than
            // only after already having appended it.
            if bytes.len().saturating_add(chunk.len()) > MAX_ATTACHMENT_SIZE {
                return Err(TransportError::Codec(
                    "attachment exceeded the maximum accepted size".to_string(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(TransportError::NotFound);
        }

        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(&bytes);
        let actual: [u8; 32] = hasher.finalize().into();
        if actual != hash {
            return Err(TransportError::Codec(
                "received attachment content did not match its requested hash".to_string(),
            ));
        }
        Ok(bytes)
    }

    /// Registers this device's own MLS `DeviceId` and the `SpaceMembership`
    /// source used to resolve `elect_sequencer` routing for join requests
    /// (Task 10). Not needed for any purpose other than the join flow --
    /// every other `Transport` method works fine with this never having
    /// been called.
    pub async fn configure_membership(
        &self,
        own_device_id: DeviceId,
        membership: Arc<dyn SpaceMembership>,
    ) {
        *self.membership.lock().await = Some((own_device_id, membership));
    }

    /// Synchronous by design (per this task's Interfaces note): only reads
    /// `self.endpoint_id()` (a plain field read, no lock/network access) and
    /// constructs a value. `space_id`/`join_token` are trusted verbatim from
    /// the caller -- this crate never validates or interprets either.
    pub fn generate_invite(&self, space_id: &str, join_token: impl Into<String>) -> Invite {
        Invite {
            endpoint_id: *self.endpoint_id().as_bytes(),
            space_id: space_id.to_string(),
            join_token: join_token.into(),
        }
    }

    /// Dials the inviting device named in `invite` and sends a
    /// `JoinRequest` over an `MlsControl`-category stream scoped to
    /// `invite.space_id`. The inviter routes it onward per
    /// `handle_join_request` below -- this method's only job is delivering
    /// the request to *a* member; routing to the actual sequencer is the
    /// receiving side's responsibility, not the joiner's (this is the
    /// concrete meaning of "invite-based joins still route through the
    /// space's elected sequencer" even though the inviter need not already
    /// be the sequencer).
    pub async fn join_via_invite(
        &self,
        invite: &Invite,
        payload: Vec<u8>,
    ) -> Result<(), TransportError> {
        let inviter_id = iroh::EndpointId::from_bytes(&invite.endpoint_id)
            .map_err(|e| TransportError::Codec(e.to_string()))?;

        // Important I4 fix: only dial if not already connected to the
        // inviter -- mirrors the check `handle_join_request` already does
        // before dialing the elected sequencer (see its own `conns.lock()...
        // is_none()` guard below). Without this, `join_via_invite` dialed
        // the inviter unconditionally even when a connection to them
        // already existed, spawning a redundant second connection --
        // survivable only because of the `stable_id`-based dedup guard in
        // `run_connection`, but unnecessary and avoidable.
        if self.conns.lock().await.get(&inviter_id).is_none() {
            self.dial(addr_via_own_relay(&self.endpoint, inviter_id).await).await?;

            // `dial` spawns `run_connection` in the background; give the
            // control-stream digest handshake a moment to complete and
            // register the connection before looking it up. A production
            // implementation would await a `Connected` event instead of
            // sleeping -- left as a known simplification, since this plan's
            // tests tolerate the fixed delay (the same pattern several Task
            // 9 tests already use for the same reason).
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        let conn = self
            .conns
            .lock()
            .await
            .get(&inviter_id)
            .cloned()
            .ok_or(TransportError::Timeout)?;

        let manager = StreamManager::new(conn);
        let mut handle = manager.open(&invite.space_id, Category::MlsControl).await?;
        let request = JoinRequest {
            space_id: invite.space_id.clone(),
            joiner_endpoint_id: *self.endpoint_id().as_bytes(),
            payload,
            hops: 0,
        };
        let mut bytes = Vec::new();
        ciborium::into_writer(&request, &mut bytes).map_err(|e| TransportError::Codec(e.to_string()))?;
        write_frame(&mut handle.send, &bytes).await
    }
}

/// Builds a dialable `EndpointAddr` for the bare `EndpointId` `id` by
/// attaching whatever relay URL `endpoint` -- i.e. THIS LOCAL device's own
/// endpoint -- currently uses (if any).
///
/// Important I2 fix (doc correction, no behavior change): this function is
/// called *unconditionally* at every call site (`join_via_invite`,
/// `handle_join_request`'s forward-to-sequencer path) -- there is no check
/// anywhere that discovery already failed or isn't configured before this
/// runs. It unconditionally attaches the LOCAL endpoint's own relay URL to
/// the REMOTE peer's `EndpointAddr`, which is only correct in this crate's
/// own test topology, where every device shares exactly one relay
/// (`bootstrap.rs`'s "self-hosted local relay" preset, which also disables
/// discovery). In a real multi-relay or production deployment, the local
/// endpoint's home relay is frequently NOT the remote peer's home relay --
/// attaching it could seed a bad/misleading address for the remote peer
/// rather than leaving a bare `EndpointId` for real discovery (pkarr/DNS
/// under the `N0` preset) to resolve correctly on its own. In short: this is
/// a test-topology-specific workaround for this crate's single-shared-relay
/// deployment shape, not a general "falls back only when discovery comes up
/// empty" mechanism -- callers outside that topology should not rely on it.
async fn addr_via_own_relay(endpoint: &iroh::Endpoint, id: iroh::EndpointId) -> iroh::EndpointAddr {
    let mut addr = iroh::EndpointAddr::from(id);
    // `endpoint.addr()` is a plain snapshot -- right after `bind`, it can
    // still be empty of relay info (the same race `bootstrap.rs`'s own
    // tests document, there worked around by building an `EndpointAddr` by
    // hand from the relay URL the test already has in scope). This
    // function doesn't have that luxury -- it only has `endpoint` -- so it
    // uses `iroh::Watcher::updated()` (cancel-safe per its own doc) to wait
    // for the endpoint's own relay to actually populate, with a bounded
    // timeout so a genuinely relay-less endpoint doesn't hang forever.
    use iroh::Watcher;
    let mut watcher = endpoint.watch_addr();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let current = watcher.get();
        if let Some(relay_url) = current.relay_urls().next() {
            addr = addr.with_relay_url(relay_url.clone());
            break;
        }
        // Critical #1 fix: `timeout_at` only wraps the OUTER `Result` --
        // `Err` means the deadline elapsed, but `Ok(Err(_))` means the
        // watcher itself resolved (e.g. it disconnected because the
        // endpoint is closing) WITHOUT timing out. `.is_err()` on the outer
        // result alone missed that second case entirely. That's worse than
        // a plain missed-break: once a `Watcher` is disconnected,
        // `updated()` resolves to `Ready` on every single poll (it never
        // goes `Pending` again), and `tokio::time::Timeout` polls its inner
        // future before checking the deadline -- so the old code, on
        // disconnect, looped back around, called `watcher.updated()` again,
        // got `Ready` immediately again, and never yielded to the
        // scheduler: a livelock pinning a tokio worker thread at 100% CPU
        // rather than a bounded wait. Matching on BOTH the outer and inner
        // `Result` explicitly makes "timed out" OR "watcher disconnected"
        // break the loop either way, so this always terminates in bounded
        // time (at most the 5s deadline) regardless of which happens.
        match tokio::time::timeout_at(deadline, watcher.updated()).await {
            Err(_) | Ok(Err(_)) => break, // timed out, OR the watcher disconnected
            Ok(Ok(_)) => {} // got an update -- loop again to check for a relay URL
        }
    }
    addr
}

/// Dials `addr` and spawns `run_connection` against the resulting
/// connection, threading through the same shared state every other
/// connection gets. Factored out of `Transport::dial` so
/// `handle_join_request`'s forward-to-sequencer path (Task 10) can reuse
/// exactly the same dial/spawn behavior when it isn't already connected to
/// the space's elected sequencer, rather than duplicating it.
async fn dial_and_spawn(
    endpoint: &iroh::Endpoint,
    addr: impl Into<iroh::EndpointAddr>,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    membership: MembershipState,
    events: mpsc::UnboundedSender<TransportEvent>,
) -> Result<(), TransportError> {
    let conn = endpoint
        .connect(addr, ALPN)
        .await
        .map_err(|e| TransportError::Connection(e.to_string()))?;
    tokio::spawn(run_connection(
        conn,
        true,
        endpoint.clone(),
        spaces,
        conns,
        attachments,
        membership,
        events,
    ));
    Ok(())
}

/// Handles one incoming `JoinRequest` on an `MlsControl` stream: if this
/// device is the space's elected sequencer, surfaces it as a
/// `TransportEvent::JoinRequest` for a higher layer (a future
/// `space-chat-openmls` integration) to actually act on; otherwise forwards
/// the same request, unmodified, to whichever device *is* the elected
/// sequencer -- dialing it fresh via `dial_and_spawn` if not already
/// connected. This is what "invite-based joins still route through the
/// space's elected sequencer" (the transport spec's Pairing/discovery
/// section) means concretely: the inviter is not required to already be the
/// sequencer, nor even already connected to it.
#[allow(clippy::too_many_arguments)]
async fn handle_join_request(
    mut handle: crate::streams::StreamHandle,
    space_id: String,
    endpoint: iroh::Endpoint,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    membership: MembershipState,
    conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    events: mpsc::UnboundedSender<TransportEvent>,
) {
    let Ok(bytes) = read_frame(&mut handle.recv).await else { return };
    let Ok(request): Result<JoinRequest, _> = ciborium::from_reader(bytes.as_slice()) else { return };

    // Important I1 fix: `space_id` (this function's parameter) comes from
    // the STREAM ENVELOPE and is used below to look up membership / elect
    // the sequencer, while `request.space_id` is the PAYLOAD's own,
    // independently peer-controlled field. A peer could set the envelope's
    // space_id to name space-A (routing via space-A's membership/sequencer)
    // while the request payload names space-B, so a space-B join request
    // ends up routed/delivered via space-A's sequencer. Require the two to
    // agree; drop the request otherwise rather than process or forward it.
    if request.space_id != space_id {
        return;
    }

    // Destructured into `membership_source` (the `Arc<dyn SpaceMembership>`)
    // to avoid shadowing the outer `membership: Arc<Mutex<Option<...>>>`
    // parameter, which is still needed below to pass on to `dial_and_spawn`
    // (so the freshly-dialed connection to the sequencer also carries
    // membership config, same as every other connection this device makes).
    let Some((own_device_id, membership_source)) = membership.lock().await.clone() else { return };
    let members = membership_source.members(&space_id);
    let Some(sequencer) = elect_sequencer(&members) else { return };

    if sequencer == own_device_id {
        let _ = events.send(TransportEvent::JoinRequest(request));
        return;
    }

    let Some(sequencer_endpoint) = membership_source.endpoint_for(sequencer) else { return };

    // Critical #2 defensive fix: if the elected sequencer's endpoint
    // resolves to THIS device's own `EndpointId`, even though `sequencer`
    // is NOT this device's own `DeviceId` (already ruled out just above),
    // the membership mapping is inconsistent/malformed -- dialing "the
    // sequencer" here would actually mean dialing ourselves and then
    // processing our own forwarded request, looping. Drop instead of
    // dialing.
    if sequencer_endpoint == endpoint.id() {
        return;
    }

    // Critical #2 hop-count fix: the design assumes every peer computes the
    // same sequencer for a given space via `elect_sequencer`, so forwarding
    // converges in one hop. But two devices at different MLS-state epochs
    // can have different `members()` views and each elect the OTHER as
    // sequencer, forwarding the same request back and forth forever, each
    // hop spawning a fresh connection/task with no termination. Cap the
    // number of hops (`JoinRequest::MAX_HOPS`) and drop -- rather than
    // forward again -- once reached.
    if request.hops >= JoinRequest::MAX_HOPS {
        return;
    }
    let mut forwarded_request = request;
    forwarded_request.hops += 1;
    let mut forward_bytes = Vec::new();
    if ciborium::into_writer(&forwarded_request, &mut forward_bytes).is_err() {
        return;
    }

    // Not already connected to the sequencer? Dial it fresh, exactly like
    // `Transport::dial` would, then give the handshake a moment to land --
    // mirroring `join_via_invite`'s own fixed-delay simplification above,
    // for the same reason. If the dial itself fails (sequencer unreachable),
    // this request is simply dropped: there is no further fallback in this
    // milestone (a known gap -- see this plan's closing notes on retry /
    // queuing strategies for an offline sequencer).
    if conns.lock().await.get(&sequencer_endpoint).is_none() {
        let sequencer_addr = addr_via_own_relay(&endpoint, sequencer_endpoint).await;
        let _ = dial_and_spawn(
            &endpoint,
            sequencer_addr,
            spaces.clone(),
            conns.clone(),
            attachments.clone(),
            membership.clone(),
            events.clone(),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    let conn = conns.lock().await.get(&sequencer_endpoint).cloned();
    let Some(conn) = conn else { return }; // dial failed or handshake didn't land in time
    let manager = StreamManager::new(conn);
    if let Ok(mut forward_handle) = manager.open(&space_id, Category::MlsControl).await {
        let _ = write_frame(&mut forward_handle.send, &forward_bytes).await;
    }
}

// Deliberately NOT an `async fn`: `run_connection` (via the `MlsControl`
// dispatch arm below, Task 10) spawns `handle_join_request`, which -- to
// forward a join request to a sequencer this device isn't already connected
// to -- calls `dial_and_spawn`, which itself calls back into
// `run_connection`. That mutual recursion makes the ordinary `async fn`
// desugaring (an opaque, self-referential `impl Future`) something rustc
// cannot resolve ("cannot check whether the hidden type of opaque type
// satisfies auto traits") -- the same class of "recursive `async fn`" issue
// that requires boxing even for direct self-recursion. Returning an
// explicit, already-boxed trait object here breaks the cycle: nothing
// upstream needs to infer `run_connection`'s own hidden opaque type as part
// of resolving anything else's, because the signature already says
// precisely what it returns.
#[allow(clippy::too_many_arguments)]
fn run_connection(
    conn: iroh::endpoint::Connection,
    is_dialer: bool,
    endpoint: iroh::Endpoint,
    spaces: Arc<Mutex<HashMap<String, SpaceEntry>>>,
    conns: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    membership: MembershipState,
    events: mpsc::UnboundedSender<TransportEvent>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
    // Adaptation vs. the brief: verified against `iroh` 1.2.0's source
    // (`src/endpoint/connection.rs`) — `Connection::remote_id(&self)`
    // returns a plain `EndpointId`, not `Result<EndpointId, _>`, for a
    // normal (non-0-RTT) connection, exactly as `bootstrap.rs`'s own test
    // already documented for this same type. The brief's `let Ok(remote_id)
    // = conn.remote_id() else { return }` doesn't compile against the real
    // API, so this calls it directly.
    let remote_id = conn.remote_id();

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
        // Important #4 fix: `conns` was never populated for this connection
        // (the insert now happens below, only AFTER this handshake
        // succeeds), so there is nothing to remove here -- this early
        // return simply never having inserted is what makes this path
        // correct, not a compensating removal.
        //
        // Review fix (finding #1): no `Connected` has been sent for this
        // connection either -- it's now only sent once the handshake
        // below succeeds -- so this failure path must NOT send
        // `Disconnected` here. Doing so would report a `Disconnected` for
        // an `endpoint_id` a consumer was never told `Connected` about,
        // breaking the invariant that every `Disconnected` pairs with a
        // prior `Connected` (and vice versa). A handshake failure is
        // simply never announced at all.
        return;
    };
    // Keyed by `space_id` (not a `HashSet<String>` of names) so the dialer
    // branch below can check `epoch` too, not just "the remote also knows
    // this space_id" (Important #4 fix).
    let remote_digests: HashMap<String, SpaceDigest> =
        remote_hello.digests.into_iter().map(|d| (d.space_id.clone(), d)).collect();
    // Important #5 fix: the set of space_ids the remote peer *claims* to
    // share (self-asserted via its own `ControlHello`, with no membership
    // proof -- see `serve_attachment_request`'s doc comment for why this is
    // a best-effort check, not a security boundary), threaded into
    // `serve_attachment_request` so it can refuse to serve an attachment
    // under a `space_id` the requester didn't even claim to share.
    // `Arc`-wrapped so every `serve_attachment_request` task spawned below
    // can cheaply clone a handle to the same set rather than cloning the
    // set itself.
    let remote_spaces: Arc<HashSet<String>> = Arc::new(remote_digests.keys().cloned().collect());

    // Important #4 fix: only make this connection visible to
    // `Transport::request_attachment` (via `conns`) once the control-stream
    // handshake above has actually completed. Populating `conns` any
    // earlier left a window where a caller could `request_attachment`
    // against this connection before `exchange_digests` had claimed its
    // control stream -- an attachment-transfer stream opened in that
    // window could race with (and be misread as) the control stream's own
    // first stream on the accepting side.
    conns.lock().await.insert(remote_id, conn.clone());
    // Review fix (finding #1): `Connected` is only announced here, AFTER
    // the control-stream handshake has succeeded and `conns` has been
    // populated -- i.e. once this connection is genuinely usable by a
    // consumer reacting to the event (e.g. immediately calling
    // `Transport::request_attachment`). Previously this fired right at
    // the top of the function, before either had happened, so a consumer
    // could race `request_attachment` against a `conns` entry that didn't
    // exist yet and get a spurious `TransportError::NotFound`.
    let _ = events.send(TransportEvent::Connected { endpoint_id: remote_id });
    // Important #3 fix: `stable_id()` (verified against `iroh` 1.2.0's
    // source, `src/endpoint/connection.rs` -- defined on the generic
    // `impl<T: ConnectionState> Connection<T>`, so it's available on this
    // plain `Connection`) is "a stable identifier for this connection...
    // fixed for the lifetime of the connection" even though "peer
    // addresses and connection IDs can change." Captured now, before
    // `conn` is moved into `StreamManager`, so the removal at the bottom of
    // this function can tell whether the `conns` entry still stored under
    // `remote_id` is THIS task's own connection, or some other (newer)
    // connection to the same peer that has since replaced it in a
    // reconnect race -- in which case this task must NOT remove it.
    let conn_stable_id = conn.stable_id();
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
    }

    // Important #1 fix: this accept loop now runs for BOTH roles, not just
    // the accepter. Previously the dialer branch (above) opened its sync
    // streams and then just blocked on the connection's own `.closed()`,
    // never calling `accept_next()` -- so if the *dialer* side of a
    // connection was asked (via `conns`, which is populated for both
    // roles) to serve an `AttachmentTransfer` request, the requester's
    // `manager.open()` would succeed (QUIC permits opening a stream in
    // either direction once connected) but the dialer would never accept
    // it, hanging the requester's `read_frame` forever. Since QUIC
    // connections are bidirectional once established, and Automerge sync
    // streams only need to be *opened* by one side (the same bidirectional
    // stream serves both directions of sync once opened -- the dialer-only
    // block above), there is no remaining reason for the dialer and
    // accepter to run different loops after that point: both now dispatch
    // whatever the peer opens, symmetrically.
    //
    // This also replaces the old `conn_for_close.closed().await` workaround
    // that Task 7 added purely so the dialer branch had *something* to
    // block on for the connection's lifetime (it used to return
    // immediately after opening its sync streams, firing a false
    // `Disconnected` on an otherwise-healthy connection). `accept_next`'s
    // underlying `accept_bi()` blocks until either a stream arrives or the
    // connection itself ends (surfacing as `Err(TransportError::Connection(_))`
    // in the latter case) -- exactly the same "block until this connection
    // ends" behavior `.closed()` provided, so nothing is lost by removing
    // it, and the dialer now also gets to observe incoming streams, which
    // is the whole point of this fix.
    loop {
        match manager.accept_next().await {
            Ok((envelope, handle)) => match envelope.category {
                Category::AutomergeSync => {
                    let guard = spaces.lock().await;
                    if let Some(entry) = guard.get(&envelope.space_id) {
                        // Epoch check (review finding #1): mirror the
                        // dialer-side epoch gate above. Without this, an
                        // incoming stream naming a locally-tracked
                        // `space_id` would be synced regardless of whether
                        // the remote peer is actually on the same epoch --
                        // syncing mismatched-epoch segments against each
                        // other is wrong for the same reason it's wrong on
                        // the dialer side. If the epoch doesn't match, the
                        // stream (and its `handle`) is simply dropped here
                        // without being spawned; nothing reads or writes it
                        // afterward, so it doesn't hang open -- the peer
                        // that opened it will see it go idle/closed when
                        // `handle` is dropped at the end of this iteration.
                        let same_epoch = remote_digests.get(&envelope.space_id).map(|d| d.epoch) == Some(entry.epoch);
                        if same_epoch {
                            tokio::spawn(run_automerge_sync(handle, entry.segment.clone(), entry.notify.clone(), events.clone()));
                        }
                    }
                }
                Category::AttachmentTransfer => {
                    // Task 9: serve one attachment request. Never relays to
                    // any other peer: the handler below only ever answers
                    // from this process's own `attachments` map. Important
                    // #5 fix: `remote_spaces` (this connection's peer's own
                    // *claimed* space_ids) and the requested `envelope.space_id`
                    // are threaded through so the handler can refuse to serve
                    // a hash under a space the requester didn't even claim to
                    // share, rather than serving any hash to any connected
                    // peer with no space check at all. See
                    // `serve_attachment_request`'s doc comment for why this
                    // is only a best-effort filter, not real cross-space
                    // isolation, in this milestone.
                    tokio::spawn(serve_attachment_request(
                        handle,
                        attachments.clone(),
                        remote_spaces.clone(),
                        envelope.space_id,
                    ));
                }
                Category::MlsControl => {
                    // Task 10: route this join request per
                    // `handle_join_request`'s doc comment -- surface it if
                    // this device is the space's elected sequencer,
                    // otherwise forward it on. Coexists with the
                    // `AutomergeSync` epoch gate and `AttachmentTransfer`
                    // space gate above/below without touching either: this
                    // arm only reads `membership` (its own dedicated lock,
                    // never `spaces`/`conns` held across it) and dispatches
                    // to its own handler.
                    tokio::spawn(handle_join_request(
                        handle,
                        envelope.space_id,
                        endpoint.clone(),
                        spaces.clone(),
                        membership.clone(),
                        conns.clone(),
                        attachments.clone(),
                        events.clone(),
                    ));
                }
                // Gossip, Ephemeral: not opened by this milestone's
                // implementation (see this function's earlier comment on
                // Gossip) or handled by a later task.
                Category::Gossip | Category::Ephemeral => {}
            },
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

    // Important #3 fix: don't blindly `remove` -- a reconnect race could
    // mean a *different*, newer connection to this same `remote_id` has
    // already been inserted into `conns` by another `run_connection` task
    // by the time this one's accept loop exits. Compare `stable_id()`
    // (captured before `conn` was moved into `manager`, above) against
    // whatever connection is currently stored under `remote_id`, and only
    // remove the entry if it's still THIS task's own connection --
    // otherwise leave the newer connection's entry alone.
    {
        let mut guard = conns.lock().await;
        if guard.get(&remote_id).map(|c| c.stable_id()) == Some(conn_stable_id) {
            guard.remove(&remote_id);
        }
    }
    let _ = events.send(TransportEvent::Disconnected { endpoint_id: remote_id });
    })
}

/// Serves one incoming attachment request: reads the requested hash, writes
/// back either the content in fixed-size chunks followed by an empty
/// terminator frame, or just the empty terminator if this device doesn't
/// have that hash (or `remote_spaces` doesn't contain the requested
/// `space_id` -- see the space-scoping comment below, and its important
/// caveats). Never relays the request to any other peer -- there is no code
/// path here that could, which is the point (see
/// `Transport::request_attachment`'s doc comment on direct-endpoint-only
/// behavior).
async fn serve_attachment_request(
    mut handle: crate::streams::StreamHandle,
    attachments: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    remote_spaces: Arc<HashSet<String>>,
    space_id: String,
) {
    let Ok(hash_bytes) = read_frame(&mut handle.recv).await else { return };
    let Ok(hash): Result<[u8; 32], _> = hash_bytes.try_into() else { return };

    // Important #5 fix: `request_attachment`'s API takes a `space_id`,
    // implying attachments are scoped to a shared space, but until this fix
    // nothing checked it at all -- any connected peer could fetch any hash
    // in `attachments` regardless of which spaces it claimed to share.
    // `remote_spaces` is this connection's peer's own claimed space_ids
    // (from its `ControlHello`, computed once in `run_connection`); if
    // `space_id` isn't among them, this is treated exactly like "hash not
    // found" below (just the empty terminator frame), rather than silently
    // ignoring the space entirely.
    //
    // Review finding (honesty correction): despite the name, this is NOT a
    // real cross-space authorization boundary in this milestone, for two
    // separate reasons, either one of which is sufficient to defeat it:
    //   1. `remote_spaces` comes entirely from the peer's own
    //      `ControlHello`/`SpaceDigest`, which is self-asserted with no
    //      membership proof whatsoever -- any peer can simply claim to be
    //      in any `space_id` it likes. Real membership verification (MLS
    //      group membership) is Task 10 and does not exist yet.
    //   2. Even given a genuinely-verified `space_id`, the `attachments`
    //      map (`HashMap<[u8; 32], Vec<u8>>`) has no space dimension at
    //      all -- `serve_attachment` stores/looks up by hash only, with no
    //      association to the space the content came from. So a peer that
    //      *does* legitimately share space-1 could still fetch an
    //      attachment that actually belongs to space-2, purely by naming
    //      space-1 in its request envelope, because nothing here ties a
    //      stored hash to the space it was attached to.
    // This check is left in place as a harmless best-effort filter and a
    // reasonable foundation for real enforcement once Task 10 lands, but it
    // must not be read as providing actual cross-space isolation today.
    let bytes = if remote_spaces.contains(&space_id) {
        attachments.lock().await.get(&hash).cloned()
    } else {
        None
    };
    if let Some(bytes) = bytes {
        for chunk in bytes.chunks(ATTACHMENT_CHUNK_SIZE) {
            if write_frame(&mut handle.send, chunk).await.is_err() {
                return;
            }
        }
    }
    let _ = write_frame(&mut handle.send, &[]).await;
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
                // Review finding #2 fix: rather than comparing against a
                // remembered cursor value (which a *different* sync task's
                // purely-local mutation, or a sibling peer's successful
                // merge, could have already bumped since this task last
                // looked -- since `cursor` is a single counter shared across
                // the whole `Segment`, not per-peer), check directly whether
                // THIS `receive_sync_message` call actually merged new
                // content, by comparing `heads()` immediately before and
                // immediately after it, both while still holding the same
                // lock guard so nothing else can mutate `seg` in between.
                // `receive_sync_message` only merges what's contained in
                // `msg`, so if heads are unchanged afterward, nothing new
                // was incorporated as a result of receiving THIS message --
                // whether because the peer sent something already known, or
                // any other reason -- so there's genuinely nothing new to
                // report from this receive.
                let mut seg = segment.lock().await;
                let heads_before = seg.heads();
                if seg.receive_sync_message(&mut state, msg).is_ok() && seg.heads() != heads_before {
                    // Important #9 (known, not fixed here): `latest_change`
                    // does a full Automerge document `save()` on every
                    // call, while `seg`'s lock is held -- a "deliberate
                    // simplification" per its own doc comment in
                    // `space-chat-core/src/segment.rs`, inherited from
                    // Milestone 1's design. A real scaling concern for
                    // long-lived spaces, not something to fix in this pass.
                    let change = seg.latest_change();
                    let _ = events.send(TransportEvent::IncomingChange(change));
                    // Important #5 fix: wake any OTHER sync task for this
                    // space (talking to a different peer) right away
                    // instead of leaving it to its own up-to-200ms poll.
                    // This is best-effort push, not a guarantee:
                    // `Notify::notify_waiters()` only wakes tasks currently
                    // parked in `notified()` at the moment it's called -- a
                    // sibling task that's mid-write (or not yet back around
                    // to its `select!`) misses the wake and simply falls
                    // back to its own 200ms poll floor, so propagation is
                    // push-when-possible with a bounded polling fallback,
                    // not guaranteed-instant.
                    notify.notify_waiters();
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
    /// push. Honest scope of what this proves: it exercises the live-push /
    /// `notify_waiters()` path structurally -- convergence works when
    /// content is appended after the connection and sync streams are
    /// already established, rather than only via initial catch-up sync --
    /// and where the multi-hop `notify_waiters()` wake (Important #5)
    /// actually gets a chance to matter instead of being masked by the
    /// initial catch-up sync. It does NOT reliably force the exact
    /// `select!`-cancels-a-partially-read-frame race that motivated the
    /// Critical #1 cancellation-safety fix: the frames involved here are
    /// small enough to complete within a single poll almost always, so this
    /// test does not prove that specific timing window was hit, even though
    /// the underlying reader-task/channel restructuring is correct
    /// regardless, for the more fundamental cancellation-safety reasons
    /// documented on `run_automerge_sync` above.
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

    #[tokio::test]
    async fn attachment_transfer_is_direct_endpoint_only_and_hash_verified() {
        use sha2::{Digest, Sha256};

        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();
        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let (alice, _alice_events) = Transport::bind(&alice_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let (bob, _bob_events) = Transport::bind(&bob_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();

        alice.add_space("space-1", 0, Arc::new(Mutex::new(Segment::new("space-1", 0)))).await;
        bob.add_space("space-1", 0, Arc::new(Mutex::new(Segment::new("space-1", 0)))).await;

        let content = b"a rather large attachment, in spirit if not in this test's actual byte count".to_vec();
        let mut hasher = Sha256::new();
        hasher.update(&content);
        let hash: [u8; 32] = hasher.finalize().into();
        alice.serve_attachment(hash, content.clone()).await;

        bob.dial(alice.endpoint_addr()).await.unwrap();
        // Give the connection's control-stream handshake a moment to land
        // before requesting -- request_attachment requires an already-tracked
        // connection (see this task's Interfaces note) and does not implicitly
        // dial or wait.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let fetched = bob
            .request_attachment("space-1", hash, alice.endpoint_id())
            .await
            .expect("bob should fetch the attachment directly from alice");
        assert_eq!(fetched, content);

        // Requesting a hash alice never served must fail, not hang or panic.
        let missing_hash = [0xffu8; 32];
        let result = bob.request_attachment("space-1", missing_hash, alice.endpoint_id()).await;
        assert!(result.is_err(), "requesting an unserved hash should return an error, not succeed");
    }

    /// Self-review-driven addition (not in the task brief's own test list):
    /// the brief's own test content (~78 bytes) never exceeds
    /// `ATTACHMENT_CHUNK_SIZE` (64 KiB), so it can't distinguish "actually
    /// streamed in chunks" from "sent as one giant frame" -- a bug class the
    /// self-review checklist explicitly calls out. This test uses content
    /// several times larger than the chunk size, forcing
    /// `serve_attachment_request` to write multiple chunk frames and
    /// `request_attachment` to reassemble them via its `loop`, and confirms
    /// the reassembled bytes are byte-for-byte identical and hash-verified.
    #[tokio::test]
    async fn attachment_larger_than_one_chunk_reassembles_correctly() {
        use sha2::{Digest, Sha256};

        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();
        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let (alice, _alice_events) = Transport::bind(&alice_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let (bob, _bob_events) = Transport::bind(&bob_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();

        alice.add_space("space-1", 0, Arc::new(Mutex::new(Segment::new("space-1", 0)))).await;
        bob.add_space("space-1", 0, Arc::new(Mutex::new(Segment::new("space-1", 0)))).await;

        // 3.5x the 64 KiB chunk size, with a non-repeating-looking pattern
        // (byte value cycling through 0..=255) so a bug that dropped,
        // duplicated, or misordered a chunk would corrupt the content in a
        // way `assert_eq!` -- and the hash check inside `request_attachment`
        // itself -- would actually catch, rather than accidentally matching.
        let content: Vec<u8> = (0..(ATTACHMENT_CHUNK_SIZE * 7 / 2)).map(|i| (i % 256) as u8).collect();
        let mut hasher = Sha256::new();
        hasher.update(&content);
        let hash: [u8; 32] = hasher.finalize().into();
        alice.serve_attachment(hash, content.clone()).await;

        bob.dial(alice.endpoint_addr()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let fetched = bob
            .request_attachment("space-1", hash, alice.endpoint_id())
            .await
            .expect("bob should fetch the multi-chunk attachment directly from alice");
        assert_eq!(fetched.len(), content.len());
        assert_eq!(fetched, content);
    }

    /// Regression test for Important #1: before the fix, `run_connection`
    /// had two asymmetric branches -- the DIALER branch opened its
    /// `AutomergeSync` streams up front and then just blocked on
    /// `conn_for_close.closed().await`, never calling `manager.accept_next()`.
    /// Only the accepter branch ran an `accept_next()` loop that could
    /// receive an incoming `AttachmentTransfer` stream. Since `conns` (the
    /// map `request_attachment` looks peers up in) is populated for BOTH
    /// roles, a peer could ask the DIALER-role side of a connection for an
    /// attachment: `manager.open()` on the requester's side would succeed
    /// (QUIC permits opening a stream in either direction once connected),
    /// but the dialer would never accept it, and the requester's
    /// `read_frame` would hang forever.
    ///
    /// Here, bob dials alice (so alice is the ACCEPTER and bob is the
    /// DIALER for this connection), and then ALICE -- the accepter --
    /// requests an attachment FROM bob -- the dialer. This is exactly the
    /// direction that used to hang. The whole exchange is wrapped in a
    /// generous timeout so that if this regresses, the test fails cleanly
    /// instead of hanging the test suite.
    #[tokio::test]
    async fn accepter_can_request_an_attachment_from_the_dialer() {
        use sha2::{Digest, Sha256};

        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();
        let alice_identity = TransportIdentity::generate();
        let bob_identity = TransportIdentity::generate();
        let (alice, _alice_events) = Transport::bind(&alice_identity, TransportConfig { relay: Some((relay_map.clone(), relay_url.clone())) }).await.unwrap();
        let (bob, _bob_events) = Transport::bind(&bob_identity, TransportConfig { relay: Some((relay_map, relay_url)) }).await.unwrap();

        alice.add_space("space-1", 0, Arc::new(Mutex::new(Segment::new("space-1", 0)))).await;
        bob.add_space("space-1", 0, Arc::new(Mutex::new(Segment::new("space-1", 0)))).await;

        let content = b"an attachment that only the dialer-role peer holds".to_vec();
        let mut hasher = Sha256::new();
        hasher.update(&content);
        let hash: [u8; 32] = hasher.finalize().into();
        // Registered on BOB -- the peer that will be in the DIALER role
        // below (bob.dial(alice)).
        bob.serve_attachment(hash, content.clone()).await;

        // Bob dials alice: bob is the dialer, alice is the accepter for
        // this connection.
        bob.dial(alice.endpoint_addr()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Alice -- the ACCEPTER -- asks bob -- the DIALER -- for the
        // attachment. Before the Important #1 fix, bob's `run_connection`
        // task never ran an `accept_next()` loop, so this would hang
        // forever; the timeout below turns that into a clean test failure
        // rather than an actual hang.
        let fetched = tokio::time::timeout(
            Duration::from_secs(10),
            alice.request_attachment("space-1", hash, bob.endpoint_id()),
        )
        .await
        .expect(
            "alice's request_attachment to the dialer-role peer should not hang -- \
             this is exactly the direction Important #1's fix addresses",
        )
        .expect("alice should fetch the attachment directly from bob");
        assert_eq!(fetched, content);
    }
}

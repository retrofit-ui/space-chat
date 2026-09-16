pub mod attachment_protocol;
pub mod commands;
pub mod conversation_spec;
pub mod events;
pub mod invite;
pub mod live_spec;
pub mod membership;
pub mod network;
pub mod observed_at;
pub mod spec;
pub mod state;

use tauri::Manager;

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {name}! space-chat is running.")
}

/// Seeds `space_id`'s membership with this device plus every named peer, using
/// `deterministic_device_id_for_actor` to derive each peer's `DeviceId` from
/// its actor name -- the multi-actor E2E harness's way of establishing a
/// shared space without a real invite/MLS exchange (which doesn't exist yet;
/// `membership.rs` is an explicit placeholder).
///
/// Runtime-gated behind `SPACECHAT_TEST_HOOKS` rather than
/// `#[cfg(debug_assertions)]`: `tauri::generate_handler!` takes a fixed
/// command list, and conditionally compiling an entry out of it would mean
/// maintaining two variants of that list. A runtime check keeps one list while
/// still making this command an immediate error in any launch that hasn't
/// explicitly opted in. `SPACECHAT_TEST_HOOKS` is never set by a real user
/// launch, the same way `SPACECHAT_DATA_DIR`/`SPACECHAT_ACTOR_NAME` aren't.
#[tauri::command]
async fn seed_membership_for_testing(
    state: tauri::State<'_, std::sync::Arc<state::AppState>>,
    space_id: String,
    peer_names: Vec<String>,
) -> Result<(), String> {
    seed_membership_impl(&state, &space_id, &peer_names)
}

/// The body of `seed_membership_for_testing`, split out the same way
/// `commands.rs` splits `open_conversation_impl` -- so the gate and the
/// seeding can be tested against a real `AppState` without constructing a
/// `tauri::State`.
fn seed_membership_impl(state: &state::AppState, space_id: &str, peer_names: &[String]) -> Result<(), String> {
    if std::env::var("SPACECHAT_TEST_HOOKS").is_err() {
        return Err("seed_membership_for_testing is only available when SPACECHAT_TEST_HOOKS is set".to_string());
    }
    use crate::membership::SpaceMembership;
    let local_display_name = std::env::var("SPACECHAT_ACTOR_NAME").unwrap_or_else(|_| "Me".to_string());
    let mut membership = state.membership.lock().map_err(|e| e.to_string())?;
    membership.create_space(space_id, state.local_device, local_display_name);
    for name in peer_names {
        membership.add_member(space_id, deterministic_device_id_for_actor(name), name.clone());
    }
    Ok(())
}

/// The fixed attachment hash `send_message_with_missing_attachment_for_testing`
/// uses. Fixed rather than random: it is the ONE hash that scenario ever asks
/// about, so a constant keeps the test deterministic and trivially greppable in
/// a log. It is not the hash of anything, which is the point -- nothing ever
/// calls `save_attachment` with it until the scenario explicitly says the bytes
/// "arrived."
const MISSING_ATTACHMENT_HASH: [u8; 32] = [0x42u8; 32];

/// Sends a message in `DEFAULT_SPACE_ID` carrying one attachment whose hash is
/// deliberately never passed to `AttachmentBlobStore::save_attachment` --
/// exactly the cache-miss case Task 13's `handle_attachment_request` exists to
/// handle, and therefore exactly what makes `AttachmentImage` render its
/// placeholder rather than a loaded image.
///
/// Returns the hex-encoded hash so the caller (a test step) can later hand the
/// same value to `simulate_attachment_arrival_for_testing`. Runtime-gated
/// behind `SPACECHAT_TEST_HOOKS`, same as `seed_membership_for_testing`.
///
/// Emits the conversation patch itself (via `commands::emit_patch_if_active`)
/// for the same reason `commands::send_message` does: `mutate_and_persist`
/// advances the active conversation's `LiveSpec` but does not emit, so without
/// this the frontend would never learn a message had been appended.
#[tauri::command]
async fn send_message_with_missing_attachment_for_testing(
    state: tauri::State<'_, std::sync::Arc<state::AppState>>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if std::env::var("SPACECHAT_TEST_HOOKS").is_err() {
        return Err(
            "send_message_with_missing_attachment_for_testing is only available when SPACECHAT_TEST_HOOKS is set"
                .to_string(),
        );
    }
    let hash = MISSING_ATTACHMENT_HASH;
    commands::send_message_with_attachments_impl(
        &state,
        DEFAULT_SPACE_ID,
        "here's an attachment".to_string(),
        vec![space_chat_core::domain::AttachmentRef {
            hash,
            size: ARRIVED_ATTACHMENT_BYTES.len() as u64,
            mime: "image/png".to_string(),
            wrapped_key: vec![],
        }],
    )
    .await?;
    commands::emit_patch_if_active(&app, &state, DEFAULT_SPACE_ID);
    Ok(hex_encode(&hash))
}

/// A real, valid, DECODABLE 1x1 RGBA PNG -- deliberately NOT the same file
/// Task 13's own placeholder response serves (a 64x64 grayscale PNG at
/// `assets/attachment-placeholder.png`). Review found that using arbitrary
/// non-image bytes (`b"fake image bytes for testing"`) let the scenario
/// pass while proving nothing: `AttachmentImage`'s placeholder state
/// renders a `<div>`, not an `<img>`, so no `spacechat://` request happens
/// until `ready()` flips, and checking only the loaded `<img>` element's
/// *presence* doesn't prove it decoded. The natural next fix -- checking
/// `naturalWidth > 0` -- turned out to ALSO pass without this fix, for a
/// subtler reason: `handle_attachment_request`'s cache-miss path returns
/// the real placeholder PNG's bytes (status 200, valid, decodable) as a
/// deliberate design choice (Task 13's own lazy-fetch policy), so a
/// `naturalWidth` check alone can't tell "the real attachment arrived"
/// apart from "still serving the fallback placeholder image" -- both
/// decode fine. Using a 1x1 image here, distinct from the placeholder's
/// 64x64, and asserting the loaded `<img>`'s exact dimensions (not just
/// that they're nonzero) is what actually distinguishes the two.
const ARRIVED_ATTACHMENT_BYTES: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0A, 0x49,
    0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00,
    0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
];

/// Writes real (valid, decodable, 1x1) PNG bytes into the local
/// `AttachmentBlobStore` for `hash_hex` (as if a real fetch-over-network had
/// just completed) and emits the same `attachment-ready:<hash>` event Task
/// 13's real (not-yet-built) fetch pipeline would eventually emit.
/// `AttachmentImage` (Task 16) doesn't care how the bytes arrived, only
/// that the event fires once they have -- so this is a faithful stand-in
/// for the *frontend's* half of lazy fetch even though the transport half
/// doesn't exist yet. See `ARRIVED_ATTACHMENT_BYTES`'s doc comment for why
/// this must be a real, distinctly-sized image, not arbitrary bytes.
#[tauri::command]
async fn simulate_attachment_arrival_for_testing(
    state: tauri::State<'_, std::sync::Arc<state::AppState>>,
    app: tauri::AppHandle,
    hash_hex: String,
) -> Result<(), String> {
    if std::env::var("SPACECHAT_TEST_HOOKS").is_err() {
        return Err(
            "simulate_attachment_arrival_for_testing is only available when SPACECHAT_TEST_HOOKS is set".to_string()
        );
    }
    let hash = decode_hash_hex(&hash_hex)?;
    {
        use space_chat_core::storage::AttachmentBlobStore;
        let mut store = state.attachment_store.lock().map_err(|e| e.to_string())?;
        store.save_attachment(&hash, ARRIVED_ATTACHMENT_BYTES).map_err(|e| e.to_string())?;
    }
    use tauri::Emitter;
    // `hash_hex` may arrive with different casing than what `AttachmentSpec`'s
    // URL embeds (always lowercase, via `hex_encode`'s own `{b:02x}` format) --
    // normalize before emitting, or an uppercase-hex caller would store under
    // the correct hash but emit an event name the frontend isn't listening
    // for, a silent no-op rather than an error.
    let _ = app.emit(&events::attachment_ready_event_name(&hash_hex.to_lowercase()), ());
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decodes exactly 64 hex characters into a 32-byte hash. Uses `str::get`
/// rather than direct slicing so a non-ASCII input returns an error instead of
/// panicking on a char-boundary split.
fn decode_hash_hex(hash_hex: &str) -> Result<[u8; 32], String> {
    if hash_hex.len() != 64 {
        return Err(format!("hash_hex must be 64 hex characters, got {}", hash_hex.len()));
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        let pair = hash_hex.get(i * 2..i * 2 + 2).ok_or_else(|| "hash_hex is not ASCII hex".to_string())?;
        *byte = u8::from_str_radix(pair, 16).map_err(|e| format!("hash_hex is not valid hex: {e}"))?;
    }
    Ok(out)
}

/// The one space this app opens. Must stay in lockstep with `App.tsx`'s own
/// `DEFAULT_SPACE_ID` constant: the frontend opens exactly this space on
/// launch and offers no way to open another, and `run()` below pre-registers
/// exactly this space with `Transport` before dialing. A multi-space app
/// needs a real space list (and a matching "register every space before
/// connecting" story) rather than a wider constant here.
pub const DEFAULT_SPACE_ID: &str = "space-default";

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let data_dir = dirs_data_dir();
    let local_device = load_or_create_local_device_id(&data_dir);

    let builder = tauri::Builder::default().plugin(tauri_plugin_opener::init());
    let builder = attachment_protocol::register_attachment_protocol(builder);

    builder
        .setup(move |app| {
            // TODO: this generates a fresh random TransportIdentity on every
            // launch instead of persisting/reloading one the way
            // `load_or_create_local_device_id` does for `local_device`. A
            // stable identity is needed for real peers to keep dialing the
            // same endpoint address across restarts, but `TransportIdentity`
            // has no serialization support yet in `space-chat-transport`
            // (only `TransportIdentity::generate()` exists, no
            // `to_bytes`/`from_bytes`). Fixing this needs either adding that
            // to `space-chat-transport` (out of scope here) or deriving one
            // deterministically from `local_device`'s bytes (a design
            // decision for a later task, not this one).
            let identity = space_chat_transport::identity::TransportIdentity::generate();
            let transport_config = transport_config_from_env();
            let (network, events) =
                tauri::async_runtime::block_on(network::AppNetwork::bind(&identity, transport_config))
                    .expect("failed to bind AppNetwork");
            let app_state = state::AppState::new(&data_dir, local_device, network, events, app.handle().clone())
                .expect("failed to initialize AppState");
            // Register this app's one space with `Transport` BEFORE this
            // process can DIAL any peer. This ordering is load-bearing, not
            // stylistic: `Transport::add_space`'s own doc comment states
            // that the control-stream digest exchange runs exactly once per
            // connection and its remote-digest snapshot is frozen for that
            // connection's whole lifetime, so "a space added while a
            // connection already exists will not sync over that pre-
            // existing connection at all -- not eventually, not on retry,
            // never."
            //
            // Until this call existed, the ONLY `add_space` in the app came
            // from `segment_arc`, reached via the `open_conversation` command
            // -- i.e. from the frontend's `onMount`, strictly after
            // `announce_and_dial_from_env` had already dialed. A dialing peer
            // therefore negotiated every connection with an EMPTY space set
            // and no message ever synced. Caught by the golden-path E2E
            // scenario (the local send/render path is unaffected, which is
            // why every prior test passed).
            //
            // NOTE this only covers the DIAL side. `Transport::bind` (just
            // above) starts accepting inbound connections immediately, so
            // any peer that manages to dial THIS process during the window
            // between `bind` and this line (all of `AppState::new` --
            // directory creation, redb setup with several fsyncs, membership
            // load) would still hit the exact same gap on the accept side.
            // That window is unreachable today only because nothing can
            // currently dial this app at all: `TransportIdentity` is
            // regenerated every launch and never persisted or announced
            // outside a test harness (see the TODO above). The moment that
            // gap is closed, this accept-side window becomes live and would
            // need its own fix (e.g. registering the space before `bind`
            // even starts accepting, which needs restructuring since
            // `AppState`/its storage currently come after `bind`).
            tauri::async_runtime::block_on(app_state.segment_arc(DEFAULT_SPACE_ID));
            announce_and_dial_from_env(&app_state.network);
            // Must come BEFORE `app.manage(app_state)`: `subscribe_status()`
            // borrows `app_state.network`, and `manage` takes `app_state` by
            // value.
            spawn_connection_status_bridge(app.handle().clone(), app_state.network.subscribe_status());
            app.manage(app_state);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            greet,
            commands::open_conversation,
            commands::close_conversation,
            commands::resync_conversation,
            commands::send_message,
            commands::react,
            commands::delete_message,
            commands::fetch_older_page,
            commands::create_space,
            commands::generate_invite,
            commands::join_via_invite,
            seed_membership_for_testing,
            send_message_with_missing_attachment_for_testing,
            simulate_attachment_arrival_for_testing,
        ])
        .run(tauri::generate_context!())
        .expect("error while running space-chat-app");
}

/// Bridges `AppNetwork::subscribe_status`'s watch channel to a Tauri event so
/// the frontend can show a "reconnecting" affordance -- the app-shell spec's
/// Composition & Tauri IPC section asks for this, and Task 7 built
/// `subscribe_status()` for it, but nothing consumed it until now. Runs for
/// the app's lifetime; `AppState.network` never changes after startup in this
/// plan's scope, so there is nothing to re-subscribe to.
///
/// The payload is `format!("{status:?}")` -- i.e. the literal strings
/// `"Connected"` / `"Disconnected"`, which `App.tsx` renders verbatim.
///
/// Emits the CURRENT value first and only then awaits changes, so a listener
/// attached before this task starts sees a value without waiting for a
/// transition. A listener that attaches later (the webview's `onMount` races
/// this) can miss that first emit; the frontend's signal therefore defaults to
/// the same `Disconnected` initial value `watch::channel` is seeded with in
/// `network.rs`, so the two agree either way.
///
/// `tauri::async_runtime::spawn`, not bare `tokio::spawn` -- this is called
/// from inside `run()`'s non-async `.setup()` closure, where no ambient tokio
/// runtime is guaranteed (the same hazard `announce_and_dial_from_env` and
/// `state.rs`'s `spawn_network_event_loop` both document, caught empirically
/// there).
///
/// Generic over `R: tauri::Runtime` for the same reason `state.rs`'s
/// `spawn_network_event_loop` is: a bare `tauri::AppHandle` defaults to the
/// `Wry` runtime, which `tauri::test::mock_builder()` can't produce, so the
/// non-generic form would be untestable.
fn spawn_connection_status_bridge<R: tauri::Runtime>(
    app_handle: tauri::AppHandle<R>,
    mut status_rx: tokio::sync::watch::Receiver<network::ConnectionStatus>,
) {
    tauri::async_runtime::spawn(async move {
        use tauri::Emitter;
        loop {
            let status = *status_rx.borrow();
            let _ = app_handle.emit(events::connection_status_event_name(), format!("{status:?}"));
            if status_rx.changed().await.is_err() {
                break; // sender dropped -- AppNetwork (and the whole app) is shutting down
            }
        }
    });
}

fn dirs_data_dir() -> std::path::PathBuf {
    std::env::var("SPACECHAT_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from(".spacechat-data"))
}

/// Builds the `TransportConfig` for a single named relay. Split out from
/// `transport_config_from_env` so the (non-trivial, `iroh`-version-sensitive)
/// relay-map construction is unit-testable without mutating process-global
/// environment state.
///
/// There is no `RelayMap::from_url` constructor in the pinned `iroh` 1.2.0 --
/// `space-chat-transport/src/bin/test_peer.rs` hit and documented this same
/// gap. `RelayMap::from(RelayUrl)` exists but defaults the relay's QUIC
/// address-discovery port to `DEFAULT_RELAY_QUIC_PORT`, which is wrong for
/// `iroh::test_utils::run_relay_server()`'s relay (it binds its QUIC listener
/// to a random OS-assigned port). Passing `None` for the QUIC config sidesteps
/// that: it disables only the relay's own QUIC-assisted NAT-traversal probing
/// (a direct-connection *upgrade* helper), not relay-mediated connectivity,
/// which flows over the relay's ordinary HTTPS/WebSocket endpoint. This
/// harness only needs relay-mediated delivery, not hole-punching.
fn transport_config_for_relay(relay_url: iroh::RelayUrl) -> space_chat_transport::bootstrap::TransportConfig {
    let relay_map: iroh::RelayMap = iroh::RelayConfig::new(relay_url.clone(), None).into();
    space_chat_transport::bootstrap::TransportConfig { relay: Some((relay_map, relay_url)) }
}

/// `SPACECHAT_RELAY_URL` (set only by the E2E harness) points this process at
/// a single local test relay instead of the production `n0` relay/discovery
/// preset. Unset -- every real user launch -- yields `relay: None`, which is
/// exactly what `run()` used unconditionally before this existed.
fn transport_config_from_env() -> space_chat_transport::bootstrap::TransportConfig {
    match std::env::var("SPACECHAT_RELAY_URL") {
        Ok(url_str) => transport_config_for_relay(
            url_str.parse().expect("SPACECHAT_RELAY_URL must be a valid relay URL"),
        ),
        Err(_) => space_chat_transport::bootstrap::TransportConfig { relay: None },
    }
}

fn encode_endpoint_id_hex(id: &iroh::EndpointId) -> String {
    id.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_endpoint_id_hex(hex: &str) -> Option<iroh::EndpointId> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    iroh::EndpointId::from_bytes(&bytes).ok()
}

/// Reconstructs a dialable address from a peer's bare `EndpointId` plus the
/// relay URL every actor in a scenario already shares.
///
/// A bare `EndpointAddr::from(peer_id)` carries no relay/IP info at all, and
/// `TransportConfig { relay: Some(..) }` builds on `presets::Minimal` -- no
/// pkarr/DNS discovery -- so `dial` would have no path to reach the peer.
/// Attaching the known relay URL is the same thing `test_peer.rs` and
/// `bootstrap.rs`'s `addr_via_own_relay` helper both do, and it's why the
/// address file only needs to carry 64 hex characters rather than a
/// serialized `EndpointAddr`.
fn dial_addr_from_hex(hex: &str, relay_url: &iroh::RelayUrl) -> Option<iroh::EndpointAddr> {
    Some(iroh::EndpointAddr::new(decode_endpoint_id_hex(hex)?).with_relay_url(relay_url.clone()))
}

/// Publishes this process's endpoint id for other actors in an E2E scenario to
/// dial, and dials whichever already-running actors the scenario wants this
/// one connected to. Both halves are no-ops in a normal launch, where neither
/// `SPACECHAT_ENDPOINT_ADDR_FILE` nor `SPACECHAT_DIAL_ADDRS` is set.
///
/// The announcement is written to a sibling `.tmp` path and then renamed into
/// place, so a harness polling the target path never observes a
/// partially-written (or zero-length) file and mistake it for a complete
/// announcement -- the file's existence is the readiness signal.
fn announce_and_dial_from_env(network: &network::AppNetwork) {
    if let Ok(addr_file) = std::env::var("SPACECHAT_ENDPOINT_ADDR_FILE") {
        let hex = encode_endpoint_id_hex(&network.transport.endpoint_id());
        let path = std::path::PathBuf::from(&addr_file);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &hex).expect("failed to write SPACECHAT_ENDPOINT_ADDR_FILE");
        std::fs::rename(&tmp, &path).expect("failed to publish SPACECHAT_ENDPOINT_ADDR_FILE");
    }

    let (Ok(dial_files), Ok(relay_url_str)) =
        (std::env::var("SPACECHAT_DIAL_ADDRS"), std::env::var("SPACECHAT_RELAY_URL"))
    else {
        return;
    };
    let relay_url: iroh::RelayUrl =
        relay_url_str.parse().expect("SPACECHAT_RELAY_URL must be a valid relay URL");
    for path in dial_files.split(',').filter(|s| !s.is_empty()) {
        let hex = std::fs::read_to_string(path).expect("failed to read a SPACECHAT_DIAL_ADDRS target file");
        let peer_addr = dial_addr_from_hex(&hex, &relay_url)
            .unwrap_or_else(|| panic!("{path} should contain a 64-character endpoint id hex string"));
        let transport = network.transport.clone();
        // `tauri::async_runtime::spawn`, not `tokio::spawn`: this runs inside
        // `run()`'s non-async `.setup()` closure, where there is no ambient
        // tokio runtime (see `state.rs`'s `spawn_network_event_loop` for the
        // same hazard, caught there empirically).
        tauri::async_runtime::spawn(async move {
            let _ = transport.dial(peer_addr).await;
        });
    }
}

/// Derives a stable `DeviceId` from an actor's name, so every actor in a
/// multi-actor E2E scenario can independently compute every OTHER named
/// actor's device id without any out-of-band exchange -- which is what makes
/// `seed_membership_for_testing` need nothing but a list of names.
///
/// NOT cryptographically meaningful: same caveat as `rand_byte` below, this is
/// test-harness scaffolding under the existing placeholder membership model,
/// not identity material. Only reachable when `SPACECHAT_ACTOR_NAME` is set.
fn deterministic_device_id_for_actor(name: &str) -> space_chat_core::domain::DeviceId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut id = [0u8; 32];
    for (chunk_index, chunk) in id.chunks_mut(8).enumerate() {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        chunk_index.hash(&mut hasher);
        let hash = hasher.finish().to_le_bytes();
        chunk.copy_from_slice(&hash[..chunk.len()]);
    }
    space_chat_core::domain::DeviceId(id)
}

fn load_or_create_local_device_id(data_dir: &std::path::Path) -> space_chat_core::domain::DeviceId {
    if let Ok(actor_name) = std::env::var("SPACECHAT_ACTOR_NAME") {
        return deterministic_device_id_for_actor(&actor_name);
    }
    let path = data_dir.join("device_id");
    if let Ok(bytes) = std::fs::read(&path) {
        if let Ok(id) = <[u8; 32]>::try_from(bytes.as_slice()) {
            return space_chat_core::domain::DeviceId(id);
        }
    }
    let mut id = [0u8; 32];
    for byte in id.iter_mut() {
        *byte = rand_byte();
    }
    let _ = std::fs::create_dir_all(data_dir);
    let _ = std::fs::write(&path, id);
    space_chat_core::domain::DeviceId(id)
}

fn rand_byte() -> u8 {
    use std::time::{SystemTime, UNIX_EPOCH};
    // Not cryptographically secure -- fine for a locally-generated device
    // identifier in this placeholder membership model (see Task 3); replace
    // alongside the rest of `space-chat-openmls`'s eventual real credential
    // generation.
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
    (nanos ^ (nanos >> 8)) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greet_includes_the_given_name() {
        assert_eq!(greet("Alice"), "Hello, Alice! space-chat is running.");
    }

    /// The whole point of `deterministic_device_id_for_actor`: two separate
    /// processes that only know each other's *names* must land on the same
    /// `DeviceId`, or `seed_membership_for_testing` would seed each actor with
    /// a different idea of who the other is.
    #[test]
    fn deterministic_device_ids_are_stable_and_distinct_per_actor() {
        assert_eq!(
            deterministic_device_id_for_actor("alice"),
            deterministic_device_id_for_actor("alice"),
            "the same actor name must always derive the same device id"
        );
        assert_ne!(
            deterministic_device_id_for_actor("alice"),
            deterministic_device_id_for_actor("bob"),
            "different actor names must derive different device ids"
        );
        // Every chunk must be seeded independently -- a naive implementation
        // that hashed once and repeated the same 8 bytes would still pass the
        // two assertions above while being far weaker than it looks.
        let id = deterministic_device_id_for_actor("alice").0;
        assert_ne!(&id[0..8], &id[8..16], "each 8-byte chunk should be independently derived");
    }

    /// `announce_and_dial_from_env`'s two halves have to agree byte-for-byte:
    /// the announcing process encodes, the dialing process decodes.
    #[test]
    fn endpoint_id_hex_round_trips_through_the_address_file_format() {
        let id = space_chat_transport::identity::TransportIdentity::generate().endpoint_id();
        let hex = encode_endpoint_id_hex(&id);
        assert_eq!(hex.len(), 64, "an endpoint id is 32 bytes, so exactly 64 hex characters");
        assert_eq!(decode_endpoint_id_hex(&hex), Some(id));
        // Real files come back from `read_to_string` with whatever trailing
        // whitespace/newline the writer left; decoding must tolerate that.
        assert_eq!(decode_endpoint_id_hex(&format!("{hex}\n")), Some(id));
    }

    #[test]
    fn decode_endpoint_id_hex_rejects_partial_or_malformed_content() {
        assert_eq!(decode_endpoint_id_hex(""), None, "an empty/not-yet-written file must not decode");
        assert_eq!(decode_endpoint_id_hex(&"ab".repeat(20)), None, "a truncated write must not decode");
        assert_eq!(decode_endpoint_id_hex(&"zz".repeat(32)), None, "non-hex content must not decode");
    }

    /// Proves the dial address a peer reconstructs really does carry the relay
    /// URL -- the exact thing a bare `EndpointAddr::from(peer_id)` lacks, and
    /// without which `dial` has no path to the peer under `presets::Minimal`.
    #[test]
    fn dial_addr_from_hex_attaches_the_shared_relay_url() {
        let id = space_chat_transport::identity::TransportIdentity::generate().endpoint_id();
        let relay_url: iroh::RelayUrl = "https://relay.invalid.".parse().unwrap();
        let addr = dial_addr_from_hex(&encode_endpoint_id_hex(&id), &relay_url)
            .expect("a well-formed hex endpoint id should produce a dial address");

        assert_eq!(addr.id, id);
        assert!(
            addr.relay_urls().any(|url| *url == relay_url),
            "the reconstructed dial address must carry the shared relay url, got {addr:?}"
        );
    }

    /// The relay-map construction this crate had to adapt for `iroh` 1.2.0's
    /// missing `RelayMap::from_url`; asserts it yields a config the transport
    /// layer will actually treat as relay-pinned rather than production
    /// discovery.
    #[test]
    fn transport_config_for_relay_pins_the_given_relay() {
        let relay_url: iroh::RelayUrl = "https://relay.invalid.".parse().unwrap();
        let config = transport_config_for_relay(relay_url.clone());
        let (relay_map, configured_url) = config.relay.expect("a relay url must produce relay: Some(..)");
        assert_eq!(configured_url, relay_url);
        assert!(relay_map.contains(&relay_url), "the relay map should contain the configured relay");
    }

    /// The E2E harness relies on `SPACECHAT_ACTOR_NAME` overriding the normal
    /// random-generate-and-persist path entirely -- including when a
    /// `device_id` file already exists, which it will for any relaunched
    /// actor.
    #[test]
    fn spacechat_actor_name_overrides_the_persisted_device_id() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("device_id"), [7u8; 32]).unwrap();

        assert_eq!(
            load_or_create_local_device_id(dir.path()),
            space_chat_core::domain::DeviceId([7u8; 32]),
            "without the env var, the persisted id must still win"
        );

        std::env::set_var("SPACECHAT_ACTOR_NAME", "alice");
        let id = load_or_create_local_device_id(dir.path());
        std::env::remove_var("SPACECHAT_ACTOR_NAME");

        assert_eq!(id, deterministic_device_id_for_actor("alice"));
        assert_ne!(id, space_chat_core::domain::DeviceId([7u8; 32]));
    }

    /// Design decisions #3 and #4 together: the seeding hook refuses to run
    /// unless a scenario explicitly opted in, and when it does run it derives
    /// peers' device ids from their names alone -- which is the property that
    /// lets two separate actor processes agree on each other's identity with
    /// no ID exchange at all.
    ///
    /// Uses `SPACECHAT_TEST_HOOKS`, which no other test in this crate touches,
    /// so it does not race the env-var wiring test below.
    #[tokio::test]
    async fn seed_membership_is_gated_on_test_hooks_and_seeds_deterministic_peer_ids() {
        use crate::membership::SpaceMembership;
        use space_chat_transport::bootstrap::TransportConfig;
        use space_chat_transport::identity::TransportIdentity;

        let dir = tempfile::tempdir().unwrap();
        let identity = TransportIdentity::generate();
        let (network, events) =
            network::AppNetwork::bind(&identity, TransportConfig { relay: None }).await.unwrap();
        let app_handle = tauri::test::mock_builder()
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap()
            .handle()
            .clone();
        let local_device = deterministic_device_id_for_actor("alice");
        let state = state::AppState::new(dir.path(), local_device, network, events, app_handle).unwrap();

        std::env::remove_var("SPACECHAT_TEST_HOOKS");
        let refused = seed_membership_impl(&state, "space-1", &["bob".to_string()]);
        assert!(refused.is_err(), "the hook must refuse without SPACECHAT_TEST_HOOKS, got {refused:?}");
        assert!(
            state.membership.lock().unwrap().members("space-1").is_empty(),
            "a refused call must not have mutated membership"
        );

        std::env::set_var("SPACECHAT_TEST_HOOKS", "1");
        let seeded = seed_membership_impl(&state, "space-1", &["bob".to_string(), "carol".to_string()]);
        std::env::remove_var("SPACECHAT_TEST_HOOKS");
        seeded.expect("the hook should succeed once SPACECHAT_TEST_HOOKS is set");

        let members = state.membership.lock().unwrap().members("space-1");
        assert_eq!(members.len(), 3, "the local device plus both named peers");
        // The whole point: another actor process, knowing only the NAME
        // "bob", computes this exact same id.
        for name in ["alice", "bob", "carol"] {
            assert!(
                members.contains(&deterministic_device_id_for_actor(name)),
                "{name}'s name-derived device id should be a member"
            );
        }
        assert_eq!(state.membership.lock().unwrap().display_name(&deterministic_device_id_for_actor("bob")), "bob");
    }

    /// The round trip that matters for the attachment lazy-fetch scenario:
    /// `send_message_with_missing_attachment_for_testing` returns
    /// `hex_encode(MISSING_ATTACHMENT_HASH)`, and the value the test step hands
    /// back to `simulate_attachment_arrival_for_testing` must decode to the
    /// same 32 bytes -- otherwise the bytes would be saved under a hash the
    /// rendered `<img>`'s URL never asks about, and the scenario would hang on
    /// a placeholder that never resolves.
    #[test]
    fn the_missing_attachment_hash_round_trips_through_hex() {
        let encoded = hex_encode(&MISSING_ATTACHMENT_HASH);
        assert_eq!(encoded.len(), 64);
        assert_eq!(decode_hash_hex(&encoded).unwrap(), MISSING_ATTACHMENT_HASH);
        // The same hex the frontend's `spacechat://attachment/<hash>` URL
        // carries, which `parse_attachment_hash` must agree on.
        assert_eq!(
            crate::attachment_protocol::parse_attachment_hash(&format!("/{encoded}")),
            Some(MISSING_ATTACHMENT_HASH)
        );
    }

    #[test]
    fn decode_hash_hex_rejects_wrong_length_and_non_hex_input() {
        assert!(decode_hash_hex("").is_err());
        assert!(decode_hash_hex(&"42".repeat(31)).is_err(), "too short");
        assert!(decode_hash_hex(&"42".repeat(33)).is_err(), "too long");
        assert!(decode_hash_hex(&"zz".repeat(32)).is_err(), "not hex");
        // 64 *bytes* but fewer than 64 chars would slice mid-char without
        // `str::get`; this must error, not panic.
        assert!(decode_hash_hex(&"é".repeat(32)).is_err());
    }

    /// End-to-end proof of everything `run()`'s `.setup()` closure now does
    /// with the harness's environment variables, minus Tauri itself: one
    /// `AppNetwork` publishes its endpoint id to `SPACECHAT_ENDPOINT_ADDR_FILE`,
    /// a second is bound from `transport_config_from_env()` alone and dials
    /// the first purely from `SPACECHAT_DIAL_ADDRS`, and the dialer really
    /// reaches `Connected` over a real local relay.
    ///
    /// This is the one test that can catch a wiring mistake the pure
    /// unit tests above cannot -- e.g. a relay URL that parses but produces a
    /// config the endpoint can't actually use, or a dial address missing the
    /// relay hint and therefore silently unreachable.
    ///
    /// Deliberately the ONLY test in this crate that mutates these particular
    /// environment variables: env is process-global and `cargo test` runs
    /// tests concurrently in one process, so splitting this into several
    /// env-mutating tests would make them race each other.
    #[tokio::test]
    async fn env_var_wiring_announces_an_endpoint_and_dials_it_over_a_real_relay() {
        use crate::network::ConnectionStatus;
        use space_chat_transport::identity::TransportIdentity;
        use std::time::Duration;

        let (_relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let listener_file = dir.path().join("listener_endpoint");
        let dialer_file = dir.path().join("dialer_endpoint");

        std::env::set_var("SPACECHAT_RELAY_URL", relay_url.to_string());

        // The listener: bound via the env-driven config, then announcing
        // itself exactly as a harness actor with no dial targets would.
        let listener_identity = TransportIdentity::generate();
        let (listener, _listener_events) =
            network::AppNetwork::bind(&listener_identity, transport_config_from_env()).await.unwrap();
        std::env::set_var("SPACECHAT_ENDPOINT_ADDR_FILE", &listener_file);
        std::env::remove_var("SPACECHAT_DIAL_ADDRS");
        announce_and_dial_from_env(&listener);

        assert_eq!(
            decode_endpoint_id_hex(&std::fs::read_to_string(&listener_file).unwrap()),
            Some(listener.transport.endpoint_id()),
            "the announced file must decode back to the announcing endpoint's real id"
        );

        // The dialer: knows nothing about the listener except the path to its
        // announcement file.
        let dialer_identity = TransportIdentity::generate();
        let (dialer, _dialer_events) =
            network::AppNetwork::bind(&dialer_identity, transport_config_from_env()).await.unwrap();
        let mut dialer_status = dialer.subscribe_status();
        assert_eq!(*dialer_status.borrow(), ConnectionStatus::Disconnected);

        std::env::set_var("SPACECHAT_ENDPOINT_ADDR_FILE", &dialer_file);
        std::env::set_var("SPACECHAT_DIAL_ADDRS", listener_file.to_string_lossy().to_string());
        announce_and_dial_from_env(&dialer);

        let connected = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if *dialer_status.borrow() == ConnectionStatus::Connected {
                    return;
                }
                dialer_status.changed().await.unwrap();
            }
        })
        .await;

        std::env::remove_var("SPACECHAT_RELAY_URL");
        std::env::remove_var("SPACECHAT_ENDPOINT_ADDR_FILE");
        std::env::remove_var("SPACECHAT_DIAL_ADDRS");

        connected.expect("the dialer should connect to the endpoint it read from SPACECHAT_DIAL_ADDRS");
        assert!(dialer_file.exists(), "the dialer should also have announced its own endpoint id");
    }

    /// The behaviour that actually matters for the "reconnecting" affordance,
    /// which `events::tests`' name-stability test cannot reach: the bridge
    /// emits the watch channel's CURRENT value immediately (so a listener
    /// that attached first isn't left blank until the next transition) and
    /// then one event per subsequent change, with the `Debug`-formatted
    /// payload `App.tsx` renders verbatim.
    ///
    /// Drives a plain `watch::channel` rather than a real `AppNetwork` on
    /// purpose -- `network.rs` already has its own test proving
    /// `subscribe_status()` reaches `Connected` after a real dial over a real
    /// relay; what's unproven is this function's half of the seam.
    #[tokio::test]
    async fn connection_status_bridge_emits_the_current_status_then_every_change() {
        use std::time::Duration;
        use tauri::Listener;

        let app = tauri::test::mock_builder()
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .expect("failed to build mock app");
        let (status_tx, status_rx) = tokio::sync::watch::channel(network::ConnectionStatus::Disconnected);
        let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        app.handle().listen(events::connection_status_event_name(), move |event| {
            let _ = seen_tx.send(event.payload().to_string());
        });

        spawn_connection_status_bridge(app.handle().clone(), status_rx);

        async fn next(rx: &mut tokio::sync::mpsc::UnboundedReceiver<String>) -> String {
            tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("the bridge should have emitted a connection-status event")
                .expect("the emit channel should still be open")
        }

        // Payloads are JSON-encoded by `emit`, so a Rust `String` arrives
        // quoted -- this is exactly what the frontend's `event.payload` is.
        assert_eq!(next(&mut seen_rx).await, "\"Disconnected\"", "the initial value must be emitted up front");
        status_tx.send(network::ConnectionStatus::Connected).unwrap();
        assert_eq!(next(&mut seen_rx).await, "\"Connected\"");
        status_tx.send(network::ConnectionStatus::Disconnected).unwrap();
        assert_eq!(next(&mut seen_rx).await, "\"Disconnected\"");
    }

    /// IPC-level test: dispatches `greet` through Tauri's real invoke pipeline
    /// (ACL/permission check + command routing), not just the bare Rust
    /// function. Uses `tauri::test::mock_builder()` with a `MockRuntime` and
    /// `tauri::test::get_ipc_response`, mirroring tauri's own doctest at
    /// `tauri::test::get_ipc_response` (src/test/mod.rs).
    #[test]
    fn greet_command_is_reachable_via_ipc() {
        let app = tauri::test::mock_builder()
            .invoke_handler(tauri::generate_handler![greet])
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .expect("failed to build mock app");
        let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
            .build()
            .expect("failed to build mock webview");

        let res = tauri::test::get_ipc_response(
            &webview,
            tauri::webview::InvokeRequest {
                cmd: "greet".into(),
                callback: tauri::ipc::CallbackFn(0),
                error: tauri::ipc::CallbackFn(1),
                url: if cfg!(any(windows, target_os = "android")) {
                    "http://tauri.localhost"
                } else {
                    "tauri://localhost"
                }
                .parse()
                .unwrap(),
                body: tauri::ipc::InvokeBody::Json(serde_json::json!({ "name": "Bob" })),
                headers: Default::default(),
                invoke_key: tauri::test::INVOKE_KEY.to_string(),
            },
        );

        assert_eq!(
            res.expect("greet command should be reachable over IPC")
                .deserialize::<String>()
                .unwrap(),
            "Hello, Bob! space-chat is running."
        );
    }
}

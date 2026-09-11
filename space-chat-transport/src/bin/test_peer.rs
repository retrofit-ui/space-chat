//! A small scripted process, driven entirely by CLI arguments, used by
//! `tests/two_process_convergence.rs` to prove that convergence works
//! across genuinely separate OS processes (spawned via
//! `std::process::Command`), not just separate in-process `tokio::spawn`
//! tasks sharing one process's memory (which is all every other test in
//! this crate, including the multi-hop test, actually exercises).
//!
//! Usage: `test_peer <relay-url> <role> [<peer-endpoint-hex> ...] [roam]`
//!
//! Roles:
//!   - "alice": appends one message, notifies, then (if "roam" trails the
//!     argument list) sleeps, simulates a network change, and sends a
//!     second message, then sleeps a bit more to give sync time to land
//!     before exiting 0.
//!   - "bob"/"carol": dials every given peer, then waits until its segment
//!     shows the expected message count, prints "CONVERGED <n>", and exits
//!     0 -- or prints "TIMEOUT" and exits 1.
//!
//! Any argument literally equal to "roam" is stripped from the peer-hex
//! list and instead sets a `roam` flag, regardless of which role reads it
//! or where in the argument list it appears -- this lets the same "roam"
//! flag both change alice's behavior (send a second message after a
//! simulated network change) and the receiving role's expected message
//! count (1 vs 2), without a separate CLI switch for each.
use space_chat_core::domain::{DeviceId, Message};
use space_chat_core::segment::Segment;
use space_chat_transport::bootstrap::TransportConfig;
use space_chat_transport::identity::TransportIdentity;
use space_chat_transport::transport::Transport;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(s: &str) -> [u8; 32] {
    let bytes: Vec<u8> = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex from a peer's own ENDPOINT line"))
        .collect();
    bytes.try_into().expect("endpoint id hex should decode to exactly 32 bytes")
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let relay_url: iroh::RelayUrl = args[1].parse().expect("valid relay url");
    let role = args[2].clone();
    let peer_hexes: Vec<String> = args[3..].iter().filter(|a| *a != "roam").cloned().collect();
    let roam = args[3..].iter().any(|a| a == "roam");

    // Adaptation vs. the brief: there is no `iroh::RelayMap::from_url`
    // constructor in the pinned `iroh` 1.2.0 (verified against
    // `iroh-relay-1.2.0/src/relay_map.rs`) -- only `RelayMap::from(RelayUrl)`
    // / `RelayMap::try_from_iter`, both of which default the relay's QUIC
    // address-discovery port to `DEFAULT_RELAY_QUIC_PORT`. That default is
    // wrong for `iroh::test_utils::run_relay_server()`'s relay, which binds
    // its QUIC listener to a random OS-assigned port
    // (`QuicConfig::new((Ipv4Addr::LOCALHOST, 0))` -- see
    // `iroh-1.2.0/src/test_utils.rs`), and `RelayQuicConfig` itself isn't
    // even re-exported from the `iroh` crate root for this process to name
    // if it wanted to guess the right port. Passing `quic: None` here
    // sidesteps the mismatch entirely: it only disables the relay's own
    // QUIC-assisted NAT-traversal probing (a direct-connection upgrade
    // helper), not relay-mediated connectivity itself, which flows over the
    // relay's ordinary HTTPS/WebSocket endpoint independent of `quic`. Since
    // this whole test's point is proving convergence *over a relay*, not
    // proving direct hole-punching, that's an acceptable (and arguably more
    // deterministic) trade.
    let relay_config = iroh::RelayConfig::new(relay_url.clone(), None);
    let relay_map: iroh::RelayMap = relay_config.into();

    let identity = TransportIdentity::generate();
    let (transport, mut events) = Transport::bind(&identity, TransportConfig { relay: Some((relay_map, relay_url.clone())) })
        .await
        .expect("test_peer should bind its endpoint");

    println!("ENDPOINT {}", hex_encode(transport.endpoint_id().as_bytes().as_slice()));
    std::io::stdout().flush().unwrap();

    let segment = Arc::new(Mutex::new(Segment::new("space-1", 0)));
    transport.add_space("space-1", 0, segment.clone()).await;

    // Adaptation vs. the brief: `iroh::EndpointAddr::from(peer_id)` (the
    // brief's literal example) builds a BARE address with no relay/IP
    // info attached. Every peer in this test binds under
    // `TransportConfig { relay: Some(..) }`, which (per `bootstrap.rs`)
    // builds on `presets::Minimal` -- no pkarr/DNS discovery at all -- so a
    // bare `EndpointAddr` gives `dial` no path to actually reach the peer.
    // Every process here already knows the shared relay URL (it's argv[1]),
    // so attaching it directly (the same pattern `bootstrap.rs`'s own test
    // and `join_via_invite`'s `addr_via_own_relay` helper both use) is both
    // correct and simpler than this crate's own relay-discovery-waiting
    // workaround, which exists only for when the caller does *not* already
    // know the relay URL.
    for hex in &peer_hexes {
        let peer_id = iroh::EndpointId::from_bytes(&hex_decode(hex)).expect("valid endpoint id");
        let peer_addr = iroh::EndpointAddr::new(peer_id).with_relay_url(relay_url.clone());
        transport.dial(peer_addr).await.expect("test_peer should be able to dial its configured peer");
    }

    // Drain events in the background so the mpsc channel never backs up;
    // this test only cares about message_count() converging, not about
    // asserting on individual events (Task 7/9's inline tests already do
    // that at the unit level).
    tokio::spawn(async move { while events.recv().await.is_some() {} });

    match role.as_str() {
        "alice" => {
            segment.lock().await.append_message(&Message {
                sender: DeviceId([1u8; 32]),
                content: "before roam".to_string(),
                attachments: vec![],
            });
            transport.notify_local_change("space-1").await;

            if roam {
                tokio::time::sleep(Duration::from_secs(2)).await;
                transport.simulate_network_change().await;
                segment.lock().await.append_message(&Message {
                    sender: DeviceId([1u8; 32]),
                    content: "after roam".to_string(),
                    attachments: vec![],
                });
                transport.notify_local_change("space-1").await;
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
            println!("DONE");
            std::io::stdout().flush().unwrap();
        }
        "bob" | "carol" => {
            let expected = if roam { 2 } else { 1 };
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                if segment.lock().await.message_count() >= expected {
                    println!("CONVERGED {expected}");
                    std::io::stdout().flush().unwrap();
                    return;
                }
                if tokio::time::Instant::now() > deadline {
                    println!("TIMEOUT");
                    std::io::stdout().flush().unwrap();
                    std::process::exit(1);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        other => panic!("unknown role: {other}"),
    }
}

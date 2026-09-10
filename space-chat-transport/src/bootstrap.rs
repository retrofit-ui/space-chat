use crate::error::TransportError;
use crate::identity::TransportIdentity;

/// The single ALPN this crate's connections negotiate. space-chat has one
/// wire protocol (this crate's own), not several, so one fixed ALPN
/// suffices — no `iroh::protocol::Router`-style multi-protocol dispatch is
/// needed on top of it.
pub const ALPN: &[u8] = b"space-chat/1";

/// `relay: None` uses `iroh`'s production preset (n0's public relay
/// network plus its pkarr/DNS discovery services, per this plan's Global
/// Constraints).
///
/// `relay: Some((map, url))` is **not** a general "bring your own relay,
/// keep everything else" option — it's specifically for local test relays
/// started via `iroh::test_utils::run_relay_server()`. Every test in this
/// plan supplies that function's `(RelayMap, RelayUrl)` here so tests never
/// touch production infrastructure. Setting it does two things: it swaps
/// in the given relay in place of n0's, and it also disables production
/// discovery entirely (no pkarr publish/resolve, no DNS lookup) by
/// switching the endpoint's base preset from `N0` to `Minimal`. A real
/// self-hosted relay meant to sit alongside full discovery is not what
/// this option is for.
pub struct TransportConfig {
    pub relay: Option<(iroh::RelayMap, iroh::RelayUrl)>,
}

/// Binds a real `iroh::Endpoint` under `identity`'s keypair, ready to
/// `connect`/`accept` on `ALPN`. `config.relay: None` uses the full
/// production preset; `Some(..)` is for local test relays only — see
/// `TransportConfig`'s doc comment.
//
// Adaptations vs. the plan's example (verified against the vendored `iroh`
// 1.2.0 source, since the plan's own Global Constraints flagged these as
// drafted from memory):
// - The preset module lives at `iroh::endpoint::presets`, not
//   `iroh::presets` — the latter doesn't exist at the crate root.
// - `RelayMode::Custom(RelayMap)` matches the plan's guess exactly.
// - `EndpointAddr`'s builder methods are `EndpointAddr::new(id)` plus
//   `.with_relay_url(url)` / `.with_ip_addr(addr)` / `.with_addrs(..)` to
//   attach addresses to a bare `EndpointId`. `Endpoint::addr()` is a
//   shortcut that returns the endpoint's current best-known `EndpointAddr`
//   directly, but this crate's own test below builds one by hand instead
//   (see that test's comment) rather than using `addr()`.
// - Not flagged in the plan, but required for the local relay to work at
//   all: `iroh::test_utils::run_relay_server()` serves its relay/QUIC
//   endpoints over a self-signed TLS certificate. Trusting it requires
//   `.ca_tls_config(CaTlsConfig::insecure_skip_verify())` on the builder —
//   confirmed by iroh's own relay-backed endpoint tests, which all set this
//   whenever they hand the builder a custom `RelayMap`. Without it, the
//   endpoint can't complete TLS with the test relay and any path that
//   depends on relay signaling silently fails.
//   `CaTlsConfig::insecure_skip_verify` itself only exists when `iroh`'s
//   own `test-utils` feature is on (it's a deliberately test-only escape
//   hatch), which this crate forwards as its own `test-utils` feature and
//   enables on itself as a dev-dependency — so the call is behind
//   `#[cfg(any(test, feature = "test-utils"))]` below, not plain
//   `#[cfg(test)]`: `cfg(test)` is only true while compiling this crate's
//   *own* `src/`-based unit tests, but integration tests under `tests/`
//   (and any other crate's tests that exercise this one) compile this
//   crate as an ordinary dependency with `cfg(test)` false — the
//   `test-utils` feature is what actually turns this on for those. That
//   also happens to be the right behavior, not just a compile-time
//   workaround: a real custom relay (as opposed to this plan's local test
//   relay) would have a proper certificate and should go through normal CA
//   verification, so skipping it should never happen outside tests.
// - Bigger deviation, found empirically: the plan's example builds on
//   `presets::N0` unconditionally and only swaps `relay_mode` when a custom
//   relay is given. `presets::N0` unconditionally also wires up
//   `PkarrPublisher`/`DnsAddressLookup` pointed at n0's *production* DNS
//   infrastructure — swapping only the relay leaves those production
//   network calls active, which contradicts this module's own doc
//   ("optionally pointed at a local test relay instead of production
//   relay/discovery defaults") and, in a network-restricted environment,
//   made the two-endpoint test above hang until QUIC's idle timeout (~45s)
//   before failing. So `relay: Some(..)` builds on `presets::Minimal`
//   instead (crypto provider only, no discovery, no relay) and adds just
//   the custom relay + CA override; `relay: None` keeps the full `N0`
//   preset for real production use.
pub async fn bind_endpoint(
    identity: &TransportIdentity,
    config: TransportConfig,
) -> Result<iroh::Endpoint, TransportError> {
    let builder = match config.relay {
        Some((relay_map, _relay_url)) => {
            let builder = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                .relay_mode(iroh::RelayMode::Custom(relay_map));
            #[cfg(any(test, feature = "test-utils"))]
            let builder = builder.ca_tls_config(iroh::tls::CaTlsConfig::insecure_skip_verify());
            builder
        }
        None => iroh::Endpoint::builder(iroh::endpoint::presets::N0),
    };

    builder
        .secret_key(identity.secret_key())
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .map_err(|e| TransportError::Io(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::TransportIdentity;

    #[tokio::test]
    async fn two_endpoints_over_a_local_relay_can_connect_and_exchange_bytes() {
        let (relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server()
            .await
            .expect("local test relay should start");

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

        let bob_id = bob.id();
        // Built from the relay URL directly, rather than `bob.addr()`: right
        // after `bind_endpoint` returns, `bob`'s `EndpointAddr` may still be
        // empty of relay info (it only reflects the relay once the endpoint
        // has gone "online"), and separately, on multi-homed hosts
        // `bob.addr()` also carries every local interface iroh could see
        // (Docker bridges, VPN interfaces, etc.), which are meaningless
        // outside this host and only invite direct-connection attempts this
        // test doesn't need. Handing alice just the relay URL is enough:
        // relay signaling carries whatever direct-address discovery iroh
        // wants to do on top.
        let bob_addr = iroh::EndpointAddr::new(bob_id).with_relay_url(relay_url);
        // Captured up front (same reason as `bob_id`/`bob_addr` above):
        // `alice` is used both inside `bob_task`'s `async move` block and
        // after it via `alice.connect(..)`, and `async move` moves every
        // captured variable regardless of how it's used inside — so
        // `alice` itself can't be referenced from both places.
        let alice_id = alice.id();

        // Spawn bob's accept loop before alice dials, so the connection has
        // somewhere to land.
        let bob_task = tokio::spawn(async move {
            let incoming = bob.accept().await.expect("bob should see an incoming connection");
            let conn = incoming.await.expect("incoming connection should complete the handshake");
            // A normal (non-0-RTT) `Connection`'s `remote_id()` returns the
            // `EndpointId` directly, not a `Result` — the fallible variant
            // in `iroh` 1.2.0 belongs to the 0-RTT connection states, which
            // this test doesn't use.
            assert_eq!(conn.remote_id(), alice_id);
            let (mut send, mut recv) = conn.accept_bi().await.expect("bob should accept alice's stream");
            let mut buf = [0u8; 5];
            tokio::io::AsyncReadExt::read_exact(&mut recv, &mut buf).await.unwrap();
            assert_eq!(&buf, b"hello");
            tokio::io::AsyncWriteExt::write_all(&mut send, b"world").await.unwrap();
            // `bob`'s task would otherwise return right here, which drops
            // `bob` (the `Endpoint`) before "world" is necessarily off the
            // wire. `iroh::Endpoint`'s `Drop` treats a not-yet-`close()`d
            // endpoint as an abrupt, ungraceful abort of every open
            // connection — confirmed by running this test and observing
            // `iroh::socket`'s "Endpoint dropped without calling
            // `Endpoint::close`. Aborting ungracefully." error log, which
            // discarded the just-written "world" bytes before alice's read
            // below ever saw them, hanging her `read_exact` until the
            // connection's idle timeout. Waiting here for alice to close
            // the connection (which she only does after reading "world"
            // successfully, below) is what actually guarantees delivery
            // before bob tears anything down; calling `bob.close()`
            // immediately after the write (tried first) was too eager —
            // it started closing before the write had necessarily reached
            // alice, and produced the same lost-data symptom via a
            // `ConnectionLost(ApplicationClosed(..))` error instead.
            conn.closed().await;
            bob.close().await;
        });

        let conn = alice.connect(bob_addr, ALPN).await.expect("alice should connect to bob");
        assert_eq!(conn.remote_id(), bob_id);
        let (mut send, mut recv) = conn.open_bi().await.expect("alice should open a stream");
        tokio::io::AsyncWriteExt::write_all(&mut send, b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        tokio::io::AsyncReadExt::read_exact(&mut recv, &mut buf).await.unwrap();
        assert_eq!(&buf, b"world");
        // Signals bob's `conn.closed().await` above, so his task can
        // finish and close its endpoint only after this point.
        conn.close(0u32.into(), b"");

        bob_task.await.unwrap();
        alice.close().await;
    }
}

# space-chat: transport & discovery design

Sub-project 4 of 5 in the space-chat initiative. Covers how two devices establish a connection and exchange the bytes defined by [the protocol & sync design](2026-09-03-protocol-and-sync-design.md). Builds on that spec's message categories and does not redefine them.

## Goals

- Direct device-to-device connections where possible ("Tailscale-style"), without requiring either device to have a stable public IP or manual port forwarding.
- A fallback path when direct connection isn't possible, without that fallback being able to read message content.
- Works across a phone switching networks mid-conversation, not just stationary desktops.
- No new server dependency for the common case of "two people who already know each other want to chat."

## Foundation: `iroh`

Rather than hand-rolling NAT traversal, hole-punching, and relay-selection — a genuinely hard, easy-to-get-subtly-wrong networking problem, the same category of risk that justified reusing Automerge and OpenMLS instead of building our own — space-chat builds transport on **`iroh`** (n0-computer), which shipped a stable 1.0 in June 2026:

- Devices dial each other by public key ("endpoint ID") rather than IP address.
- QUIC-based hole-punching succeeds directly roughly 9 times out of 10 (per n0's published numbers); the remainder falls back to relay servers.
- Relays are stateless and forward only encrypted packets addressed to a specific endpoint ID — they cannot read traffic. This is defense-in-depth here, not load-bearing: MLS already encrypts content above the transport layer regardless of relay behavior.
- Chosen over `rust-libp2p` (more general-purpose, DHT-oriented, ~70% direct hole-punch success rate) because our need is closer to "dial this specific known peer" than "discover and route through an open swarm" — the narrower tool fits better.
- Chosen over hand-assembling WireGuard (`boringtun`) + a separate NAT-traversal/relay layer because QUIC already provides the encrypted, multiplexed tunnel that combination would have required building.

**v1 relay policy: n0's public relay network**, not self-hosted. Zero infrastructure to run for the fallback path; the tradeoff is a third-party dependency for the ~10% of connections that need it. The source issue's own "n-n-1 peer-server-at-a-price" idea is a real fit for a *self-hosted* relay later — deliberately deferred, not designed away.

## Identity

**Endpoint identity is a separate keypair from the MLS device signing key**, despite both representing "this device." Reusing one keypair across two protocols (QUIC/TLS identity vs. MLS signatures) is poor crypto hygiene — the two protocols carry different assumptions about how a key is used, and an issue discovered in one context shouldn't be able to compromise the other. Both keys are managed together at the storage layer as one device identity, but remain cryptographically independent.

## Pairing / discovery

There is no directory or username-lookup service in v1. A device wanting to invite someone generates a **link or QR code** encoding its iroh endpoint ID and enough context to request joining a specific space. The recipient opens it through whatever out-of-band channel already exists between them (text, email, in person) — the same first-contact model Signal/WhatsApp device linking and most p2p tools use.

**Invite-based joins still route through the space's elected sequencer**, from the protocol spec, rather than using MLS's external-commit self-join mechanism to bypass it. An external commit is still a Commit; letting invite joins skip the sequencer would reopen the concurrent-commit-forking problem that electing a sequencer exists to avoid, growing a second inconsistent membership-change path instead of keeping the one already-documented liveness limitation.

## Wire integration: stream mapping

**Streams are per-`(space_id, category)`, not a fixed five per connection.** Two devices are frequently members of more than one shared space, and the protocol spec's envelope tags `(space_id, category)` for exactly this reason — a fixed five streams total would have no way to disambiguate space A's gossip from space B's gossip on the same stream. QUIC streams are cheap to open relative to connections, so this scales fine even across many shared spaces; a per-space stream set is opened lazily as spaces become active between two peers and closed when idle, rather than every shared space's streams staying open regardless of activity.

**Bootstrapping order**: the per-space digest exchange that kicks off the protocol spec's sync flow (step 2) happens on one connection-level control stream, opened first, before any per-space stream sets exist — it's how the two peers agree *which* shared spaces are even active before opening streams for them. Only after that negotiation do the per-`(space_id, category)` streams get opened for whichever spaces are found to have activity worth syncing.

Within a given space's stream set, isolation still works the same way as previously described: one long-lived QUIC stream per protocol message category (MLS control, Automerge sync, gossip, ephemeral, attachment transfer) gives real isolation at the delivery-ordering level — packet loss on one stream doesn't stall already-arrived data on another.

**This alone does not guarantee attachments won't degrade messaging** — all streams, across all spaces, share one congestion-controlled connection, so a large transfer can still consume available bandwidth and slow other streams' throughput even without blocking their ordering. **Stream prioritization** (control/sync/gossip ranked above bulk attachment transfer, consistently across every active space) is what actually delivers the protocol spec's "attachments never head-of-line-block message sync" goal. Worth being explicit that multiplexing and prioritization are two different mechanisms addressing two different problems (ordering vs. bandwidth contention), not one mechanism doing both.

## Resilience properties

- **Roaming**: QUIC identifies connections by connection ID rather than IP:port tuple, so a device switching networks (wifi → cellular) doesn't require tearing down and re-establishing the connection. This falls out of building on QUIC rather than something built separately.
- **Multi-hop convergence for messages (emergent, not designed machinery) — but not for attachment bytes.** Because Automerge sync/gossip operate per-space over whatever connections currently exist, a device with no direct-or-relayed path to another member can still receive that member's *messages* through any third device in the same space connected to both — the same way `git fetch` doesn't care which remote actually authored a commit. This requires no new mechanism for messages. **Attachment transfer does not get this property in v1**: it is strictly direct-endpoint (the peer requesting bytes ↔ a peer that actually has them), with no swarm-style relaying through an intermediate peer that happens to have already fetched the blob. Building that would need its own "who has this blob" discovery/announcement layer across hops — real, BitTorrent-shaped complexity deliberately not taken on now. Consequence worth being explicit about: a device can be fully reachable for *messages* (via a hop) while still unable to fetch a specific *attachment* if it has no direct-or-relay path to a peer that holds those particular bytes — message reachability and attachment reachability are not the same set. See the storage spec's GC section, which depends on this distinction.
- **Chunked, resumable attachment transfer**: a dropped connection mid-transfer means re-requesting missing chunks on reconnect, not restarting, consistent with attachments already being content-hash-addressed and chunked.

## Error handling

- **Backgrounded/killed mobile app → no live connection at all, by design, not a bug.** iroh connections don't survive the OS suspending or killing the app process. Whether and how this gets mitigated per platform (it does, partially, on Android; it structurally can't on iOS without violating the project's server-free decision) is covered in the app-shell spec's "Background delivery (mobile)" section — this transport spec doesn't attempt to solve it, since the fix (or lack of one) lives at the platform/app-shell layer, not the connection layer.
- Direct hole-punch and relay both fail → falls back to whatever multi-hop path exists through other space members, or waits for connectivity; treated identically to any other offline period via reconcile-on-reconnect. Not a new failure class.
- Sequencer unreachable over transport → the documented liveness limitation from the protocol spec (membership changes stall, messaging continues). Transport doesn't introduce a new failure mode here, only the trigger condition for an existing, already-accepted one.

## Testing

Uses the shared multi-actor `cucumber-rs`+`fantoccini` harness defined in the app-shell spec's Testing section, against `iroh`'s local test relay (`iroh::test_utils::run_relay_server()`) rather than production relay infrastructure:

```gherkin
Feature: Message delivery survives connectivity failure modes

  Scenario: Content still converges with no direct path, via a third member
    Given Alice, Bob, and Carol are members of a space
    And Alice and Carol have no direct or relay path to each other
    And Alice and Bob, and Bob and Carol, are each connected
    When Alice sends the message "hello"
    Then Carol's conversation view shows "hello" within 10 seconds

  Scenario: A network interface change mid-conversation doesn't drop the conversation
    Given Alice and Bob are connected and Alice sends "before roam"
    When Alice's device switches network interfaces
    Then Alice's conversation view still shows "before roam"
    When Alice sends "after roam"
    Then Bob's conversation view shows "after roam" within 2 seconds
```

- Force hole-punch failure and verify clean fallback to relay — same harness, asserting message delivery still succeeds rather than inspecting connection internals directly.
- Attachment-specific reachability (the narrower, direct-endpoint-only bound from the Resilience properties section above) gets its own scenario distinct from the message-convergence one, since the two have different reachability guarantees and conflating them in one scenario would hide that distinction.

## Open questions carried forward

- Self-hosted relay ("peer server at a price") remains a real future direction, explicitly deferred rather than designed now.
- Stream prioritization tuning (exact priority weights between control/sync/gossip streams) is an implementation-tuning parameter, not a design fork.
- **Networks that block UDP outright (some corporate/hotel/airport networks permit only TCP 80/443) will fail to connect at all**, since iroh is QUIC/UDP-based with no TCP fallback. This surfaced during stress-testing and is documented here as a known limitation rather than designed around — it's an environmental constraint outside this project's control today, not a gap in the transport design itself. A TCP-based fallback transport is the only real fix and would be a genuine future direction, not something to build speculatively now.

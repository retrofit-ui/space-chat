//! Step definitions for the multi-actor E2E harness.
//!
//! Every step here drives a real, rendered `space-chat-app` webview through
//! WebDriver -- there is no in-process shortcut into an actor's backend. The
//! one exception is deliberate and documented on `seed_membership` below.
use crate::world::SpaceChatWorld;
use cucumber::{given, then, when};
use fantoccini::{Client, Locator};
use space_chat_app_lib::DEFAULT_SPACE_ID;
use std::time::Duration;

/// CSS selector for the composer input `App.tsx` renders unconditionally
/// (before a conversation is even loaded -- see `wait_for_app_ready`'s doc
/// comment for why that makes it the wrong readiness gate).
const MESSAGE_INPUT: &str = "[data-testid=message-input]";
const SEND_BUTTON: &str = "[data-testid=send-button]";

/// `AttachmentImage.tsx`'s two mutually-exclusive states.
const ATTACHMENT_PLACEHOLDER: &str = "[data-testid=attachment-placeholder]";
const ATTACHMENT_LOADED: &str = "[data-testid=attachment-loaded]";

/// Brings `name` online: a real relay-connected `space-chat-app` process in
/// its own data directory, driven through its own WebDriver session.
#[given(regex = r"^(\w+) is a device in the space, online$")]
async fn given_actor_online(world: &mut SpaceChatWorld, name: String) {
    world.spawn_actor(&name, &[]).await;
}

/// Same, but also states the topology: `name` dials `target` at startup.
/// Actors bind under a relay-pinned `presets::Minimal` config with no
/// discovery, so somebody has to dial explicitly -- there is no ambient
/// peer-finding.
#[given(regex = r"^(\w+) is a device in the space, online and connected to (\w+)$")]
async fn given_actor_online_connected_to(world: &mut SpaceChatWorld, name: String, target: String) {
    world.spawn_actor(&name, &[target.as_str()]).await;
}

/// Seeds each named actor's LOCAL membership store with the other as a member
/// of `DEFAULT_SPACE_ID`, via the real `seed_membership_for_testing` Tauri
/// command (Task 17), invoked through WebDriver since this harness has no
/// other channel into a running actor's backend state.
///
/// Membership propagation over the real network is explicitly out of scope for
/// this whole plan (Task 12's documented limitation) -- this is the harness's
/// substitute, seeding each side directly rather than exercising a real join
/// flow. Everything else in the scenario (storage, segment sync, transport,
/// spec regeneration, rendering) is the real thing.
#[given(regex = r"^(\w+) and (\w+) share membership in the space$")]
async fn given_actors_share_membership(world: &mut SpaceChatWorld, a: String, b: String) {
    seed_membership(world.client(&a), &a, &[b.clone()]).await;
    seed_membership(world.client(&b), &b, &[a]).await;
}

async fn seed_membership(client: &Client, actor_name: &str, peer_names: &[String]) {
    // `arguments`'s last element is the completion callback the remote end
    // appends (W3C "Execute Async Script"); `fantoccini::Client::execute_async`
    // passes `args` straight through with no wrapping of its own.
    const SCRIPT: &str = r#"
        const [spaceId, peerNames, callback] = arguments;
        window.__TAURI__.core.invoke("seed_membership_for_testing", { spaceId, peerNames })
            .then(() => callback(null))
            .catch((err) => callback(String(err)));
    "#;
    wait_for_app_ready(client, actor_name).await;
    let result = client
        .execute_async(
            SCRIPT,
            vec![serde_json::json!(DEFAULT_SPACE_ID), serde_json::json!(peer_names)],
        )
        .await
        .unwrap_or_else(|e| panic!("WebDriver could not run the seeding script for {actor_name:?}: {e}"));
    assert!(
        result.is_null(),
        "seed_membership_for_testing should succeed for {actor_name:?} \
         (SPACECHAT_TEST_HOOKS is set by spawn_actor); got: {result:?}"
    );
}

#[when(regex = r#"^(\w+) sends the message "([^"]+)"$"#)]
async fn when_actor_sends_message(world: &mut SpaceChatWorld, name: String, content: String) {
    let client = world.client(&name);
    wait_for_app_ready(client, &name).await;
    let input = client
        .find(Locator::Css(MESSAGE_INPUT))
        .await
        .unwrap_or_else(|e| panic!("{name}'s composer input was not findable: {e}"));
    input
        .send_keys(&content)
        .await
        .unwrap_or_else(|e| panic!("could not type into {name}'s composer input: {e}"));
    let button = client
        .find(Locator::Css(SEND_BUTTON))
        .await
        .unwrap_or_else(|e| panic!("{name}'s send button was not findable: {e}"));
    button.click().await.unwrap_or_else(|e| panic!("could not click {name}'s send button: {e}"));
}

/// Delegates to the existing `when_actor_sends_message` step function -- same
/// real send path (composer input + send button, through the real running
/// app), just phrased as a `Given` (a precondition for this scenario) rather
/// than a `When` (the thing under test, which is the restart, not the send).
#[given(regex = r#"^(\w+) has sent the message "([^"]+)"$"#)]
async fn given_actor_has_sent_message(world: &mut SpaceChatWorld, name: String, content: String) {
    when_actor_sends_message(world, name, content).await;
}

/// Models the app-shell spec's "kill and restart the core mid-session" as
/// killing and relaunching the whole `space-chat-app` process against the same
/// on-disk data directory -- `space-chat-core` runs embedded in that process
/// and has no separate backend to kill independently.
///
/// `kill_actor`/`relaunch_actor` (Task 17) already do exactly this: the kill
/// signals the actor's whole process group (`xvfb-run` -> `tauri-driver` ->
/// `WebKitWebDriver` -> app) while keeping the actor's `TempDir` alive, and the
/// relaunch spawns a brand-new process tree -- new driver, new WebDriver
/// session, new webview -- pointed at that same `SPACECHAT_DATA_DIR`, waiting
/// for a freshly published endpoint id before returning. Nothing of the old
/// process (in-memory `AppState`, live-spec subscription, rendered DOM)
/// survives; the only thing carried across is what was written to disk.
///
/// The `client(..)` every later step calls is resolved through the actor's
/// CURRENT `RunningActor`, so the post-restart assertion necessarily runs
/// against the new session -- there is no way for it to accidentally re-read
/// the killed webview.
#[when(regex = r"^(\w+)'s app process is killed and relaunched against the same data directory$")]
async fn when_actor_process_killed_and_relaunched(world: &mut SpaceChatWorld, name: String) {
    let data_dir_before = world.actor(&name).data_dir.path().to_path_buf();
    world.kill_actor(&name).await;
    assert!(
        world.actor(&name).running.is_none(),
        "kill_actor did not clear {name:?}'s RunningActor -- later steps would resolve to a stale \
         WebDriver session rather than the fresh one relaunch_actor is about to create (this checks \
         the field kill_actor clears, not that the underlying OS process has actually died -- see \
         kill_process_group for the real teardown signal)"
    );
    world.relaunch_actor(&name).await;
    // Cheap guard against the one way this whole scenario could pass for the
    // wrong reason: a "relaunch" that quietly started against a DIFFERENT (or
    // wiped) directory would prove nothing about on-disk recovery, and a
    // relaunch that reused the old process would prove nothing about restart
    // at all.
    assert_eq!(
        world.actor(&name).data_dir.path(),
        data_dir_before,
        "the relaunched {name:?} must be pointed at the same on-disk data directory as before the kill"
    );
}

/// Polls the receiving actor's REAL rendered DOM (not its backend state) until
/// the message shows up, or the scenario's stated bound elapses.
///
/// Scoped to `.conversation-view` (`ConversationView.tsx`'s root) rather than
/// the whole page source, so a value sitting in the composer's own input can
/// never satisfy this assertion -- it must have gone through the conversation
/// spec and been rendered as a message.
#[then(regex = r#"^(\w+)'s conversation view shows "([^"]+)" within (\d+) seconds$"#)]
async fn then_conversation_view_shows(world: &mut SpaceChatWorld, name: String, content: String, seconds: u64) {
    let client = world.client(&name);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    let mut last_seen = String::new();

    loop {
        // `.text()` on the conversation view, not `client.source()`: the
        // rendered text is what a user would actually see, and it excludes
        // attribute values / script contents that could match by accident.
        if let Ok(view) = client.find(Locator::Css(".conversation-view")).await {
            if let Ok(text) = view.text().await {
                if text.contains(&content) {
                    return;
                }
                last_seen = text;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {name}'s conversation view to show {content:?} within {seconds}s, it never did. \
             Last rendered conversation text was: {last_seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Sends a message carrying one attachment whose bytes were deliberately never
/// saved to the local blob store -- the cache-miss case Task 13's protocol
/// handler answers with placeholder bytes. The hash the app reports back is
/// remembered on the `World` so the later `When` step can name the exact same
/// attachment; cucumber step functions cannot return values to one another.
///
/// Goes through a real Tauri command rather than the UI because there is no
/// attachment picker in the app yet (an acknowledged gap in this plan) -- but
/// `send_message_with_missing_attachment_for_testing` reaches the message
/// through `send_message_with_attachments_impl`, the very same
/// persist/notify/patch path `send_message` itself uses, so everything after
/// the send (spec regeneration, patch emission, rendering) is the real thing.
#[given(regex = r"^(\w+) has sent a message with an attachment not yet present in her attachment store$")]
async fn given_message_with_missing_attachment(world: &mut SpaceChatWorld, name: String) {
    const SCRIPT: &str = r#"
        const [callback] = arguments;
        window.__TAURI__.core.invoke("send_message_with_missing_attachment_for_testing")
            .then((hash) => callback(hash))
            .catch((err) => callback("ERROR:" + String(err)));
    "#;
    let client = world.client(&name);
    wait_for_app_ready(client, &name).await;
    let result = client
        .execute_async(SCRIPT, vec![])
        .await
        .unwrap_or_else(|e| panic!("WebDriver could not run the attachment-send script for {name:?}: {e}"));
    let hash = result
        .as_str()
        .unwrap_or_else(|| panic!("expected a hex hash string back for {name:?}, got: {result:?}"))
        .to_string();
    assert!(
        !hash.starts_with("ERROR:"),
        "send_message_with_missing_attachment_for_testing failed for {name:?} \
         (SPACECHAT_TEST_HOOKS is set by spawn_actor); got: {hash}"
    );
    world.remember_attachment_hash(&name, hash);
}

/// The cache-miss half of the assertion: `AttachmentImage` starts in its
/// placeholder state and stays there until an `attachment-ready` event arrives,
/// so seeing this element proves the message rendered AND that the bytes are
/// (correctly) not yet considered available.
#[then(regex = r"^(\w+)'s conversation view shows an attachment placeholder$")]
async fn then_shows_attachment_placeholder(world: &mut SpaceChatWorld, name: String) {
    let client = world.client(&name);
    client
        .wait()
        .at_most(Duration::from_secs(10))
        .for_element(Locator::Css(ATTACHMENT_PLACEHOLDER))
        .await
        .unwrap_or_else(|e| panic!("expected an attachment placeholder in {name:?}'s conversation view: {e}"));
}

/// Stands in for the fetch-over-network pipeline that doesn't exist yet: the
/// bytes land in the real local `AttachmentBlobStore` and the real
/// `attachment-ready:<hash>` event fires, which is all the frontend ever
/// observes of a completed fetch.
#[when(regex = r"^the attachment bytes become available in (\w+)'s attachment store$")]
async fn when_attachment_becomes_available(world: &mut SpaceChatWorld, name: String) {
    const SCRIPT: &str = r#"
        const [hashHex, callback] = arguments;
        window.__TAURI__.core.invoke("simulate_attachment_arrival_for_testing", { hashHex })
            .then(() => callback(null))
            .catch((err) => callback(String(err)));
    "#;
    let hash = world.attachment_hash(&name);
    let client = world.client(&name);
    let result = client
        .execute_async(SCRIPT, vec![serde_json::json!(hash)])
        .await
        .unwrap_or_else(|e| panic!("WebDriver could not run the attachment-arrival script for {name:?}: {e}"));
    assert!(
        result.is_null(),
        "simulate_attachment_arrival_for_testing failed for {name:?} (hash {hash}); got: {result:?}"
    );
}

/// The transition itself: the placeholder must be gone and the real `<img>`
/// present AND actually decoded. Asserting the placeholder's
/// *disappearance* too (not just the image's appearance) is what makes this
/// a transition rather than two unrelated elements coexisting.
///
/// Checking element *presence* alone would pass even if the `<img>`'s `src`
/// 404'd or the bytes weren't a real image at all -- an `<img>` element
/// exists in the DOM regardless of whether it loaded. A bare
/// `naturalWidth > 0` check ALSO isn't enough: Task 13's own cache-miss
/// path (`handle_attachment_request`) deliberately returns the real,
/// valid, decodable placeholder PNG (its own lazy-fetch policy), so a
/// nonzero-width `<img>` doesn't distinguish "the real attachment arrived"
/// from "still serving the fallback." `lib.rs`'s `simulate_attachment_arrival_for_testing`
/// saves a 1x1 image deliberately different in size from the placeholder's
/// 64x64 -- asserting the EXACT expected width is what actually proves the
/// `spacechat://` request round-tripped the real bytes through the
/// protocol handler, not just that a DOM node with the right
/// `data-testid` showed up decoding some image or other.
const EXPECTED_ARRIVED_WIDTH: i64 = 1;

#[then(regex = r"^(\w+)'s conversation view shows the loaded attachment within (\d+) seconds$")]
async fn then_shows_loaded_attachment(world: &mut SpaceChatWorld, name: String, seconds: u64) {
    let client = world.client(&name);
    client
        .wait()
        .at_most(Duration::from_secs(seconds))
        .for_element(Locator::Css(ATTACHMENT_LOADED))
        .await
        .unwrap_or_else(|e| {
            panic!("expected {name:?}'s conversation view to show a loaded attachment within {seconds}s: {e}")
        });

    const NATURAL_WIDTH_SCRIPT: &str = r#"
        const el = document.querySelector(arguments[0]);
        return el ? el.naturalWidth : -1;
    "#;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    loop {
        let width = client
            .execute(NATURAL_WIDTH_SCRIPT, vec![serde_json::json!(ATTACHMENT_LOADED)])
            .await
            .unwrap_or_else(|e| panic!("could not read {name:?}'s attachment naturalWidth: {e}"));
        let last_width = width.as_i64().unwrap_or(-1);
        if last_width == EXPECTED_ARRIVED_WIDTH {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{name}'s loaded attachment <img> never decoded the expected {EXPECTED_ARRIVED_WIDTH}px-wide \
             \"arrived\" image within {seconds}s -- last observed naturalWidth: {last_width} \
             (0 means never decoded; the placeholder's own width would be 64, which would mean the \
             real attachment bytes never actually replaced the fallback placeholder)"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let leftover = client
        .find_all(Locator::Css(ATTACHMENT_PLACEHOLDER))
        .await
        .unwrap_or_else(|e| panic!("could not re-check {name:?}'s placeholders: {e}"));
    assert!(
        leftover.is_empty(),
        "{name}'s attachment placeholder should be gone once the real image rendered, \
         but {} placeholder element(s) remain",
        leftover.len()
    );
}

/// Waits until an actor's frontend bundle has actually rendered a
/// conversation -- not just its composer, which `App.tsx` renders
/// unconditionally before its `onMount`'s `openConversation(...)` call has
/// resolved. Gating on the composer alone would let a step proceed before
/// `state.active` is populated on the backend, which could make
/// `seed_membership`/`send_message` land in a narrow window where a sent
/// message's patch is never pushed (no active conversation to push it to
/// yet) -- gating on `.conversation-view` (only rendered once `spec()` is
/// set) closes that window.
///
/// A WebDriver session is created the moment the app process starts, which
/// is well before the webview has loaded the page at all -- so without this
/// wait, the very first `find` in a step can race the page load and fail
/// with "no such element" against an app that is perfectly healthy.
async fn wait_for_app_ready(client: &Client, actor_name: &str) {
    let Err(e) =
        client.wait().at_most(Duration::from_secs(30)).for_element(Locator::Css(".conversation-view")).await
    else {
        return;
    };
    // Whatever the webview *did* load is the only useful evidence here, so
    // report it rather than just "timed out". Truncate by char, not byte --
    // the page can contain non-ASCII text (e.g. the composer's placeholder
    // has a U+2026 ellipsis), and `String::truncate` panics if the byte
    // index it's given isn't on a char boundary, which would mask the real
    // failure behind an unrelated panic.
    let url = client.current_url().await.map(|u| u.to_string()).unwrap_or_else(|e| format!("<unavailable: {e}>"));
    let source: String = client
        .source()
        .await
        .unwrap_or_else(|e| format!("<unavailable: {e}>"))
        .chars()
        .take(2000)
        .collect();
    panic!(
        "actor {actor_name:?}'s frontend never rendered a conversation view: {e}\n  url: {url}\n  source: {source}"
    );
}

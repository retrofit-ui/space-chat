//! Step definitions for the multi-actor E2E harness.
//!
//! Every step here drives a real, rendered `space-chat-app` webview through
//! WebDriver -- there is no in-process shortcut into an actor's backend. The
//! one exception is deliberate and documented on `seed_membership` below.
use crate::world::SpaceChatWorld;
use cucumber::{given, then, when};
use fantoccini::{Client, Locator};
use std::time::Duration;

/// The space `App.tsx` always opens on launch (Task 16's hardcoded
/// `DEFAULT_SPACE_ID`) -- there is no mechanism for a scenario to make a
/// running actor open any other space, so every scenario in this harness that
/// needs a "shared space" must use exactly this id.
const DEFAULT_SPACE_ID: &str = "space-default";

/// CSS selector for the composer input `App.tsx` renders unconditionally.
/// Also this harness's "the frontend bundle has actually run" signal -- see
/// `wait_for_app_ready`.
const MESSAGE_INPUT: &str = "[data-testid=message-input]";
const SEND_BUTTON: &str = "[data-testid=send-button]";

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

/// Waits until an actor's frontend bundle has actually rendered. A WebDriver
/// session is created the moment the app process starts, which is well before
/// the webview has loaded the page -- so without this, the first `find` in a
/// step can race the page load and fail with "no such element" against an app
/// that is perfectly healthy.
async fn wait_for_app_ready(client: &Client, actor_name: &str) {
    let Err(e) =
        client.wait().at_most(Duration::from_secs(30)).for_element(Locator::Css(MESSAGE_INPUT)).await
    else {
        return;
    };
    // Whatever the webview *did* load is the only useful evidence here, so
    // report it rather than just "timed out".
    let url = client.current_url().await.map(|u| u.to_string()).unwrap_or_else(|e| format!("<unavailable: {e}>"));
    let mut source = client.source().await.unwrap_or_else(|e| format!("<unavailable: {e}>"));
    source.truncate(2000);
    panic!(
        "actor {actor_name:?}'s frontend never rendered its composer: {e}\n  url: {url}\n  source: {source}"
    );
}

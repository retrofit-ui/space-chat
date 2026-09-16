//! Step definitions. Populated by Tasks 18-20; this task only establishes the
//! one step every scenario needs (bringing an actor online) so the auto-wiring
//! between `#[given]`/`#[when]`/`#[then]` and `SpaceChatWorld` is proven to
//! compile and resolve.
use crate::world::SpaceChatWorld;
use cucumber::given;

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

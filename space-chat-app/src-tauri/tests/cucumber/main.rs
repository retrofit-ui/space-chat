//! Entry point for the multi-actor E2E harness (`cargo test --test cucumber`).
//!
//! `harness = false` in `Cargo.toml`: cucumber supplies its own runner and
//! reporting rather than libtest's.
//!
//! Running this requires `tauri-driver`, `Xvfb`/`xvfb-run`, and
//! `/usr/bin/WebKitWebDriver` to be installed -- see `world.rs`.
//!
//! With an empty `features/` directory this reports zero scenarios and exits
//! successfully. With a *missing* one it panics with "1 parsing error"
//! (verified, not assumed), which is why `features/.gitkeep` exists: git does
//! not track empty directories, so without it a fresh clone would fail this
//! target outright.
mod steps;
mod world;

#[tokio::main]
async fn main() {
    use cucumber::World as _;
    // Cucumber defaults to running up to 64 scenarios concurrently
    // (confirmed in cucumber-0.23.0's own runner default). Each scenario
    // gets a fresh `SpaceChatWorld`, whose WebDriver/native port numbers are
    // a per-`World` counter starting at a FIXED value -- concurrent
    // scenarios would silently collide on those ports (one scenario's
    // `wait_for_port` succeeding against a DIFFERENT scenario's already-
    // listening driver, connecting to an actor with the wrong environment).
    // Capped to 1 to make scenarios run sequentially instead.
    world::SpaceChatWorld::cucumber()
        .max_concurrent_scenarios(1)
        .run("tests/cucumber/features")
        .await;
}

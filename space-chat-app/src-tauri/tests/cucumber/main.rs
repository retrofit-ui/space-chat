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
    world::SpaceChatWorld::run("tests/cucumber/features").await;
}

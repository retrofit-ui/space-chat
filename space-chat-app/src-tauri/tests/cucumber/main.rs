//! Entry point for the multi-actor E2E harness (`cargo test --test cucumber`).
//!
//! `harness = false` in `Cargo.toml`: cucumber supplies its own runner and
//! reporting rather than libtest's.
//!
//! Running this requires `tauri-driver`, `Xvfb`/`xvfb-run`, and
//! `/usr/bin/WebKitWebDriver` to be installed -- see `world.rs`. It also
//! requires the frontend to have been built at least once (`pnpm build` in
//! `space-chat-app/`): the `[dev-dependencies]` `tauri` entry turns on
//! `custom-protocol` so the app under test loads the REAL embedded frontend
//! instead of `build.devUrl`'s dev server, and `tauri::generate_context!`
//! fails the build outright if `../dist` is missing. See `Cargo.toml`.
//!
//! Because `harness = false`, cucumber -- not libtest -- parses this target's
//! arguments, and it has no positional test-name filter. `cargo test --test
//! cucumber golden_path` therefore fails with "unexpected argument"; the
//! working forms are `cargo test --test cucumber` (everything),
//! `cargo test --test cucumber -- -i '<glob>.feature'`, or
//! `cargo test --test cucumber -- -n '<scenario name regex>'`.
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
    // `.run_and_exit`, NOT `.run`: `World::run()`'s own default impl is
    // `Self::cucumber().run_and_exit(input)` -- `run_and_exit` panics if
    // `writer.execution_has_failed()`, which is what makes a failed step,
    // scenario, or feature-parse error actually fail `cargo test`. Plain
    // `.run(..)` returns the writer and does that check NOT AT ALL --
    // silently exiting 0 regardless of outcome. Confirmed by reading both
    // methods directly in the installed cucumber-0.23.0 source.
    world::SpaceChatWorld::cucumber()
        .max_concurrent_scenarios(1)
        .run_and_exit("tests/cucumber/features")
        .await;
}

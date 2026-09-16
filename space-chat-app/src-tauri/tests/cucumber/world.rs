//! The multi-actor E2E `World`: one real local `iroh` relay per scenario,
//! plus one genuinely separate OS process per named actor, each driven
//! through its own `tauri-driver` -> `WebKitWebDriver` -> WebView bridge.
//!
//! This mirrors `space-chat-transport/tests/two_process_convergence.rs`'s
//! proof strategy (real relay, real `std::process::Command` children, `Drop`
//! that reaps them even when a step panics), with the CLI `test_peer` binary
//! swapped for the real `space-chat-app` GUI binary under WebDriver.
//!
//! ## How an actor's environment actually reaches the app process
//!
//! This is the one place the design had to diverge from the obvious approach
//! ("spawn the app ourselves with `Command::env`"), and it's worth stating
//! plainly because it is not optional:
//!
//! `tauri-driver` launches the application *itself*. On session creation it
//! translates the `tauri:options.application` capability into
//! `webkitgtk:browserOptions.binary` and hands that to `WebKitWebDriver`,
//! which spawns the binary (verified against `tauri-driver` 2.0.6's
//! `src/server.rs`). If this harness also spawned the app directly, every
//! actor would be running *two* app instances, and the one WebDriver actually
//! controls would be the one with none of our environment applied -- wrong
//! data dir, no relay, no test hooks.
//!
//! `tauri:options` supports only `application` and `args`; it has **no `env`
//! field** (same file). So per-actor environment is instead applied to the
//! `tauri-driver` process, which spawns `WebKitWebDriver` with a plain
//! `Command` and no `env_clear` (verified in `tauri-driver`'s
//! `src/webdriver.rs`), which in turn spawns the app. The environment
//! therefore propagates down the whole chain. That is why every actor gets a
//! *dedicated* `tauri-driver` (and its own `--port`/`--native-port` pair)
//! rather than sharing one: the driver process is the carrier of the actor's
//! identity.
// Task 18's golden-path scenario exercises `spawn_actor`/`client(..)`, but
// `kill_actor`/`relaunch_actor` and the port fields are still only used by the
// restart-recovery scenarios Tasks 19-20 add. Kept (and allowed) rather than
// deleted, because they are the parts of this harness whose design -- keeping
// the data dir across a restart, one driver per actor -- is the whole reason
// `Actor`/`RunningActor` are split the way they are.
#![allow(dead_code)]
use cucumber::World;
use fantoccini::{Client, ClientBuilder};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Where `tauri-driver` finds the real WebDriver implementation on Linux.
/// Matches the invocation this harness's environment was smoke-tested with.
const NATIVE_DRIVER: &str = "/usr/bin/WebKitWebDriver";

/// First port of the range this harness hands out in `--port`/`--native-port`
/// pairs. Chosen to sit above `tauri-driver`'s own 4444/4445 defaults so a
/// manually-started driver can coexist with a harness run.
const FIRST_WEBDRIVER_PORT: u16 = 9515;

/// How long to wait for a freshly-spawned `tauri-driver` to start accepting
/// connections, and for a launched app to publish its endpoint id.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// The per-actor processes that exist only while an actor is "running".
/// Separated from `Actor` so `kill_actor` can tear all of this down while
/// keeping the actor's on-disk state (its temp data dir) intact for a later
/// `relaunch_actor` -- which is precisely what a restart-recovery scenario
/// needs.
pub struct RunningActor {
    /// The `xvfb-run` child, spawned into its own process group so the whole
    /// `Xvfb`/`tauri-driver`/`WebKitWebDriver`/app tree can be signalled at
    /// once. See `kill_process_group`.
    driver: Child,
    pub client: Client,
    pub webdriver_port: u16,
    pub native_port: u16,
}

pub struct Actor {
    pub name: String,
    /// Survives `kill_actor` on purpose: a relaunched actor must come back up
    /// against the same segments/listing/membership it had before.
    pub data_dir: tempfile::TempDir,
    /// Lives inside `data_dir`, so it is created and cleaned up with the
    /// actor and never needs its own temp handle. The app writes it via
    /// write-to-`.tmp`-then-rename, so its mere existence is a complete,
    /// readable announcement -- see `wait_for_endpoint_file`.
    pub endpoint_addr_file: PathBuf,
    /// Names of the actors this one dials at startup, remembered so
    /// `relaunch_actor` reproduces the same topology.
    pub dial_targets: Vec<String>,
    pub running: Option<RunningActor>,
}

impl Actor {
    /// The WebDriver client for this actor, panicking with a useful message
    /// if a step tries to drive an actor that is currently killed.
    pub fn client(&self) -> &Client {
        &self
            .running
            .as_ref()
            .unwrap_or_else(|| panic!("actor {:?} is not currently running", self.name))
            .client
    }
}

#[derive(World)]
#[world(init = Self::new)]
pub struct SpaceChatWorld {
    pub relay_url: Option<String>,
    /// Keeps this scenario's relay alive. `iroh::test_utils::run_relay_server()`
    /// returns `(RelayMap, RelayUrl, iroh_relay::server::Server)` and the
    /// `Server` shuts the relay down when dropped -- but `iroh` only
    /// re-exports `RelayConfig`/`RelayMap` from `iroh_relay`, not `Server`
    /// (verified in `iroh` 1.2.0's `src/lib.rs`), and `iroh_relay` is not a
    /// dependency of this crate. The handle's concrete type is therefore not
    /// nameable here, so it is boxed as `dyn Any` purely to be *held*, never
    /// downcast.
    pub relay_server: Option<Box<dyn std::any::Any + Send>>,
    pub actors: HashMap<String, Actor>,
    next_webdriver_port: u16,
}

impl std::fmt::Debug for SpaceChatWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpaceChatWorld")
            .field("relay_url", &self.relay_url)
            .field("actors", &self.actors.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl SpaceChatWorld {
    fn new() -> Self {
        Self {
            relay_url: None,
            relay_server: None,
            actors: HashMap::new(),
            next_webdriver_port: FIRST_WEBDRIVER_PORT,
        }
    }

    /// Starts this scenario's one real local `iroh` relay, idempotently.
    /// Exactly the call `space-chat-transport/tests/two_process_convergence.rs`
    /// and `network.rs`'s own integration test already use.
    pub async fn ensure_relay(&mut self) -> String {
        if let Some(url) = &self.relay_url {
            return url.clone();
        }
        let (_relay_map, relay_url, relay_server) =
            iroh::test_utils::run_relay_server().await.expect("failed to start a local iroh test relay");
        let relay_url = relay_url.to_string();
        self.relay_url = Some(relay_url.clone());
        self.relay_server = Some(Box::new(relay_server));
        relay_url
    }

    /// Registers a new actor with its own temp data dir and brings it up.
    /// `dial_targets` names actors that must already have been spawned; this
    /// actor dials each of them at startup, which is how a scenario states
    /// its connection topology.
    pub async fn spawn_actor(&mut self, name: &str, dial_targets: &[&str]) {
        assert!(!self.actors.contains_key(name), "actor {name:?} was already spawned");
        let data_dir = tempfile::tempdir().expect("failed to create actor temp dir");
        let endpoint_addr_file = data_dir.path().join("endpoint_id");
        self.actors.insert(
            name.to_string(),
            Actor {
                name: name.to_string(),
                data_dir,
                endpoint_addr_file,
                dial_targets: dial_targets.iter().map(|t| t.to_string()).collect(),
                running: None,
            },
        );
        self.launch(name).await;
    }

    /// Brings a currently-killed actor back up against its existing on-disk
    /// state.
    ///
    /// **Known limitation, stated rather than hidden:** the app generates a
    /// fresh random `TransportIdentity` on every launch (an existing,
    /// documented TODO in `lib.rs` -- `TransportIdentity` has no
    /// serialization yet), so a relaunched actor comes back with a *different*
    /// endpoint id than it had before. Peers that dialed its old id are not
    /// automatically reconnected. A restart scenario that needs the peers
    /// reconnected must have the relaunched actor dial *out* to them (which
    /// works, since their ids are unchanged), not rely on their old inbound
    /// connections surviving.
    pub async fn relaunch_actor(&mut self, name: &str) {
        assert!(self.actors.contains_key(name), "cannot relaunch actor {name:?}, which was never spawned");
        assert!(
            self.actors[name].running.is_none(),
            "actor {name:?} is still running; call kill_actor before relaunch_actor"
        );
        self.launch(name).await;
    }

    /// Kills `name`'s whole process tree but keeps its temp data dir, so
    /// `relaunch_actor` can point a fresh process at the same on-disk state.
    pub async fn kill_actor(&mut self, name: &str) {
        let Some(actor) = self.actors.get_mut(name) else { return };
        let Some(mut running) = actor.running.take() else { return };
        // Best-effort graceful session teardown first: it lets WebKitWebDriver
        // close the webview itself rather than having it SIGKILLed out from
        // under the driver. Failure here is fine -- the process-group kill
        // below is the actual guarantee.
        let _ = running.client.close().await;
        kill_process_group(&mut running.driver);
    }

    /// The shared bring-up path for both `spawn_actor` and `relaunch_actor`.
    async fn launch(&mut self, name: &str) {
        let relay_url = self.ensure_relay().await;
        let (webdriver_port, native_port) = self.next_port_pair();

        // Resolve dial targets to their endpoint-id file paths, waiting for
        // each to be published. Done before spawning this actor so a missing
        // target fails fast with a clear message instead of leaving a live
        // driver behind.
        let mut dial_files = Vec::new();
        for target in self.actors[name].dial_targets.clone() {
            let target_actor = self
                .actors
                .get(&target)
                .unwrap_or_else(|| panic!("dial target {target:?} must be spawned before {name:?}"));
            let path = target_actor.endpoint_addr_file.clone();
            wait_for_endpoint_file(&path, &target).await;
            dial_files.push(path.to_string_lossy().to_string());
        }

        let actor = &self.actors[name];
        let data_dir = actor.data_dir.path().to_path_buf();
        let endpoint_addr_file = actor.endpoint_addr_file.clone();

        // Clear any stale endpoint-id file from a PRIOR launch of this same
        // actor before spawning -- relaunch_actor calls this same function
        // against an actor whose data_dir (and therefore endpoint_addr_file)
        // already exists from before it was killed. Without this,
        // wait_for_endpoint_file below would return instantly against the
        // OLD file, before the new process has published anything -- and any
        // peer dialing this actor via SPACECHAT_DIAL_ADDRS would read a dead
        // endpoint id (the app generates a fresh TransportIdentity every
        // launch, so the old id can never be reached again regardless).
        let _ = std::fs::remove_file(&endpoint_addr_file);

        // The per-actor environment goes on `tauri-driver`, NOT on the app --
        // see this module's doc comment for why that is the only thing that
        // works.
        let mut command = Command::new("xvfb-run");
        command
            .arg("--auto-servernum")
            .arg("tauri-driver")
            .arg("--native-driver")
            .arg(NATIVE_DRIVER)
            .arg("--port")
            .arg(webdriver_port.to_string())
            .arg("--native-port")
            .arg(native_port.to_string())
            .env("SPACECHAT_DATA_DIR", &data_dir)
            .env("SPACECHAT_ACTOR_NAME", name)
            .env("SPACECHAT_RELAY_URL", &relay_url)
            .env("SPACECHAT_ENDPOINT_ADDR_FILE", &endpoint_addr_file)
            .env("SPACECHAT_TEST_HOOKS", "1")
            // `tauri-driver` already nulls the native driver's stdout to keep
            // its own clean; keep stderr so a failed launch is diagnosable.
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        if dial_files.is_empty() {
            command.env_remove("SPACECHAT_DIAL_ADDRS");
        } else {
            command.env("SPACECHAT_DIAL_ADDRS", dial_files.join(","));
        }
        set_own_process_group(&mut command);

        let driver = command
            .spawn()
            .expect("failed to spawn `xvfb-run tauri-driver` (are xvfb-run and tauri-driver on PATH?)");

        // A real readiness signal instead of a fixed sleep: poll the port
        // `tauri-driver` was told to listen on until it accepts a TCP
        // connection.
        wait_for_port(webdriver_port).await;

        let mut capabilities = fantoccini::wd::Capabilities::new();
        capabilities.insert(
            "tauri:options".to_string(),
            serde_json::json!({ "application": env!("CARGO_BIN_EXE_space-chat-app") }),
        );
        let client = ClientBuilder::native()
            .capabilities(capabilities)
            .connect(&format!("http://127.0.0.1:{webdriver_port}"))
            .await
            .expect("failed to open a WebDriver session against tauri-driver");

        // The session request is what actually launches the app, so only now
        // can the endpoint announcement appear. Waiting for it here means any
        // later step can assume this actor's transport is bound.
        wait_for_endpoint_file(&endpoint_addr_file, name).await;

        self.actors.get_mut(name).expect("actor was just inserted").running =
            Some(RunningActor { driver, client, webdriver_port, native_port });
    }

    /// Hands out a fresh `(webdriver_port, native_port)` pair. Sequential
    /// rather than OS-assigned because `tauri-driver` takes port *numbers*,
    /// not pre-bound listeners, so there is no way to hand it an ephemeral
    /// port without a bind-then-release race anyway; a per-`World` counter is
    /// simpler and each scenario gets a fresh `World`.
    fn next_port_pair(&mut self) -> (u16, u16) {
        let webdriver_port = self.next_webdriver_port;
        self.next_webdriver_port += 2;
        (webdriver_port, webdriver_port + 1)
    }

    pub fn actor(&self, name: &str) -> &Actor {
        self.actors.get(name).unwrap_or_else(|| panic!("no actor named {name:?} in this scenario"))
    }

    /// Convenience for steps: the WebDriver client for a named actor.
    pub fn client(&self, name: &str) -> &Client {
        self.actor(name).client()
    }
}

impl Drop for SpaceChatWorld {
    fn drop(&mut self) {
        // Cannot `await` here, so the graceful `client.close()` that
        // `kill_actor` does is skipped -- killing the process group takes the
        // webview down with the driver regardless. This runs even when a step
        // panicked partway through, which is exactly the point.
        for actor in self.actors.values_mut() {
            if let Some(running) = actor.running.as_mut() {
                kill_process_group(&mut running.driver);
            }
        }
    }
}

/// Puts `command`'s child into a brand-new process group of its own, so its
/// entire descendant tree can later be signalled with one `kill(-pgid, ..)`.
fn set_own_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
}

/// Kills the whole process group led by `child` (`xvfb-run` plus every
/// descendant: `Xvfb`, `tauri-driver`, `WebKitWebDriver`, and the app
/// itself), then reaps `child` so it does not linger as a zombie.
///
/// SIGTERM first so the app gets a chance to flush/close cleanly, then
/// SIGKILL to guarantee it. Signalling only `child.kill()` would leave the
/// rest of the tree orphaned and still holding X displays and ports.
fn kill_process_group(child: &mut Child) {
    let pgid = child.id() as i32;
    unsafe {
        libc::kill(-pgid, libc::SIGTERM);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
            _ => {
                unsafe {
                    libc::kill(-pgid, libc::SIGKILL);
                }
                let _ = child.wait();
                break;
            }
        }
    }
    // Belt and braces: even after the group leader exits, a descendant that
    // ignored SIGTERM could still be alive in the group.
    unsafe {
        libc::kill(-pgid, libc::SIGKILL);
    }
}

/// Polls until something accepts TCP connections on `port`, which is
/// `tauri-driver`'s real readiness signal (a fixed sleep would be both slower
/// and flakier).
async fn wait_for_port(port: u16) {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "tauri-driver never started listening on port {port}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Waits for an actor to publish its endpoint id.
///
/// Existence alone is a sufficient signal because the app writes the file to a
/// sibling `.tmp` path and renames it into place (`announce_and_dial_from_env`
/// in `lib.rs`) -- there is no window in which the file exists but is empty or
/// half-written. The length is still asserted, so a future regression that
/// dropped the atomic-rename would fail loudly here rather than silently
/// producing undialable addresses.
async fn wait_for_endpoint_file(path: &std::path::Path, actor_name: &str) {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if let Ok(contents) = std::fs::read_to_string(path) {
            let contents = contents.trim();
            if contents.len() == 64 {
                return;
            }
            assert!(
                contents.is_empty(),
                "actor {actor_name:?} published a malformed endpoint id ({} chars) at {}",
                contents.len(),
                path.display()
            );
        }
        assert!(
            Instant::now() < deadline,
            "actor {actor_name:?} never published its endpoint id at {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

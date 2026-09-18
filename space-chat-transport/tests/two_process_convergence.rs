//! The milestone's exit-criteria proof: unlike every other test in this
//! crate (including `multi_hop_convergence.rs`'s three-way test), the
//! peers here are genuinely separate OS processes, spawned via
//! `std::process::Command`, not `tokio::spawn` tasks sharing one process's
//! memory. Each one runs the `test_peer` binary (`src/bin/test_peer.rs`)
//! and reports over its own stdout, which this harness reads.
use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};

/// Wraps one spawned `test_peer` child process together with a
/// persistent `BufReader` over its stdout.
///
/// Two things this deliberately gets right that a more naive version
/// wouldn't:
/// - The `BufReader` is created once and kept alive across multiple
///   `read_line` calls, rather than a fresh one per call. A `BufReader`
///   fills its internal buffer from whatever the OS pipe happens to
///   deliver in one read -- which can (and in practice sometimes does)
///   contain more than just the one line asked for. Constructing a new
///   `BufReader` on every call would silently discard any such
///   over-read-but-unconsumed bytes when the old one is dropped, losing
///   part of a later line. Keeping one `BufReader` for the process's
///   whole lifetime avoids that.
/// - `Drop` kills the child if it's still running, so a test failing
///   (assert panic, timeout) partway through never leaves an orphaned
///   `test_peer` process behind -- whether the test passes or fails.
struct Peer {
    child: Child,
    stdout: BufReader<ChildStdout>,
}

impl Peer {
    fn spawn(relay_url: &str, role: &str, extra_args: &[String]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_test_peer"))
            .arg(relay_url)
            .arg(role)
            .args(extra_args)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to spawn the test_peer binary");
        let stdout = child.stdout.take().expect("child stdout should be piped");
        Self { child, stdout: BufReader::new(stdout) }
    }

    /// This process's real OS process ID -- distinct from this test
    /// harness's own PID and from every other spawned peer's PID, which is
    /// exactly what makes this a genuine separate-process test rather than
    /// an in-process task dressed up to look like one.
    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("child should print a line");
        assert!(!line.is_empty(), "child stdout closed before printing a line (process likely exited/panicked early)");
        line.trim().to_string()
    }

    fn read_endpoint(&mut self) -> String {
        self.read_line().strip_prefix("ENDPOINT ").expect("first line should be an ENDPOINT announcement").to_string()
    }

    fn wait_success(&mut self) {
        let status = self.child.wait().expect("failed to wait on test_peer child process");
        assert!(status.success(), "test_peer process should exit successfully, got {status:?}");
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        // Best-effort cleanup: if the child hasn't already been reaped by
        // `wait_success` (e.g. because an earlier assertion in the test
        // panicked first), kill it rather than leaving it running after
        // this test function returns.
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// The milestone's exit criterion, part 1: two separate OS processes (not
/// in-process function calls, unlike Tasks 7-11's tests) converge over a
/// real local iroh relay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_separate_os_processes_converge() {
    let (_relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

    let mut bob = Peer::spawn(relay_url.as_str(), "bob", &[]);
    let bob_endpoint = bob.read_endpoint();

    let mut alice = Peer::spawn(relay_url.as_str(), "alice", &[bob_endpoint]);

    // Genuine process separation, not tokio tasks: three distinct PIDs
    // (this test's own, bob's, alice's), none of which is zero/equal.
    let this_pid = std::process::id();
    assert_ne!(bob.pid(), this_pid);
    assert_ne!(alice.pid(), this_pid);
    assert_ne!(bob.pid(), alice.pid());

    alice.wait_success();

    let bob_output = bob.read_line();
    assert_eq!(bob_output, "CONVERGED 1");
    bob.wait_success();
}

/// The milestone's exit criterion, part 2: three separate OS processes,
/// where Alice and Carol are never directly connected, still converge on
/// Alice's message via Bob -- the same multi-hop property
/// `multi_hop_convergence.rs` proved in-process, now proved across real
/// process/OS boundaries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_separate_os_processes_converge_via_a_middle_hop() {
    let (_relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

    let mut bob = Peer::spawn(relay_url.as_str(), "bob", &[]);
    let bob_endpoint = bob.read_endpoint();

    let mut carol = Peer::spawn(relay_url.as_str(), "carol", std::slice::from_ref(&bob_endpoint));
    // Every `test_peer` process prints its own `ENDPOINT ...` line first,
    // regardless of role -- carol's isn't needed by anyone here (nobody
    // dials carol), but it still has to be consumed off her stdout before
    // her later `CONVERGED ...` line can be read.
    let _carol_endpoint = carol.read_endpoint();
    let mut alice = Peer::spawn(relay_url.as_str(), "alice", &[bob_endpoint]);

    let this_pid = std::process::id();
    assert_ne!(bob.pid(), this_pid);
    assert_ne!(carol.pid(), this_pid);
    assert_ne!(alice.pid(), this_pid);
    assert_ne!(bob.pid(), carol.pid());
    assert_ne!(bob.pid(), alice.pid());
    assert_ne!(carol.pid(), alice.pid());

    alice.wait_success();

    let carol_output = carol.read_line();
    assert_eq!(carol_output, "CONVERGED 1", "carol should converge via bob without ever dialing alice");
    carol.wait_success();

    let bob_output = bob.read_line();
    assert_eq!(bob_output, "CONVERGED 1");
    bob.wait_success();
}

/// The milestone's exit criterion, part 3: roaming survival, across real
/// OS processes rather than an in-process simulation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_roaming_process_still_delivers_its_second_message() {
    let (_relay_map, relay_url, _relay_server) = iroh::test_utils::run_relay_server().await.unwrap();

    let mut bob = Peer::spawn(relay_url.as_str(), "bob", &["roam".to_string()]);
    let bob_endpoint = bob.read_endpoint();

    let mut alice = Peer::spawn(relay_url.as_str(), "alice", &[bob_endpoint, "roam".to_string()]);

    alice.wait_success();

    let bob_output = bob.read_line();
    assert_eq!(bob_output, "CONVERGED 2", "bob should see both the before- and after-roam messages");
    bob.wait_success();
}

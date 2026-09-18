use serde::Serialize;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

/// How many prior versions `LiveSpec` retains for diffing. See this plan's
/// Global Constraints for the 5-10 -> 8 rationale.
const ROLLING_WINDOW: usize = 8;

/// Adapted from `tenju-tofu/src-tauri/src/live_spec.rs`, with a rolling
/// window of prior versions (see `ROLLING_WINDOW`) in place of the
/// original's single-previous-version tracking. Delivery-mechanism-agnostic:
/// this plan pushes `diff_since`'s result over a Tauri event (Task 9) rather
/// than the original's HTTP-polled response, but nothing about that changes
/// this type itself.
#[derive(Clone)]
pub struct LiveSpec {
    state: Arc<RwLock<LiveSpecState>>,
}

struct LiveSpecState {
    version: u64,
    current: Value,
    // Front = oldest retained version, back = most recent. Bounded to
    // ROLLING_WINDOW entries.
    history: VecDeque<(u64, Value)>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PatchResponse {
    Unchanged { version: u64 },
    Patch { version: u64, patch: json_patch::Patch },
    Full { version: u64, spec: Value },
}

impl LiveSpec {
    pub fn new(initial: Value) -> Self {
        Self {
            state: Arc::new(RwLock::new(LiveSpecState {
                version: 0,
                current: initial,
                history: VecDeque::new(),
            })),
        }
    }

    pub fn update(&self, new_value: Value) {
        let mut state = self.state.write().unwrap();
        let old_version = state.version;
        let old_value = std::mem::replace(&mut state.current, new_value);
        state.history.push_back((old_version, old_value));
        while state.history.len() > ROLLING_WINDOW {
            state.history.pop_front();
        }
        state.version += 1;
    }

    pub fn snapshot(&self) -> (u64, Value) {
        let state = self.state.read().unwrap();
        (state.version, state.current.clone())
    }

    pub fn diff_since(&self, since: u64) -> PatchResponse {
        let state = self.state.read().unwrap();
        if since == state.version {
            return PatchResponse::Unchanged { version: state.version };
        }
        if let Some((_, since_value)) = state.history.iter().find(|(v, _)| *v == since) {
            let patch = json_patch::diff(since_value, &state.current);
            return PatchResponse::Patch { version: state.version, patch };
        }
        PatchResponse::Full { version: state.version, spec: state.current.clone() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fresh_live_spec_is_unchanged_when_queried_at_its_own_version() {
        let live = LiveSpec::new(json!({"a": 1}));
        let (version, _) = live.snapshot();
        assert_eq!(version, 0);
        match live.diff_since(0) {
            PatchResponse::Unchanged { version } => assert_eq!(version, 0),
            other => panic!("expected Unchanged, got {other:?}"),
        }
    }

    #[test]
    fn one_update_produces_a_patch_against_the_immediately_prior_version() {
        let live = LiveSpec::new(json!({"a": 1}));
        live.update(json!({"a": 2}));

        match live.diff_since(0) {
            PatchResponse::Patch { version, patch } => {
                assert_eq!(version, 1);
                assert_eq!(patch.0.len(), 1, "expected exactly one JSON Patch operation");
            }
            other => panic!("expected Patch, got {other:?}"),
        }
    }

    #[test]
    fn a_client_within_the_rolling_window_still_gets_a_patch() {
        let live = LiveSpec::new(json!({"n": 0}));
        for i in 1..=7 {
            live.update(json!({"n": i}));
        }
        // Client last saw version 0; 7 updates have happened since, still
        // within the 8-version rolling window (versions 0..=7 all retained).
        match live.diff_since(0) {
            PatchResponse::Patch { version, .. } => assert_eq!(version, 7),
            other => panic!("expected Patch within the rolling window, got {other:?}"),
        }
    }

    #[test]
    fn a_client_further_behind_than_the_rolling_window_gets_a_full_resend() {
        let live = LiveSpec::new(json!({"n": 0}));
        for i in 1..=9 {
            live.update(json!({"n": i}));
        }
        // Client last saw version 0; 9 updates have happened, exceeding the
        // 8-version window, so version 0's snapshot is no longer retained.
        match live.diff_since(0) {
            PatchResponse::Full { version, spec } => {
                assert_eq!(version, 9);
                assert_eq!(spec, json!({"n": 9}));
            }
            other => panic!("expected Full, got {other:?}"),
        }
    }

    /// Proves the backend-restart-needs-no-new-mechanism property from this
    /// plan's Global Constraints directly against the code, not just in
    /// prose: a freshly-constructed `LiveSpec` (as if the backend just
    /// restarted and re-opened this conversation) has no history at all, so
    /// any nonzero version a frontend quotes back from before the restart is
    /// unrecognized and falls through to `Full` -- exactly the same path a
    /// too-far-behind client takes, with no restart-specific branch needed.
    #[test]
    fn diff_since_on_a_freshly_constructed_live_spec_returns_full_for_any_prior_version() {
        let live = LiveSpec::new(json!({"n": 0}));
        match live.diff_since(7) {
            PatchResponse::Full { version, spec } => {
                assert_eq!(version, 0);
                assert_eq!(spec, json!({"n": 0}));
            }
            other => panic!("expected Full for a version this fresh instance never produced, got {other:?}"),
        }
    }
}

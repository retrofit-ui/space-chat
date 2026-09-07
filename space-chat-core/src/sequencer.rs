use crate::domain::DeviceId;

/// Deterministic sequencer election per the protocol spec: the member with
/// the lowest DeviceId (compared byte-for-byte) is the sequencer. Every
/// member computes this independently from current membership — there is
/// no election message, so removing the sequencer from membership and
/// recomputing this function is the entire "handoff."
///
/// Returns `None` for an empty slice rather than panicking -- `members` is
/// caller-supplied (ultimately derived from current membership state), and
/// every other public function in this crate that takes potentially-
/// untrusted/caller-supplied input returns `Result`/`Option` instead of
/// panicking, per the last two review rounds.
pub fn elect_sequencer(members: &[DeviceId]) -> Option<DeviceId> {
    members.iter().min_by_key(|d| d.0).copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::DeviceId;

    #[test]
    fn sequencer_is_lowest_device_id() {
        let members = vec![
            DeviceId([5u8; 32]),
            DeviceId([1u8; 32]),
            DeviceId([9u8; 32]),
        ];
        assert_eq!(elect_sequencer(&members).unwrap(), DeviceId([1u8; 32]));
    }

    #[test]
    fn sequencer_recomputes_after_removal() {
        // Per the protocol spec: removing the current sequencer from
        // membership must deterministically elect the next-lowest id,
        // with no separate handoff message needed.
        let members = vec![DeviceId([5u8; 32]), DeviceId([9u8; 32])];
        assert_eq!(elect_sequencer(&members).unwrap(), DeviceId([5u8; 32]));
    }

    /// Finding 3 of the Milestone 1 final review, round 3: `elect_sequencer`
    /// used to `.expect()` a non-empty slice, panicking on empty membership
    /// -- the last remaining panic-on-caller-input in the crate's non-test
    /// code. This proves it now returns `None` instead of panicking.
    #[test]
    fn elect_sequencer_returns_none_for_an_empty_slice() {
        let members: Vec<DeviceId> = vec![];
        assert_eq!(elect_sequencer(&members), None);
    }
}

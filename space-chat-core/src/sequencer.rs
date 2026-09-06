use crate::domain::DeviceId;

/// Deterministic sequencer election per the protocol spec: the member with
/// the lowest DeviceId (compared byte-for-byte) is the sequencer. Every
/// member computes this independently from current membership — there is
/// no election message, so removing the sequencer from membership and
/// recomputing this function is the entire "handoff."
pub fn elect_sequencer(members: &[DeviceId]) -> DeviceId {
    *members
        .iter()
        .min_by_key(|d| d.0)
        .expect("elect_sequencer requires at least one member")
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
        assert_eq!(elect_sequencer(&members), DeviceId([1u8; 32]));
    }

    #[test]
    fn sequencer_recomputes_after_removal() {
        // Per the protocol spec: removing the current sequencer from
        // membership must deterministically elect the next-lowest id,
        // with no separate handoff message needed.
        let members = vec![DeviceId([5u8; 32]), DeviceId([9u8; 32])];
        assert_eq!(elect_sequencer(&members), DeviceId([5u8; 32]));
    }
}

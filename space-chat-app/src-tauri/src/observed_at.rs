use std::collections::HashMap;

/// Records, per message, the Unix-millis timestamp at which *this device*
/// first observed the message -- not when the original sender sent it. See
/// this plan's opening note on why: `space_chat_core::domain::Message` has
/// no sender-side timestamp field.
pub trait ObservedAtStore: Send + Sync {
    /// Records `now_unix_ms` for `message_key` if nothing is recorded yet,
    /// and returns whichever timestamp ends up stored (the new one, or the
    /// pre-existing one) -- so a caller never needs a separate `get` call
    /// right after this to find out what was actually recorded.
    fn record_if_absent(&mut self, message_key: &str, now_unix_ms: u64) -> u64;
    fn get(&self, message_key: &str) -> Option<u64>;
}

#[derive(Debug, Default)]
pub struct InMemoryObservedAtStore {
    data: HashMap<String, u64>,
}

impl ObservedAtStore for InMemoryObservedAtStore {
    fn record_if_absent(&mut self, message_key: &str, now_unix_ms: u64) -> u64 {
        *self.data.entry(message_key.to_string()).or_insert(now_unix_ms)
    }

    fn get(&self, message_key: &str) -> Option<u64> {
        self.data.get(message_key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_if_absent_stores_the_first_timestamp_seen() {
        let mut store = InMemoryObservedAtStore::default();
        let stored = store.record_if_absent("msg:1", 1000);
        assert_eq!(stored, 1000);
        assert_eq!(store.get("msg:1"), Some(1000));
    }

    #[test]
    fn record_if_absent_does_not_overwrite_an_existing_timestamp() {
        let mut store = InMemoryObservedAtStore::default();
        store.record_if_absent("msg:1", 1000);
        let stored = store.record_if_absent("msg:1", 5000);
        assert_eq!(stored, 1000, "a second record_if_absent call must not move an already-recorded timestamp");
        assert_eq!(store.get("msg:1"), Some(1000));
    }

    #[test]
    fn get_returns_none_for_an_unseen_message() {
        let store = InMemoryObservedAtStore::default();
        assert_eq!(store.get("msg:unknown"), None);
    }
}

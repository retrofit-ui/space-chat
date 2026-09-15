use serde::{Deserialize, Serialize};
use space_chat_core::domain::DeviceId;
use space_chat_core::sequencer::elect_sequencer;
use std::collections::HashMap;
use std::path::PathBuf;

pub trait SpaceMembership: Send + Sync {
    fn members(&self, space_id: &str) -> Vec<DeviceId>;
    fn display_name(&self, id: &DeviceId) -> String;
    fn add_member(&mut self, space_id: &str, id: DeviceId, display_name: String);
    fn create_space(&mut self, space_id: &str, local_device: DeviceId, local_display_name: String);
    fn sequencer(&self, space_id: &str) -> Option<DeviceId>;
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct MembershipData {
    // hex-encoded DeviceId -> display name, global across all spaces this
    // device knows about -- a placeholder simplification; real MLS
    // membership is scoped per-space credential, not a flat global map.
    display_names: HashMap<String, String>,
    // space_id -> hex-encoded DeviceIds
    spaces: HashMap<String, Vec<String>>,
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<DeviceId> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(DeviceId(out))
}

/// Plaintext, unauthenticated, JSON-file-backed membership -- a placeholder
/// for real MLS-backed membership (`space-chat-openmls`, which doesn't exist
/// yet). See this module's doc comment.
pub struct PlaintextMembership {
    path: PathBuf,
    data: MembershipData,
}

impl PlaintextMembership {
    pub fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let data = if path.exists() {
            let bytes = std::fs::read(&path)?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            MembershipData::default()
        };
        Ok(Self { path, data })
    }

    fn persist(&self) {
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(bytes) = serde_json::to_vec_pretty(&self.data) {
            let _ = std::fs::write(&self.path, bytes);
        }
    }
}

impl SpaceMembership for PlaintextMembership {
    fn members(&self, space_id: &str) -> Vec<DeviceId> {
        self.data
            .spaces
            .get(space_id)
            .map(|ids| ids.iter().filter_map(|s| unhex(s)).collect())
            .unwrap_or_default()
    }

    fn display_name(&self, id: &DeviceId) -> String {
        let hex_id = hex(&id.0);
        self.data
            .display_names
            .get(&hex_id)
            .cloned()
            .unwrap_or_else(|| hex_id[..8].to_string())
    }

    fn add_member(&mut self, space_id: &str, id: DeviceId, display_name: String) {
        let hex_id = hex(&id.0);
        self.data.display_names.insert(hex_id.clone(), display_name);
        let members = self.data.spaces.entry(space_id.to_string()).or_default();
        if !members.contains(&hex_id) {
            members.push(hex_id);
        }
        self.persist();
    }

    fn create_space(&mut self, space_id: &str, local_device: DeviceId, local_display_name: String) {
        self.add_member(space_id, local_device, local_display_name);
    }

    fn sequencer(&self, space_id: &str) -> Option<DeviceId> {
        elect_sequencer(&self.members(space_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::domain::DeviceId;

    #[test]
    fn create_space_registers_the_local_device_as_a_member() {
        let dir = tempfile::tempdir().unwrap();
        let mut membership = PlaintextMembership::new(dir.path().join("membership.json")).unwrap();

        membership.create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());

        assert_eq!(membership.members("space-1"), vec![DeviceId([1u8; 32])]);
        assert_eq!(membership.display_name(&DeviceId([1u8; 32])), "Alice");
    }

    #[test]
    fn add_member_and_sequencer_election_matches_lowest_device_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut membership = PlaintextMembership::new(dir.path().join("membership.json")).unwrap();

        membership.create_space("space-1", DeviceId([5u8; 32]), "Bob".to_string());
        membership.add_member("space-1", DeviceId([1u8; 32]), "Alice".to_string());

        let mut members = membership.members("space-1");
        members.sort_by_key(|d| d.0);
        assert_eq!(members, vec![DeviceId([1u8; 32]), DeviceId([5u8; 32])]);
        assert_eq!(membership.sequencer("space-1"), Some(DeviceId([1u8; 32])));
    }

    #[test]
    fn membership_persists_across_a_fresh_instance_at_the_same_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("membership.json");
        {
            let mut membership = PlaintextMembership::new(&path).unwrap();
            membership.create_space("space-1", DeviceId([1u8; 32]), "Alice".to_string());
        }
        let reloaded = PlaintextMembership::new(&path).unwrap();
        assert_eq!(reloaded.members("space-1"), vec![DeviceId([1u8; 32])]);
        assert_eq!(reloaded.display_name(&DeviceId([1u8; 32])), "Alice");
    }

    #[test]
    fn display_name_falls_back_to_a_short_hex_id_for_unknown_devices() {
        let dir = tempfile::tempdir().unwrap();
        let membership = PlaintextMembership::new(dir.path().join("membership.json")).unwrap();
        let name = membership.display_name(&DeviceId([0xabu8; 32]));
        assert!(name.starts_with("ab"), "expected fallback name to start with the device id's hex prefix, got {name:?}");
    }
}

use crate::membership::SpaceMembership;
use crate::state::AppState;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct InviteToken {
    pub space_id: String,
    pub issued_at_unix_ms: u64,
}

/// Base64 (standard, no padding) of the token's JSON encoding. Deliberately
/// **not signed or encrypted** -- see this task's module-level caveat.
pub fn encode_invite(token: &InviteToken) -> String {
    use base64::Engine;
    let json = serde_json::to_vec(token).expect("InviteToken always serializes");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

pub fn decode_invite(encoded: &str) -> Result<InviteToken, String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|e| format!("invalid invite encoding: {e}"))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("invalid invite payload: {e}"))
}

fn now_unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

pub async fn create_space_impl(state: &AppState, title: String) -> String {
    let space_id = format!("space-{}", Uuid::new_v4());
    // `title` is not yet persisted anywhere (see the title-clobber fixes in
    // Tasks 9/10's history) -- recorded here as the local device's own
    // display name is the only per-space metadata this placeholder
    // membership model tracks.
    let _ = &title;
    state
        .membership
        .lock()
        .unwrap()
        .create_space(&space_id, state.local_device, "Me".to_string());
    space_id
}

pub async fn generate_invite_impl(state: &AppState, space_id: &str) -> String {
    let _ = state; // present for signature symmetry with the other impls; a
                    // real (signed) invite would need to consult membership
                    // state to prove the caller may invite to this space.
    encode_invite(&InviteToken { space_id: space_id.to_string(), issued_at_unix_ms: now_unix_ms() })
}

pub async fn join_via_invite_impl(
    state: &AppState,
    invite_token: &str,
    local_display_name: &str,
) -> Result<String, String> {
    let token = decode_invite(invite_token)?;
    state.membership.lock().unwrap().add_member(
        &token.space_id,
        state.local_device,
        local_display_name.to_string(),
    );
    Ok(token.space_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invite_tokens_round_trip_through_encode_decode() {
        let token = InviteToken { space_id: "space-1".to_string(), issued_at_unix_ms: 123 };
        let encoded = encode_invite(&token);
        let decoded = decode_invite(&encoded).unwrap();
        assert_eq!(decoded.space_id, "space-1");
        assert_eq!(decoded.issued_at_unix_ms, 123);
    }

    #[test]
    fn decode_invite_rejects_garbage_input() {
        assert!(decode_invite("not a real invite token").is_err());
    }

    #[tokio::test]
    async fn create_space_registers_the_local_device_and_returns_a_fresh_id() {
        let (_dir, state) = crate::commands::tests::fresh_state_for_invite_tests().await;
        let space_id = create_space_impl(&state, "General".to_string()).await;

        assert!(state.membership.lock().unwrap().members(&space_id).contains(&state.local_device));
    }

    #[tokio::test]
    async fn generate_then_decode_invite_names_the_right_space() {
        let (_dir, state) = crate::commands::tests::fresh_state_for_invite_tests().await;
        let space_id = create_space_impl(&state, "General".to_string()).await;

        let token = generate_invite_impl(&state, &space_id).await;
        let decoded = decode_invite(&token).unwrap();

        assert_eq!(decoded.space_id, space_id);
    }

    #[tokio::test]
    async fn join_via_invite_adds_the_local_device_to_the_named_space() {
        let (_dir, state) = crate::commands::tests::fresh_state_for_invite_tests().await;
        let token = encode_invite(&InviteToken {
            space_id: "space-from-someone-else".to_string(),
            issued_at_unix_ms: 0,
        });

        let joined = join_via_invite_impl(&state, &token, "Bob").await.unwrap();

        assert_eq!(joined, "space-from-someone-else");
        assert!(state
            .membership
            .lock()
            .unwrap()
            .members("space-from-someone-else")
            .contains(&state.local_device));
    }
}

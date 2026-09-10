use crate::error::TransportError;
use serde::{Deserialize, Serialize};

const INVITE_SCHEME_PREFIX: &str = "spacechat://join/";

/// Everything a link or QR code needs to encode for the pairing flow: the
/// inviting device's iroh endpoint ID and enough context to request joining
/// a specific space. Per this plan's Global Constraints, turning
/// `encode_link`'s output into an actual scannable QR image is a UI-layer
/// concern (Milestone 4) — this type only produces/parses the string a QR
/// code would encode.
///
/// `endpoint_id` is stored as raw bytes (not `iroh::EndpointId`) so that
/// (de)serialization never has to reconstruct a `PublicKey` — and thus
/// never hits `iroh::PublicKey::from_bytes`'s fallible validation — inside
/// this type. Callers that need an `iroh::EndpointId` (e.g. Task 10's
/// `Transport::join_via_invite`) are expected to do that fallible
/// conversion themselves and map failures into their own `TransportError`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Invite {
    pub endpoint_id: [u8; 32],
    pub space_id: String,
    /// Opaque, caller-supplied token (e.g. a short-lived random nonce).
    /// This crate never interprets it — expiry/single-use validation is a
    /// future layer's job (see this task's Interfaces note).
    pub join_token: String,
}

impl Invite {
    /// Hex-encodes a CBOR-serialized `Invite` behind a `spacechat://join/`
    /// scheme, matching the transport spec's "link or QR code encoding its
    /// iroh endpoint ID and enough context" description. Hex (not
    /// base32/base64) to avoid a new dependency, consistent with how
    /// `space_chat_core::segment::objid_to_target_string` already encodes
    /// binary data for this codebase.
    pub fn encode_link(&self) -> String {
        let mut cbor = Vec::new();
        ciborium::into_writer(self, &mut cbor).expect("Invite always CBOR-encodes");
        let hex: String = cbor.iter().map(|b| format!("{b:02x}")).collect();
        format!("{INVITE_SCHEME_PREFIX}{hex}")
    }

    pub fn decode_link(link: &str) -> Result<Self, TransportError> {
        let hex = link.strip_prefix(INVITE_SCHEME_PREFIX).ok_or_else(|| {
            TransportError::Codec(format!("missing {INVITE_SCHEME_PREFIX} scheme"))
        })?;
        if hex.len() % 2 != 0 {
            return Err(TransportError::Codec("odd-length hex".to_string()));
        }
        // Guard against non-ASCII/multi-byte UTF-8 content *before* doing any
        // byte-offset slicing below: `&str` indexing panics (rather than
        // erroring) if a slice boundary falls inside a multi-byte character,
        // and the length check above only ensures an even *byte* length, not
        // that every 2-byte step lands on a char boundary. Validating that
        // every byte is an ASCII hex digit up front rules that out entirely,
        // since ASCII bytes are always single-byte UTF-8 characters.
        if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(TransportError::Codec(
                "non-hex-digit character in payload".to_string(),
            ));
        }
        let cbor = (0..hex.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&hex[i..i + 2], 16)
                    .map_err(|e| TransportError::Codec(e.to_string()))
            })
            .collect::<Result<Vec<u8>, TransportError>>()?;
        ciborium::from_reader(cbor.as_slice()).map_err(|e| TransportError::Codec(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invite_round_trips_through_its_link_encoding() {
        let invite = Invite {
            endpoint_id: [7u8; 32],
            space_id: "space-1".to_string(),
            join_token: "nonce-abc123".to_string(),
        };
        let link = invite.encode_link();
        assert!(link.starts_with("spacechat://join/"));

        let decoded = Invite::decode_link(&link).unwrap();
        assert_eq!(decoded, invite);
    }

    #[test]
    fn decode_link_rejects_a_missing_scheme() {
        assert!(Invite::decode_link("not-a-link-at-all").is_err());
    }

    #[test]
    fn decode_link_rejects_malformed_hex_after_the_scheme() {
        assert!(Invite::decode_link("spacechat://join/not-hex-zz").is_err());
    }

    /// Reproduces a reviewer-found panic: the payload `"aé1"` is 4 *bytes*
    /// long (`a` = 1 byte, `é` = 2 bytes, `1` = 1 byte), so it passes the
    /// even-byte-length check, but stepping through it 2 bytes at a time and
    /// slicing the `&str` (`hex[1..3]`) lands inside `é`'s 2-byte UTF-8
    /// encoding, which panics with "byte index 2 is not a char boundary"
    /// instead of returning an error. This must return `Err`, never panic,
    /// since `decode_link` parses attacker/QR-scan-controlled input.
    #[test]
    fn decode_link_rejects_multibyte_utf8_in_payload_without_panicking() {
        let link = format!("{INVITE_SCHEME_PREFIX}aé1");
        let result = Invite::decode_link(&link);
        assert!(matches!(result, Err(TransportError::Codec(_))));
    }

    /// Same shape of bug, but with the multi-byte character positioned so it
    /// straddles a *different* 2-byte step boundary, to make sure the fix
    /// isn't accidentally specific to one offset.
    #[test]
    fn decode_link_rejects_multibyte_utf8_at_various_offsets_without_panicking() {
        for payload in ["é1", "1é", "aaé", "éaa", "aéaé"] {
            let link = format!("{INVITE_SCHEME_PREFIX}{payload}");
            let result = Invite::decode_link(&link);
            assert!(
                matches!(result, Err(TransportError::Codec(_))),
                "payload {payload:?} should be rejected, not panic"
            );
        }
    }

    /// `encode_link` CBOR-encodes and then hex-encodes, both of which are
    /// binary-safe operations, so unicode content in `space_id`/`join_token`
    /// should already round-trip cleanly. This confirms that's actually true
    /// (as opposed to merely assumed) rather than only exercising ASCII, as
    /// the pre-existing round-trip test did.
    #[test]
    fn invite_round_trips_unicode_space_id_and_join_token() {
        let invite = Invite {
            endpoint_id: [9u8; 32],
            space_id: "spacé-日本語-🚀".to_string(),
            join_token: "nonce-é-日本語-🎉".to_string(),
        };
        let link = invite.encode_link();

        let decoded = Invite::decode_link(&link).unwrap();
        assert_eq!(decoded, invite);
    }
}

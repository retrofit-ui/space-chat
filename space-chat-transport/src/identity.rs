/// This device's transport-layer identity: an `iroh` endpoint keypair.
///
/// Deliberately a separate keypair from the MLS device signing key (see the
/// transport spec's Identity section) — different crypto hygiene domains,
/// even though both represent "this device." This type has no idea an MLS
/// key exists; a future composition root (`space-chat-app`) is expected to
/// persist both keys together as one on-disk device identity, but that
/// pairing decision doesn't belong here.
pub struct TransportIdentity {
    secret_key: iroh::SecretKey,
}

impl TransportIdentity {
    /// Generates a fresh random identity via `iroh::SecretKey::generate()`,
    /// which uses the OS RNG internally — not derived from, or related to,
    /// any MLS key material.
    pub fn generate() -> Self {
        Self {
            secret_key: iroh::SecretKey::generate(),
        }
    }

    /// Restores a previously-generated identity from its raw 32-byte
    /// secret. Persistence itself (where these bytes live on disk) is out
    /// of scope here — a storage-layer or app-shell concern.
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self {
            secret_key: iroh::SecretKey::from_bytes(bytes),
        }
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        self.secret_key.to_bytes()
    }

    /// This device's iroh endpoint ID — the public key other devices dial
    /// to reach it, and the value encoded into invite links (Task 3).
    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.secret_key.public()
    }

    /// Exposes the raw `iroh::SecretKey` to other modules in this crate
    /// (e.g. the endpoint builder in a later task, which binds with it).
    /// Unused for now — Task 1 only produces `TransportIdentity` itself —
    /// hence the explicit `allow`; later tasks are expected to call this.
    #[allow(dead_code)]
    pub(crate) fn secret_key(&self) -> iroh::SecretKey {
        self.secret_key.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_identities_have_distinct_endpoint_ids() {
        let a = TransportIdentity::generate();
        let b = TransportIdentity::generate();
        assert_ne!(a.endpoint_id(), b.endpoint_id());
    }

    #[test]
    fn identity_round_trips_through_bytes() {
        let original = TransportIdentity::generate();
        let bytes = original.to_bytes();
        let restored = TransportIdentity::from_bytes(&bytes);
        assert_eq!(original.endpoint_id(), restored.endpoint_id());
    }
}

use space_chat_core::storage::AttachmentBlobStore;

pub enum AttachmentResponse {
    Found(Vec<u8>, String),
    Placeholder,
    NotAHash,
}

impl std::fmt::Debug for AttachmentResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttachmentResponse::Found(bytes, mime) => {
                write!(f, "Found({} bytes, {mime:?})", bytes.len())
            }
            AttachmentResponse::Placeholder => write!(f, "Placeholder"),
            AttachmentResponse::NotAHash => write!(f, "NotAHash"),
        }
    }
}

impl PartialEq for AttachmentResponse {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (AttachmentResponse::Placeholder, AttachmentResponse::Placeholder)
                | (AttachmentResponse::NotAHash, AttachmentResponse::NotAHash)
        ) || matches!((self, other), (AttachmentResponse::Found(a, am), AttachmentResponse::Found(b, bm)) if a == b && am == bm)
    }
}

/// Parses the `<hash>` component out of a `spacechat://attachment/<hash>`
/// request's path, expecting 64 lowercase-or-uppercase hex characters.
pub fn parse_attachment_hash(uri_path: &str) -> Option<[u8; 32]> {
    let trimmed = uri_path.trim_start_matches('/');
    if trimmed.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(trimmed.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

/// The pure core of the protocol handler: cache hit -> the bytes (MIME type
/// is not tracked by `AttachmentBlobStore` itself, so this returns a generic
/// `application/octet-stream` -- refining this to the real MIME from
/// `AttachmentMetadataStore` is a natural follow-up, not attempted in this
/// plan's first pass); cache miss -> `Placeholder`, returned immediately, per
/// this plan's lazy-fetch decision -- never blocks on a network fetch.
pub fn handle_attachment_request(store: &dyn AttachmentBlobStore, hash: &[u8; 32]) -> AttachmentResponse {
    match store.load_attachment(hash) {
        Ok(Some(bytes)) => AttachmentResponse::Found(bytes, "application/octet-stream".to_string()),
        Ok(None) | Err(_) => AttachmentResponse::Placeholder,
    }
}

const PLACEHOLDER_BYTES: &[u8] = include_bytes!("../assets/attachment-placeholder.png");

/// Registers the `spacechat://` scheme on the given Tauri builder. Does NOT
/// take an `Arc<AppState>` parameter -- `AppState` doesn't exist yet at the
/// point in `run()` where builder methods are chained (it's constructed
/// asynchronously inside `.setup()`, per Task 8). Instead, every request
/// looks up the already-`.manage()`d state fresh via the handler's own
/// `UriSchemeContext::app_handle()` -- safe because no URI scheme request
/// can arrive before `.setup()` has run (the webview that would make one
/// doesn't load until after `.setup()` completes and calls `.manage()`).
pub fn register_attachment_protocol(builder: tauri::Builder<tauri::Wry>) -> tauri::Builder<tauri::Wry> {
    builder.register_asynchronous_uri_scheme_protocol("spacechat", move |ctx, request, responder| {
        use tauri::Manager;
        let app_handle = ctx.app_handle().clone();
        let path = request.uri().path().to_string();

        tauri::async_runtime::spawn(async move {
            let response = match parse_attachment_hash(&path) {
                None => AttachmentResponse::NotAHash,
                Some(hash) => {
                    let state = app_handle.state::<std::sync::Arc<crate::state::AppState>>();
                    let store = state.attachment_store.lock().unwrap();
                    handle_attachment_request(&*store, &hash)
                }
            };

            let (status, body, mime): (u16, Vec<u8>, &str) = match response {
                AttachmentResponse::Found(bytes, mime) => (200, bytes, Box::leak(mime.into_boxed_str())),
                AttachmentResponse::Placeholder => (200, PLACEHOLDER_BYTES.to_vec(), "image/png"),
                AttachmentResponse::NotAHash => (400, Vec::new(), "text/plain"),
            };

            let http_response = tauri::http::Response::builder()
                .status(status)
                .header("Content-Type", mime)
                .body(body)
                .unwrap();
            responder.respond(http_response);
        });
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use space_chat_core::storage::{AttachmentBlobStore, StorageError};
    use std::collections::HashMap;

    #[derive(Default)]
    struct FakeAttachmentStore {
        data: HashMap<[u8; 32], Vec<u8>>,
    }

    impl AttachmentBlobStore for FakeAttachmentStore {
        fn save_attachment(&mut self, hash: &[u8; 32], bytes: &[u8]) -> Result<(), StorageError> {
            self.data.insert(*hash, bytes.to_vec());
            Ok(())
        }
        fn load_attachment(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(hash).cloned())
        }
        fn delete_attachment(&mut self, hash: &[u8; 32]) -> Result<(), StorageError> {
            self.data.remove(hash);
            Ok(())
        }
    }

    #[test]
    fn parse_attachment_hash_decodes_a_well_formed_hex_path() {
        let hex = "01".repeat(32);
        let parsed = parse_attachment_hash(&format!("/{hex}"));
        assert_eq!(parsed, Some([1u8; 32]));
    }

    #[test]
    fn parse_attachment_hash_rejects_malformed_paths() {
        assert_eq!(parse_attachment_hash("/not-hex-at-all"), None);
        assert_eq!(parse_attachment_hash("/0102"), None); // too short
        assert_eq!(parse_attachment_hash(""), None);
    }

    #[test]
    fn a_cached_attachment_returns_found_with_its_bytes() {
        let mut store = FakeAttachmentStore::default();
        store.save_attachment(&[7u8; 32], b"image bytes").unwrap();

        match handle_attachment_request(&store, &[7u8; 32]) {
            AttachmentResponse::Found(bytes, _mime) => assert_eq!(bytes, b"image bytes"),
            other => panic!("expected Found, got {other:?}"),
        }
    }

    #[test]
    fn a_cache_miss_returns_a_placeholder_immediately_not_an_error() {
        let store = FakeAttachmentStore::default();
        match handle_attachment_request(&store, &[9u8; 32]) {
            AttachmentResponse::Placeholder => {}
            other => panic!("expected Placeholder, got {other:?}"),
        }
    }
}

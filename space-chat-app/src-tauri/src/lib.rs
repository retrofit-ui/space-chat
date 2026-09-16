pub mod commands;
pub mod conversation_spec;
pub mod events;
pub mod live_spec;
pub mod membership;
pub mod network;
pub mod observed_at;
pub mod spec;
pub mod state;

use tauri::Manager;

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {name}! space-chat is running.")
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let data_dir = dirs_data_dir();
    let local_device = load_or_create_local_device_id(&data_dir);

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(move |app| {
            // TODO: this generates a fresh random TransportIdentity on every
            // launch instead of persisting/reloading one the way
            // `load_or_create_local_device_id` does for `local_device`. A
            // stable identity is needed for real peers to keep dialing the
            // same endpoint address across restarts, but `TransportIdentity`
            // has no serialization support yet in `space-chat-transport`
            // (only `TransportIdentity::generate()` exists, no
            // `to_bytes`/`from_bytes`). Fixing this needs either adding that
            // to `space-chat-transport` (out of scope here) or deriving one
            // deterministically from `local_device`'s bytes (a design
            // decision for a later task, not this one).
            let identity = space_chat_transport::identity::TransportIdentity::generate();
            let (network, events) = tauri::async_runtime::block_on(network::AppNetwork::bind(
                &identity,
                space_chat_transport::bootstrap::TransportConfig { relay: None },
            ))
            .expect("failed to bind AppNetwork");
            let app_state = state::AppState::new(&data_dir, local_device, network, events, app.handle().clone())
                .expect("failed to initialize AppState");
            app.manage(app_state);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            greet,
            commands::open_conversation,
            commands::close_conversation,
            commands::resync_conversation
        ])
        .run(tauri::generate_context!())
        .expect("error while running space-chat-app");
}

fn dirs_data_dir() -> std::path::PathBuf {
    std::env::var("SPACECHAT_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from(".spacechat-data"))
}

fn load_or_create_local_device_id(data_dir: &std::path::Path) -> space_chat_core::domain::DeviceId {
    let path = data_dir.join("device_id");
    if let Ok(bytes) = std::fs::read(&path) {
        if let Ok(id) = <[u8; 32]>::try_from(bytes.as_slice()) {
            return space_chat_core::domain::DeviceId(id);
        }
    }
    let mut id = [0u8; 32];
    for byte in id.iter_mut() {
        *byte = rand_byte();
    }
    let _ = std::fs::create_dir_all(data_dir);
    let _ = std::fs::write(&path, id);
    space_chat_core::domain::DeviceId(id)
}

fn rand_byte() -> u8 {
    use std::time::{SystemTime, UNIX_EPOCH};
    // Not cryptographically secure -- fine for a locally-generated device
    // identifier in this placeholder membership model (see Task 3); replace
    // alongside the rest of `space-chat-openmls`'s eventual real credential
    // generation.
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
    (nanos ^ (nanos >> 8)) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greet_includes_the_given_name() {
        assert_eq!(greet("Alice"), "Hello, Alice! space-chat is running.");
    }

    /// IPC-level test: dispatches `greet` through Tauri's real invoke pipeline
    /// (ACL/permission check + command routing), not just the bare Rust
    /// function. Uses `tauri::test::mock_builder()` with a `MockRuntime` and
    /// `tauri::test::get_ipc_response`, mirroring tauri's own doctest at
    /// `tauri::test::get_ipc_response` (src/test/mod.rs).
    #[test]
    fn greet_command_is_reachable_via_ipc() {
        let app = tauri::test::mock_builder()
            .invoke_handler(tauri::generate_handler![greet])
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .expect("failed to build mock app");
        let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
            .build()
            .expect("failed to build mock webview");

        let res = tauri::test::get_ipc_response(
            &webview,
            tauri::webview::InvokeRequest {
                cmd: "greet".into(),
                callback: tauri::ipc::CallbackFn(0),
                error: tauri::ipc::CallbackFn(1),
                url: if cfg!(any(windows, target_os = "android")) {
                    "http://tauri.localhost"
                } else {
                    "tauri://localhost"
                }
                .parse()
                .unwrap(),
                body: tauri::ipc::InvokeBody::Json(serde_json::json!({ "name": "Bob" })),
                headers: Default::default(),
                invoke_key: tauri::test::INVOKE_KEY.to_string(),
            },
        );

        assert_eq!(
            res.expect("greet command should be reachable over IPC")
                .deserialize::<String>()
                .unwrap(),
            "Hello, Bob! space-chat is running."
        );
    }
}

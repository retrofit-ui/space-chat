#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {name}! space-chat is running.")
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![greet])
        .run(tauri::generate_context!())
        .expect("error while running space-chat-app");
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

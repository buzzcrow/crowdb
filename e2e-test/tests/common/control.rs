// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_REQUEST: AtomicU64 = AtomicU64::new(0);

pub(crate) async fn node(action: &str, index: usize) -> Value {
    request(json!({"action": action, "node": index})).await
}

pub(crate) async fn configure_chunkdb(options: Value) -> Value {
    request(json!({"action": "configure-chunkdb", "node": 4, "options": options})).await
}

pub(crate) async fn request(body: Value) -> Value {
    request_with_timeout(body, Duration::from_secs(65)).await
}

pub(crate) async fn native_browser() -> Value {
    // Host dependency preparation (120s) plus the original native browser
    // scenario budget (180s). DOM and request assertion deadlines stay intact.
    request_with_timeout(json!({"action": "run-native-browser"}), Duration::from_secs(305)).await
}

pub(crate) async fn native_browser_with_journal(bucket: &str) -> Value {
    request_with_timeout(
        json!({"action":"run-native-browser", "journal_bucket":bucket}),
        Duration::from_secs(305),
    )
    .await
}

pub(crate) async fn native_browser_with_chunks(chunks: Value) -> Value {
    request_with_timeout(
        json!({"action":"run-native-browser", "chunks": chunks}),
        Duration::from_secs(305),
    )
    .await
}

pub(crate) async fn native_transition() -> Value {
    // Original production-policy browser budget plus host dependency preparation.
    request_with_timeout(
        json!({"action":"run-native-browser"}),
        Duration::from_secs(12 * 60 + 125),
    )
    .await
}

pub(crate) async fn native_inspection(chunks: Value) -> Value {
    // Fourteen original native cases retain 180s each, plus host preparation.
    request_with_timeout(
        json!({"action":"run-native-browser", "chunks": chunks}),
        Duration::from_secs(14 * 180 + 125),
    )
    .await
}

pub(crate) async fn recover_service(service: &str, signal: &str) -> Value {
    request_with_timeout(
        json!({"action": "recover-service", "node": 0, "service": service, "signal": signal}),
        Duration::from_secs(95),
    )
    .await
}

pub(crate) async fn monitor_failure(action: &str) -> Value {
    request_with_timeout(json!({"action": action, "node": 0}), Duration::from_secs(495)).await
}

async fn request_with_timeout(body: Value, timeout: Duration) -> Value {
    let directory = PathBuf::from(std::env::var("CROWDB_E2E_CONTROL").expect("owned lifecycle control"));
    let id = format!(
        "{}-{}",
        std::process::id(),
        NEXT_REQUEST.fetch_add(1, Ordering::Relaxed)
    );
    let path = directory.join(format!("{id}.request.json"));
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, body.to_string()).unwrap();
    std::fs::rename(temporary, path).unwrap();
    let response = directory.join(format!("{id}.response.json"));
    let deadline = Instant::now() + timeout;
    loop {
        if response.exists() {
            let response: Value = serde_json::from_slice(&std::fs::read(response).unwrap()).unwrap();
            assert_eq!(response["ok"], true, "fixture action failed: {response}");
            return response["result"].clone();
        }
        assert!(Instant::now() < deadline, "fixture action {body} timed out");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

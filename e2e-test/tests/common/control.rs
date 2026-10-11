// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_REQUEST: AtomicU64 = AtomicU64::new(0);

pub(crate) async fn node(action: &str, index: usize) -> Value {
    let directory = PathBuf::from(std::env::var("CROWDB_E2E_CONTROL").expect("owned lifecycle control"));
    let id = format!(
        "{}-{}",
        std::process::id(),
        NEXT_REQUEST.fetch_add(1, Ordering::Relaxed)
    );
    let path = directory.join(format!("{id}.request.json"));
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, json!({"action": action, "node": index}).to_string()).unwrap();
    std::fs::rename(temporary, path).unwrap();
    let response = directory.join(format!("{id}.response.json"));
    let deadline = Instant::now() + Duration::from_secs(65);
    loop {
        if response.exists() {
            let response: Value = serde_json::from_slice(&std::fs::read(response).unwrap()).unwrap();
            assert_eq!(response["ok"], true, "fixture action failed: {response}");
            return response["result"].clone();
        }
        assert!(
            Instant::now() < deadline,
            "fixture action {action} node {index} timed out"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

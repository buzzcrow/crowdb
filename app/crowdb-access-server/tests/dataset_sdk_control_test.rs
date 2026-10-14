// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
mod dataset;

use dataset::{TestDatasetHttp, TestStore};
use std::{path::Path, process::Command, sync::Arc};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_rust_consumer_uses_production_http_control_routes() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../sdk/Cargo.toml");
    let origin = test.origin.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new("pixi")
            .args(["run", "--", "cargo", "run", "--locked", "--manifest-path"])
            .arg(manifest)
            .args(["--example", "control"])
            .env("CROWDB_DATASET_ENDPOINT", origin)
            .env("CROWDB_DATASET_NAME", "sdk/control consumer")
            .env("CROWDB_DATASET_READ_TOKEN", "secret")
            .output()
            .expect("standalone SDK consumer starts")
    })
    .await
    .unwrap();
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    assert!(output.status.success(), "standalone SDK consumer failed");
    assert!(String::from_utf8_lossy(&output.stdout).contains("published snapshot"));
    test.stop().await;
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset.rs"]
mod dataset;
use dataset::{TestDatasetHttp, TestStore};
use std::{path::Path, process::Command, sync::Arc};
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_payload_consumer_streams_above_window_and_restores_progress() {
    let test = TestDatasetHttp::start_payload(Arc::new(TestStore::default())).await;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../sdk/Cargo.toml");
    let origin = test.origin.clone();
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().to_owned();
    tokio::task::spawn_blocking(move || {
        for phase in ["write", "verify"] {
            let output = Command::new("pixi")
                .args([
                    "run",
                    "--",
                    "cargo",
                    "run",
                    "--release",
                    "--locked",
                    "--manifest-path",
                ])
                .arg(&manifest)
                .args(["--example", "payload"])
                .env("CROWDB_DATASET_ENDPOINT", &origin)
                .env("CROWDB_DATASET_NAME", "payload/consumer")
                .env("CROWDB_DATASET_READ_TOKEN", "secret")
                .env("CROWDB_DATASET_CONSUMER_ROOT", &directory)
                .env("CROWDB_DATASET_PHASE", phase)
                .output()
                .unwrap();
            print!("{}", String::from_utf8_lossy(&output.stdout));
            eprint!("{}", String::from_utf8_lossy(&output.stderr));
            assert!(output.status.success(), "standalone payload phase {phase} failed");
        }
    })
    .await
    .unwrap();
    test.stop().await;
}

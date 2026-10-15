// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset.rs"]
mod dataset;
use dataset::{TestDatasetHttp, TestStore};
use std::{path::Path, process::Command, sync::Arc};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CROWDB_DATASET_PYTHON pointing to an installed wheel consumer"]
async fn installed_python_split_sdk_uses_the_real_http_listener() {
    let python = std::env::var("CROWDB_DATASET_PYTHON").expect("installed wheel Python path");
    let test = TestDatasetHttp::start_payload(Arc::new(TestStore::default())).await;
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../sdk/tests/split_consumer.py");
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("split_consumer.py");
    std::fs::copy(source, &script).unwrap();
    let origin = test.origin.clone();
    let output = tokio::task::spawn_blocking(move || {
        let _directory = directory;
        Command::new("pixi")
            .args(["run", "--"])
            .arg(python)
            .arg(&script)
            .current_dir(script.parent().unwrap())
            .env("CROWDB_DATASET_ENDPOINT", origin)
            .env("CROWDB_DATASET_NAME", "python/split consumer")
            .env("CROWDB_DATASET_READ_TOKEN", "secret")
            .output()
            .expect("installed Python consumer starts")
    })
    .await
    .unwrap();
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    assert!(
        output.status.success(),
        "installed Python Split SDK consumer failed"
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("installed Python Split SDK round trip passed"));
    test.stop().await;
}

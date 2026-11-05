// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{Config, Crowdbtree, PageStoreBackend, TreeRuntime, TreeRuntimeOptions};
use std::sync::Arc;

#[tokio::test]
async fn block_host_driver_survives_tree_and_cpu_runtime_then_joins_real_release() {
    let path = std::env::temp_dir().join(format!("crowdb-block-release-{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let tree = Arc::new(
        Crowdbtree::open(&Config {
            runtime: Some(runtime.clone()),
            path: Some(path.to_string_lossy().into_owned()),
            backend: PageStoreBackend::Block,
            block_size: 1 << 20,
            ..Default::default()
        })
        .unwrap(),
    );
    let service = tree.backend_release_service().unwrap();
    tree.apply_put(1, b"key", b"value").unwrap();
    tree.snapshot().unwrap();
    let mut snapshot = tree.snapshot_export_begin(64).unwrap();
    drop(tree);
    drop(runtime);
    assert!(!snapshot.read_owned(0).unwrap().bytes.is_empty());
    drop(snapshot);
    service.shutdown().await;
    service.shutdown().await;
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn memory_backend_does_not_start_a_release_driver() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    assert!(tree.backend_release_service().is_none());
}

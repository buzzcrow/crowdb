// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{
    AsyncCrowdbtree, CompactionOptions, Config, CtError, PageStoreBackend, TreeRuntime, TreeRuntimeOptions,
};
use std::sync::Arc;

#[tokio::test(flavor = "current_thread")]
async fn explicit_block_pass_is_send_and_runs_with_bounded_operation_slots() {
    fn require_send<T: Send>(_: T) {}
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-async-compaction");
    let runtime = Arc::new(
        TreeRuntime::new(TreeRuntimeOptions {
            workers: 1,
            ..Default::default()
        })
        .unwrap(),
    );
    let mut config = Config {
        runtime: Some(runtime),
        path: Some(directory.path().to_string_lossy().into_owned()),
        backend: PageStoreBackend::Block,
        block_size: 64 * 1024,
        frame_bytes: 4096,
        ..Default::default()
    };
    config.runtime_binding.cpu_queue_limit = 2;
    let tree = AsyncCrowdbtree::open(&config).unwrap();
    require_send(tree.compact_sparse_blocks());
    let report = tree.compact_sparse_blocks().await.unwrap();
    assert_eq!(report.status, Ok(()));
    assert_eq!(report.blocks_selected, 0);
    assert_eq!(tree.handle().durable_coverage(), 0);
    let service = tree.handle().backend_release_service().unwrap();
    drop(tree);
    drop(config);
    service.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_policy_rejects_invalid_threshold_and_skips_volatile_backend() {
    let volatile = AsyncCrowdbtree::open(&Config::default()).unwrap();
    let skipped = volatile.compact_sparse_blocks().await.unwrap();
    assert_eq!(skipped.status, Ok(()));
    assert_eq!(skipped.blocks_selected, 0);
    assert_eq!(skipped.blocks_deleted, 0);
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-async-compaction-invalid");
    let tree = AsyncCrowdbtree::open(&Config {
        runtime: Some(Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())),
        path: Some(directory.path().to_string_lossy().into_owned()),
        backend: PageStoreBackend::Block,
        block_size: 64 * 1024,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        tree.compact_sparse_blocks_with(CompactionOptions {
            free_threshold: f64::NAN,
            ..Default::default()
        })
        .await
        .unwrap_err(),
        CtError::InvalidArgument
    );
    let report = tree
        .compact_sparse_blocks_with(CompactionOptions {
            max_relocation_bytes: 0,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(report.status, Ok(()));
    assert_eq!(report.blocks_deleted, 0);
    let service = tree.handle().backend_release_service().unwrap();
    drop(tree);
    service.shutdown().await;
}

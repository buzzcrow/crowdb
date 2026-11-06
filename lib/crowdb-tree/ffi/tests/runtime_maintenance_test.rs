// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::sync::Arc;

use crowdb_tree_ffi::{
    AsyncCrowdbtree, Config, FlushPhase, PageStore, SyncMode, TreeRuntime, TreeRuntimeOptions,
};

fn tree() -> AsyncCrowdbtree {
    let runtime = Arc::new(
        TreeRuntime::new(TreeRuntimeOptions {
            workers: 1,
            ..Default::default()
        })
        .unwrap(),
    );
    AsyncCrowdbtree::open(&Config {
        runtime: Some(runtime),
        page_store: Some(Arc::new(PageStore::open_mem(4096).unwrap())),
        frame_bytes: 4096,
        max_inline_value: 64,
        sync_mode: SyncMode::Skip,
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn maintenance_futures_can_move_to_the_host_executor() {
    fn require_send<T: Send>(_: T) {}
    let tree = tree();
    require_send(tree.flush_report());
    require_send(tree.writeback_report());
    require_send(tree.checkpoint_report());
    require_send(tree.flush());
    require_send(tree.snapshot());
}

#[tokio::test(flavor = "current_thread")]
async fn flush_report_keeps_both_stages_and_actual_frontiers() {
    let tree = tree();
    tree.handle().apply_put(1, b"a", b"first").unwrap();
    tree.handle().apply_put(2, b"b", b"second").unwrap();
    let report = tree.flush_report().await.unwrap();
    assert_eq!(report.status, Ok(()));
    assert_eq!(report.handoff_status, Ok(()));
    assert_eq!(report.maintenance_status, Ok(()));
    assert!(report.handoff_completed);
    assert!(report.handoff_advanced);
    assert_eq!(report.floor, 0);
    assert_eq!(report.frontier, 2);
    assert_eq!(report.published_coverage, 2);
    assert_eq!(report.phase, FlushPhase::Complete);
    let repeated = tree.flush_report().await.unwrap();
    assert!(repeated.handoff_completed);
    assert!(!repeated.handoff_advanced);
    tree.flush().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn writeback_does_not_advance_the_durable_checkpoint() {
    let tree = tree();
    tree.handle().apply_put(1, b"key", &vec![b'v'; 8192]).unwrap();
    let written = tree.writeback_report().await.unwrap();
    assert_eq!(written.status, Ok(()));
    assert!(written.writeback_completed);
    assert!(!written.durable_committed);
    assert_eq!(written.captured_coverage, 1);
    assert_eq!(written.durable_coverage, 0);
    assert!(written.written_frames > written.primary_frames);
    assert!(written.rebased_pages > 0);
    let durable = tree.checkpoint_report().await.unwrap();
    assert_eq!(durable.status, Ok(()));
    assert!(durable.durable_committed);
    assert_eq!(durable.durable_coverage, 1);
    assert!(durable.snapshot_sequence > 0);
    assert_eq!(tree.snapshot().await.unwrap(), 1);
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{Config, Crowdbtree, PageStore, TreeRuntime};
use std::sync::Arc;

#[test]
fn collector_observes_local_backing_after_tree_close_without_pinning_storage() {
    let runtime = Arc::new(TreeRuntime::new(Default::default()).unwrap());
    let mut config = Config {
        runtime: Some(runtime.clone()),
        page_store: Some(Arc::new(PageStore::open_mem(4096).unwrap())),
        frame_bytes: 4096,
        max_inline_value: 64,
        ..Default::default()
    };
    let tree = Crowdbtree::open(&config).unwrap();
    tree.apply_put(1, b"key", &vec![b'v'; 9000]).unwrap();
    let (sequence, coverage) = tree.snapshot_info().unwrap();
    let snapshot = tree.immutable_snapshot(sequence, coverage).unwrap();
    let collector = tree.work_metrics_collector().unwrap();
    let before = tree.backing_stats().unwrap();
    assert_ne!(before.scope_id, 0);
    assert!(before.allocated_bytes > 0);
    assert_eq!(collector.backing_stats().unwrap(), before);
    drop(tree);
    let retained = collector.backing_stats().unwrap();
    assert_eq!(retained.scope_id, before.scope_id);
    assert!(retained.allocated_bytes > 0);
    assert_eq!(snapshot.get(b"key").unwrap().unwrap().1, vec![b'v'; 9000]);
    drop(snapshot);
    runtime.drain_cpu();
    assert_eq!(collector.backing_stats().unwrap().allocated_bytes, 0);
    // Host drain stops admission. A new host can claim the same backend while
    // the old sampler remains alive and owns no storage domain.
    config.runtime = Some(Arc::new(TreeRuntime::new(Default::default()).unwrap()));
    let reopened = Crowdbtree::open(&config).unwrap();
    assert_ne!(reopened.backing_stats().unwrap().scope_id, before.scope_id);
    drop(reopened);
    drop(config);
    drop(runtime);
    assert_eq!(collector.backing_stats().unwrap().allocated_bytes, 0);
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{Config, Crowdbtree, TreeRuntime};
use std::sync::Arc;

#[test]
fn inherited_source_outlives_parent_and_collector_outlives_child() {
    let runtime = Arc::new(TreeRuntime::new(Default::default()).unwrap());
    let config = Config {
        runtime: Some(runtime.clone()),
        frame_bytes: 4096,
        ..Default::default()
    };
    let parent = Crowdbtree::open(&config).unwrap();
    let child = Crowdbtree::open(&config).unwrap();
    parent.apply_put(1, b"key", &vec![b'v'; 4096]).unwrap();
    child.install_split_memtable_overlay(&parent, 1).unwrap();
    let collector = parent.work_metrics_collector().unwrap();
    drop(parent);
    assert_eq!(child.get(b"key").unwrap().unwrap().1, vec![b'v'; 4096]);
    // This is the terminal host boundary; no new reads are admitted after it.
    runtime.drain_cpu();
    let window = Arc::new(collector.capture().unwrap());
    let consumer = Arc::clone(&window);
    drop(child);
    runtime.drain_cpu();
    drop(config);
    drop(runtime);
    let closed = collector.capture().unwrap();
    assert!(closed.window_sequence > consumer.window_sequence);
    assert!(Arc::ptr_eq(&consumer, &window));
}

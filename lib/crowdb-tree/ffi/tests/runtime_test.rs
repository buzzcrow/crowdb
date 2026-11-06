// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_tree_ffi::{Config, Crowdbtree, CtError, PageStore, TreeRuntime, TreeRuntimeOptions};

#[test]
fn runtime_rejects_invalid_or_unfunded_storage() {
    let options = TreeRuntimeOptions {
        workers: 0,
        ..Default::default()
    };
    assert_eq!(TreeRuntime::new(options).unwrap_err(), CtError::InvalidArgument);
    let options = TreeRuntimeOptions {
        lifecycle_bytes: 1,
        ..Default::default()
    };
    assert_eq!(TreeRuntime::new(options).unwrap_err(), CtError::ResourceExhausted);
}

#[test]
fn runtime_snapshot_preserves_cumulative_work_after_tree_close() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let before = runtime.snapshot().unwrap();
    let config = Config {
        runtime: Some(runtime.clone()),
        ..Default::default()
    };
    let tree = Crowdbtree::open(&config).unwrap();
    tree.apply_put(1, b"key", b"value").unwrap();
    assert_eq!(tree.get(b"key").unwrap().unwrap().1, b"value");
    drop(tree);
    runtime.stop_admission();
    runtime.drain_cpu();
    let after = runtime.snapshot().unwrap();
    assert_eq!(after.bindings, 0);
    assert!(after.stopped);
    assert_eq!(after.cpu.queued, 0);
    assert_eq!(after.cpu.inflight, 0);
    assert!(after.cpu.completed > before.cpu.completed);
    assert!(after.cpu.execute_ns > before.cpu.execute_ns);
    assert!(after.budget_grants > before.budget_grants);
    assert_eq!(runtime.snapshot().unwrap(), after);
}

#[test]
fn shared_runtime_opens_owned_core_and_survives_caller_handle_drop() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let config = Config {
        runtime: Some(runtime.clone()),
        ..Default::default()
    };
    drop(runtime);
    let tree = Crowdbtree::open(&config).unwrap();
    tree.apply_put(1, b"key", b"value").unwrap();
    assert_eq!(tree.get(b"key").unwrap().unwrap().1, b"value");
    drop(tree);
    let clone = config.clone();
    drop(config);
    let child = std::thread::spawn(move || {
        let tree = Crowdbtree::open(&clone).unwrap();
        tree.apply_put(1, b"other", b"value").unwrap();
        drop(tree);
        clone.runtime.as_ref().unwrap().stop_admission();
        clone.runtime.as_ref().unwrap().drain_cpu();
    });
    child.join().unwrap();
}

#[test]
fn standalone_source_rebuilds_into_an_explicit_host() {
    let source = Crowdbtree::open(&Config::default()).unwrap();
    source.apply_put(1, b"key", b"value").unwrap();
    let config = Config {
        runtime: Some(Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())),
        page_store: Some(Arc::new(PageStore::open_mem(1).unwrap())),
        ..Default::default()
    };
    let (child, stats) = source.rebuild_range(&config).unwrap();
    assert_eq!(stats.entries_emitted, 1);
    assert!(stats.pages_rebuilt > 0);
    drop(source);
    assert_eq!(child.get(b"key").unwrap(), Some((1, b"value".to_vec())));
    assert_eq!(child.snapshot_state().unwrap().1, 1);
}

#[test]
fn standalone_custom_writer_limit_rejects_before_accepting() {
    let mut config = Config::default();
    config.runtime_binding.limits.writer_bytes = 1024 * 1024;
    let tree = Crowdbtree::open(&config).unwrap();
    assert_eq!(
        tree.apply_put(1, b"key", &vec![0; 2 * 1024 * 1024]),
        Err(CtError::ResourceExhausted)
    );
    assert_eq!(tree.stats().contiguous_slot, 0);
}

#[test]
fn standalone_binding_rejects_an_unfunded_open() {
    let mut config = Config::default();
    config.runtime_binding.lifecycle_bytes = 1;
    assert_eq!(Crowdbtree::open(&config).unwrap_err(), CtError::ResourceExhausted);
}

#[test]
fn standalone_live_and_snapshot_rebuilds_honor_destination_binding() {
    let source = Crowdbtree::open(&Config::default()).unwrap();
    source.apply_put(1, b"key", b"value").unwrap();
    let (generation, coverage) = source.snapshot_info().unwrap();
    let snapshot = source.immutable_snapshot(generation, coverage).unwrap();
    let mut destination = Config {
        page_store: Some(Arc::new(PageStore::open_mem(1).unwrap())),
        ..Default::default()
    };
    destination.runtime_binding.lifecycle_bytes = 1;
    assert_eq!(
        source.rebuild_range(&destination).unwrap_err(),
        CtError::ResourceExhausted
    );
    assert_eq!(
        snapshot.rebuild_range(&destination).unwrap_err(),
        CtError::ResourceExhausted
    );
    assert_eq!(source.get(b"key").unwrap(), Some((1, b"value".to_vec())));
}

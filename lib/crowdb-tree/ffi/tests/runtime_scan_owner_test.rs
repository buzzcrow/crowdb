// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_tree_ffi::{AsyncCrowdbtree, Config, Crowdbtree, TreeRuntime, TreeRuntimeOptions};
use std::sync::Arc;

fn tree() -> Crowdbtree {
    Crowdbtree::open(&Config {
        runtime: Some(Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())),
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn scan_owner_keeps_clones_and_slices_alive_after_tree_close() {
    let tree = tree();
    tree.apply_put(1, b"key", b"value").unwrap();
    let (entries, _) = tree.scan(b"", b"", b"", 10, 1024, false, 0, false).unwrap();
    assert_eq!(entries.len(), 1);
    let value = entries[0].value.slice(1..4);
    let clone = entries[0].value.clone();
    drop(entries);
    drop(tree);
    std::thread::spawn(move || {
        assert_eq!(&value[..], b"alu");
        assert_eq!(&clone[..], b"value");
    })
    .join()
    .unwrap();
}

#[test]
fn runtime_scan_preserves_empty_key_bounds_and_strict_budget() {
    let tree = tree();
    tree.apply_put(1, b"", b"empty").unwrap();
    tree.apply_put(2, b"a", b"value").unwrap();
    let (entries, _) = tree.scan_reverse(Some(b""), true, b"", 10, 1024).unwrap();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].key.is_empty());
    let (entries, _) = tree.scan_from_bounded(b"", false, b"", 10, 1024).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(&entries[0].key[..], b"a");
}

#[test]
fn reverse_seek_retains_the_runtime_scan_owner() {
    let tree = tree();
    tree.apply_put(1, b"key", b"value").unwrap();
    let found = tree.seek_reverse(b"key", true, b"").unwrap().unwrap();
    drop(tree);
    assert_eq!(&found.key[..], b"key");
    assert_eq!(&found.value[..], b"value");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_scan_owner_survives_async_tree_and_future_drop() {
    let tree = AsyncCrowdbtree::from_sync(tree());
    tree.handle().apply_put(1, b"key", b"value").unwrap();
    let (entries, _) = tree
        .scan(Vec::new(), Vec::new(), Vec::new(), 10, 1024, false, 0)
        .await
        .unwrap();
    let key = entries[0].key.clone();
    drop(entries);
    drop(tree);
    assert_eq!(&key[..], b"key");
}

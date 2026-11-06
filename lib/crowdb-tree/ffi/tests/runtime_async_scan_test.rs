// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_tree_ffi::{
    AsyncCrowdbtree, Config, Crowdbtree, CtError, ScanDirection, ScanOptions, ScanOutcome, TreeRuntime,
    TreeRuntimeOptions,
};
use std::sync::Arc;

fn tree() -> AsyncCrowdbtree {
    let tree = Crowdbtree::open(&Config {
        runtime: Some(Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())),
        ..Default::default()
    })
    .unwrap();
    tree.apply_put(1, b"", b"empty").unwrap();
    tree.apply_put(2, b"a", b"value").unwrap();
    AsyncCrowdbtree::from_sync(tree)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_empty_cursor_advances_bounded_async_windows() {
    let tree = tree();
    let (first, truncated) = tree
        .scan_with_options(ScanOptions {
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(truncated);
    assert_eq!(first.len(), 1);
    assert!(first[0].key.is_empty());
    let cursor = first[0].key.to_vec();
    let (second, truncated) = tree
        .scan_with_options(ScanOptions {
            start: Some(cursor),
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(second.len(), 1);
    assert_eq!(&second[0].key[..], b"a");
    let value = second[0].value.slice(1..4);
    drop(second);
    drop(first);
    drop(tree);
    assert_eq!(&value[..], b"alu");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immediate_scan_supports_an_inclusive_empty_reverse_bound() {
    let tree = tree();
    let outcome = tree.try_scan_with_options(ScanOptions {
        start: Some(Vec::new()),
        start_inclusive: true,
        direction: ScanDirection::Reverse,
        ..Default::default()
    });
    let (entries, truncated) = match outcome {
        ScanOutcome::Ready(result) => result,
        ScanOutcome::Pending(future) => future.await,
    }
    .unwrap();
    assert!(!truncated);
    assert_eq!(entries.len(), 1);
    assert!(entries[0].key.is_empty());
    drop(tree);
    assert_eq!(&entries[0].value[..], b"empty");
}

#[tokio::test]
async fn explicit_scan_rejects_invalid_budget_and_supports_standalone_tree() {
    let tree = tree();
    assert_eq!(
        tree.scan_with_options(ScanOptions {
            strict_byte_budget: true,
            ..Default::default()
        })
        .await
        .unwrap_err(),
        CtError::InvalidArgument
    );
    let standalone = AsyncCrowdbtree::open(&Config::default()).unwrap();
    standalone.handle().apply_put(1, b"default", b"value").unwrap();
    let (entries, truncated) = standalone
        .scan_with_options(ScanOptions::default())
        .await
        .unwrap();
    assert!(!truncated);
    drop(standalone);
    assert_eq!(entries.len(), 1);
    assert_eq!(&entries[0].key[..], b"default");
    assert_eq!(&entries[0].value[..], b"value");
}

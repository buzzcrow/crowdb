// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{
    AsyncCrowdbtree, Config, Crowdbtree, OwnedGetOutcome, TreeRuntime, TreeRuntimeOptions,
};
use std::{alloc::Layout, sync::Arc};
fn tree() -> (Arc<TreeRuntime>, Arc<Crowdbtree>) {
    let runtime = Arc::new(
        TreeRuntime::new(TreeRuntimeOptions {
            workers: 1,
            ..Default::default()
        })
        .unwrap(),
    );
    let tree = Arc::new(
        Crowdbtree::open(&Config {
            runtime: Some(runtime.clone()),
            ..Default::default()
        })
        .unwrap(),
    );
    tree.apply_put(1, b"key", b"value").unwrap();
    (runtime, tree)
}
fn accounted(bytes: &bytes::Bytes) {
    assert!(
        bytes.owner_allocation().is_some(),
        "runtime output lost its allocation account"
    );
    let too_large = Layout::from_size_align(isize::MAX as usize, 1).unwrap();
    assert!(bytes.try_owner_allocation(too_large).is_err());
}
#[tokio::test(flavor = "current_thread")]
async fn owned_get_retains_paid_bytes_after_tree_and_runtime_close() {
    let (runtime, tree) = tree();
    let asynchronous = AsyncCrowdbtree::from_sync(Arc::try_unwrap(tree).ok().unwrap());
    let value = match asynchronous.try_get_owned(b"key") {
        OwnedGetOutcome::Ready(value) => value,
        OwnedGetOutcome::Pending(value) => value.await,
    }
    .unwrap()
    .unwrap()
    .1;
    accounted(&value);
    let slice = value.slice(1..4);
    drop(value);
    drop(asynchronous);
    drop(runtime);
    assert_eq!(&slice[..], b"alu");
    accounted(&slice);
}
#[test]
fn packed_scan_clones_and_slices_keep_the_paid_control_owner() {
    let (runtime, tree) = tree();
    let (entries, _) = tree.scan(b"", b"", b"", 10, 1024, false, 0, false).unwrap();
    accounted(&entries[0].key);
    accounted(&entries[0].value);
    let value = entries[0].value.clone();
    drop(entries);
    drop(tree);
    drop(runtime);
    assert_eq!(&value[..], b"value");
    accounted(&value);
}
#[test]
fn snapshot_window_retains_its_own_control_budget_after_stream_close() {
    let (runtime, tree) = tree();
    tree.flush().unwrap();
    let mut export = tree.snapshot_export_begin(64).unwrap();
    let chunk = export.read_owned(0).unwrap();
    accounted(&chunk.bytes);
    let clone = chunk.bytes.clone();
    let expected = clone.to_vec();
    drop(chunk);
    drop(export);
    drop(tree);
    drop(runtime);
    assert_eq!(&clone[..], expected);
    accounted(&clone);
}

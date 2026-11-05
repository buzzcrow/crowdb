// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{Config, Crowdbtree, CtError, TreeRuntime, TreeRuntimeOptions};
use std::sync::Arc;

#[test]
fn retained_chunk_slices_survive_the_stream_tree_and_runtime() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let tree = Arc::new(
        Crowdbtree::open(&Config {
            runtime: Some(Arc::clone(&runtime)),
            ..Default::default()
        })
        .unwrap(),
    );
    tree.apply_put(1, b"key", &vec![b'v'; 8192]).unwrap();
    tree.flush().unwrap();
    let mut export = tree.snapshot_export_begin(64).unwrap();
    let first = export.read_owned(0).unwrap();
    assert_eq!(first.bytes.len(), 64);
    assert!(!first.done);
    let retained = first.bytes.slice(2..32);
    let expected = retained.to_vec();
    let copied = export.read(64).unwrap();
    assert_eq!(copied.bytes.len(), 64);
    assert_eq!(export.offset(), 128);
    drop(first);
    drop(export);
    drop(tree);
    drop(runtime);
    assert_eq!(retained.as_ref(), expected);
    let cloned = retained.clone();
    drop(retained);
    assert_eq!(cloned.as_ref(), expected);
}

#[test]
fn invalid_offset_preserves_the_next_owned_chunk() {
    let tree = Arc::new(Crowdbtree::open(&Config::default()).unwrap());
    tree.apply_put(1, b"key", b"value").unwrap();
    tree.flush().unwrap();
    let mut export = tree.snapshot_export_begin(64).unwrap();
    assert_eq!(export.read_owned(1).unwrap_err(), CtError::InvalidArgument);
    assert_eq!(export.offset(), 0);
    assert!(!export.read_owned(0).unwrap().bytes.is_empty());
}

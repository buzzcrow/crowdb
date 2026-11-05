// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{CrowdbPartitionTree, MutationOperation, PartitionTree};
use crowdb_tree_ffi::{Config, PageStore};
use std::sync::Arc;

async fn tree() -> CrowdbPartitionTree {
    let tree = CrowdbPartitionTree::open(
        43,
        &Config {
            page_store: Some(Arc::new(PageStore::open_mem(4096).unwrap())),
            frame_bytes: 4096,
            ..Config::default()
        },
    )
    .unwrap();
    for (slot, key) in [(1, b"".as_slice()), (2, b"a".as_slice())] {
        tree.apply(
            slot,
            &MutationOperation::Put {
                key: key.to_vec(),
                value: b"value".to_vec(),
            },
        )
        .await
        .unwrap();
    }
    tree
}

async fn check_empty_key_continuation(tree: &dyn PartitionTree) {
    for (limit, budget) in [(0, 0), (1, 1024)] {
        let empty = tree
            .scan_forward(None, true, Some(b""), limit, budget)
            .await
            .unwrap();
        assert_eq!(empty, (Vec::new(), false));
        let empty = tree
            .scan_forward_bounded(None, true, Some(b""), limit, budget)
            .await
            .unwrap();
        assert_eq!(empty, (Vec::new(), false));
    }
    let (first, more) = tree.scan_forward(None, true, None, 1, 1024).await.unwrap();
    assert!(more);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].key.as_ref(), b"");
    let (next, more) = tree
        .scan_forward(Some(first[0].key.as_ref()), false, None, 1, 1024)
        .await
        .unwrap();
    assert!(!more);
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].key.as_ref(), b"a");
    let (inclusive, more) = tree.scan_forward(Some(b""), true, None, 1, 1024).await.unwrap();
    assert!(more);
    assert_eq!(inclusive.len(), 1);
    assert_eq!(inclusive[0].key.as_ref(), b"");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_default_tree_resumes_exclusively_after_an_empty_key() {
    let tree = tree().await;
    check_empty_key_continuation(&tree).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn captured_default_tree_preserves_the_empty_key_cursor_after_writer_close() {
    let tree = tree().await;
    let (_, coverage, snapshot) = tree.checkpoint_snapshot(0).await.unwrap();
    assert_eq!(coverage, 2);
    drop(tree);
    check_empty_key_continuation(snapshot.as_ref()).await;
}

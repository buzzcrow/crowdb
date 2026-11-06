// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_chunk_kv::{ChunkKvError, CrowdbPartitionTree, MutationOperation, PartitionRange, PartitionTree};
use crowdb_tree_ffi::{Config, PageStore, TreeRuntime, TreeRuntimeOptions};
use std::sync::Arc;

fn config(runtime: Arc<TreeRuntime>) -> Config {
    Config {
        runtime: Some(runtime),
        page_store: Some(Arc::new(PageStore::open_mem(4096).unwrap())),
        frame_bytes: 4096,
        max_inline_value: 64,
        ..Config::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_split_base_is_read_only_and_rebuilds_after_source_writer_drop() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let tree = CrowdbPartitionTree::open(41, &config(runtime.clone())).unwrap();
    tree.apply(
        1,
        &MutationOperation::Put {
            key: b"a".to_vec(),
            value: b"left".to_vec(),
        },
    )
    .await
    .unwrap();
    tree.apply(
        2,
        &MutationOperation::Put {
            key: b"z".to_vec(),
            value: vec![b'v'; 9000],
        },
    )
    .await
    .unwrap();
    let (generation, coverage, snapshot) = tree.checkpoint_snapshot(0).await.unwrap();
    assert_eq!(coverage, 2);
    tree.apply(
        3,
        &MutationOperation::Put {
            key: b"z".to_vec(),
            value: b"new".to_vec(),
        },
    )
    .await
    .unwrap();
    drop(tree);
    assert_eq!(snapshot.checkpoint_state().unwrap(), (generation, coverage));
    assert!(matches!(
        snapshot.advance_noop(3).await,
        Err(ChunkKvError::InvalidRequest(_))
    ));
    assert!(matches!(
        snapshot
            .apply(3, &MutationOperation::Delete { key: b"z".to_vec() })
            .await,
        Err(ChunkKvError::InvalidRequest(_))
    ));
    assert!(matches!(
        snapshot.checkpoint(0).await,
        Err(ChunkKvError::InvalidRequest(_))
    ));
    assert_eq!(snapshot.get(b"z").await.unwrap().unwrap().value, vec![b'v'; 9000]);
    let (entries, _) = snapshot.scan_reverse(None, None, 8, 0).await.unwrap();
    assert_eq!(entries[0].key.as_ref(), b"z");
    let (child, stats) = snapshot
        .rebuild_range(
            42,
            &PartitionRange {
                start: Some(b"m".to_vec()),
                end: None,
            },
            config(runtime),
        )
        .await
        .unwrap();
    assert_eq!(child.last_applied_seq(), coverage);
    assert_eq!(stats.entries_emitted, 1);
    assert_eq!(child.get(b"z").await.unwrap().unwrap().value, vec![b'v'; 9000]);
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{CrowdbPartitionTree, MutationOperation, PartitionTree};
use crowdb_tree_ffi::{ChunkPageStoreOptions, ChunkRootCatalog, Config, PageStore};
use std::sync::Arc;

#[tokio::test]
async fn native_checkpoint_drives_chunk_io_completions_and_publishes_the_exact_frontier() {
    let store = Arc::new(
        PageStore::open_chunk(
            ChunkPageStoreOptions {
                tree_id: 91,
                owner_epoch: 1,
                open_generation: 0,
                pack_bytes: 65_501,
                iu_size: 1,
                max_concurrent_packs: 4,
                materialization_bytes_per_pass: 1024 * 1024,
                max_chunk_bytes: 1024 * 1024,
                mirror_copies: 0,
            },
            Arc::new(ChunkRootCatalog::open_memory(1).unwrap()),
            None,
        )
        .unwrap(),
    );
    let config = Config {
        page_store: Some(store.clone()),
        ..Config::default()
    };
    let tree = CrowdbPartitionTree::open(91, &config).unwrap();
    for sequence in 1_u64..=32 {
        tree.apply(
            sequence,
            &MutationOperation::Put {
                key: sequence.to_be_bytes().to_vec(),
                value: vec![u8::try_from(sequence % 251).unwrap(); 64 * 1024],
            },
        )
        .await
        .unwrap();
    }
    assert_eq!(store.chunk_stats().unwrap().completion_wakeups, 0);
    let (generation, applied) = tree.checkpoint(4096).await.unwrap();
    assert!(generation > 0);
    assert_eq!(applied, 32);
    assert_eq!(store.wal_replay_offset().unwrap(), 4096);
    assert!(
        store.chunk_stats().unwrap().completion_wakeups > 0,
        "native checkpoints must drive the asynchronous chunk IO completion path"
    );
    assert!(store.chunk_estimated_bytes().unwrap() >= 2 * 1024 * 1024);
    drop(tree);

    let recovered = CrowdbPartitionTree::open(91, &config).unwrap();
    assert_eq!(recovered.checkpoint_state().unwrap(), (generation, applied));
    for sequence in [1_u64, 16, 32] {
        let record = recovered.get(&sequence.to_be_bytes()).await.unwrap().unwrap();
        assert_eq!(record.revision, sequence);
        assert_eq!(
            record.value,
            vec![u8::try_from(sequence % 251).unwrap(); 64 * 1024]
        );
    }
}

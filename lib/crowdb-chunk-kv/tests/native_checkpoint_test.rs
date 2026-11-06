// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{CrowdbPartitionTree, MutationOperation, PartitionTree};
use crowdb_tree_ffi::chunk::test_util::TestChunkBackend;

async fn check_native_checkpoint(frame_bytes: u32, reclaim_bytes: usize) {
    let mut backend = TestChunkBackend::new(91, 1).unwrap();
    let store = backend.page_store(0).unwrap();
    let mut config = backend.config();
    config.page_store = Some(store.clone());
    config.frame_bytes = frame_bytes;
    config.runtime_binding.limits.reclaim_bytes = reclaim_bytes;
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
    let completed_before = backend.runtime.snapshot().unwrap().backend.completed;
    let (generation, applied) = tree.checkpoint(4096).await.unwrap();
    assert!(generation > 0);
    assert_eq!(applied, 32);
    assert_eq!(store.wal_replay_offset().unwrap(), 4096);
    assert!(
        backend.runtime.snapshot().unwrap().backend.completed > completed_before,
        "native checkpoints must drive the asynchronous chunk IO completion path"
    );
    assert!(tree.backing_stats().unwrap().unwrap().allocated_bytes >= 2 * 1024 * 1024);
    drop(tree);
    drop(config);
    drop(store);
    backend.next_writer().unwrap();
    let mut config = backend.config();
    config.page_store = Some(backend.page_store(0).unwrap());
    config.frame_bytes = frame_bytes;
    config.runtime_binding.limits.reclaim_bytes = reclaim_bytes;

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

#[tokio::test]
async fn native_checkpoint_drives_chunk_io_completions_and_publishes_the_exact_frontier() {
    check_native_checkpoint(64 * 1024, 64 * 1024 * 1024).await;
}

#[tokio::test]
async fn small_frames_checkpoint_with_sufficient_host_pin_capacity() {
    check_native_checkpoint(4096, 64 * 1024 * 1024).await;
}

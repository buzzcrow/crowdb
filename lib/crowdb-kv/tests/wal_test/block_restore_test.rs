// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_kv::paxos::roles::PxBallot;
use crowdb_kv::wal::record::WALRecord;
use crowdb_kv::wal::replay::replay_group;
use crowdb_kv::wal::segment::WalSegment;
use crowdb_kv::wal::{IoBackend, OpenOptions};

#[tokio::test]
async fn direct_block_replay_recovers_sealed_and_unsealed_records() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("block-wal-replay");
    let backend = Arc::new(IoBackend::block_device());
    let group = root.path().join("group1");
    for id in 1..=2 {
        let mut segment = WalSegment::create(&backend, &group, id, 1).await.unwrap();
        segment
            .append(&WALRecord::from_promised(1, id, id, PxBallot::new(0, 1)))
            .await
            .unwrap();
        if id == 1 {
            segment.seal().await.unwrap();
        } else {
            segment.fdatasync().await.unwrap();
        }
    }
    let recovered = replay_group(&backend, &[root.path().to_path_buf()], 1)
        .await
        .unwrap();
    assert_eq!(recovered.records.len(), 2);
    assert_eq!(recovered.current_term, 2);
}

#[tokio::test]
async fn direct_block_reads_unaligned_ranges_and_eof() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("block-wal-read");
    let backend = IoBackend::block_device();
    let path = root.path().join("segment");
    let mut file = backend.open(&path, OpenOptions::create_rw()).await.unwrap();
    let bytes: Vec<u8> = (0..8192)
        .map(|offset| u8::try_from(offset % 251).unwrap())
        .collect();
    file.write_at(&bytes, 0).await.unwrap();
    file.fdatasync().await.unwrap();
    drop(file);
    let mut file = backend.open(&path, OpenOptions::read_only()).await.unwrap();
    let mut result = [0_u8; 32];
    file.read_exact_at(&mut result, 4089).await.unwrap();
    assert_eq!(result, bytes[4089..4121]);
    assert_eq!(file.read_at(&mut result, 8185).await.unwrap(), 7);
    assert_eq!(&result[..7], &bytes[8185..]);
    assert_eq!(file.read_at(&mut result, 8192).await.unwrap(), 0);
    assert_eq!(file.read_at(&mut [], 1).await.unwrap(), 0);
}

#[tokio::test]
async fn unreadable_segment_prevents_recovery() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("block-wal-unreadable");
    let group = root.path().join("group1");
    std::fs::create_dir_all(&group).unwrap();
    std::fs::write(group.join("seg-0000001.ck"), b"invalid segment").unwrap();
    let backend = Arc::new(IoBackend::block_device());
    assert!(replay_group(&backend, &[root.path().to_path_buf()], 1)
        .await
        .is_err());
}

#[tokio::test]
async fn empty_tail_is_removed_but_empty_middle_prevents_recovery() {
    for backend in [IoBackend::File, IoBackend::block_device()] {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("wal-empty-tail");
        let group = root.path().join("group1");
        let mut segment = WalSegment::create(&backend, &group, 1, 1).await.unwrap();
        segment
            .append(&WALRecord::from_promised(1, 1, 1, PxBallot::new(0, 1)))
            .await
            .unwrap();
        segment.seal().await.unwrap();
        drop(segment);
        let tail = group.join("seg-0000002.ck");
        std::fs::write(&tail, []).unwrap();
        let backend = Arc::new(backend);
        let disks = [root.path().to_path_buf()];
        let replay = replay_group(&backend, &disks, 1).await.unwrap();
        assert_eq!(replay.records.len(), 1);
        assert_eq!(replay.max_segment_id, 2);
        assert!(!tail.exists());
        assert_eq!(replay_group(&backend, &disks, 1).await.unwrap().records.len(), 1);
        std::fs::write(group.join("seg-0000000.ck"), []).unwrap();
        assert!(replay_group(&backend, &disks, 1).await.is_err());
    }
}

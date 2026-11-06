// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_paxos_kv::wal::gc::run_gc_pass;

#[tokio::test]
async fn restored_wal_keeps_old_segments_visible_to_safe_reclamation() {
    let temp = crowdb_test_harness::test_dirs::tempdir_in_test_data("startup-wal-reclamation");
    let config = PaxosKvConfig {
        wal_root: temp.path().join("wal"),
        config_root: temp.path().join("conf"),
        data_root: temp.path().join("data"),
        ..PaxosKvConfig::for_tests()
    };
    let backend = Arc::new(IoBackend::File);
    let mut wal_config = WalConfig::with_root(store_wal_root(&config.wal_root, 0));
    wal_config.wal_segment_size = 256;
    let wal = WalEngine::create(backend.clone(), wal_config, 1).await.unwrap();
    for slot in 1..=20 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(3, 1),
            term: 3,
            payload: Bytes::from(encode_put_payload(b"key", &slot.to_le_bytes())),
        };
        wal.append(&WALRecord::from_accepted(1, &entry)).await.unwrap();
    }
    wal.seal_all().await.unwrap();
    drop(wal);
    let group = create_group_with_wal(
        0,
        1,
        1,
        PxLocalReplicaRole::Leader,
        &config,
        backend.clone(),
        CrowdbTreeBackend::File,
    )
    .await
    .unwrap();
    let restored_wal = group.local_replica().wal().unwrap();
    assert_eq!(
        restored_wal.index().slot_count(),
        20,
        "startup discarded replayed slot locations"
    );
    assert!(restored_wal.index().segments_snapshot().len() > 2);
    assert_eq!(
        run_gc_pass(restored_wal, 20).await.unwrap(),
        0,
        "no durable snapshot yet"
    );
    group.local_replica().learner.engine().flush();
    assert_eq!(group.local_replica().learner.engine().persist_snapshot(), 20);
    drop(group);
    let restarted = create_group_with_wal(
        0,
        1,
        1,
        PxLocalReplicaRole::Leader,
        &config,
        backend.clone(),
        CrowdbTreeBackend::File,
    )
    .await
    .unwrap();
    let restarted_wal = restarted.local_replica().wal().unwrap();
    assert_eq!(
        restarted_wal.snapshot_slot(),
        20,
        "restored durable floor must be immediately available"
    );
    assert_eq!(
        run_gc_pass(restarted_wal, 1).await.unwrap(),
        0,
        "lagging learner still blocks GC"
    );
    assert!(
        run_gc_pass(restarted_wal, 20).await.unwrap() > 0,
        "safe old segments must be reclaimable after restart"
    );
    let replay = replay_group(&backend, &[store_wal_root(&config.wal_root, 0)], 1)
        .await
        .unwrap();
    assert!(
        replay.records.len() < 20,
        "reclaimed history must not return on the next restart"
    );
    assert_eq!(replay.current_term, 3);
    assert_eq!(
        restarted.local_replica().learner.engine_get(b"key").await,
        Some((20, 20_u64.to_le_bytes().to_vec()))
    );
}

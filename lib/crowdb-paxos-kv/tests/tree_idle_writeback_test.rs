// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;

use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::cluster::group_election::LeaderElection;
use crowdb_paxos_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::common::config::PxElectionConfig;
use crowdb_paxos_kv::kv::{CrowdbTreeConfig, CrowdbTreeEngine};
use crowdb_paxos_kv::paxos::roles::{Learner, PxBallot, PxLogEntry};
use crowdb_paxos_kv::wal::replay::replay_group;
use crowdb_paxos_kv::wal::wal_engine::WalEngine;
use crowdb_paxos_kv::wal::{IoBackend, MemBlockDevice, WalConfig};
use crowdb_tree_ffi::{PageStore, SyncMode, TreeRuntime, TreeRuntimeOptions};

fn native_engine() -> CrowdbTreeEngine {
    let runtime = Arc::new(
        TreeRuntime::new(TreeRuntimeOptions {
            workers: 1,
            ..Default::default()
        })
        .unwrap(),
    );
    CrowdbTreeEngine::open(&CrowdbTreeConfig {
        runtime: Some(runtime),
        page_store: Some(Arc::new(PageStore::open_mem(4096).unwrap())),
        frame_bytes: 4096,
        max_inline_value: 64,
        sync_mode: SyncMode::Skip,
        ..Default::default()
    })
    .unwrap()
}

async fn native_replica(engine: CrowdbTreeEngine) -> (PxLocalReplica, Arc<WalEngine>) {
    let backend = Arc::new(IoBackend::MemBlock(MemBlockDevice::new()));
    let replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    let wal = WalEngine::create(
        Arc::clone(&backend),
        WalConfig {
            wal_disks: vec![PathBuf::from("/wal")],
            ..Default::default()
        },
        1,
    )
    .await
    .unwrap();
    let mut replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Follower,
        &replay,
        Box::new(engine),
    )
    .await
    .unwrap();
    replica.set_wal(Arc::clone(&wal));
    (replica, wal)
}

async fn learn_prefix(replica: &PxLocalReplica) {
    for slot in 1..=256_u64 {
        let mut payload = vec![1, 0, 0];
        payload.extend_from_slice(&8_u32.to_le_bytes());
        payload.extend_from_slice(&slot.to_be_bytes());
        payload.extend_from_slice(&512_u32.to_le_bytes());
        payload.extend_from_slice(&[7; 512]);
        replica
            .learner
            .learn(
                PxLogEntry {
                    slot,
                    ballot: PxBallot::new(0, 1),
                    term: 1,
                    payload: Bytes::from(payload),
                },
                &[],
            )
            .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn idle_host_pass_retries_abandoned_writeback_without_checkpoint_or_new_writes() {
    let engine = native_engine();
    let tree = engine.handle();
    let (replica, wal) = native_replica(engine).await;
    learn_prefix(&replica).await;
    assert_eq!(replica.contiguous_applied(), 256);
    let engine = replica.learner.engine();
    let flushed = engine.flush_async().unwrap().await.unwrap();
    assert_eq!(flushed.status(), Ok(()));
    assert!(engine.writeback_pending());
    let before = tree.state_stats().unwrap();
    assert!(before.logical.writeback_candidate_pages > 1);
    assert_eq!(tree.durable_coverage(), 0);
    drop(engine.writeback_async().unwrap());
    assert_eq!(
        tree.state_stats().unwrap().logical.writeback_candidate_pages,
        before.logical.writeback_candidate_pages
    );

    let mut group = PxGroup::new(1, replica);
    group.set_election_config(PxElectionConfig {
        snapshot_slot_threshold: u64::MAX,
        snapshot_time_threshold_ms: u64::MAX,
        snapshot_flush_count_threshold: 0,
        wal_flush_interval_ms: 0,
        merge_gc_interval_ms: 0,
        ..PxElectionConfig::for_tests()
    });
    let mut passes = 0;
    while tree.state_stats().unwrap().logical.writeback_candidate_pages != 0 && passes < 16 {
        let candidates = tree.state_stats().unwrap().logical.writeback_candidate_pages;
        group.run_maintenance_pass_for_tests().await;
        passes += 1;
        assert!(tree.state_stats().unwrap().logical.writeback_candidate_pages < candidates);
        assert_eq!(tree.durable_coverage(), 0);
        assert_eq!(wal.snapshot_slot(), 0);
        assert_eq!(tree.last_applied_slot(), 256);
        assert_eq!(group.local_replica().contiguous_applied(), 256);
    }
    assert!(passes > 1);
    assert_eq!(tree.state_stats().unwrap().logical.writeback_candidate_pages, 0);
    let settled = tree.state_stats().unwrap();
    for _ in 0..3 {
        group.run_maintenance_pass_for_tests().await;
        let idle = tree.state_stats().unwrap();
        assert_eq!(idle.logical, settled.logical);
        assert_eq!(tree.durable_coverage(), 0);
        assert_eq!(wal.snapshot_slot(), 0);
    }
    assert_eq!(
        tree.get(&256_u64.to_be_bytes()).unwrap(),
        Some((256, vec![7; 512]))
    );
}

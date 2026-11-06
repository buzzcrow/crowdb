// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::cluster::group_election::LeaderElection;
use crowdb_paxos_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::common::config::PxElectionConfig;
use crowdb_paxos_kv::kv::{
    Batch, CrowdbTreeConfig, CrowdbTreeEngine, EngineCheckpointReport, EngineCheckpointWork,
    EngineFlushReport, KVEngine, KVFuture, ScanDirection,
};
use crowdb_paxos_kv::paxos::roles::{Learner, PxBallot, PxLogEntry};
use crowdb_paxos_kv::wal::replay::replay_group;
use crowdb_paxos_kv::wal::wal_engine::WalEngine;
use crowdb_paxos_kv::wal::{IoBackend, MemBlockDevice, WalConfig};
use crowdb_tree_ffi::{PageStore, SyncMode, TreeRuntime, TreeRuntimeOptions};

#[derive(Default)]
struct Calls {
    flush: AtomicU64,
    checkpoint: AtomicU64,
    compaction: AtomicU64,
}
struct ScriptedMaintenance {
    calls: Arc<Calls>,
}
impl KVEngine for ScriptedMaintenance {
    fn apply(&self, _: u64, _: &Batch) -> KVFuture<Result<(), String>> {
        KVFuture::ready(Ok(()))
    }
    fn get(&self, _: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>> {
        KVFuture::ready(None)
    }
    fn scan_directional(
        &self,
        _: &[u8],
        _: &[u8],
        _: &[u8],
        _: usize,
        _: usize,
        _: bool,
        _: u64,
        _: ScanDirection,
    ) -> KVFuture<Result<(Vec<(Bytes, u64, Bytes)>, bool), String>> {
        KVFuture::ready(Ok((vec![], false)))
    }
    fn clear(&self) -> Result<(), String> {
        Ok(())
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn flush(&self) {
        panic!("runtime engine used blocking compatibility flush")
    }
    fn persist_snapshot(&self) -> u64 {
        panic!("runtime engine used blocking compatibility checkpoint")
    }
    fn compact_sparse_blocks(&self) -> Result<(), String> {
        panic!("runtime engine used blocking compatibility compaction")
    }
    fn compact_sparse_blocks_async(&self) -> Option<KVFuture<Result<(), String>>> {
        let calls = Arc::clone(&self.calls);
        Some(KVFuture::Pending(Box::pin(async move {
            tokio::task::yield_now().await;
            calls.compaction.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })))
    }
    fn flush_async(&self) -> Option<KVFuture<Result<EngineFlushReport, String>>> {
        let call = self.calls.flush.fetch_add(1, Ordering::Relaxed);
        if call == 0 {
            return Some(KVFuture::ready(Err("admission exhausted".to_owned())));
        }
        Some(KVFuture::ready(Ok(EngineFlushReport {
            handoff_status: Ok(()),
            maintenance_status: if call == 1 {
                Err("maintenance failed".to_owned())
            } else {
                Ok(())
            },
            handoff_completed: true,
            handoff_advanced: call == 1 || call == 3,
            floor: u64::from(call >= 3),
            frontier: if call < 3 { 1 } else { 2 },
            published_coverage: if call < 3 { 1 } else { 2 },
            deferred_pages: u64::from(call == 1),
        })))
    }
    fn persist_snapshot_async(&self) -> Option<KVFuture<Result<EngineCheckpointReport, String>>> {
        self.calls.checkpoint.fetch_add(1, Ordering::Relaxed);
        Some(KVFuture::ready(Ok(EngineCheckpointReport {
            status: Err("post-commit rebase failed".to_owned()),
            durable_committed: true,
            durable_coverage: 2,
            work: Some(EngineCheckpointWork {
                writeback_completed: true,
                captured_coverage: 2,
                written_frames: 1,
                reused_frames: 0,
                rebase_deferred: 1,
            }),
        })))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn host_counts_only_new_handoffs_and_retains_post_commit_durability() {
    let backend = Arc::new(IoBackend::MemBlock(MemBlockDevice::new()));
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
    let replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    let calls = Arc::new(Calls::default());
    let mut replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &replay,
        Box::new(ScriptedMaintenance {
            calls: Arc::clone(&calls),
        }),
    )
    .await
    .unwrap();
    replica.set_wal(Arc::clone(&wal));
    for slot in 1..=2 {
        replica
            .learner
            .learn(
                PxLogEntry {
                    slot,
                    ballot: PxBallot::new(0, 1),
                    term: 1,
                    payload: Bytes::new(),
                },
                &[],
            )
            .await;
    }
    let mut group = PxGroup::new(1, replica);
    group.set_election_config(PxElectionConfig {
        snapshot_slot_threshold: u64::MAX,
        snapshot_time_threshold_ms: u64::MAX,
        snapshot_flush_count_threshold: 2,
        wal_flush_interval_ms: 0,
        merge_gc_interval_ms: 0,
        ..PxElectionConfig::for_tests()
    });
    for _ in 0..3 {
        group.run_maintenance_pass_for_tests().await;
        assert_eq!(calls.checkpoint.load(Ordering::Relaxed), 0);
        assert_eq!(wal.snapshot_slot(), 0);
    }
    group.run_maintenance_pass_for_tests().await;
    assert_eq!(calls.checkpoint.load(Ordering::Relaxed), 1);
    assert_eq!(wal.snapshot_slot(), 2);
    group.run_maintenance_pass_for_tests().await;
    assert_eq!(calls.checkpoint.load(Ordering::Relaxed), 1);
}

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

#[tokio::test(flavor = "current_thread")]
async fn native_idle_writeback_and_checkpoint_use_async_progress() {
    let engine = native_engine();
    engine.handle().apply_put(1, b"key", &vec![b'v'; 8192]).unwrap();
    let abandoned = engine.flush_async().unwrap();
    drop(abandoned);
    assert!(engine.flush_pending());
    let handoff = engine.flush_async().unwrap().await.unwrap();
    assert!(handoff.handoff_advanced);
    assert_eq!(handoff.status(), Ok(()));
    assert!(engine.writeback_pending());
    let written = engine.writeback_async().unwrap().await.unwrap();
    assert_eq!(written.status, Ok(()));
    assert!(written.work.as_ref().unwrap().writeback_completed);
    assert!(!written.durable_committed);
    assert_eq!(written.durable_coverage, 0);
    assert!(!engine.writeback_pending());
    assert!(!engine.flush_pending());
    let durable = engine.persist_snapshot_async().unwrap().await.unwrap();
    assert_eq!(durable.status, Ok(()));
    assert!(durable.durable_committed);
    assert_eq!(durable.durable_coverage, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_awaits_async_stages_and_preserves_both_terminal_errors() {
    let calls = Arc::new(Calls::default());
    let replica = shutdown_replica(Box::new(ScriptedMaintenance {
        calls: Arc::clone(&calls),
    }))
    .await;
    let report = replica.shutdown(std::time::Duration::from_secs(2)).await;
    assert_eq!(calls.flush.load(Ordering::Acquire), 1);
    assert_eq!(calls.checkpoint.load(Ordering::Acquire), 1);
    assert_eq!(report.errors.len(), 2);
    assert!(report.errors[0].contains("admission exhausted"));
    assert!(report.errors[1].contains("post-commit rebase failed"));
    assert!(replica
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .is_clean());
    assert_eq!(calls.flush.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn volatile_runtime_shutdown_has_no_false_durable_frontier() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let engine = CrowdbTreeEngine::open(&CrowdbTreeConfig {
        runtime: Some(runtime),
        ..Default::default()
    })
    .unwrap();
    let tree = engine.handle();
    let replica = shutdown_replica(Box::new(engine)).await;
    replica
        .learner
        .learn(
            PxLogEntry {
                slot: 1,
                ballot: PxBallot::new(1, 1),
                term: 1,
                payload: Bytes::new(),
            },
            &[],
        )
        .await;
    let report = replica.shutdown(std::time::Duration::from_secs(2)).await;
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_eq!(tree.last_applied_slot(), 1);
    assert_eq!(tree.durable_coverage(), 0);
    assert_eq!(replica.learner.engine().resume_from_slot(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_shutdown_waits_for_the_backend_checkpoint_before_returning() {
    let engine = native_engine();
    let tree = engine.handle();
    let replica = shutdown_replica(Box::new(engine)).await;
    replica
        .learner
        .learn(
            PxLogEntry {
                slot: 1,
                ballot: PxBallot::new(1, 1),
                term: 1,
                payload: Bytes::from_static(&[
                    1, 0, 0, 3, 0, 0, 0, b'k', b'e', b'y', 5, 0, 0, 0, b'v', b'a', b'l', b'u', b'e',
                ]),
            },
            &[],
        )
        .await;
    assert_eq!(tree.last_applied_slot(), 0);
    let flushed = replica.learner.engine().flush_async().unwrap().await.unwrap();
    assert!(flushed.status().is_ok());
    assert_eq!(tree.last_applied_slot(), 1);
    assert_eq!(tree.durable_coverage(), 0);
    assert_eq!(replica.learner.engine().resume_from_slot(), 0);
    let report = replica.shutdown(std::time::Duration::from_secs(2)).await;
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_eq!(tree.last_applied_slot(), 1);
    assert_eq!(tree.durable_coverage(), 1);
    assert_eq!(replica.learner.engine().resume_from_slot(), 1);
    assert_eq!(tree.get(b"key").unwrap(), Some((1, b"value".to_vec())));
}

async fn shutdown_replica(engine: Box<dyn KVEngine>) -> PxLocalReplica {
    let backend = Arc::new(IoBackend::MemBlock(MemBlockDevice::new()));
    let replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    PxLocalReplica::restore_from_replay_with_engine(1, PxLocalReplicaRole::Follower, &replay, engine)
        .await
        .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn configured_compaction_cadence_awaits_the_native_engine_path() {
    let calls = Arc::new(Calls::default());
    let replica = shutdown_replica(Box::new(ScriptedMaintenance {
        calls: Arc::clone(&calls),
    }))
    .await;
    let mut group = PxGroup::new(1, replica);
    group.set_election_config(PxElectionConfig {
        merge_gc_interval_ms: 1,
        ..PxElectionConfig::for_tests()
    });
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    group.run_maintenance_pass_for_tests().await;
    assert_eq!(calls.compaction.load(Ordering::Acquire), 1);
}

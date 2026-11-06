// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use crowdb_paxos_kv::cluster::replica::{HeartbeatRequestPayload, ReplicaHandler};
use crowdb_paxos_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::common::config::WalConfig;
use crowdb_paxos_kv::kv::{CrowdbTreeConfig, CrowdbTreeEngine};
use crowdb_paxos_kv::metrics::MetricsRegistry;
use crowdb_paxos_kv::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry};
use crowdb_paxos_kv::wal::record::WALRecord;
use crowdb_paxos_kv::wal::replay::{replay_group, ReplayResult};
use crowdb_paxos_kv::wal::{IoBackend, WalEngine};
use crowdb_tree_ffi::{TreeRuntime, TreeRuntimeOptions};

fn engine() -> CrowdbTreeEngine {
    CrowdbTreeEngine::open(&CrowdbTreeConfig {
        runtime: Some(Arc::new(
            TreeRuntime::new(TreeRuntimeOptions {
                workers: 1,
                ..Default::default()
            })
            .unwrap(),
        )),
        ..Default::default()
    })
    .unwrap()
}

fn entry(slot: u64) -> PxLogEntry {
    let mut payload = vec![1, 0, 0];
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.push(b'k');
    payload.extend_from_slice(&8u32.to_le_bytes());
    payload.extend_from_slice(&slot.to_le_bytes());
    PxLogEntry {
        slot,
        term: 1,
        ballot: PxBallot::new(1, 2),
        payload: Bytes::from(payload),
    }
}

async fn heartbeat(replica: &PxLocalReplica, slot: u64) {
    let reply = ReplicaHandler::on_heartbeat(
        replica,
        HeartbeatRequestPayload {
            term: 1,
            leader_id: 2,
            prev_log_slot: 0,
            prev_log_term: 0,
            committed_safe_slot: slot,
            lease_grant_until_ms_mono: 0,
            t_send_ms_mono: 0,
            gc_before_slot: 0,
            gc_membership_epoch: 0,
        },
        1,
    )
    .await
    .unwrap();
    assert!(reply.success);
}

async fn replay(entries: &[PxLogEntry]) -> ReplayResult {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("runtime-gap-replay");
    let backend = Arc::new(IoBackend::File);
    let wal = WalEngine::create(
        Arc::clone(&backend),
        WalConfig::with_root(directory.path().to_path_buf()),
        1,
    )
    .await
    .unwrap();
    for entry in entries {
        wal.append(&WALRecord::from_accepted(1, entry)).await.unwrap();
    }
    wal.seal_all().await.unwrap();
    drop(wal);
    replay_group(&backend, &[directory.path().to_path_buf()], 1)
        .await
        .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn background_gap_repair_wakes_the_original_runtime_apply_loop() {
    let replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Follower,
        &replay(&[]).await,
        Box::new(engine()),
    )
    .await
    .unwrap();
    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    replica.set_metrics_registry(&registry, 1, 1);
    let skipped = registry
        .lock()
        .unwrap()
        .register_counter("s.1.g.1.paxos.apply_loop.skip.c");
    assert!(matches!(
        replica.on_accept(&entry(2)).await,
        PxAcceptReply::Accepted { .. }
    ));
    heartbeat(&replica, 2).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while skipped.snapshot().total == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the real background loop must observe its first gap");
    assert_eq!(replica.contiguous_applied(), 0);
    assert!(replica.learner.engine_get(b"k").await.is_none());

    // No second heartbeat: acceptance of the missing chosen value must wake
    // the loop already waiting for slot one, rather than leave it awaiting two.
    assert!(matches!(
        replica.on_accept(&entry(1)).await,
        PxAcceptReply::Accepted { .. }
    ));
    tokio::time::timeout(Duration::from_secs(2), replica.await_apply_fence(2))
        .await
        .expect("the repaired gap must allow both real entries to apply");
    assert_eq!(
        replica.learner.engine_get(b"k").await,
        Some((2, 2u64.to_le_bytes().to_vec()))
    );
    assert!(replica.shutdown(Duration::from_secs(2)).await.is_clean());
}

#[tokio::test(flavor = "current_thread")]
async fn sparse_durable_replay_stops_at_gap_and_preserves_the_accepted_suffix() {
    let entries = [entry(1), entry(3)];
    let recovered = replay(&entries).await;
    let replica = tokio::time::timeout(
        Duration::from_secs(2),
        PxLocalReplica::restore_from_replay_with_engine(
            1,
            PxLocalReplicaRole::Follower,
            &recovered,
            Box::new(engine()),
        ),
    )
    .await
    .expect("startup must not await a missing slot that only live repair can supply")
    .unwrap();
    assert_eq!(replica.contiguous_applied(), 1);
    assert_eq!(replica.contiguous_chosen(), 1);
    assert_eq!(replica.last_chosen_slot(), 1);
    assert_eq!(replica.learner.engine().resume_from_slot(), 0);
    assert!(replica.accepted_at(2).await.is_none());
    assert_eq!(replica.accepted_at(3).await.unwrap().payload, entries[1].payload);
    assert_eq!(
        replica.learner.engine_get(b"k").await,
        Some((1, 1u64.to_le_bytes().to_vec()))
    );

    heartbeat(&replica, 3).await;
    assert!(matches!(
        replica.on_accept(&entry(2)).await,
        PxAcceptReply::Accepted { .. }
    ));
    tokio::time::timeout(Duration::from_secs(2), replica.await_apply_fence(3))
        .await
        .expect("repair must resume the retained accepted suffix");
    assert_eq!(
        replica.learner.engine_get(b"k").await,
        Some((3, 3u64.to_le_bytes().to_vec()))
    );
    assert!(replica.shutdown(Duration::from_secs(2)).await.is_clean());
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_durable_runtime_replay_returns_the_real_apply_failure() {
    let mut invalid = entry(1);
    invalid.payload = Bytes::from_static(&[1, 0]);
    let recovered = replay(&[invalid]).await;
    let error = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Follower,
        &recovered,
        Box::new(engine()),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("restore slot 1"));
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_paxos_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_paxos_kv::cluster::px_kv_store::PxKvStore;
use crowdb_paxos_kv::cluster::replica::{HeartbeatRequestPayload, ReplicaHandler};
use crowdb_paxos_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::kv::{CrowdbTreeConfig, CrowdbTreeEngine};
use crowdb_paxos_kv::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry};
use crowdb_paxos_kv::wal::replay::replay_group;
use crowdb_paxos_kv::wal::IoBackend;
use crowdb_tree_ffi::{PageStore, SyncMode, TreeRuntime, TreeRuntimeOptions};

#[path = "runtime_shutdown_rpc_test.rs"]
mod rpc;

#[path = "runtime_shutdown_owner_test.rs"]
mod owner;

const DEADLINE: Duration = Duration::from_secs(2);

async fn group(role: PxLocalReplicaRole) -> PxGroup {
    let engine = CrowdbTreeEngine::open(&CrowdbTreeConfig {
        runtime: Some(Arc::new(
            TreeRuntime::new(TreeRuntimeOptions {
                workers: 1,
                ..Default::default()
            })
            .unwrap(),
        )),
        page_store: Some(Arc::new(PageStore::open_mem(4096).unwrap())),
        frame_bytes: 4096,
        max_inline_value: 64,
        sync_mode: SyncMode::Skip,
        ..Default::default()
    })
    .unwrap();
    let replay = replay_group(&Arc::new(IoBackend::File), &[], 1).await.unwrap();
    let replica = PxLocalReplica::restore_from_replay_with_engine(1, role, &replay, Box::new(engine))
        .await
        .unwrap();
    PxGroup::new(1, replica)
}

fn entry(slot: u64) -> PxLogEntry {
    let mut payload = vec![1, 0, 0];
    payload.extend_from_slice(&8u32.to_le_bytes());
    payload.extend_from_slice(&slot.to_le_bytes());
    payload.extend_from_slice(&8u32.to_le_bytes());
    payload.extend_from_slice(&slot.to_le_bytes());
    PxLogEntry {
        slot,
        term: 0,
        ballot: PxBallot::new(1, 1),
        payload: Bytes::from(payload),
    }
}

async fn accept(replica: &PxLocalReplica, slot: u64) {
    assert!(matches!(
        replica.on_accept(&entry(slot)).await,
        PxAcceptReply::Accepted { .. }
    ));
}

async fn assert_applied(replica: &PxLocalReplica) {
    assert_eq!(replica.contiguous_applied(), 2);
    for slot in 1u64..=2 {
        assert_eq!(
            replica.learner.engine_get(&slot.to_le_bytes()).await,
            Some((slot, slot.to_le_bytes().to_vec()))
        );
    }
    assert_eq!(replica.learner.engine().resume_from_slot(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn leader_shutdown_repairs_accepted_gap_before_cancelling_owned_tasks() {
    let group = Arc::new(group(PxLocalReplicaRole::Leader).await);
    accept(group.local_replica(), 1).await;
    accept(group.local_replica(), 2).await;
    group.local_replica().spawn_learn_chosen(entry(2), &[]);
    group.pause_repair_for_tests();
    let shutdown = tokio::spawn({
        let group = group.clone();
        async move { group.shutdown(DEADLINE).await }
    });
    tokio::time::timeout(DEADLINE, async {
        while !group.repair_is_paused_for_tests() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        group.propose(entry(3).payload.to_vec(), None, None).await,
        ProposeResult::Busy
    ));
    assert!(group
        .status()
        .messages
        .iter()
        .any(|message| message.contains("shutdown waiting for accepted Tree work")));
    assert_eq!(group.local_replica().contiguous_applied(), 0);
    group.resume_repair_for_tests();
    let report = shutdown.await.unwrap();
    assert!(report.is_clean(), "{:?}", report.errors);
    // Real Prepare/Accept must adopt slot one's accepted payload, never replace
    // it with an invented no-op merely to unblock slot two's native apply.
    assert_applied(group.local_replica()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_synchronous_chosen_wait_keeps_accepted_apply_owned() {
    let group = Arc::new(group(PxLocalReplicaRole::Leader).await);
    accept(group.local_replica(), 1).await;
    accept(group.local_replica(), 2).await;
    let apply = tokio::spawn({
        let group = group.clone();
        async move { group.local_replica().learn_chosen(&entry(2), &[]).await }
    });
    tokio::time::timeout(DEADLINE, async {
        while group.local_replica().last_chosen_slot() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("chosen must be visible while its native apply is awaiting the gap");
    assert_eq!(group.local_replica().contiguous_applied(), 0);
    apply.abort();
    assert!(apply.await.unwrap_err().is_cancelled());
    let report = group.shutdown(DEADLINE).await;
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_applied(group.local_replica()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn follower_store_timeout_keeps_gap_repair_alive_and_retry_drains() {
    let store = PxKvStore::new(1, "127.0.0.1:0".parse().unwrap());
    store.add_group_without_election(group(PxLocalReplicaRole::Follower).await);
    let group = store.get_group(1).unwrap();
    let replica = group.local_replica();
    accept(replica, 2).await;
    let reply = ReplicaHandler::on_heartbeat(
        replica,
        HeartbeatRequestPayload {
            term: 0,
            leader_id: 2,
            prev_log_slot: 0,
            prev_log_term: 0,
            committed_safe_slot: 2,
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
    let first = store.shutdown(Duration::from_millis(20)).await;
    assert!(!first.is_clean());
    assert!(first
        .errors
        .iter()
        .any(|error| error.contains("accepted Tree work did not drain")));
    assert_eq!(replica.contiguous_applied(), 0);
    assert_eq!(replica.learner.engine().resume_from_slot(), 0);
    assert!(group
        .status()
        .messages
        .iter()
        .any(|message| message.contains("shutdown waiting")));
    accept(replica, 1).await;
    // No new heartbeat or new apply loop: accepted gap repair must wake the
    // same retained loop after the first bounded shutdown attempt returned.
    tokio::time::timeout(DEADLINE, replica.await_apply_fence(2))
        .await
        .unwrap();
    let retried = store.shutdown(DEADLINE).await;
    assert!(retried.is_clean(), "{:?}", retried.errors);
    assert_applied(replica).await;
}

#[tokio::test(flavor = "current_thread")]
async fn closing_runtime_proposal_window_wakes_queued_callers_without_panic() {
    let mut group = group(PxLocalReplicaRole::Leader).await;
    group.set_inflight_config(1, false);
    let group = Arc::new(group);
    let permit = group.inflight_semaphore().acquire().await.unwrap();
    let proposal = tokio::spawn({
        let group = group.clone();
        async move { group.propose(entry(1).payload.to_vec(), None, None).await }
    });
    tokio::task::yield_now().await;
    let shutdown = tokio::spawn({
        let group = group.clone();
        async move { group.shutdown(DEADLINE).await }
    });
    assert!(matches!(
        tokio::time::timeout(DEADLINE, proposal).await.unwrap().unwrap(),
        ProposeResult::Busy
    ));
    drop(permit);
    let report = shutdown.await.unwrap();
    assert!(report.is_clean(), "{:?}", report.errors);
}

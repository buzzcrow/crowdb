// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use crowdb_paxos_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_paxos_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::kv::{CrowdbTreeConfig, CrowdbTreeEngine};
use crowdb_paxos_kv::wal::replay::replay_group;
use crowdb_paxos_kv::wal::{IoBackend, MemBlockDevice};
use crowdb_tree_ffi::{TreeRuntime, TreeRuntimeOptions};
use tokio::sync::Notify;

async fn group() -> PxGroup {
    let backend = Arc::new(IoBackend::MemBlock(MemBlockDevice::new()));
    let replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    let engine = CrowdbTreeEngine::open(&CrowdbTreeConfig {
        runtime: Some(Arc::new(
            TreeRuntime::new(TreeRuntimeOptions {
                workers: 1,
                ..Default::default()
            })
            .unwrap(),
        )),
        ..Default::default()
    })
    .unwrap();
    let replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &replay,
        Box::new(engine),
    )
    .await
    .unwrap();
    let mut group = PxGroup::new(1, replica);
    group.set_inflight_config(1, false);
    group.set_async_engine_apply(true);
    group
}

fn payload(value: u8) -> Vec<u8> {
    vec![1, 0, 0, 1, 0, 0, 0, b'k', 1, 0, 0, 0, value]
}

#[tokio::test]
async fn chosen_reply_keeps_the_native_apply_inside_the_proposal_window() {
    let group = group().await;
    let gate = Arc::new(Notify::new());
    group
        .local_replica()
        .learner
        .set_apply_gate_for_tests(Arc::clone(&gate));
    let chosen = group.propose(payload(b'a'), None, None).await;
    let available_before_apply = group.inflight_semaphore().available_permits();
    let applied_before_release = group.local_replica().contiguous_applied();
    gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), group.local_replica().await_apply_fence(1))
        .await
        .unwrap();
    assert!(matches!(chosen, ProposeResult::Chosen { slot: 1 }));
    assert_eq!(
        applied_before_release, 0,
        "Chosen must still return before native apply"
    );
    assert_eq!(
        available_before_apply, 0,
        "the acknowledged native apply still owns the window"
    );
    assert_eq!(group.inflight_semaphore().available_permits(), 1);
}

#[tokio::test]
async fn queued_proposal_waits_for_native_apply_without_allocating_another_slot() {
    let group = group().await;
    let gate = Arc::new(Notify::new());
    group
        .local_replica()
        .learner
        .set_apply_gate_for_tests(Arc::clone(&gate));
    let first = group.propose(payload(b'a'), None, None).await;
    let mut second = Box::pin(group.propose(payload(b'b'), None, None));
    let queued = matches!(futures::poll!(&mut second), Poll::Pending);
    let chosen_before_release = group.local_replica().last_chosen_slot();
    gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), group.local_replica().await_apply_fence(1))
        .await
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .unwrap();
    gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), group.local_replica().await_apply_fence(2))
        .await
        .unwrap();
    assert!(matches!(first, ProposeResult::Chosen { slot: 1 }));
    assert!(queued, "queue policy must backpressure before native admission");
    assert_eq!(chosen_before_release, 1);
    assert!(matches!(second, ProposeResult::Chosen { slot: 2 }));
    assert_eq!(
        group.local_replica().learner.engine_get(b"k").await,
        Some((2, vec![b'b']))
    );
    assert_eq!(group.inflight_semaphore().available_permits(), 1);
}

#[tokio::test]
async fn cancelling_a_queued_proposal_removes_its_waiter_without_releasing_the_active_apply() {
    let group = group().await;
    let gate = Arc::new(Notify::new());
    group
        .local_replica()
        .learner
        .set_apply_gate_for_tests(Arc::clone(&gate));
    let first = group.propose(payload(b'a'), None, None).await;
    let mut queued = Box::pin(group.propose(payload(b'b'), None, None));
    let pending = matches!(futures::poll!(&mut queued), Poll::Pending);
    let before_cancel = group.status().inflight.unwrap();
    drop(queued);
    let after_cancel = group.status().inflight.unwrap();
    gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), group.local_replica().await_apply_fence(1))
        .await
        .unwrap();
    assert!(matches!(first, ProposeResult::Chosen { slot: 1 }));
    assert!(pending);
    assert_eq!(before_cancel.waiting, 1);
    assert_eq!(after_cancel.waiting, 0);
    assert_eq!(after_cancel.occupied, 1);
    assert_eq!(group.local_replica().last_chosen_slot(), 1);
    assert_eq!(group.inflight_semaphore().available_permits(), 1);
}

#[tokio::test]
async fn cancelling_a_chosen_synchronous_wait_keeps_the_window_with_owned_native_apply() {
    let mut group = group().await;
    group.set_async_engine_apply(false);
    let gate = Arc::new(Notify::new());
    group
        .local_replica()
        .learner
        .set_apply_gate_for_tests(Arc::clone(&gate));
    let mut proposal = Box::pin(group.propose(payload(b'a'), None, None));
    let pending = matches!(futures::poll!(&mut proposal), Poll::Pending);
    let chosen = group.local_replica().last_chosen_slot();
    drop(proposal);
    let available_after_cancel = group.inflight_semaphore().available_permits();
    gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), group.local_replica().await_apply_fence(1))
        .await
        .unwrap();
    assert!(pending);
    assert_eq!(chosen, 1);
    assert_eq!(available_after_cancel, 0);
    assert_eq!(group.inflight_semaphore().available_permits(), 1);
    assert_eq!(
        group.local_replica().learner.engine_get(b"k").await,
        Some((1, vec![b'a']))
    );
}

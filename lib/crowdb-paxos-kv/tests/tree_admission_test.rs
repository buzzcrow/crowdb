// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crowdb_paxos_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_paxos_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::kv::{
    AdmittedApply, ApplyAdmissionError, ApplyAdmissionResult, Batch, KVEngine, KVFuture, ScanDirection,
};
use crowdb_paxos_kv::paxos::roles::{Learner, PxAcceptReply, PxBallot, PxLogEntry};
use crowdb_paxos_kv::wal::replay::replay_group;
use crowdb_paxos_kv::wal::{IoBackend, MemBlockDevice};
use tokio::sync::Notify;

#[derive(Default)]
struct Calls {
    busy: AtomicBool,
    hold_admission: AtomicBool,
    hold_apply: AtomicBool,
    fail_noop: AtomicBool,
    admitted: AtomicU64,
    released: AtomicU64,
    admitted_slot: AtomicU64,
    fallback_slot: AtomicU64,
    admission_entered: Notify,
    admission_release: Notify,
    apply_entered: Notify,
    apply_release: Notify,
}

struct TestEngine(Arc<Calls>);
struct TestPermit(Arc<Calls>);

impl Drop for TestPermit {
    fn drop(&mut self) {
        self.0.released.fetch_add(1, Ordering::Relaxed);
    }
}

impl AdmittedApply for TestPermit {
    fn apply(self: Box<Self>, slot: u64) -> KVFuture<Result<(), String>> {
        KVFuture::Pending(Box::pin(async move {
            self.0.apply_entered.notify_one();
            if self.0.hold_apply.load(Ordering::Acquire) {
                self.0.apply_release.notified().await;
            }
            self.0.admitted_slot.store(slot, Ordering::Release);
            Ok(())
        }))
    }
}

impl KVEngine for TestEngine {
    fn admit_proposal(&self, _: Bytes) -> KVFuture<ApplyAdmissionResult> {
        if self.0.busy.load(Ordering::Acquire) {
            return KVFuture::ready(Err(ApplyAdmissionError::Busy));
        }
        self.0.admitted.fetch_add(1, Ordering::Relaxed);
        let permit: Box<dyn AdmittedApply> = Box::new(TestPermit(Arc::clone(&self.0)));
        let calls = Arc::clone(&self.0);
        KVFuture::Pending(Box::pin(async move {
            calls.admission_entered.notify_one();
            if calls.hold_admission.load(Ordering::Acquire) {
                calls.admission_release.notified().await;
            }
            Ok(Some(permit))
        }))
    }

    fn apply(&self, slot: u64, _: &Batch) -> KVFuture<Result<(), String>> {
        self.0.fallback_slot.store(slot, Ordering::Release);
        KVFuture::ready(Ok(()))
    }

    fn apply_noop(&self, _: u64) -> KVFuture<Result<(), String>> {
        KVFuture::ready(if self.0.fail_noop.load(Ordering::Acquire) {
            Err("gap progress unavailable".to_owned())
        } else {
            Ok(())
        })
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
}

async fn group(calls: &Arc<Calls>) -> PxGroup {
    let backend = Arc::new(IoBackend::MemBlock(MemBlockDevice::new()));
    let replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    let replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &replay,
        Box::new(TestEngine(Arc::clone(calls))),
    )
    .await
    .unwrap();
    PxGroup::new(1, replica)
}

fn payload(value: u8) -> Vec<u8> {
    vec![1, 0, 0, 1, 0, 0, 0, b'k', 1, 0, 0, 0, value]
}

#[tokio::test]
async fn resource_rejection_precedes_slot_allocation_and_acceptance() {
    let calls = Arc::new(Calls::default());
    calls.busy.store(true, Ordering::Release);
    let group = group(&calls).await;
    assert!(matches!(
        group.propose(payload(b'a'), None, None).await,
        ProposeResult::Busy
    ));
    assert_eq!(group.local_replica().last_chosen_slot(), 0);
    assert!(group.local_replica().accepted_at(1).await.is_none());
    calls.busy.store(false, Ordering::Release);
    assert!(matches!(
        group.propose(payload(b'b'), None, None).await,
        ProposeResult::Chosen { slot: 1 }
    ));
    assert_eq!(calls.admitted_slot.load(Ordering::Acquire), 1);
    assert_eq!(calls.fallback_slot.load(Ordering::Acquire), 0);
    assert_eq!(calls.released.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn adopted_foreign_value_does_not_consume_the_client_reservation() {
    let calls = Arc::new(Calls::default());
    let mut group = group(&calls).await;
    group.set_force_classic(true);
    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(10, 99),
        term: 0,
        payload: Bytes::from(payload(b'f')),
    };
    assert!(matches!(
        group.local_replica().on_accept(&entry).await,
        PxAcceptReply::Accepted { .. }
    ));
    assert!(matches!(
        group.propose(payload(b'o'), None, None).await,
        ProposeResult::Chosen { slot: 2 }
    ));
    assert_eq!(calls.admitted.load(Ordering::Acquire), 1);
    assert_eq!(calls.fallback_slot.load(Ordering::Acquire), 1);
    assert_eq!(calls.admitted_slot.load(Ordering::Acquire), 2);
    assert_eq!(calls.released.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn chosen_background_apply_retains_credit_until_terminal() {
    let calls = Arc::new(Calls::default());
    calls.hold_apply.store(true, Ordering::Release);
    let mut group = group(&calls).await;
    group.set_async_engine_apply(true);
    assert!(matches!(
        group.propose(payload(b'v'), None, None).await,
        ProposeResult::Chosen { slot: 1 }
    ));
    calls.apply_entered.notified().await;
    assert_eq!(group.local_replica().contiguous_chosen(), 1);
    assert_eq!(group.local_replica().learner.contiguous_applied(), 0);
    assert_eq!(calls.released.load(Ordering::Acquire), 0);
    calls.apply_release.notify_one();
    group.local_replica().await_apply_fence(1).await;
    assert_eq!(calls.released.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn cancelling_before_consensus_returns_the_reserved_credit() {
    let calls = Arc::new(Calls::default());
    calls.hold_admission.store(true, Ordering::Release);
    let group = group(&calls).await;
    let mut future = Box::pin(group.propose(payload(b'v'), None, None));
    tokio::select! {
        result = &mut future => panic!("admission unexpectedly completed: {result:?}"),
        () = calls.admission_entered.notified() => {}
    }
    assert_eq!(calls.released.load(Ordering::Acquire), 0);
    drop(future);
    assert_eq!(calls.released.load(Ordering::Acquire), 1);
    assert_eq!(group.local_replica().last_chosen_slot(), 0);
}

#[tokio::test]
async fn failed_noop_cannot_advance_the_applied_frontier() {
    let calls = Arc::new(Calls::default());
    calls.fail_noop.store(true, Ordering::Release);
    let group = group(&calls).await;
    group
        .local_replica()
        .learner
        .learn(
            PxLogEntry {
                slot: 1,
                ballot: PxBallot::new(0, 1),
                term: 0,
                payload: Bytes::new(),
            },
            &[],
        )
        .await;
    assert_eq!(group.local_replica().contiguous_chosen(), 1);
    assert_eq!(group.local_replica().learner.contiguous_applied(), 0);
}

#[tokio::test]
async fn malformed_chosen_payload_cannot_become_a_noop_or_partial_apply() {
    let calls = Arc::new(Calls::default());
    let group = group(&calls).await;
    group
        .local_replica()
        .learner
        .learn(
            PxLogEntry {
                slot: 1,
                ballot: PxBallot::new(0, 1),
                term: 0,
                payload: Bytes::from_static(&[1, 0]),
            },
            &[],
        )
        .await;
    assert_eq!(group.local_replica().contiguous_chosen(), 1);
    assert_eq!(group.local_replica().learner.contiguous_applied(), 0);
    assert_eq!(calls.fallback_slot.load(Ordering::Relaxed), 0);
}

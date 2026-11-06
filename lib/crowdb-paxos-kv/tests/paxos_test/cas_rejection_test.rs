// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crowdb_paxos_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_paxos_kv::cluster::group_operations::{
    KvGroupOperationError, KvGroupOperations, KvReadConsistency,
};
use crowdb_paxos_kv::cluster::kv_store::KvStore;
use crowdb_paxos_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::kv::{
    ApplyAdmissionError, ApplyAdmissionResult, Batch, CrowdbTreeConfig, CrowdbTreeEngine, KVEngine, KVFuture,
    ScanDirection,
};
use crowdb_paxos_kv::wal::replay::replay_group;
use crowdb_paxos_kv::wal::IoBackend;
use crowdb_tree_ffi::{TreeRuntime, TreeRuntimeOptions};

const FENCE: &[u8] = b"/diskdb/ownership-fence/read-admission";
const RECORD: &[u8] = b"record";

#[derive(Default)]
struct TestFailures {
    read: AtomicBool,
    pending_read: AtomicBool,
    proposal: AtomicBool,
}

struct TestEngine {
    inner: CrowdbTreeEngine,
    failures: Arc<TestFailures>,
}

impl KVEngine for TestEngine {
    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<Result<(), String>> {
        self.inner.apply(slot, batch)
    }

    fn admit_proposal(&self, payload: Bytes) -> KVFuture<ApplyAdmissionResult> {
        if self.failures.proposal.load(Ordering::Acquire) {
            KVFuture::ready(Err(ApplyAdmissionError::Rejected(
                "test proposal rejection".into(),
            )))
        } else {
            self.inner.admit_proposal(payload)
        }
    }

    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>> {
        self.inner.get(key)
    }

    fn get_versioned(&self, key: &[u8]) -> KVFuture<Result<Option<(u64, Bytes)>, String>> {
        if key == RECORD && self.failures.read.load(Ordering::Acquire) {
            let error = "ResourceExhausted before proposal".to_owned();
            if self.failures.pending_read.load(Ordering::Acquire) {
                KVFuture::Pending(Box::pin(async move {
                    tokio::task::yield_now().await;
                    Err(error)
                }))
            } else {
                KVFuture::ready(Err(error))
            }
        } else {
            self.inner.get_versioned(key)
        }
    }

    fn scan_directional(
        &self,
        prefix: &[u8],
        start: &[u8],
        end: &[u8],
        limit: usize,
        bytes: usize,
        keys: bool,
        deadline: u64,
        direction: ScanDirection,
    ) -> KVFuture<Result<(Vec<(Bytes, u64, Bytes)>, bool), String>> {
        self.inner
            .scan_directional(prefix, start, end, limit, bytes, keys, deadline, direction)
    }

    fn clear(&self) -> Result<(), String> {
        self.inner.clear()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

async fn replica() -> (PxLocalReplica, Arc<TestFailures>) {
    let failures = Arc::new(TestFailures::default());
    let engine = TestEngine {
        inner: CrowdbTreeEngine::open(&CrowdbTreeConfig {
            runtime: Some(Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())),
            ..Default::default()
        })
        .unwrap(),
        failures: failures.clone(),
    };
    let replay = replay_group(&Arc::new(IoBackend::File), &[], 1).await.unwrap();
    let replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &replay,
        Box::new(engine),
    )
    .await
    .unwrap();
    (replica, failures)
}

async fn group() -> (Arc<PxGroup>, Arc<TestFailures>) {
    let (replica, failures) = replica().await;
    let group = Arc::new(PxGroup::new(1, replica));
    group.set_self_weak();
    (group, failures)
}

#[tokio::test]
async fn point_read_errors_never_become_successful_absence() {
    let (replica, failures) = replica().await;
    let store = PxKvStore::new(1, "127.0.0.1:0".parse().unwrap());
    store.add_group_without_election(PxGroup::new(1, replica));
    let operations = store.group_operations(1).unwrap();
    failures.read.store(true, Ordering::Release);
    for pending in [false, true] {
        failures.pending_read.store(pending, Ordering::Release);
        let error = operations
            .get(RECORD, KvReadConsistency::Linearizable)
            .await
            .unwrap_err();
        assert_eq!(
            error,
            KvGroupOperationError::Unavailable("ResourceExhausted before proposal".into())
        );
        let response = store.kv_get(1, RECORD, 0, 0, 1, 0).await;
        assert!(!response.ok);
        assert!(!response.not_found);
        assert_eq!(response.error, "ResourceExhausted before proposal");
    }
    failures.read.store(false, Ordering::Release);
    let response = store.kv_get(1, RECORD, 0, 0, 2, 0).await;
    assert!(response.not_found);
}

fn payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = vec![1, 0, 0];
    payload.extend_from_slice(&u32::try_from(key.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(value);
    payload
}

async fn readable(group: &Arc<PxGroup>, chosen: u64) {
    assert_eq!(group.local_replica().last_chosen_slot(), chosen);
    assert!(group.local_replica().accepted_at(chosen + 1).await.is_none());
    KvGroupOperations::new(group.clone(), 1024)
        .bind_current_leader_tenure()
        .await
        .expect("definite rejection preserves read readiness");
}

#[tokio::test]
async fn failed_cas_read_does_not_poison_the_leader_or_consume_a_slot() {
    let (group, failures) = group().await;
    failures.read.store(true, Ordering::Release);
    let key = Bytes::from_static(RECORD);
    let result = group
        .propose_cas(payload(RECORD, b"value"), key.clone(), 0, 1, 1)
        .await;
    assert!(matches!(result, ProposeResult::CasBusy), "{result:?}");
    readable(&group, 0).await;
    failures.read.store(false, Ordering::Release);
    assert!(matches!(
        group.propose_cas(payload(RECORD, b"value"), key, 0, 1, 2).await,
        ProposeResult::Chosen { slot: 1 }
    ));
}

#[tokio::test]
async fn rejected_owner_handover_releases_its_admission_without_poisoning_it() {
    let (group, failures) = group().await;
    let key = Bytes::from_static(FENCE);
    failures.proposal.store(true, Ordering::Release);
    let result = group
        .propose_cas(payload(FENCE, b"owner"), key.clone(), 0, 1, 1)
        .await;
    assert!(matches!(result, ProposeResult::Rejected(_)), "{result:?}");
    assert!(!group.owner_change_pending_for_tests(&key));
    readable(&group, 0).await;
    failures.proposal.store(false, Ordering::Release);
    assert!(matches!(
        group.propose_cas(payload(FENCE, b"owner"), key, 0, 1, 2).await,
        ProposeResult::Chosen { slot: 1 }
    ));
}

#[tokio::test]
async fn nested_owner_cas_read_rejection_preserves_the_owner_and_leader() {
    let (group, failures) = group().await;
    let key = Bytes::from_static(FENCE);
    assert!(matches!(
        group
            .propose_cas(payload(FENCE, b"owner"), key.clone(), 0, 1, 1)
            .await,
        ProposeResult::Chosen { slot: 1 }
    ));
    failures.read.store(true, Ordering::Release);
    let result = group
        .propose_owner_write_for_tests(
            payload(RECORD, b"value"),
            key.clone(),
            Bytes::from_static(b"owner"),
            Some((Bytes::from_static(RECORD), 0)),
            1,
            2,
        )
        .await;
    assert!(matches!(result, ProposeResult::CasBusy), "{result:?}");
    readable(&group, 1).await;
    failures.read.store(false, Ordering::Release);
    let result = group
        .propose_owner_write_for_tests(
            payload(RECORD, b"value"),
            key,
            Bytes::from_static(b"owner"),
            Some((Bytes::from_static(RECORD), 0)),
            1,
            3,
        )
        .await;
    assert!(matches!(result, ProposeResult::Chosen { slot: 2 }), "{result:?}");
}

#[tokio::test]
async fn invalid_cas_identity_is_definitely_rejected_without_consuming_a_slot() {
    let (group, _) = group().await;
    let key = Bytes::from_static(RECORD);
    let result = group
        .propose_cas(payload(RECORD, b"value"), key.clone(), 0, 0, 1)
        .await;
    assert!(matches!(result, ProposeResult::Rejected(_)), "{result:?}");
    readable(&group, 0).await;
    assert!(matches!(
        group.propose_cas(payload(RECORD, b"value"), key, 0, 1, 2).await,
        ProposeResult::Chosen { slot: 1 }
    ));
}

#[tokio::test]
async fn nested_owner_cas_invalid_identity_preserves_owner_and_leader_progress() {
    let (group, _) = group().await;
    let key = Bytes::from_static(FENCE);
    assert!(matches!(
        group
            .propose_cas(payload(FENCE, b"owner"), key.clone(), 0, 1, 1)
            .await,
        ProposeResult::Chosen { slot: 1 }
    ));
    let result = group
        .propose_owner_write_for_tests(
            payload(RECORD, b"value"),
            key.clone(),
            Bytes::from_static(b"owner"),
            Some((Bytes::from_static(RECORD), 0)),
            0,
            2,
        )
        .await;
    assert!(!matches!(result, ProposeResult::Chosen { .. }), "{result:?}");
    // Validation rejected before a proposal. This barrier checks that the
    // nested owner guard did not turn the known rejection into unresolved work.
    readable(&group, 1).await;
    assert!(matches!(result, ProposeResult::Rejected(_)), "{result:?}");
    let result = group
        .propose_owner_write_for_tests(
            payload(RECORD, b"value"),
            key,
            Bytes::from_static(b"owner"),
            Some((Bytes::from_static(RECORD), 0)),
            1,
            3,
        )
        .await;
    assert!(matches!(result, ProposeResult::Chosen { slot: 2 }), "{result:?}");
}

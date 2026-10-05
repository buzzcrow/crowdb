// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use std::sync::Arc;

fn encode(key: &Bytes, value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1_u16.to_le_bytes());
    payload.push(0);
    payload.extend_from_slice(&u32::try_from(key.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(value);
    payload
}

async fn owner_group() -> (Arc<PxGroup>, Bytes) {
    let group = Arc::new(PxGroup::new(
        1,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    group.set_self_weak();
    let key = Bytes::from_static(b"/diskdb/ownership-fence/1/1/1");
    assert!(matches!(
        group
            .propose_cas(encode(&key, b"old"), key.clone(), 0, 1, 1)
            .await,
        ProposeResult::Chosen { slot: 1 }
    ));
    (group, key)
}

#[tokio::test]
async fn concurrent_owner_writes_do_not_advance_or_contend_on_the_fence() {
    let (group, fence) = owner_group().await;
    let writes = futures::future::join_all((1..=32).map(|id| {
        let group = group.clone();
        let fence = fence.clone();
        async move {
            let record = Bytes::from(format!("zone-{id}"));
            group
                .propose_owner_write_for_tests(
                    encode(&record, b"zone"),
                    fence,
                    Bytes::from_static(b"old"),
                    None,
                    2,
                    id,
                )
                .await
        }
    }))
    .await;
    for result in writes {
        assert!(matches!(result, ProposeResult::Chosen { .. }), "{result:?}");
    }
    assert_eq!(
        group
            .local_replica()
            .learner
            .engine()
            .get_versioned(&fence)
            .await
            .unwrap(),
        Some((1, Bytes::from_static(b"old")))
    );
}

#[tokio::test]
async fn handover_drains_old_writes_even_when_the_claim_caller_is_cancelled() {
    let (group, fence) = owner_group().await;
    let admitted = group.hold_owner_write_for_tests(fence.clone());
    let claim = {
        let group = group.clone();
        let fence = fence.clone();
        tokio::spawn(async move { group.propose_cas(encode(&fence, b"new"), fence, 1, 3, 1).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !group.owner_change_pending_for_tests(&fence) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!claim.is_finished());
    let record = Bytes::from_static(b"zone");
    assert!(matches!(
        group
            .propose_owner_write_for_tests(
                encode(&record, b"old"),
                fence.clone(),
                Bytes::from_static(b"old"),
                None,
                4,
                1
            )
            .await,
        ProposeResult::CasBusy
    ));
    claim.abort();
    drop(admitted);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while group.owner_change_pending_for_tests(&fence) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        group
            .local_replica()
            .learner
            .engine()
            .get_versioned(&fence)
            .await
            .unwrap(),
        Some((2, Bytes::from_static(b"new")))
    );
    assert!(matches!(
        group
            .propose_owner_write_for_tests(
                encode(&record, b"stale"),
                fence.clone(),
                Bytes::from_static(b"old"),
                None,
                4,
                2
            )
            .await,
        ProposeResult::CasFailed { .. }
    ));
    assert!(matches!(
        group
            .propose_owner_write_for_tests(
                encode(&record, b"new"),
                fence,
                Bytes::from_static(b"new"),
                None,
                4,
                3
            )
            .await,
        ProposeResult::Chosen { .. }
    ));
}

#[tokio::test]
async fn owner_admission_preserves_business_record_cas() {
    let (group, fence) = owner_group().await;
    let record = Bytes::from_static(b"chunk-info");
    let writes = futures::future::join_all((1..=8).map(|id| {
        let group = group.clone();
        let fence = fence.clone();
        let record = record.clone();
        async move {
            group
                .propose_owner_write_for_tests(
                    encode(&record, b"committed"),
                    fence,
                    Bytes::from_static(b"old"),
                    Some((record, 0)),
                    5,
                    id,
                )
                .await
        }
    }))
    .await;
    assert_eq!(
        writes
            .iter()
            .filter(|result| matches!(result, ProposeResult::Chosen { .. }))
            .count(),
        1
    );
    assert!(writes.iter().all(|result| matches!(
        result,
        ProposeResult::Chosen { .. } | ProposeResult::CasFailed { .. } | ProposeResult::CasBusy
    )));
}

#[tokio::test]
async fn topology_replacement_preserves_the_handover_barrier() {
    let (prior, fence) = owner_group().await;
    let admitted = prior.hold_owner_write_for_tests(fence.clone());
    let mut replacement = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Leader));
    replacement.inherit_local_state_from(&prior);
    let replacement = Arc::new(replacement);
    replacement.set_self_weak();
    let claim = {
        let replacement = replacement.clone();
        let fence = fence.clone();
        tokio::spawn(async move {
            replacement
                .propose_cas(encode(&fence, b"new"), fence, 1, 7, 1)
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !prior.owner_change_pending_for_tests(&fence) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!claim.is_finished());
    assert!(matches!(
        prior
            .propose_cas(encode(&fence, b"competing"), fence.clone(), 1, 8, 1)
            .await,
        ProposeResult::CasBusy
    ));
    drop(admitted);
    assert!(matches!(claim.await.unwrap(), ProposeResult::Chosen { .. }));
    assert_eq!(
        prior
            .local_replica()
            .learner
            .engine()
            .get_versioned(&fence)
            .await
            .unwrap(),
        Some((2, Bytes::from_static(b"new")))
    );
}

#[tokio::test]
async fn new_leader_tenure_does_not_reuse_a_closed_old_admission() {
    use crowdb_kv::cluster::group_election::LeaderElection;
    let (group, fence) = owner_group().await;
    let admitted = group.hold_owner_write_for_tests(fence.clone());
    let old_claim = {
        let group = group.clone();
        let fence = fence.clone();
        tokio::spawn(async move {
            group
                .propose_cas(encode(&fence, b"old-claim"), fence, 1, 9, 1)
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !group.owner_change_pending_for_tests(&fence) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    group.local_replica().become_follower(1);
    group.local_replica().become_leader();
    group.stamp_proposing_term(1);
    assert!(matches!(
        group
            .propose_cas(encode(&fence, b"new"), fence.clone(), 1, 10, 1)
            .await,
        ProposeResult::Chosen { .. }
    ));
    drop(admitted);
    assert!(!matches!(old_claim.await.unwrap(), ProposeResult::Chosen { .. }));
    let record = Bytes::from_static(b"new-leader-zone");
    assert!(matches!(
        group
            .propose_owner_write_for_tests(
                encode(&record, b"zone"),
                fence,
                Bytes::from_static(b"new"),
                None,
                10,
                2
            )
            .await,
        ProposeResult::Chosen { .. }
    ));
}

#[tokio::test]
async fn unresolved_old_proposal_blocks_a_topology_replacement_from_claiming_ownership() {
    let (prior, fence) = owner_group().await;
    let proposal = prior.hold_owner_proposal_for_tests(fence.clone());
    let mut replacement = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Leader));
    replacement.inherit_local_state_from(&prior);
    let replacement = Arc::new(replacement);
    replacement.set_self_weak();
    // Simulate an accepted proposal whose task terminates without a result,
    // after the new topology already copied the old readiness snapshot.
    drop(proposal);
    assert!(replacement.leader_read_ready());
    assert!(matches!(
        replacement
            .propose_cas(encode(&fence, b"new"), fence.clone(), 1, 11, 1)
            .await,
        ProposeResult::OutcomeUnknown
    ));
    assert!(!replacement.leader_read_ready());
    assert_eq!(
        replacement
            .local_replica()
            .learner
            .engine()
            .get_versioned(&fence)
            .await
            .unwrap(),
        Some((1, Bytes::from_static(b"old")))
    );
    let record = Bytes::from_static(b"zone");
    assert!(matches!(
        prior
            .propose_owner_write_for_tests(
                encode(&record, b"zone"),
                fence,
                Bytes::from_static(b"old"),
                None,
                11,
                2
            )
            .await,
        ProposeResult::OutcomeUnknown
    ));
}

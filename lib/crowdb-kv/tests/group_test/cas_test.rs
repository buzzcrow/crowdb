// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};

fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(11 + key.len() + value.len());
    payload.extend_from_slice(&1_u16.to_le_bytes());
    payload.push(0);
    payload.extend_from_slice(&u32::try_from(key.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(value);
    payload
}

fn leader_group() -> Arc<PxGroup> {
    let group = Arc::new(PxGroup::new(
        1,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    group.set_self_weak();
    group
}

#[tokio::test]
async fn cas_matches_current_revision_and_rejects_stale_revision() {
    let group = leader_group();
    let key = Bytes::from_static(b"cas-key");
    assert!(matches!(
        group.propose(encode_put(&key, b"v1"), Some(9), Some(1)).await,
        ProposeResult::Chosen { slot: 1 }
    ));

    assert!(matches!(
        group
            .propose_cas(encode_put(&key, b"v2"), key.clone(), 1, 9, 2)
            .await,
        ProposeResult::Chosen { slot: 2 }
    ));
    assert!(matches!(
        group.propose_cas(encode_put(&key, b"stale"), key, 1, 9, 3).await,
        ProposeResult::CasFailed { current_revision: 2 }
    ));
}

#[tokio::test]
async fn create_if_absent_cas_succeeds_once() {
    let group = leader_group();
    let key = Bytes::from_static(b"new-key");
    assert!(matches!(
        group
            .propose_cas(encode_put(&key, b"first"), key.clone(), 0, 10, 1)
            .await,
        ProposeResult::Chosen { slot: 1 }
    ));
    assert!(matches!(
        group
            .propose_cas(encode_put(&key, b"second"), key, 0, 11, 1)
            .await,
        ProposeResult::CasFailed { current_revision: 1 }
    ));
}

#[tokio::test]
async fn same_expected_revision_allows_exactly_one_cas() {
    let group = leader_group();
    let key = Bytes::from_static(b"contended-key");
    let first = {
        let group = Arc::clone(&group);
        let key = key.clone();
        tokio::spawn(async move { group.propose_cas(encode_put(&key, b"one"), key, 0, 20, 1).await })
    };
    let second = {
        let group = Arc::clone(&group);
        let key = key.clone();
        tokio::spawn(async move { group.propose_cas(encode_put(&key, b"two"), key, 0, 21, 1).await })
    };
    let outcomes = [first.await.unwrap(), second.await.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ProposeResult::Chosen { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| { matches!(outcome, ProposeResult::CasBusy | ProposeResult::CasFailed { .. }) })
            .count(),
        1
    );
}

#[tokio::test]
async fn create_if_absent_waits_for_acknowledged_async_apply() {
    let mut group = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Leader));
    group.set_async_engine_apply(true);
    let group = Arc::new(group);
    group.set_self_weak();
    let apply_gate = Arc::new(tokio::sync::Notify::new());
    group
        .local_replica()
        .learner
        .set_apply_gate_for_tests(Arc::clone(&apply_gate));
    let key = Bytes::from_static(b"async-cas-key");
    assert!(matches!(
        group
            .propose(encode_put(&key, b"winner"), Some(91), Some(1))
            .await,
        ProposeResult::Chosen { slot: 1 }
    ));
    assert_eq!(group.local_replica().contiguous_applied(), 0);
    let contender = Arc::clone(&group);
    let mut pending = tokio::spawn(async move {
        contender
            .propose_cas(encode_put(&key, b"loser"), key, 0, 92, 1)
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(
        group.local_replica().contiguous_chosen(),
        1,
        "CAS must not choose a replacement while its acknowledged predecessor is unapplied"
    );
    apply_gate.notify_one();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, ProposeResult::CasFailed { current_revision: 1 }),
        "{result:?}"
    );
    let value = group
        .local_replica()
        .learner
        .engine_get_bytes(b"async-cas-key")
        .await
        .unwrap();
    assert_eq!(value.1.as_ref(), b"winner");
}

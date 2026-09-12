// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::group_operations::{
    KvGroupMutation, KvGroupOperationError, KvGroupScanRequest, KvReadConsistency, KvRequestIdentity,
};
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};

fn group_zero_store() -> PxKvStore {
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    store.add_group(PxGroup::new(
        0,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    store
}

#[tokio::test]
async fn rpc_and_internal_operations_share_applied_reads_writes_and_scans() {
    let store = group_zero_store();
    let operations = store.group_operations(0).unwrap();
    let write = operations
        .write(
            &[KvGroupMutation::Put {
                key: Bytes::from_static(b"/control/a"),
                value: Bytes::from_static(b"one"),
            }],
            Some(KvRequestIdentity {
                client_id: 41,
                sequence: 1,
            }),
        )
        .await
        .unwrap();
    let read = operations
        .get(
            b"/control/a",
            KvReadConsistency::MinAppliedSlot(write.chosen_slot),
        )
        .await
        .unwrap();
    assert_eq!(read.value.as_deref(), Some(b"one".as_slice()));
    assert_eq!(read.revision, write.chosen_slot);

    let scan = operations
        .scan(&KvGroupScanRequest {
            prefix: Bytes::from_static(b"/control/"),
            start_after: Bytes::new(),
            end_key: Bytes::new(),
            limit: 10,
            consistency: KvReadConsistency::Linearizable,
            keys_only: false,
            count_only: false,
            deadline_ms: 0,
            bounded: true,
            requested_scan_cutoff: 0,
        })
        .await
        .unwrap();
    assert_eq!(scan.items.len(), 1);
    assert_eq!(scan.items[0].key, Bytes::from_static(b"/control/a"));
}

#[tokio::test]
async fn leader_bound_operations_cannot_cross_a_tenure_change() {
    let store = group_zero_store();
    let stale = store
        .group_operations(0)
        .unwrap()
        .bind_current_leader_tenure()
        .await
        .unwrap();
    assert_eq!(stale.leader_term(), Some(0));

    let group = store.get_group(0).unwrap();
    group.local_replica().become_follower(1);
    group.local_replica().become_leader();
    group.stamp_proposing_term(1);

    let error = stale
        .write(
            &[KvGroupMutation::Put {
                key: Bytes::from_static(b"/control/stale"),
                value: Bytes::from_static(b"bad"),
            }],
            Some(KvRequestIdentity {
                client_id: 42,
                sequence: 1,
            }),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, KvGroupOperationError::NotLeader { .. }));
    assert!(group
        .local_replica()
        .learner
        .engine_get_bytes(b"/control/stale")
        .await
        .is_none());
}

#[tokio::test]
async fn conditional_write_uses_the_same_apply_fence_and_revision() {
    let store = group_zero_store();
    let operations = store.group_operations(0).unwrap();
    let key = Bytes::from_static(b"/control/cas");
    let write = operations
        .compare_and_write(
            &[KvGroupMutation::Put {
                key: key.clone(),
                value: Bytes::from_static(b"created"),
            }],
            key.clone(),
            0,
            KvRequestIdentity {
                client_id: 43,
                sequence: 1,
            },
        )
        .await
        .unwrap();
    let read = operations
        .get(&key, KvReadConsistency::MinAppliedSlot(write.chosen_slot))
        .await
        .unwrap();
    assert_eq!(read.revision, write.chosen_slot);

    let error = operations
        .compare_and_write(
            &[KvGroupMutation::Delete { key: key.clone() }],
            key,
            0,
            KvRequestIdentity {
                client_id: 43,
                sequence: 2,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        KvGroupOperationError::CompareFailed { current_revision } if current_revision == write.chosen_slot
    ));
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::group_operations::KvGroupOperationError;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv_server::group0_control_plane::Group0ControlPlane;

fn group_zero_store() -> Arc<PxKvStore> {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        0,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    store
}

#[tokio::test]
async fn facade_reads_scans_and_revision_checks_without_rpc_loopback() {
    let store = group_zero_store();
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    let first_revision = control
        .compare_and_put(Bytes::from_static(b"/domain/a"), Bytes::from_static(b"one"), 0)
        .await
        .unwrap();
    let read = control.get(b"/domain/a").await.unwrap();
    assert_eq!(read.revision, first_revision);
    assert_eq!(read.value.as_deref(), Some(b"one".as_slice()));

    let (items, truncated, cutoff) = control
        .scan_prefix(Bytes::from_static(b"/domain/"), Bytes::new(), 10, 0)
        .await
        .unwrap();
    assert_eq!(items.len(), 1);
    assert!(!truncated);
    assert!(cutoff >= first_revision);

    let error = control
        .compare_and_put(Bytes::from_static(b"/domain/a"), Bytes::from_static(b"stale"), 0)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        KvGroupOperationError::CompareFailed { current_revision } if current_revision == first_revision
    ));
}

#[tokio::test]
async fn facade_stays_fenced_after_a_new_local_tenure() {
    let store = group_zero_store();
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    let group = store.get_group(0).unwrap();
    group.local_replica().become_follower(1);
    group.local_replica().become_leader();
    group.stamp_proposing_term(1);

    let error = control.get(b"/domain/a").await.unwrap_err();
    assert!(matches!(error, KvGroupOperationError::NotLeader { .. }));
}

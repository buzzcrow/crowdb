// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
mod common;
use common::cluster::KvCluster;
use crowdb_diskdb::ddb_kv_client::OwnershipFence;
use crowdb_diskdb::model::disk_group::DdbDiskGroup;
use std::sync::Arc;

#[tokio::test]
async fn handover_rejects_old_writes_and_cannot_reclaim_an_old_generation() {
    let cluster = KvCluster::start().await;
    let kv = cluster.make_ddb_kv_client().with_ownership_fencing();
    let fence = OwnershipFence {
        rack_id: 1,
        node_id: 1,
        disk_group_id: 1,
        instance_id: 1,
        generation: 1,
    };
    let old_group = DdbDiskGroup::new(1, 1, 1);
    old_group.set_ownership_fence(Some(Arc::new(fence.clone())));
    let old = kv.for_group(&old_group);
    let disk = crowdb_protocol::common::DiskId::default();
    assert!(kv.delete_recovery_scan_progress((0, 1), &disk).await.is_err());
    kv.claim_ownership((0, 1), &fence).await.unwrap();
    old.delete_recovery_scan_progress((0, 1), &disk).await.unwrap();
    let next = OwnershipFence {
        instance_id: 2,
        generation: 2,
        ..fence.clone()
    };
    kv.claim_ownership((0, 1), &next).await.unwrap();
    assert!(old.delete_recovery_scan_progress((0, 1), &disk).await.is_err());
    assert!(kv.claim_ownership((0, 1), &fence).await.is_err());
    let new_group = DdbDiskGroup::new(1, 1, 1);
    new_group.set_ownership_fence(Some(Arc::new(next)));
    let new = kv.for_group(&new_group);
    let results =
        futures::future::join_all((0..8).map(|_| new.delete_recovery_scan_progress((0, 1), &disk))).await;
    for result in results {
        result.unwrap();
    }
}

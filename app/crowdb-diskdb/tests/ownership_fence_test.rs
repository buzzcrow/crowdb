// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
mod common;
use common::metadata::TestMetadata;
use crowdb_diskdb::ddb_kv_client::OwnershipFence;
use crowdb_diskdb::model::disk_group::DdbDiskGroup;
use crowdb_diskdb::model::zone::DdbZone;
use crowdb_protocol::{
    common::DiskId,
    key::{BinaryKey, ZoneKey},
    ZoneValueExt,
};
use std::sync::Arc;

#[tokio::test]
async fn stale_zone_loader_cannot_mark_a_successor_disk_offline() {
    use crowdb_kv_client::ReadMode;
    use crowdb_protocol::{
        common::{HwStatus, OwnerMapValue},
        diskdb::rpc::DiskValue,
        key::{OwnerMapKey, TextKey},
    };
    let cluster = TestMetadata::start().await;
    let kv = cluster.make_ddb_kv_client().with_ownership_fencing();
    let hardware = cluster.make_hardware_client();
    let owner_key = OwnerMapKey {
        rack_id: 1,
        node_id: 1,
        disk_group_id: 1,
    }
    .to_path();
    let owner = serde_json::to_vec(&OwnerMapValue {
        instance_id: 1,
        lease_expiry_ms: u64::MAX,
    })
    .unwrap();
    let write = kv
        .kv()
        .put_cas(0, 0, owner_key.as_bytes(), &owner, 0)
        .await
        .unwrap();
    let fence = OwnershipFence {
        rack_id: 1,
        node_id: 1,
        disk_group_id: 1,
        instance_id: 1,
        generation: write.revision,
    };
    let group = DdbDiskGroup::new(1, 1, 1);
    group.set_ownership_fence(Some(Arc::new(fence)));
    let scoped = kv.for_group(&group);
    let disk = DiskId { high: 0, low: 1 };
    let online = DiskValue {
        status: HwStatus::Up as i32,
        ..Default::default()
    };
    hardware.add_disk(1, 1, 1, &disk, &online).await.unwrap();
    scoped.mark_disk_offline_if_owned(&group, &disk).await.unwrap();
    assert_eq!(
        hardware.get_disk(1, 1, 1, &disk).await.unwrap().unwrap().status,
        HwStatus::Offline as i32
    );
    hardware.add_disk(1, 1, 1, &disk, &online).await.unwrap();
    let crowdb_kv_client::GetOutcome::Found { revision, .. } = kv
        .kv()
        .get(0, 0, owner_key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("owner must remain present")
    };
    assert!(scoped.mark_disk_offline_if_owned(&group, &disk).await.is_err());
    group.set_ownership_fence(Some(Arc::new(OwnershipFence {
        rack_id: 1,
        node_id: 1,
        disk_group_id: 1,
        instance_id: 1,
        generation: revision,
    })));
    let scoped = kv.for_group(&group);
    let next = serde_json::to_vec(&OwnerMapValue {
        instance_id: 2,
        lease_expiry_ms: u64::MAX,
    })
    .unwrap();
    kv.kv()
        .put_cas(0, 0, owner_key.as_bytes(), &next, revision)
        .await
        .unwrap();
    assert!(scoped.mark_disk_offline_if_owned(&group, &disk).await.is_err());
    assert_eq!(hardware.get_disk(1, 1, 1, &disk).await.unwrap().unwrap(), online);
}

#[tokio::test]
async fn handover_rejects_old_writes_and_cannot_reclaim_an_old_generation() {
    let cluster = TestMetadata::start().await;
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

#[tokio::test]
async fn concurrent_disk_initialization_persists_every_zone_under_one_owner() {
    let cluster = TestMetadata::start().await;
    let kv = cluster.make_ddb_kv_client().with_ownership_fencing();
    let fence = OwnershipFence {
        rack_id: 1,
        node_id: 1,
        disk_group_id: 1,
        instance_id: 1,
        generation: 1,
    };
    kv.claim_ownership((0, 1), &fence).await.unwrap();
    let group = DdbDiskGroup::new(1, 1, 1);
    group.set_ownership_fence(Some(Arc::new(fence)));
    let scoped = kv.for_group(&group);
    // One sequential initialization per disk, sharing the DG ownership guard.
    let results = futures::future::join_all((1..=8).map(|id| {
        let scoped = scoped.clone();
        async move {
            let disk = DiskId { high: id, low: 1 };
            for zone_index in 0..32 {
                let snapshot = DdbZone::new(disk, zone_index, 1, 128).to_zone_value();
                scoped.put_zone((0, 1), &disk, zone_index, &snapshot).await?;
            }
            Ok::<_, crowdb_kv_client::Error>(disk)
        }
    }))
    .await;
    for result in results {
        let disk = result.expect("concurrent initialization must tolerate ownership guard contention");
        for zone_index in 0..32 {
            let key = ZoneKey {
                disk_id: disk,
                zone_index,
            };
            let record = kv
                .kv()
                .get(
                    0,
                    1,
                    &key.to_bytes(),
                    crowdb_kv_client::ReadMode::Linearizable,
                    None,
                )
                .await
                .unwrap();
            let crowdb_kv_client::GetOutcome::Found { value, .. } = record else {
                panic!("missing zone baseline")
            };
            let snapshot: crowdb_protocol::diskdb::rpc::ZoneValue = bincode::deserialize(&value).unwrap();
            assert!(snapshot.verify_checksum());
            assert_eq!(snapshot, DdbZone::new(disk, zone_index, 1, 128).to_zone_value());
        }
    }
}

#[tokio::test]
async fn ordinary_writes_cannot_bypass_ownership_handover() {
    use bytes::Bytes;
    use crowdb_kv_client::{BatchOp, GetOutcome, ReadMode};
    let cluster = TestMetadata::start().await;
    let ddb = cluster.make_ddb_kv_client();
    let kv = ddb.kv();
    let key = b"/diskdb/ownership-fence/1/1/1";
    let other = b"/diskdb/ownership-fence/1/1/2";
    kv.put_cas(0, 1, key, b"owner", 0).await.unwrap();
    assert!(kv.put(0, 1, key, b"bypass", None).await.is_err());
    assert!(kv.delete(0, 1, key, None).await.is_err());
    let ops = [BatchOp::Put {
        key: Bytes::copy_from_slice(key),
        value: Bytes::from_static(b"bypass"),
    }];
    assert!(kv.batch_write(0, 1, &ops).await.is_err());
    let two_fences = [
        ops[0].clone(),
        BatchOp::Put {
            key: Bytes::copy_from_slice(other),
            value: Bytes::from_static(b"other"),
        },
    ];
    let GetOutcome::Found { revision, .. } = kv.get(0, 1, key, ReadMode::Linearizable, None).await.unwrap()
    else {
        panic!("owner fence missing")
    };
    assert!(kv
        .batch_write_cas(0, 1, &two_fences, key, revision)
        .await
        .is_err());
    let GetOutcome::Found {
        value,
        revision: unchanged,
    } = kv.get(0, 1, key, ReadMode::Linearizable, None).await.unwrap()
    else {
        panic!("owner fence missing")
    };
    assert_eq!(value.as_ref(), b"owner");
    assert_eq!(revision, unchanged);
    assert!(matches!(
        kv.get(0, 1, other, ReadMode::Linearizable, None).await.unwrap(),
        GetOutcome::NotFound
    ));
}

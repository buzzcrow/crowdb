// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use std::{sync::Arc, time::Duration};

use common::metadata::{wait_for_disks_ready, TestMetadata};
use crowdb_diskdb::{
    ddb_config::KeepAliveConfig, liveness::keepalive::KeepAlive,
    model::disk_group_container::DdbDiskGroupContainer,
};
use crowdb_protocol::{
    common::{DiskId, HwStatus, NodeValue, RackValue},
    diskdb::rpc::{DiskGroupValue, DiskValue},
};

#[tokio::test]
async fn provisional_init_waits_for_activation_and_only_then_loads_allocatable_zones() {
    let metadata = TestMetadata::start().await;
    let id = DiskId { high: 4, low: 5 };
    let hw = prepare_hardware(&metadata, HwStatus::Init).await;
    let container = Arc::new(DdbDiskGroupContainer::new(999));
    let completion = Arc::new(tokio::sync::Notify::new());
    let keepalive = KeepAlive::new(
        metadata.make_hardware_client(),
        metadata.make_service_registry_client(),
        Arc::clone(&container),
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(metadata.make_ddb_kv_client())
    .with_sync_trigger(completion.clone());
    for _ in 0..2 {
        let outcome = keepalive.tick().await;
        assert_eq!(outcome.disks_added, 0);
        assert!(container.get_disk_group(3).unwrap().disk_snapshot().is_empty());
        assert_eq!(
            hw.get_disk(1, 2, 3, &id).await.unwrap().unwrap().status,
            HwStatus::Init as i32
        );
    }
    hw.set_disk_status(1, 2, 3, &id, HwStatus::Up).await.unwrap();
    assert_eq!(keepalive.tick().await.disks_added, 1);
    wait_for_disks_ready(&container, 3, 1, 1).await;
    let group = container.get_disk_group(3).unwrap();
    assert!(group.disk_snapshot()[0].allocatable());
    tokio::time::timeout(Duration::from_millis(100), completion.notified())
        .await
        .expect("completed zone loading must wake publication without another heartbeat interval");
    keepalive.tick().await;
    let instances = metadata
        .make_service_registry_client()
        .read_all_diskdb_instances()
        .await
        .unwrap();
    let summary = &instances[0]
        .1
        .extra
        .as_ref()
        .unwrap()
        .diskdb
        .as_ref()
        .unwrap()
        .group_usages[0];
    assert!(summary.usage_known);
    assert_eq!(summary.allocatable_disk_count, 1);
}

#[test]
fn exclusion_waits_for_confirmed_writes_and_never_accepts_cancelled_outcomes() {
    use crowdb_diskdb::model::disk::DiskWriteGate;
    let gate = DiskWriteGate::default();
    let write = gate.enter().unwrap();
    gate.close();
    assert!(!gate.quiescent());
    assert!(gate.enter().is_none());
    write.complete();
    assert!(gate.quiescent());
    let uncertain = DiskWriteGate::default();
    drop(uncertain.enter().unwrap());
    assert!(uncertain.closed());
    assert!(!uncertain.quiescent(), "an unconfirmed write must block removal");
}

#[tokio::test]
async fn excluded_disk_rejects_old_scoped_metadata_writes_and_late_loader_status() {
    use crowdb_diskdb::model::{disk::DdbDisk, disk_group::DdbDiskGroup, zone::DdbZone};
    let metadata = TestMetadata::start().await;
    let kv = metadata.make_ddb_kv_client();
    let id = DiskId { high: 4, low: 5 };
    let group = DdbDiskGroup::new(3, 2, 1);
    let disk = Arc::new(DdbDisk::new(id, 3, 2, 1, DiskValue::default()));
    group.add_disk(disk.clone());
    let scoped = kv.for_group(&group);
    let zone = DdbZone::new(id, 0, 3, 128).to_zone_value();
    scoped
        .put_zone_batch((0, 1), &id, &[(0, zone.clone())])
        .await
        .unwrap();
    disk.exclude();
    disk.set_effective_status(HwStatus::Up);
    assert_eq!(disk.effective_status(), HwStatus::Offline);
    assert!(!disk.allocatable());
    assert!(disk.write_gate.quiescent());
    assert!(scoped.put_zone_batch((0, 1), &id, &[(0, zone)]).await.is_err());
    assert!(
        disk.write_gate.quiescent(),
        "rejected writes do not create unknown outcomes"
    );
}

#[tokio::test]
async fn atomic_multi_disk_rejection_does_not_poison_unsubmitted_writes() {
    use crowdb_diskdb::model::{disk::DdbDisk, disk_group::DdbDiskGroup};
    use crowdb_protocol::diskdb::rpc::BusyBlockValue;
    let metadata = TestMetadata::start().await;
    let group = DdbDiskGroup::new(3, 2, 1);
    let first = Arc::new(DdbDisk::new(
        DiskId { high: 1, low: 1 },
        3,
        2,
        1,
        DiskValue::default(),
    ));
    let second = Arc::new(DdbDisk::new(
        DiskId { high: 1, low: 2 },
        3,
        2,
        1,
        DiskValue::default(),
    ));
    group.add_disk(first.clone());
    group.add_disk(second.clone());
    let kv = metadata.make_ddb_kv_client().for_group(&group);
    second.exclude();
    let records = vec![
        (first.disk_id, 0, 0, BusyBlockValue::default()),
        (second.disk_id, 0, 0, BusyBlockValue::default()),
    ];
    assert!(kv.persist_busy_batch((0, 1), &records).await.is_err());
    first.exclude();
    assert!(first.write_gate.quiescent(), "no batch was submitted");
    assert!(second.write_gate.quiescent());
}

async fn prepare_hardware(metadata: &TestMetadata, status: HwStatus) -> crowdb_kv_client::HardwareClient {
    let hw = metadata.make_hardware_client();
    let id = DiskId { high: 4, low: 5 };
    hw.add_rack(
        1,
        &RackValue {
            status: HwStatus::Up as i32,
            node_ids: vec![2],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_node(
        1,
        2,
        &NodeValue {
            status: HwStatus::Up as i32,
            disk_group_ids: vec![3],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_disk_group(
        1,
        2,
        3,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: vec![id],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_disk(
        1,
        2,
        3,
        &id,
        &DiskValue {
            status: status as i32,
            disk_type: 0,
            capacity_units: 128,
            zone_size_units: 128,
            unit_size_bytes: 1024 * 1024,
            zone_count: 1,
            device_path: String::new(),
        },
    )
    .await
    .unwrap();
    hw.set_owner(1, 2, 3, 999, u64::MAX).await.unwrap();
    hw.set_bind(1, 2, 3, 0, 1).await.unwrap();
    hw
}

#[tokio::test]
async fn owner_confirmation_waits_for_admitted_persistence_and_binds_exact_revision() {
    use crowdb_protocol::{disk_storage::DiskExclusionReporter, key::DiskKey};
    let metadata = TestMetadata::start().await;
    let hw = prepare_hardware(&metadata, HwStatus::Up).await;
    let id = DiskId { high: 4, low: 5 };
    let container = Arc::new(DdbDiskGroupContainer::new(999));
    let keepalive = KeepAlive::new(
        metadata.make_hardware_client(),
        metadata.make_service_registry_client(),
        container.clone(),
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ownership_fencing()
    .with_ddb_kv_client(metadata.make_ddb_kv_client().with_ownership_fencing());
    keepalive.tick().await;
    wait_for_disks_ready(&container, 3, 1, 1).await;
    let group = container.get_disk_group(3).unwrap();
    let disk = group.disk_snapshot()[0].clone();
    let write = disk.write_gate.enter().unwrap();
    hw.set_disk_status(1, 2, 3, &id, HwStatus::Offline).await.unwrap();
    let key = DiskKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id: id,
    };
    let revision = hw.get_disk_with_revision(&key).await.unwrap().unwrap().1;
    keepalive.tick().await;
    assert!(!disk.allocatable());
    assert!(hw
        .disk_exclusion(&key, DiskExclusionReporter::Diskdb, 999)
        .await
        .unwrap()
        .is_none());
    write.complete();
    keepalive.tick().await;
    let proof = hw
        .disk_exclusion(&key, DiskExclusionReporter::Diskdb, 999)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(proof.disk_revision, revision);
    assert_eq!(
        proof.owner_revision,
        Some(group.ownership_fence().unwrap().generation)
    );
}

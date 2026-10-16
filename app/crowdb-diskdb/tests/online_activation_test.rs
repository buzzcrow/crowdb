// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;
#[path = "common/online.rs"]
mod online;
use common::metadata::wait_for_disks_ready;
use crowdb_diskdb::recovery::ZoneLoader;
use crowdb_protocol::{
    common::HwStatus,
    disk_storage::{DiskExclusionRecord, DiskExclusionReporter, DiskOnlineState},
};
use online::{TestOnline, ID, KEY};
use std::sync::Arc;

#[tokio::test]
async fn validated_online_reloads_existing_occupancy_without_reopening_old_snapshots() {
    let test = TestOnline::start(false).await;
    let accepted = test
        .hardware
        .request_disk_online(&KEY, test.offline_revision)
        .await
        .unwrap();
    assert_eq!(accepted.state, DiskOnlineState::Pending);
    test.keepalive.tick().await;
    assert_eq!(test.observe_outcome().await.state, DiskOnlineState::Complete);
    test.keepalive.tick().await;
    wait_for_disks_ready(&test.container, 3, 1, 1).await;
    let group = test.container.get_disk_group(3).unwrap();
    let current = group.get_disk(ID).unwrap();
    assert!(!Arc::ptr_eq(&current, &test.old));
    assert!(current.allocatable());
    assert_eq!(current.zones.load()[0].usage_bits.count_set(), 1);
    assert!(test.old.write_gate.quiescent());
    assert!(!test.old.allocatable());
    assert!(test.old.write_gate.enter().is_none());
    assert!(test
        .hardware
        .remove_disk_storage(&KEY, test.offline_revision)
        .await
        .is_err());
    assert_eq!(
        test.hardware
            .request_disk_online(&KEY, test.offline_revision)
            .await
            .unwrap()
            .state,
        DiskOnlineState::Complete
    );
    assert!(test
        .metadata
        .make_ddb_kv_client()
        .get_busy((0, 1), &ID, 0, 5)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn corrupt_retained_metadata_stays_offline_and_startup_never_uses_blank_fallback() {
    let test = TestOnline::start(true).await;
    test.hardware
        .request_disk_online(&KEY, test.offline_revision)
        .await
        .unwrap();
    test.keepalive.tick().await;
    let failed = test.observe_outcome().await;
    assert_eq!(failed.state, DiskOnlineState::Failed);
    assert!(failed.error.unwrap().contains("checksum"));
    assert_eq!(
        test.hardware
            .get_disk_with_revision(&KEY)
            .await
            .unwrap()
            .unwrap()
            .1,
        test.offline_revision
    );
    assert!(test.old.write_gate.quiescent());
    // Even an Up observation after restart must honor the durable retained marker.
    let mut disk = test
        .hardware
        .get_disk_with_revision(&KEY)
        .await
        .unwrap()
        .unwrap()
        .0;
    disk.status = HwStatus::Up as i32;
    let loader = ZoneLoader::new(Arc::new(test.metadata.make_ddb_kv_client()), 1);
    assert!(loader
        .load_disk_group(3, 2, 1, (0, 1), &[(ID, disk)], 4)
        .await
        .is_err());
    assert_eq!(
        test.hardware
            .request_disk_online(&KEY, test.offline_revision)
            .await
            .unwrap()
            .state,
        DiskOnlineState::Pending
    );
}

#[tokio::test]
async fn owner_generation_changes_invalidate_pending_online_validation() {
    let test = TestOnline::start(false).await;
    let accepted = test
        .hardware
        .request_disk_online(&KEY, test.offline_revision)
        .await
        .unwrap();
    let old_fence = test
        .container
        .get_disk_group(3)
        .unwrap()
        .ownership_fence()
        .unwrap();
    let service = test.metadata.make_service_registry_client();
    let predecessor = service.diskdb_session_predecessor(999).await.unwrap();
    let epoch = service
        .claim_diskdb_session(999, "replacement", predecessor.0.as_deref(), predecessor.1)
        .await
        .unwrap();
    test.hardware
        .claim_owner_process_epoch(
            &crowdb_protocol::key::OwnerMapKey {
                rack_id: 1,
                node_id: 2,
                disk_group_id: 3,
            },
            999,
            epoch,
        )
        .await
        .unwrap();
    assert!(test
        .hardware
        .finish_disk_online(&KEY, &accepted, 999, old_fence.generation, None)
        .await
        .is_err());
    assert_eq!(
        test.hardware
            .get_disk_with_revision(&KEY)
            .await
            .unwrap()
            .unwrap()
            .1,
        test.offline_revision
    );
    assert!(
        test.hardware
            .remove_disk_storage(&KEY, test.offline_revision)
            .await
            .is_err(),
        "old-owner exclusion cannot authorize removal"
    );
}

#[tokio::test]
async fn remove_and_rejoin_cannot_reuse_an_old_online_authorization() {
    let test = TestOnline::start(false).await;
    let accepted = test
        .hardware
        .request_disk_online(&KEY, test.offline_revision)
        .await
        .unwrap();
    let fence = test
        .container
        .get_disk_group(3)
        .unwrap()
        .ownership_fence()
        .unwrap();
    let removed = test
        .hardware
        .remove_disk_storage(&KEY, test.offline_revision)
        .await
        .unwrap();
    test.hardware
        .rejoin_disk_storage(&KEY, &removed.operation_id)
        .await
        .unwrap();
    assert!(test
        .hardware
        .finish_disk_online(&KEY, &accepted, 999, fence.generation, None)
        .await
        .is_err());
    assert!(test
        .hardware
        .request_disk_online(&KEY, test.offline_revision)
        .await
        .is_err());
    assert_eq!(
        test.hardware
            .get_disk_with_revision(&KEY)
            .await
            .unwrap()
            .unwrap()
            .0
            .status,
        HwStatus::Offline as i32
    );
}

#[tokio::test]
async fn pending_online_waits_for_every_configured_diskio_exclusion() {
    let test = TestOnline::start(false).await;
    test.hardware
        .request_disk_online(&KEY, test.offline_revision)
        .await
        .unwrap();
    test.metadata
        .make_service_registry_client()
        .heartbeat_diskio_scoped(
            8,
            "b:7200",
            &crowdb_protocol::common::DiskioHardwareScope {
                version: 1,
                rack_id: 1,
                node_id: 2,
                disk_group_id: 0,
            },
            &[],
            &[],
        )
        .await
        .unwrap();
    test.keepalive.tick().await;
    assert_eq!(
        test.hardware
            .disk_online_operation(&KEY)
            .await
            .unwrap()
            .unwrap()
            .state,
        DiskOnlineState::Pending
    );
    assert_eq!(
        test.hardware
            .get_disk_with_revision(&KEY)
            .await
            .unwrap()
            .unwrap()
            .1,
        test.offline_revision
    );
    test.hardware
        .acknowledge_disk_exclusion(&DiskExclusionRecord {
            version: 1,
            rack_id: 1,
            node_id: 2,
            disk_group_id: 3,
            disk_id: ID,
            disk_revision: test.offline_revision,
            reporter: DiskExclusionReporter::Diskio,
            instance_id: 8,
            owner_revision: None,
        })
        .await
        .unwrap();
    test.keepalive.tick().await;
    assert_eq!(test.observe_outcome().await.state, DiskOnlineState::Complete);
}

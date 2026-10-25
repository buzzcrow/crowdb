// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, GetOutcome, HardwareClient, ReadMode};
use crowdb_monitor::{accept_file_for_tests, prepare_file_for_tests, publish_file_for_tests};
use crowdb_protocol::{
    common::{HwStatus, NodeValue, RackValue},
    disk_storage::DiskStorageRecord,
    diskdb::rpc::DiskGroupValue,
    key::DiskKey,
    mgmt::node::{
        setup::{NodeSetupChoices, NodeSetupRequest},
        storage::{FileDiskAdmission, FilePreparationPhase, PrepareDiskFile},
    },
};
use std::os::unix::fs::MetadataExt;
#[path = "common/node_setup.rs"]
mod node_setup;

#[tokio::test]
async fn managed_files_reserve_physical_identity_and_replay_old_receipts_without_migration() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("managed-file-admission");
    let (authority, hardware, request) = fixture(root.path()).await;
    accept_file_for_tests(root.path(), &request).unwrap();
    let prepared = prepare_file_for_tests(root.path(), &request.operation_id).unwrap();
    let before = std::fs::metadata(&prepared.path).unwrap();
    crowdb_monitor::resume_file_operations_for_tests(root.path())
        .await
        .unwrap();
    let progress = crowdb_monitor::file_operations_for_tests(root.path(), None, 32)
        .unwrap()
        .items
        .into_iter()
        .find(|state| state.request.operation_id == request.operation_id)
        .unwrap();
    assert_eq!(progress.phase, FilePreparationPhase::WaitingForDiskIO);
    let (high, low) = uuid::Uuid::parse_str(&request.operation_id)
        .unwrap()
        .as_u64_pair();
    let key = DiskKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id: crowdb_protocol::common::DiskId { high, low },
    };
    let record = hardware.disk_storage(&key).await.unwrap().unwrap();
    assert_eq!(record.version, 2);
    let physical = record.physical_backing.as_ref().unwrap();
    assert_eq!(physical.physical_host_id, "owned-host");
    assert_eq!(physical.resource_identity, prepared.backing_identity.unwrap());
    assert_eq!(physical.capacity_bytes, request.capacity_bytes);
    let claims = authority
        .kv
        .scan(
            0,
            0,
            b"/hw/disk-resource/",
            b"",
            b"",
            16,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(claims.items.len(), 1);
    legacy_replay(root.path(), &request, &hardware, &authority, record).await;
    assert_eq!(
        hardware
            .get_disk_with_revision(&key)
            .await
            .unwrap()
            .unwrap()
            .0
            .status,
        HwStatus::Up as i32
    );
    let after = std::fs::metadata(&prepared.path).unwrap();
    assert_eq!(
        (before.ino(), before.len(), before.blocks()),
        (after.ino(), after.len(), after.blocks())
    );
}

async fn fixture(root: &std::path::Path) -> (node_setup::TestAuthority, HardwareClient, PrepareDiskFile) {
    let setup = NodeSetupRequest {
        version: 1,
        discovery_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        choices: NodeSetupChoices::default(),
    };
    let authority = node_setup::TestAuthority::start(root, &setup).await;
    assert_eq!(authority.registry.nodes[0].physical_host_id, "owned-host");
    let hardware = HardwareClient::new(CrowdbKvClient::new(ClientConfig::new(
        authority.binding.management_seeds.clone(),
    )));
    hardware
        .add_rack(
            1,
            &RackValue {
                node_ids: vec![2],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hardware
        .add_node(
            1,
            2,
            &NodeValue {
                disk_group_ids: vec![3],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hardware
        .add_disk_group(1, 2, 3, &DiskGroupValue::default())
        .await
        .unwrap();
    let request = PrepareDiskFile {
        operation_id: uuid::Uuid::new_v4().to_string(),
        cluster_id: authority.binding.bootstrap.cluster_id.clone(),
        node_id: 2,
        capacity_bytes: 64 * 1024 * 1024,
        reserve_bytes: 0,
        sparse: false,
        admission: Some(FileDiskAdmission {
            rack_id: 1,
            disk_group_id: 3,
            unit_size_bytes: 1024 * 1024,
            zone_size_bytes: 64 * 1024 * 1024,
            media_type: None,
        }),
    };
    (authority, hardware, request)
}

async fn legacy_replay(
    root: &std::path::Path,
    request: &PrepareDiskFile,
    hardware: &HardwareClient,
    authority: &node_setup::TestAuthority,
    mut record: DiskStorageRecord,
) {
    // Represent an existing deployed schema without upgrading its interpretation.
    hardware.activate_disk_storage(&record).await.unwrap();
    record.version = 1;
    record.physical_backing = None;
    authority
        .kv
        .put(
            0,
            0,
            record.key().as_bytes(),
            &serde_json::to_vec(&record).unwrap(),
            None,
        )
        .await
        .unwrap();
    let old = authority
        .kv
        .get(0, 0, record.key().as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap();
    publish_file_for_tests(root, &request.operation_id).await.unwrap();
    let new = authority
        .kv
        .get(0, 0, record.key().as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap();
    match (old, new) {
        (
            GetOutcome::Found {
                value: a,
                revision: x,
            },
            GetOutcome::Found {
                value: b,
                revision: y,
            },
        ) => {
            assert_eq!(a, b);
            assert_eq!(x, y);
            assert_eq!(
                serde_json::from_slice::<DiskStorageRecord>(&b).unwrap().version,
                1
            );
        }
        _ => panic!("legacy receipt disappeared"),
    }
}

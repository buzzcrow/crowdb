// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::{
    common::{DiskId, HwStatus},
    disk_storage::DiskStorageRecord,
    diskdb::rpc::DiskValue,
    mgmt::node::storage::DiskBackend,
};

fn record() -> DiskStorageRecord {
    DiskStorageRecord {
        version: 1,
        physical_backing: None,
        operation_id: "operation".into(),
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id: DiskId { high: 4, low: 5 },
        backend: DiskBackend::File,
        backing_identity: "file:10:20".into(),
        media_type: None,
        sparse: false,
        geometry: DiskValue {
            disk_type: 0,
            capacity_units: 4096,
            zone_size_units: 1024,
            unit_size_bytes: 4096,
            zone_count: 4,
            status: HwStatus::Init as i32,
            device_path: "/opt/crowdb/data/disks/original.img".into(),
        },
        removed: false,
    }
}

#[test]
fn retained_identity_round_trip_keeps_unknown_media_distinct_from_legacy_type() {
    let mut record = record();
    record.removed = true;
    record.validate().unwrap();
    let encoded = serde_json::to_vec(&record).unwrap();
    let decoded: DiskStorageRecord = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, record);
    assert_eq!(decoded.media_type, None);
    assert_eq!(decoded.geometry.disk_type, 0);
    assert_eq!(
        record.key(),
        "/hw/disk-storage/1/2/3/00000000000000040000000000000005"
    );
}

#[test]
fn invalid_backing_or_schema_never_becomes_a_usable_record() {
    let original = record();
    let mut changed = original.clone();
    changed.version = 2;
    assert!(changed.validate().is_err());
    changed = original.clone();
    changed.backing_identity.clear();
    assert!(changed.validate().is_err());
    changed = original.clone();
    changed.backend = DiskBackend::BlockDevice;
    changed.sparse = true;
    assert!(changed.validate().is_err());
    changed = original.clone();
    changed.geometry.zone_count += 1;
    assert!(changed.validate().is_err());
    changed = original.clone();
    changed.geometry.capacity_units += 1;
    assert!(changed.validate().is_err());
    changed = original.clone();
    changed.geometry.capacity_units = 16;
    changed.geometry.zone_size_units = 16;
    changed.geometry.zone_count = 1;
    assert!(changed.validate().is_err());
    changed = original;
    changed.geometry.capacity_units = u64::MAX;
    assert!(changed.validate().is_err());
}

#[test]
fn exclusion_evidence_retains_exact_identity_and_rejects_invalid_generations() {
    use crowdb_protocol::disk_storage::{DiskExclusionRecord, DiskExclusionReporter};
    let mut evidence = DiskExclusionRecord {
        version: 1,
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id: DiskId {
            high: u64::MAX,
            low: u64::MAX,
        },
        disk_revision: u64::MAX,
        reporter: DiskExclusionReporter::Diskio,
        instance_id: 7,
        owner_revision: None,
    };
    evidence.validate().unwrap();
    assert_eq!(
        serde_json::from_slice::<DiskExclusionRecord>(&serde_json::to_vec(&evidence).unwrap()).unwrap(),
        evidence
    );
    assert!(evidence
        .key()
        .ends_with("/ffffffffffffffffffffffffffffffff/diskio/7"));
    evidence.reporter = DiskExclusionReporter::Diskdb;
    assert!(evidence.validate().is_err());
    evidence.owner_revision = Some(1);
    evidence.validate().unwrap();
    evidence.disk_revision = 0;
    assert!(evidence.validate().is_err());
}

#[test]
fn removal_receipt_preserves_backing_geometry_and_fixes_operation_identity() {
    use crowdb_protocol::{disk_storage::DiskRemovalRecord, key::DiskKey};
    let mut retained = record();
    let key = DiskKey {
        rack_id: retained.rack_id,
        node_id: retained.node_id,
        disk_group_id: retained.disk_group_id,
        disk_id: retained.disk_id,
    };
    assert!(DiskRemovalRecord::new(retained.clone(), 7).validate().is_err());
    retained.removed = true;
    let receipt = DiskRemovalRecord::new(retained.clone(), 7);
    receipt.validate().unwrap();
    assert_eq!(receipt.disk, retained);
    assert!(DiskRemovalRecord::key_for(&key).starts_with("/hw/disk-removal/1/2/3/"));
    assert_eq!(
        serde_json::from_slice::<DiskRemovalRecord>(&serde_json::to_vec(&receipt).unwrap()).unwrap(),
        receipt
    );
    let mut invalid = receipt.clone();
    invalid.operation_id = "changed".into();
    assert!(invalid.validate().is_err());
    invalid = receipt;
    invalid.offline_revision = 0;
    assert!(invalid.validate().is_err());
}

#[test]
fn rejoin_receipt_retains_the_exact_removed_source_without_initializing_it() {
    use crowdb_protocol::disk_storage::{DiskRejoinRecord, DiskRemovalRecord};
    let mut source = record();
    source.removed = true;
    let mut receipt = DiskRejoinRecord::new(DiskRemovalRecord::new(source.clone(), 7));
    receipt.validate().unwrap();
    assert_eq!(receipt.source.disk, source);
    let mut restored = source;
    restored.removed = false;
    assert_eq!(receipt.restored(), restored);
    assert_eq!(receipt.operation_id, "rejoin/0000000000000004-0000000000000005/7");
    assert_eq!(
        serde_json::from_slice::<DiskRejoinRecord>(&serde_json::to_vec(&receipt).unwrap()).unwrap(),
        receipt
    );
    receipt.operation_id.push_str("/changed");
    assert!(receipt.validate().is_err());
}

#[test]
fn online_authorization_fixes_offline_revision_and_validates_outcome() {
    use crowdb_protocol::disk_storage::{DiskOnlineRecord, DiskOnlineState};
    let mut operation = DiskOnlineRecord::new(record(), u64::MAX);
    operation.validate().unwrap();
    assert!(operation.operation_id.ends_with(&u64::MAX.to_string()));
    assert_eq!(
        serde_json::from_slice::<DiskOnlineRecord>(&serde_json::to_vec(&operation).unwrap()).unwrap(),
        operation
    );
    operation.state = DiskOnlineState::Failed;
    assert!(operation.validate().is_err());
    operation.error = Some("invalid existing snapshot".into());
    operation.validate().unwrap();
    operation.state = DiskOnlineState::Complete;
    assert!(operation.validate().is_err());
    operation.error = None;
    operation.validate().unwrap();
    operation.offline_revision = 0;
    assert!(operation.validate().is_err());
}

#[test]
fn physical_backing_preserves_tail_and_legacy_capacity_contract() {
    use crowdb_protocol::disk_storage::PhysicalBacking;
    let mut disk = record();
    let logical = disk.physical_capacity_bytes().unwrap();
    let legacy = serde_json::to_value(&disk).unwrap();
    assert!(legacy.get("physical_backing").is_none());
    disk.version = 2;
    assert!(disk.validate().is_err());
    disk.physical_backing = Some(PhysicalBacking {
        physical_host_id: "owned-host".into(),
        resource_identity: "file:10:20".into(),
        capacity_bytes: logical + 512,
    });
    disk.validate().unwrap();
    assert_eq!(disk.physical_capacity_bytes(), Some(logical + 512));
    assert_eq!(
        disk.geometry.capacity_units * u64::from(disk.geometry.unit_size_bytes),
        logical
    );
    assert_eq!(
        serde_json::from_value::<DiskStorageRecord>(serde_json::to_value(&disk).unwrap()).unwrap(),
        disk
    );
    disk.version = 1;
    assert!(
        disk.validate().is_err(),
        "legacy version cannot acquire a new capacity interpretation"
    );
    disk.version = 2;
    disk.physical_backing.as_mut().unwrap().capacity_bytes = logical - 1;
    assert!(disk.validate().is_err());
    disk.physical_backing.as_mut().unwrap().capacity_bytes = logical;
    disk.physical_backing.as_mut().unwrap().resource_identity = "file:010:20".into();
    assert!(disk.validate().is_err());
    disk.physical_backing.as_mut().unwrap().resource_identity = "device:20".into();
    disk.physical_backing.as_mut().unwrap().physical_host_id.clear();
    assert!(disk.validate().is_err());
}

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
        "/hw/disk-storage/1/2/3/0000000000000004-0000000000000005"
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
    changed = original;
    changed.geometry.capacity_units = u64::MAX;
    assert!(changed.validate().is_err());
}

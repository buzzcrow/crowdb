// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use common::metadata::TestMetadata;
use crowdb_diskdb::{
    model::{
        records::{BusyRecord, FreeRecord, ZoneRecords},
        zone::DdbZone,
    },
    recovery::retained::{load_retained_zone, validate_retained_zone},
};
use crowdb_protocol::{
    common::DiskId,
    diskdb::rpc::{BusyBlockValue, FreeBlockValue},
    key::{BinaryKey, BusyBlockKey, FreeBlockKey, ZoneKey},
    ZoneValueExt,
};

const DISK: DiskId = DiskId { high: 5, low: 7 };
const CAPACITY: u32 = 512;

fn baseline() -> ZoneRecords {
    let zone = DdbZone::new(DISK, 0, 9, CAPACITY);
    let _ = zone.usage_bits.range_set(300, 1);
    ZoneRecords {
        zone_value: Some(zone.to_zone_value()),
        ..Default::default()
    }
}

fn busy(offset: u64, count: u32) -> BusyRecord {
    BusyRecord {
        key: BusyBlockKey {
            disk_id: DISK,
            zone_index: 0,
            unit_offset: offset,
        },
        value: BusyBlockValue {
            unit_count: count,
            unit_size: 4096,
            allocation_ts: 11,
            ..Default::default()
        },
        commit_slot: 1,
    }
}

fn validate(records: &ZoneRecords) -> bool {
    validate_retained_zone(DISK, 0, 9, CAPACITY, 4096, records, 100).is_ok()
}

#[test]
fn retained_load_preserves_uncertain_occupancy_and_seeds_allocation_timestamps() {
    let mut records = baseline();
    records.busy.push(busy(10, 3));
    records.free.push(FreeRecord {
        key: FreeBlockKey {
            disk_id: DISK,
            zone_index: 0,
            unit_offset: 10,
            allocation_ts: 11,
        },
        value: FreeBlockValue {
            unit_count: 3,
            pre_allocation_ts: 11,
            free_ts: 20,
            ..Default::default()
        },
        commit_slot: 2,
    });
    let (zone, timestamp) = validate_retained_zone(DISK, 0, 9, CAPACITY, 4096, &records, 100).unwrap();
    assert_eq!(
        zone.usage_bits.count_set(),
        4,
        "read-only validation cannot free uncertain bytes"
    );
    assert_eq!(timestamp, 20);
    assert_eq!(
        zone.uncompacted_free_record_count
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
}

#[test]
fn missing_corrupt_and_mismatched_snapshots_are_never_blank_disks() {
    assert!(!validate(&ZoneRecords::default()));
    let mut records = baseline();
    records.zone_value.as_mut().unwrap().crc32 ^= 1;
    assert!(!validate(&records));
    records.zone_value = Some(DdbZone::new(DISK, 0, 9, 64).to_zone_value());
    assert!(!validate(&records));
    records = baseline();
    let snapshot = records.zone_value.as_mut().unwrap();
    snapshot.snapshot_slot = 101;
    snapshot.compute_checksum();
    assert!(!validate(&records));
}

#[test]
fn invalid_ranges_states_and_overlaps_block_retained_recovery() {
    for record in [busy(511, 2), busy(0, 0), busy(u64::MAX, 2)] {
        let mut records = baseline();
        records.busy.push(record);
        assert!(!validate(&records));
    }
    let mut records = baseline();
    records.busy = vec![busy(1, 3), busy(3, 2)];
    assert!(!validate(&records));
    for field in ["state", "commit", "timestamp", "unit", "slot"] {
        let mut records = baseline();
        let mut record = busy(1, 1);
        match field {
            "state" => record.value.state = 100,
            "commit" => record.value.commit_state = 100,
            "timestamp" => record.value.allocation_ts = u64::MAX,
            "unit" => record.value.unit_size = 1,
            _ => record.commit_slot = 101,
        }
        records.busy.push(record);
        assert!(!validate(&records), "invalid {field} was accepted");
    }
}

#[tokio::test]
async fn retained_scan_advances_multiple_pages_without_writing_a_baseline() {
    let metadata = TestMetadata::start().await;
    let kv = metadata.make_ddb_kv_client();
    assert!(load_retained_zone(&kv, (0, 1), DISK, 0, 9, CAPACITY, 4096)
        .await
        .is_err());
    assert!(kv.get_zone_value((0, 1), &DISK, 0).await.unwrap().is_none());
    let snapshot = baseline().zone_value.unwrap();
    kv.put_zone((0, 1), &DISK, 0, &snapshot).await.unwrap();
    for offset in 0..260 {
        let record = busy(offset, 1);
        kv.kv()
            .put(
                0,
                1,
                &record.key.to_bytes(),
                &bincode::serialize(&record.value).unwrap(),
                None,
            )
            .await
            .unwrap();
    }
    let (zone, _) = load_retained_zone(&kv, (0, 1), DISK, 0, 9, CAPACITY, 4096)
        .await
        .unwrap();
    assert_eq!(zone.usage_bits.count_set(), 261);
    assert_eq!(
        kv.get_zone_value((0, 1), &DISK, 0).await.unwrap().unwrap(),
        snapshot
    );
    let key = busy(1, 1).key.to_bytes();
    kv.kv()
        .put(0, 1, &key, b"corrupted allocation", None)
        .await
        .unwrap();
    assert!(load_retained_zone(&kv, (0, 1), DISK, 0, 9, CAPACITY, 4096)
        .await
        .is_err());
    assert_eq!(
        kv.get_zone_value((0, 1), &DISK, 0).await.unwrap().unwrap(),
        snapshot
    );
    let snapshot_key = ZoneKey {
        disk_id: DISK,
        zone_index: 0,
    }
    .to_bytes();
    let mut bytes = snapshot.to_bytes();
    bytes.push(0);
    kv.kv().put(0, 1, &snapshot_key, &bytes, None).await.unwrap();
    assert!(kv.read_retained_zone_records((0, 1), &DISK, 0).await.is_err());
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::inspection::{InspectionError, LocationInspector, MAX_REFERENCE_BYTES};
use crowdb_access_s3::metadata::{BucketId, ObjectRecord};
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;

fn record(locations: &[Location]) -> ObjectRecord {
    let length = locations.iter().map(|location| location.logical_length).sum();
    ObjectRecord {
        bucket_id: BucketId::new([7; 16]),
        key: "folder/中文.bin".as_bytes().to_vec(),
        logical_length: length,
        checksum: vec![1; 16],
        etag: "etag".into(),
        created_at_ms: 1,
        modified_at_ms: 2,
        content_type: "application/octet-stream".into(),
        attributes: Vec::new(),
        data_reference: bincode::serialize(locations).unwrap(),
        data_length: length,
    }
}
fn locations() -> Vec<Location> {
    (0..43)
        .map(|index| Location {
            chunk_id: Some(ChunkId {
                high: u64::MAX,
                low: u64::MAX - index,
            }),
            offset: 9_007_199_254_740_993 + index,
            length: 8,
            logical_offset: index * 8,
            logical_length: 8,
        })
        .collect()
}

#[test]
fn bounded_pages_are_exact_and_generation_pinned_even_for_identical_overwrites() {
    let inspector = LocationInspector::new(vec![42; 32]).unwrap();
    let record = record(&locations());
    let first = inspector.page("bucket", &record, 100, 20, None, 1).unwrap();
    assert_eq!(first.locations.len(), 20);
    assert_eq!(
        first.locations[0].chunk_id.as_deref(),
        Some("ffffffffffffffffffffffffffffffff")
    );
    assert_eq!(first.locations[0].offset, "9007199254740993");
    let second = inspector
        .page("bucket", &record, 100, 20, first.next_cursor.as_deref(), 2)
        .unwrap();
    assert_eq!(second.locations[0].index, "20");
    let last = inspector
        .page("bucket", &record, 100, 20, second.next_cursor.as_deref(), 3)
        .unwrap();
    assert_eq!(last.locations.len(), 3);
    assert_eq!(last.locations[2].index, "42");
    assert!(last.next_cursor.is_none());
    assert!(matches!(
        inspector.page("bucket", &record, 101, 20, first.next_cursor.as_deref(), 2),
        Err(InspectionError::Stale)
    ));
    let mut another = record.clone();
    another.key = b"another".to_vec();
    assert!(matches!(
        inspector.page("bucket", &another, 100, 20, first.next_cursor.as_deref(), 2),
        Err(InspectionError::Stale)
    ));
    assert!(matches!(
        inspector.page("bucket", &record, 100, 20, Some("tampered"), 2),
        Err(InspectionError::Cursor)
    ));
    assert!(matches!(
        inspector.page("bucket", &record, 100, 101, None, 1),
        Err(InspectionError::Limit)
    ));
}

#[test]
fn empty_missing_corrupt_and_oversized_references_have_explicit_states() {
    let inspector = LocationInspector::new(vec![42; 32]).unwrap();
    let empty = inspector.page("bucket", &record(&[]), 1, 20, None, 1).unwrap();
    assert!(empty.locations.is_empty());
    assert!(empty.next_cursor.is_none());
    let mut values = locations();
    values[0].chunk_id = None;
    assert!(inspector
        .page("bucket", &record(&values), 1, 20, None, 1)
        .unwrap()
        .locations[0]
        .chunk_id
        .is_none());
    values[1].logical_offset = 7;
    assert!(matches!(
        inspector.page("bucket", &record(&values), 1, 20, None, 1),
        Err(InspectionError::Reference)
    ));
    let mut corrupt = record(&locations());
    corrupt.data_reference = vec![255; 8];
    assert!(matches!(
        inspector.page("bucket", &corrupt, 1, 20, None, 1),
        Err(InspectionError::Reference)
    ));
    corrupt.data_reference = vec![0; MAX_REFERENCE_BYTES + 1];
    assert!(matches!(
        inspector.page("bucket", &corrupt, 1, 20, None, 1),
        Err(InspectionError::ReferenceLimit)
    ));
}

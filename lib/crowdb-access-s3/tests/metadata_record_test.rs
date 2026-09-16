// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metadata::{BucketId, BucketNameRecord, MetadataRecordError, ObjectRecord, TenantId};

#[test]
fn bucket_mapping_round_trips_binary_values_and_tombstone() {
    let value = BucketNameRecord {
        tenant: TenantId::new(b"tenant\0".to_vec()).unwrap(),
        name: b"bucket\0name".to_vec(),
        bucket_id: BucketId::new([9; 16]),
        tombstone: true,
    };
    assert_eq!(BucketNameRecord::decode(&value.encode()).unwrap(), value);
}

fn object_record() -> ObjectRecord {
    ObjectRecord {
        bucket_id: BucketId::new([7; 16]),
        key: b"key\0with/binary".to_vec(),
        logical_length: 42,
        checksum: b"md5-value".to_vec(),
        etag: "etag".into(),
        created_at_ms: 11,
        modified_at_ms: 12,
        content_type: "application/octet-stream".into(),
        attributes: vec![1, 2],
        data_reference: b"opaque-data-reference".to_vec(),
        data_length: 42,
    }
}

#[test]
fn complete_object_record_round_trips() {
    let record = object_record();
    assert_eq!(ObjectRecord::decode(&record.encode().unwrap()).unwrap(), record);
}

#[test]
fn inconsistent_object_is_rejected_before_chunk_access() {
    let mut record = object_record();
    record.data_length = 41;
    assert!(matches!(record.encode(), Err(MetadataRecordError::DataLength)));

    let mut encoded = object_record().encode().unwrap();
    encoded[0] ^= 1;
    assert!(ObjectRecord::decode(&encoded).is_err());
}

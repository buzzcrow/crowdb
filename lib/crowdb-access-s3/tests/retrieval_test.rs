// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/retrieval_storage.rs"]
mod storage;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crowdb_access_s3::condition::ObjectConditions;
use crowdb_access_s3::metadata::{BucketId, ObjectRecord};
use crowdb_access_s3::range::ByteRange;
use crowdb_access_s3::retrieval::{prepare_get_range, GetStreamError, RetrievalError};
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;

fn object(length: u64) -> ObjectRecord {
    ObjectRecord {
        bucket_id: BucketId::new([1; 16]),
        key: b"key".to_vec(),
        logical_length: length,
        checksum: md5::compute(b"").0.to_vec(),
        etag: format!("{:x}", md5::compute(b"")),
        created_at_ms: 1,
        modified_at_ms: 1,
        content_type: "application/octet-stream".into(),
        attributes: Vec::new(),
        data_reference: bincode::serialize(&vec![Location {
            chunk_id: Some(ChunkId { high: 1, low: 1 }),
            offset: 0,
            length,
            logical_offset: 0,
            logical_length: length,
        }])
        .unwrap(),
        data_length: length,
    }
}

fn backend() -> Arc<storage::TestReadStorage> {
    Arc::new(storage::TestReadStorage { queries: 0.into() })
}

#[tokio::test]
async fn empty_full_read_checks_integrity_and_failure_never_turns_into_eof() {
    let storage = backend();
    let client = storage.client();
    let mut record = object(0);
    let mut valid = prepare_get_range(&client, &record, None, &ObjectConditions::default())
        .unwrap()
        .stream
        .unwrap();
    assert!(valid.next_verified().await.unwrap().is_none());
    assert!(valid.next_verified().await.unwrap().is_none());
    record.checksum = vec![0; 16];
    let mut invalid = prepare_get_range(&client, &record, None, &ObjectConditions::default())
        .unwrap()
        .stream
        .unwrap();
    assert!(matches!(
        invalid.next_verified().await,
        Err(GetStreamError::Integrity)
    ));
    assert!(matches!(
        invalid.next_verified().await,
        Err(GetStreamError::Integrity)
    ));
    assert!(invalid.next_chunk().await.is_none());
    assert_eq!(storage.queries.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn storage_failure_is_sticky_without_restarting_work_after_failed_pull() {
    let storage = backend();
    let client = storage.client();
    let mut body = prepare_get_range(&client, &object(3), None, &ObjectConditions::default())
        .unwrap()
        .stream
        .unwrap();
    assert_eq!(storage.queries.load(Ordering::Relaxed), 0);
    assert!(matches!(
        body.next_verified().await,
        Err(GetStreamError::Chunk(_))
    ));
    let queries = storage.queries.load(Ordering::Relaxed);
    assert!(queries > 0);
    assert!(matches!(
        body.next_verified().await,
        Err(GetStreamError::Chunk(_))
    ));
    assert_eq!(storage.queries.load(Ordering::Relaxed), queries);
}

#[test]
fn typed_ranges_reject_invalid_intervals_and_keep_condition_precedence() {
    let client = backend().client();
    let record = object(3);
    for range in [
        ByteRange { start: 0, end: 4 },
        ByteRange { start: 3, end: 3 },
        ByteRange { start: 2, end: 1 },
    ] {
        assert!(matches!(
            prepare_get_range(&client, &record, Some(range), &ObjectConditions::default()),
            Err(RetrievalError::Range(_))
        ));
    }
    let conditions = ObjectConditions {
        if_none_match: Some(&record.etag),
        ..ObjectConditions::default()
    };
    assert!(matches!(
        prepare_get_range(
            &client,
            &record,
            Some(ByteRange { start: 3, end: 4 }),
            &conditions
        ),
        Err(RetrievalError::NotModified)
    ));
    assert!(
        prepare_get_range(
            &client,
            &record,
            Some(ByteRange { start: 1, end: 3 }),
            &ObjectConditions::default()
        )
        .unwrap()
        .partial
    );
}

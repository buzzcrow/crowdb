// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_multipart::SelectedPart;
use crowdb_access_s3::metadata::{
    new_upload_id, BucketId, MultipartPartRecord, MultipartPhase, MultipartRecordError,
    MultipartSessionRecord,
};
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;

fn session() -> MultipartSessionRecord {
    MultipartSessionRecord {
        bucket_id: BucketId::new([3; 16]),
        object_key: b"key\0part".to_vec(),
        upload_id: [7; 16],
        revision: 1,
        phase: MultipartPhase::Open,
        created_ms: 100,
        expires_ms: 200,
        content_type: "application/octet-stream".into(),
        attributes: crowdb_access_s3::metadata::UserMetadata::from_headers(&hyper::HeaderMap::from_iter([(
            hyper::header::HeaderName::from_static("x-amz-meta-mtime"),
            hyper::header::HeaderValue::from_static("123.456"),
        )]))
        .unwrap()
        .encode()
        .unwrap(),
        max_parts: 10,
        max_part_bytes: 100,
        max_object_bytes: 500,
        max_staged_bytes: 1_000,
        part_count: 0,
        staged_bytes: 0,
        pending: None,
        selection: None,
        completion_request_digest: None,
        publication_ms: None,
        object_predecessor: None,
        etag: None,
    }
}

fn part() -> MultipartPartRecord {
    MultipartPartRecord {
        bucket_id: BucketId::new([3; 16]),
        upload_id: [7; 16],
        number: 1,
        revision: 2,
        modified_ms: 150,
        length: 5,
        raw_md5: [9; 16],
        locations: vec![Location {
            chunk_id: Some(ChunkId { high: 1, low: 2 }),
            offset: 10,
            length: 39,
            logical_offset: 0,
            logical_length: 5,
        }],
    }
}

#[test]
fn generated_upload_ids_sort_by_initiation_millisecond() {
    let first = new_upload_id(100);
    let second = new_upload_id(101);
    assert!(first < second);
    assert_ne!(first, [0; 16]);
    assert_ne!(second, [0; 16]);
}

#[test]
fn session_and_part_records_round_trip_only_under_their_own_keys() {
    let session = session();
    let bytes = session.encode().unwrap();
    assert_eq!(
        MultipartSessionRecord::decode(&bytes, session.bucket_id, &session.object_key, &session.upload_id)
            .unwrap(),
        session
    );
    assert_eq!(
        MultipartSessionRecord::decode(&bytes, session.bucket_id, b"other", &session.upload_id),
        Err(MultipartRecordError::Identity)
    );

    let part = part();
    let bytes = part.encode().unwrap();
    assert_eq!(
        MultipartPartRecord::decode(&bytes, part.bucket_id, &part.upload_id, part.number).unwrap(),
        part
    );
    assert_eq!(
        MultipartPartRecord::decode(&bytes, part.bucket_id, &part.upload_id, 2),
        Err(MultipartRecordError::Identity)
    );
}

#[test]
fn completed_session_requires_a_matching_selected_count_and_etag() {
    let mut session = session();
    session.phase = MultipartPhase::Publishing;
    session.part_count = 1;
    session.staged_bytes = 5;
    session.selection = Some(vec![SelectedPart {
        number: 1,
        revision: 2,
        digest: [4; 32],
    }]);
    session.completion_request_digest = Some([5; 32]);
    session.publication_ms = Some(150);
    session.object_predecessor = Some(None);
    session.etag = Some("11111111111111111111111111111111-2".into());
    assert_eq!(session.encode(), Err(MultipartRecordError::Invalid));
    session.etag = Some("11111111111111111111111111111111-1".into());
    assert!(session.encode().is_ok());
    session.phase = MultipartPhase::Open;
    assert_eq!(session.encode(), Err(MultipartRecordError::Invalid));
}

#[test]
fn malformed_or_unbounded_records_fail_before_exposure() {
    let mut invalid_part = part();
    invalid_part.locations[0].logical_offset = 1;
    assert_eq!(invalid_part.encode(), Err(MultipartRecordError::Invalid));
    let part = part();
    let mut bytes = part.encode().unwrap();
    bytes.push(0);
    assert_eq!(
        MultipartPartRecord::decode(&bytes, part.bucket_id, &part.upload_id, part.number),
        Err(MultipartRecordError::Invalid)
    );
    let oversized = vec![0; 1024 * 1024 + 1];
    assert_eq!(
        MultipartSessionRecord::decode(&oversized, session().bucket_id, b"key", &[7; 16]),
        Err(MultipartRecordError::TooLarge)
    );
}

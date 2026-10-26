// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/read_storage.rs"]
mod storage;

use std::sync::atomic::Ordering;
use std::time::Duration;

use crowdb_access_s3::metadata::TenantId;
use crowdb_access_s3::S3ErrorCode;
use crowdb_sdk_native::s3::{ByteRange, EtagSelection, GetOptions, ReadPreconditions, S3Client};
use crowdb_sdk_native::Error;
use storage::{context, TestFixture};

#[tokio::test]
async fn native_head_and_ranged_get_retain_original_storage_without_http_or_credentials() {
    let fixture = TestFixture::new(4096).await;
    let mut record = fixture.record(0);
    record.attributes = crowdb_access_s3::metadata::UserMetadata::from_values(
        [("sample".to_owned(), "opaque value".to_owned())]
            .into_iter()
            .collect(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let revision = fixture.publish(&record).await;
    let head = fixture
        .client
        .head_object(context(2), b"bucket", b"key", &ReadPreconditions::default())
        .await
        .unwrap();
    assert_eq!(head.revision, revision);
    assert_eq!(head.user_metadata.values()["sample"], "opaque value");
    assert_eq!(fixture.backend.reads.load(Ordering::Relaxed), 0);
    let options = GetOptions {
        range: Some(ByteRange { start: 2, end: 6 }),
        ..GetOptions::default()
    };
    let mut read = fixture
        .client
        .get_object(context(3), b"bucket", b"key", &options)
        .await
        .unwrap();
    assert_eq!(read.metadata, head);
    assert_eq!(fixture.backend.reads.load(Ordering::Relaxed), 0);
    let lease = read.body.next().await.unwrap().unwrap();
    assert_eq!(lease.as_slice(), b"igin");
    assert_eq!(lease.metadata().logical_offset, 2);
    assert_eq!(lease.metadata().request_id, 3);
    assert_eq!(lease.metadata().sequence, 0);
    assert_eq!(lease.metadata().allocation_capacity, 4096);
    let frame = &fixture.backend.data[0];
    let payload_offset =
        crowdb_protocol::frame::parse_frame(frame, crowdb_protocol::common::ChunkId { high: 1, low: 1 })
            .unwrap()
            .header
            .payload_offset;
    assert_eq!(
        lease.as_slice().as_ptr(),
        frame[usize::from(payload_offset) + 2..].as_ptr()
    );
    assert!(read.body.next().await.unwrap().is_none());
    assert!(read.body.is_complete());
    assert_eq!(fixture.runtime.resources().status().requests, 0);
    assert_eq!(fixture.runtime.resources().status().held_bytes, 4096);
    assert_eq!(fixture.runtime.resources().status().compatibility_copy_bytes, 0);
    fixture.runtime.shutdown().await.unwrap();
    assert_eq!(lease.as_slice(), b"igin");
    drop(lease);
    assert_eq!(fixture.runtime.resources().status().held_bytes, 0);
}

#[tokio::test]
async fn resolved_generation_survives_overwrite_and_rejects_other_tenants() {
    let fixture = TestFixture::new(8192).await;
    let original = fixture.record(0);
    let first_revision = fixture.publish(&original).await;
    let selected = fixture
        .client
        .resolve_object(context(2), b"bucket", b"key")
        .await
        .unwrap();
    let identical_revision = fixture.publish(&original).await;
    let identical = fixture
        .client
        .resolve_object(context(3), b"bucket", b"key")
        .await
        .unwrap();
    assert_ne!(first_revision, identical_revision);
    assert_eq!(identical.metadata().revision, identical_revision);
    fixture.publish(&fixture.record(1)).await;
    let mut old = fixture
        .client
        .get_resolved(context(4), &selected, &GetOptions::default())
        .unwrap();
    assert_eq!(old.body.next().await.unwrap().unwrap().as_slice(), b"original");
    assert!(old.body.next().await.unwrap().is_none());
    let mut current = fixture
        .client
        .get_object(context(5), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        current.body.next().await.unwrap().unwrap().as_slice(),
        b"replacement"
    );
    assert!(current.body.next().await.unwrap().is_none());
    let other = S3Client::new(fixture.runtime.clone(), TenantId::new(b"other".to_vec()).unwrap()).unwrap();
    assert!(matches!(
        other.get_resolved(context(6), &selected, &GetOptions::default()),
        Err(Error::Configuration(_))
    ));
    let another_authority = TestFixture::new(8192).await;
    assert!(matches!(
        another_authority
            .client
            .get_resolved(context(7), &selected, &GetOptions::default()),
        Err(Error::Configuration(_))
    ));
}

#[tokio::test]
async fn dropping_a_pending_pull_does_not_lose_bytes_waiting_for_capacity() {
    let fixture = TestFixture::new(4096).await;
    fixture.publish(&fixture.record(0)).await;
    let mut first = fixture
        .client
        .get_object(context(2), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    let held = first.body.next().await.unwrap().unwrap();
    assert!(first.body.next().await.unwrap().is_none());
    fixture.publish(&fixture.record(1)).await;
    let mut second = fixture
        .client
        .get_object(context(3), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), second.body.next())
            .await
            .is_err()
    );
    assert_eq!(fixture.backend.reads.load(Ordering::Relaxed), 2);
    drop(held);
    let next = second.body.next().await.unwrap().unwrap();
    assert_eq!(next.as_slice(), b"replacement");
    assert_eq!(next.metadata().sequence, 0);
    assert_eq!(next.metadata().logical_offset, 0);
    assert!(second.body.next().await.unwrap().is_none());
    assert_eq!(fixture.backend.reads.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn typed_conditions_share_precedence_and_integrity_failure_stays_terminal() {
    let fixture = TestFixture::new(4096).await;
    let mut record = fixture.record(0);
    fixture.publish(&record).await;
    let conditions = ReadPreconditions {
        if_match: Some(EtagSelection::Values(vec!["wrong".into()])),
        if_unmodified_since_seconds: Some(u64::MAX),
        ..ReadPreconditions::default()
    };
    assert!(matches!(
        fixture
            .client
            .head_object(context(2), b"bucket", b"key", &conditions)
            .await,
        Err(Error::S3(S3ErrorCode::PreconditionFailed))
    ));
    let conditions = ReadPreconditions {
        if_none_match: Some(EtagSelection::Any),
        ..ReadPreconditions::default()
    };
    assert!(matches!(
        fixture
            .client
            .head_object(context(3), b"bucket", b"key", &conditions)
            .await,
        Err(Error::NotModified)
    ));
    assert_eq!(fixture.backend.reads.load(Ordering::Relaxed), 0);
    record.checksum[0] ^= 0xff;
    fixture.publish(&record).await;
    let mut read = fixture
        .client
        .get_object(context(4), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    let held = read.body.next().await.unwrap().unwrap();
    assert!(matches!(read.body.next().await, Err(Error::Integrity)));
    read.body.cancel();
    assert!(matches!(read.body.next().await, Err(Error::Integrity)));
    assert!(!read.body.is_complete());
    assert_eq!(fixture.runtime.resources().status().requests, 0);
    assert_eq!(held.as_slice(), b"original");
    assert_eq!(fixture.runtime.resources().status().held_bytes, 4096);
}

#[tokio::test]
async fn cancellation_and_shutdown_leave_delivered_leases_valid() {
    let fixture = TestFixture::new(4096).await;
    fixture.publish(&fixture.record(0)).await;
    let request = context(2);
    let mut read = fixture
        .client
        .get_object(request.clone(), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    let held = read.body.next().await.unwrap().unwrap();
    request.cancellation.cancel();
    assert!(matches!(read.body.next().await, Err(Error::Cancelled)));
    assert!(matches!(read.body.next().await, Err(Error::Cancelled)));
    assert_eq!(held.as_slice(), b"original");
    let mut second = fixture
        .client
        .get_object(context(3), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    fixture.runtime.shutdown().await.unwrap();
    assert!(matches!(second.body.next().await, Err(Error::Shutdown)));
    assert_eq!(held.as_slice(), b"original");
}

#[tokio::test]
async fn empty_native_reads_check_integrity_before_successful_eof() {
    let fixture = TestFixture::new(4096).await;
    let mut record = fixture.record(0);
    record.logical_length = 0;
    record.data_length = 0;
    record.data_reference =
        bincode::serialize(&Vec::<crowdb_protocol::chunkdb::rpc::Location>::new()).unwrap();
    let (etag, checksum) = crowdb_access_s3::integrity::SinglePartIntegrity::default().finish();
    record.etag = etag;
    record.checksum = checksum;
    fixture.publish(&record).await;
    let mut valid = fixture
        .client
        .get_object(context(2), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    assert!(!valid.body.is_complete());
    assert!(valid.body.next().await.unwrap().is_none());
    assert!(valid.body.is_complete());
    record.checksum[0] ^= 0xff;
    fixture.publish(&record).await;
    let mut invalid = fixture
        .client
        .get_object(context(3), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    assert!(matches!(invalid.body.next().await, Err(Error::Integrity)));
    assert!(matches!(invalid.body.next().await, Err(Error::Integrity)));
    assert_eq!(fixture.backend.reads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn native_body_capacity_wait_expires_without_releasing_delivered_data() {
    let fixture = TestFixture::new(4096).await;
    fixture.publish(&fixture.record(0)).await;
    let mut first = fixture
        .client
        .get_object(context(2), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    let held = first.body.next().await.unwrap().unwrap();
    drop(first);
    assert_eq!(fixture.runtime.resources().status().requests, 0);
    fixture.publish(&fixture.record(1)).await;
    let mut deadline = context(3);
    deadline.deadline = std::time::Instant::now() + Duration::from_millis(20);
    let mut second = fixture
        .client
        .get_object(deadline, b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    assert!(matches!(second.body.next().await, Err(Error::Deadline)));
    assert!(matches!(second.body.next().await, Err(Error::Deadline)));
    assert_eq!(fixture.runtime.resources().status().requests, 0);
    assert_eq!(fixture.runtime.resources().status().held_bytes, 4096);
    assert_eq!(held.as_slice(), b"original");
    drop(held);
    assert_eq!(fixture.runtime.resources().status().held_bytes, 0);
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metadata::MultipartRepositoryError;

use super::{modern_session, part, selection, MultipartPartRecord, TestIndependent};

fn sized_part(number: u16, length: u64) -> MultipartPartRecord {
    let mut value = part();
    value.number = number;
    value.length = length;
    value.locations[0].logical_length = length;
    value.locations[0].length = crowdb_protocol::frame::framed_physical_length(length).unwrap();
    value
}

#[tokio::test]
async fn tight_staged_quota_keeps_concurrent_accounting_and_never_admits_both_parts() {
    let mut session = modern_session();
    session.max_parts = 2;
    session.max_part_bytes = 6;
    session.max_object_bytes = 10;
    session.max_staged_bytes = 10;
    assert!(session.encode().unwrap().starts_with(b"S3MS\x04"));
    let test = TestIndependent::with_session(session).await;
    let mut writes = tokio::task::JoinSet::new();
    for number in 1..=2 {
        let repository = test.repository.clone();
        let session = test.session.clone();
        writes.spawn(async move {
            repository
                .put_stream_part(&session, &sized_part(number, 6), 110)
                .await
        });
    }
    let mut admitted = 0;
    let mut rejected = 0;
    while let Some(result) = writes.join_next().await {
        match result.unwrap() {
            Ok(Some(_)) => admitted += 1,
            Err(MultipartRepositoryError::InvalidPart) => rejected += 1,
            other => panic!("unexpected strict-quota result: {other:?}"),
        }
    }
    assert_eq!((admitted, rejected), (1, 1));
    let current = test.repository.load(&test.session).await.unwrap().unwrap();
    assert_eq!((current.part_count, current.staged_bytes), (1, 6));
    assert!(current.pending.is_none());
    assert_eq!(
        test.repository
            .list_parts(&test.session, 0, 10)
            .await
            .unwrap()
            .parts
            .len(),
        1
    );
    assert!(current.encode().unwrap().starts_with(b"S3MS\x04"));
}

#[tokio::test]
async fn exact_product_bound_allows_independent_parts_but_still_enforces_completion_object_limit() {
    let mut session = modern_session();
    session.max_parts = 2;
    session.max_part_bytes = 6 * 1024 * 1024;
    session.max_object_bytes = 8 * 1024 * 1024;
    session.max_staged_bytes = 12 * 1024 * 1024;
    assert!(session.encode().unwrap().starts_with(b"S3MS\x05"));
    let test = TestIndependent::with_session(session).await;
    let mut requested = Vec::new();
    for number in 1..=2 {
        let value = sized_part(number, 5 * 1024 * 1024);
        let before = test.probe.counts();
        let start = test.probe.operation_log().len();
        test.repository
            .put_stream_part(&test.session, &value, 110)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(test.delta(before), (3, 2));
        test.assert_no_upload_writes(start);
        requested.extend(selection(&value));
    }
    let before = test.probe.counts();
    assert!(matches!(
        test.repository
            .freeze_completion(&test.session, &requested, 120)
            .await,
        Err(MultipartRepositoryError::InvalidPart)
    ));
    assert_eq!(test.delta(before), (3, 0));
    assert_eq!(
        test.repository.load(&test.session).await.unwrap(),
        Some(test.session.clone())
    );
    assert_eq!(test.raw_upload().await.len(), 5);
}

#[test]
fn overflowing_quota_product_stays_strict_instead_of_wrapping_into_independent_mode() {
    let mut session = modern_session();
    session.max_parts = 2;
    session.max_part_bytes = u64::MAX / 2 + 1;
    session.max_object_bytes = session.max_part_bytes;
    session.max_staged_bytes = u64::MAX;
    assert!(session.encode().unwrap().starts_with(b"S3MS\x04"));
}

#[tokio::test]
async fn independent_number_and_size_limits_fail_before_generation_or_pointer_writes() {
    let test = TestIndependent::new().await;
    let too_big = sized_part(1, test.session.max_part_bytes + 1);
    let mut zero_number = part();
    zero_number.number = 0;
    let mut too_high = part();
    too_high.number = test.session.max_parts + 1;
    for invalid in [too_big, zero_number, too_high] {
        let before = test.probe.counts();
        assert!(matches!(
            test.repository
                .put_stream_part(&test.session, &invalid, 110)
                .await,
            Err(MultipartRepositoryError::Conflict)
        ));
        assert_eq!(test.delta(before), (1, 0));
    }
    assert_eq!(test.raw_upload().await.len(), 1);
}

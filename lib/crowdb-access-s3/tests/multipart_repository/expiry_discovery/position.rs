// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{finish_round, TestExpiry, CREATED, NOW, WEEK};
use crowdb_access_s3::{
    bucket::{delete_bucket, head_bucket, BucketError, DeleteBucketResult},
    metadata::{BucketId, MetadataKey, MultipartExpiryError, MultipartPhase, MultipartRepository, TenantId},
};

#[tokio::test]
async fn tombstoned_bucket_restarts_and_honors_the_original_seven_day_deadline() {
    let test = TestExpiry::new().await;
    let bucket = test.bucket(b"deleted-only").await;
    let upload = TestExpiry::upload(bucket, 1);
    test.begin_part(&upload).await;
    let evidence = test.evidence(&upload).await;
    assert_eq!(
        delete_bucket(&test.store, &test.tenant, b"deleted-only")
            .await
            .unwrap(),
        DeleteBucketResult::Deleted
    );
    assert_eq!(finish_round(&mut test.sweep(), CREATED + WEEK - 1, 12).await, 0);
    assert_eq!(
        test.repository.load(&upload).await.unwrap().unwrap().expires_ms,
        CREATED + WEEK
    );
    assert_eq!(finish_round(&mut test.sweep(), CREATED + WEEK, 12).await, 1);
    test.assert_phase(&upload, MultipartPhase::Aborted).await;
    assert_eq!(finish_round(&mut test.sweep(), NOW, 12).await, 0);
    test.assert_evidence(&evidence).await;
}

#[tokio::test]
async fn zero_and_maximum_bucket_ids_are_tenant_bounded_and_end_without_an_extra_scan() {
    let tenant = TenantId::new(b"tenant\0scope".to_vec()).unwrap();
    let test = TestExpiry::with_tenant(tenant).await;
    let first = TestExpiry::upload(BucketId::new([0; 16]), 1);
    let last = TestExpiry::upload(BucketId::new([255; 16]), 2);
    test.repository.begin(&first).await.unwrap();
    test.repository.begin(&last).await.unwrap();
    let other = MultipartRepository::new(
        test.store.clone(),
        TenantId::new(b"tenant\0scope\0".to_vec()).unwrap(),
    );
    let neighbor = TestExpiry::upload(BucketId::new([0; 16]), 3);
    other.begin(&neighbor).await.unwrap();
    let mut sweep = test.sweep();
    for expected in [0, 1, 0, 1] {
        let step = sweep.step(NOW).await.unwrap();
        assert_eq!(step.expired, expected);
        assert!(step.more);
    }
    let scans = test.probe.scan_counts().0;
    assert!(!sweep.step(NOW).await.unwrap().more);
    assert_eq!(
        test.probe.scan_counts().0,
        scans,
        "maximum ID must reach local EOF"
    );
    assert_eq!(
        other.load(&neighbor).await.unwrap().unwrap().phase,
        MultipartPhase::Open
    );
    test.assert_phase(&first, MultipartPhase::Aborted).await;
    test.assert_phase(&last, MultipartPhase::Aborted).await;
    assert_eq!(finish_round(&mut test.sweep(), NOW, 8).await, 0);
}

#[tokio::test]
async fn inclusive_id_successor_observes_an_oversized_boundary_key_and_reuses_its_hint() {
    let test = TestExpiry::new().await;
    let first = TestExpiry::upload(BucketId::new([0; 16]), 1);
    let mut next = [0; 16];
    next[15] = 1;
    let second = TestExpiry::upload(BucketId::new(next), 2);
    test.repository.begin(&first).await.unwrap();
    test.repository.begin(&second).await.unwrap();
    // This raw KV row is exactly the exclusive end of the previous ID prefix.
    // It is a discovery hint only; its payload is never authority to abort U.
    let boundary = test.bucket_prefix(second.bucket_id);
    let bytes = vec![7; 5 * 1024 * 1024];
    test.store
        .put_if_absent(boundary.clone(), bytes.clone())
        .await
        .unwrap();
    let mut sweep = test.sweep();
    assert_eq!(sweep.step(NOW).await.unwrap().expired, 0);
    assert_eq!(sweep.step(NOW).await.unwrap().expired, 1);
    assert!(matches!(
        sweep.step(NOW).await,
        Err(MultipartExpiryError::OversizedBucketRow)
    ));
    assert_eq!(
        sweep.step(NOW).await.unwrap().expired,
        1,
        "observed oversized key retains its ID hint"
    );
    assert!(!sweep.step(NOW).await.unwrap().more);
    assert_eq!(test.store.get(boundary).await.unwrap().unwrap().value, bytes);
    test.assert_phase(&second, MultipartPhase::Aborted).await;
}

#[tokio::test]
async fn name_payloads_are_not_authority_but_invalid_upload_bindings_still_fail() {
    let test = TestExpiry::new().await;
    for name in [b"short".as_slice(), b"a-very-long-corrupt-name"] {
        test.store
            .put_if_absent(
                MetadataKey::bucket_name(&test.tenant, name).unwrap(),
                vec![1, 2, 3],
            )
            .await
            .unwrap();
    }
    assert!(matches!(
        head_bucket(&test.store, &test.tenant, b"short").await,
        Err(BucketError::Record(
            crowdb_access_s3::metadata::MetadataRecordError::Invalid
        ))
    ));
    let upload = TestExpiry::upload(BucketId::new([128; 16]), 1);
    test.repository.begin(&upload).await.unwrap();
    let before = test.probe.counts();
    assert_eq!(finish_round(&mut test.sweep(), NOW, 12).await, 1);
    let after = test.probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    let wrong_key =
        MetadataKey::multipart_upload_index(&test.tenant, upload.bucket_id, b"bad", &[2; 16]).unwrap();
    let session_key = MetadataKey::multipart_session(&test.tenant, upload.bucket_id, &upload.upload_id);
    test.store.put_if_absent(wrong_key, session_key).await.unwrap();
    let mut sweep = test.sweep();
    let mut failures = 0;
    let mut finished = false;
    for _ in 0..12 {
        match sweep.step(NOW).await {
            Err(MultipartExpiryError::Upload(
                crowdb_access_s3::metadata::MultipartRepositoryError::Conflict,
            )) => failures += 1,
            Ok(step) if !step.more => {
                finished = true;
                break;
            }
            Ok(_) => {}
            other => panic!("unexpected expiry result: {other:?}"),
        }
    }
    assert!(finished);
    assert_eq!(failures, 1);
}

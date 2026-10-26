// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::{
    bucket::{delete_bucket, head_bucket, DeleteBucketResult},
    metadata::{CompletionPart, MultipartPhase},
};

#[path = "../common/multipart_bucket_expiry.rs"]
mod fixture;
use fixture::{finish_round, TestExpiry, CREATED, NOW, WEEK};

#[path = "expiry_discovery/fairness.rs"]
mod fairness;
#[path = "expiry_discovery/position.rs"]
mod position;

#[tokio::test]
async fn deleted_and_recreated_bucket_still_discovers_its_old_open_upload() {
    let test = TestExpiry::new().await;
    let old_bucket = test.bucket(b"same-name").await;
    let old = TestExpiry::upload(old_bucket, 1);
    test.begin_part(&old).await;
    let evidence = test.evidence(&old).await;
    let mut future = TestExpiry::upload(old_bucket, 2);
    future.expires_ms += WEEK;
    test.repository.begin(&future).await.unwrap();
    let publishing = TestExpiry::upload(old_bucket, 3);
    test.begin_part(&publishing).await;
    let publishing = test
        .repository
        .freeze_completion(
            &publishing,
            &[CompletionPart {
                number: 1,
                etag: "09".repeat(16),
            }],
            CREATED + 2,
        )
        .await
        .unwrap()
        .unwrap();
    let publishing_evidence = test.evidence(&publishing).await;
    assert_eq!(
        delete_bucket(&test.store, &test.tenant, b"same-name")
            .await
            .unwrap(),
        DeleteBucketResult::Deleted
    );
    let replacement = test.bucket(b"same-name").await;
    assert_ne!(replacement, old_bucket);
    assert_eq!(
        head_bucket(&test.store, &test.tenant, b"same-name")
            .await
            .unwrap(),
        Some(replacement)
    );
    let mut new_upload = TestExpiry::upload(replacement, 4);
    new_upload.expires_ms += WEEK;
    test.repository.begin(&new_upload).await.unwrap();
    let future_before = test.repository.load(&future).await.unwrap().unwrap();
    let new_before = test.repository.load(&new_upload).await.unwrap().unwrap();

    let before = test.probe.counts();
    let expired = finish_round(&mut test.sweep(), NOW, 20).await;
    let after = test.probe.counts();
    // The old name-based worker reaches EOF with this session still Open.
    test.assert_phase(&old, MultipartPhase::Aborted).await;
    assert_eq!(expired, 1);
    assert_eq!((after.0 - before.0, after.1 - before.1), (4, 1));
    assert_eq!(test.repository.load(&future).await.unwrap(), Some(future_before));
    assert_eq!(test.repository.load(&new_upload).await.unwrap(), Some(new_before));
    assert_eq!(test.repository.load(&publishing).await.unwrap(), Some(publishing));
    test.assert_evidence(&evidence).await;
    test.assert_evidence(&publishing_evidence).await;
}

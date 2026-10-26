// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{finish_round, TestExpiry, CREATED, NOW};
use crowdb_access_s3::{
    bucket::BucketError,
    metadata::{
        BucketId, MetadataKey, MetadataStoreError, MultipartExpiryError, MultipartPhase,
        MultipartRepositoryError,
    },
};
use crowdb_chunk_kv_client::ClientError;

#[tokio::test]
async fn failed_index_scan_skips_ten_thousand_retained_part_rows_until_the_next_pass() {
    let test = TestExpiry::new().await;
    let blocked = TestExpiry::upload(BucketId::new([128; 16]), 1);
    let healthy = TestExpiry::upload(BucketId::new([129; 16]), 2);
    test.repository.begin(&blocked).await.unwrap();
    test.repository.begin(&healthy).await.unwrap();
    test.seed_retained_parts(&blocked).await;
    test.probe.fail_scan(MetadataKey::multipart_session_prefix(
        &test.tenant,
        blocked.bucket_id,
    ));
    let mut sweep = test.sweep();
    assert_eq!(sweep.step(NOW).await.unwrap().expired, 0);
    assert!(matches!(
        sweep.step(NOW).await,
        Err(MultipartExpiryError::Upload(MultipartRepositoryError::Store(
            MetadataStoreError::Client(ClientError::Transport(_))
        )))
    ));
    let scans = test.probe.scan_counts().0;
    assert_eq!(finish_round(&mut sweep, NOW, 4).await, 1);
    assert_eq!(test.probe.scan_counts().0 - scans, 3);
    test.assert_phase(&blocked, MultipartPhase::Open).await;
    test.assert_phase(&healthy, MultipartPhase::Aborted).await;
    test.probe.resume_scans();
    assert_eq!(finish_round(&mut sweep, NOW, 5).await, 1);
    test.assert_phase(&blocked, MultipartPhase::Aborted).await;
    assert!(test
        .store
        .get(
            MetadataKey::multipart_part_generation(
                &test.tenant,
                blocked.bucket_id,
                &blocked.upload_id,
                5_000,
                1
            )
            .unwrap()
        )
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn cancelling_an_index_scan_discards_the_hint_but_restart_recovers_the_upload() {
    let test = TestExpiry::new().await;
    let blocked = TestExpiry::upload(BucketId::new([128; 16]), 1);
    let healthy = TestExpiry::upload(BucketId::new([129; 16]), 2);
    test.begin_part(&blocked).await;
    test.repository.begin(&healthy).await.unwrap();
    let evidence = test.evidence(&blocked).await;
    let mut sweep = test.sweep();
    sweep.step(NOW).await.unwrap();
    let paused = test.probe.pause_scan(MetadataKey::multipart_session_prefix(
        &test.tenant,
        blocked.bucket_id,
    ));
    let mut pending = Box::pin(sweep.step(NOW));
    tokio::select! {
        result = &mut pending => panic!("paused scan completed: {result:?}"),
        result = paused => result.unwrap(),
    }
    drop(pending);
    test.probe.resume_scans();
    assert_eq!(finish_round(&mut sweep, NOW, 4).await, 1);
    test.assert_phase(&blocked, MultipartPhase::Open).await;
    assert_eq!(finish_round(&mut test.sweep(), NOW, 5).await, 1);
    test.assert_evidence(&evidence).await;
}

#[tokio::test]
async fn discovery_transport_failure_cannot_skip_an_unobserved_key() {
    let test = TestExpiry::new().await;
    let upload = TestExpiry::upload(BucketId::new([128; 16]), 1);
    test.repository.begin(&upload).await.unwrap();
    test.probe.fail_scan(test.tenant_prefix());
    let mut sweep = test.sweep();
    let before = test.probe.counts();
    for _ in 0..2 {
        assert!(matches!(
            sweep.step(NOW).await,
            Err(MultipartExpiryError::Bucket(BucketError::Store(
                MetadataStoreError::Client(ClientError::Transport(_))
            )))
        ));
    }
    assert_eq!(test.probe.counts(), before);
    test.probe.resume_scans();
    assert_eq!(finish_round(&mut sweep, NOW, 4).await, 1);
    test.assert_phase(&upload, MultipartPhase::Aborted).await;
}

#[tokio::test]
async fn unknown_abort_stops_without_confirmation_and_late_part_or_complete_cannot_reopen_it() {
    let test = TestExpiry::new().await;
    let upload = TestExpiry::upload(BucketId::new([128; 16]), 1);
    let part = test.begin_part(&upload).await;
    let evidence = test.evidence(&upload).await;
    let mut sweep = test.sweep();
    sweep.step(NOW).await.unwrap();
    let before = test.probe.counts();
    test.probe.lose_after(0);
    assert!(matches!(
        sweep.step(NOW).await,
        Err(MultipartExpiryError::Upload(MultipartRepositoryError::Store(
            MetadataStoreError::Client(ClientError::Transport(_))
        )))
    ));
    let after = test.probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    assert_eq!(finish_round(&mut sweep, NOW, 3).await, 0);
    assert_eq!(finish_round(&mut test.sweep(), NOW, 4).await, 0);
    assert!(matches!(
        test.repository.put_stream_part(&upload, &part, CREATED + 2).await,
        Err(MultipartRepositoryError::Conflict)
    ));
    assert!(matches!(
        test.repository
            .freeze_completion(
                &upload,
                &[crowdb_access_s3::metadata::CompletionPart {
                    number: 1,
                    etag: "09".repeat(16)
                }],
                CREATED + 2
            )
            .await,
        Err(MultipartRepositoryError::Conflict)
    ));
    test.assert_evidence(&evidence).await;
}

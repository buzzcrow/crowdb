// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/multipart_actor.rs"]
#[allow(dead_code)]
mod actor;
#[path = "common/multipart_expiry_request.rs"]
mod expiry;
#[path = "common/retrieval_storage.rs"]
mod retrieval_storage;

use crowdb_access_s3::{
    execution::MultipartFailure,
    metadata::{BucketId, MultipartPhase},
    S3ErrorCode,
};
use expiry::*;

#[tokio::test]
async fn expired_refresh_reuses_the_freshly_loaded_authority_for_abort() {
    for abort in [false, true] {
        let test = TestExpiry::new(false).await;
        let before = test.probe.counts();
        let result = test.executor.refresh_multipart(&test.session, abort).await;
        if abort {
            assert_eq!(result.unwrap().phase, MultipartPhase::Aborted);
        } else {
            assert!(matches!(
                result,
                Err(MultipartFailure::S3(S3ErrorCode::NoSuchUpload))
            ));
        }
        eprintln!(
            "expired executor refresh abort={abort}: {:?} GET/mutation",
            test.delta(before)
        );
        assert_eq!(test.delta(before), (1, 1));
        test.retained(MultipartPhase::Aborted).await;
    }
}

#[tokio::test]
async fn expired_identity_admission_counts_its_bucket_read_without_reloading_the_upload() {
    for abort in [false, true] {
        let test = TestExpiry::new(false).await;
        let before = test.probe.counts();
        let result = test
            .executor
            .multipart_session(
                b"bucket",
                &test.session.object_key,
                &test.session.upload_id,
                abort,
            )
            .await;
        if abort {
            assert_eq!(result.unwrap().phase, MultipartPhase::Aborted);
        } else {
            assert!(matches!(
                result,
                Err(MultipartFailure::S3(S3ErrorCode::NoSuchUpload))
            ));
        }
        eprintln!(
            "expired identity abort={abort}: {:?} GET/mutation including bucket",
            test.delta(before)
        );
        assert_eq!(test.delta(before), (2, 1));
        test.retained(MultipartPhase::Aborted).await;
    }
}

#[tokio::test]
async fn unknown_abort_stops_both_request_paths_and_restart_reads_the_durable_result() {
    for identity in [false, true] {
        for before_apply in [false, true] {
            let test = TestExpiry::new(false).await;
            if before_apply {
                test.probe.fail_before(0);
            } else {
                test.probe.lose_after(0);
            }
            let before = test.probe.counts();
            assert_uncertain(&if identity {
                test.executor
                    .multipart_session(b"bucket", &test.session.object_key, &test.session.upload_id, true)
                    .await
            } else {
                test.executor.refresh_multipart(&test.session, true).await
            });
            assert_eq!(test.delta(before), (if identity { 2 } else { 1 }, 1));
            let restarted = test.restart();
            let before = test.probe.counts();
            assert_eq!(
                restarted
                    .refresh_multipart(&test.session, true)
                    .await
                    .unwrap()
                    .phase,
                MultipartPhase::Aborted
            );
            assert_eq!(test.delta(before), (1, usize::from(before_apply)));
            test.retained(MultipartPhase::Aborted).await;
        }
    }
}

#[tokio::test]
async fn expired_request_uses_a_returned_aborted_winner_without_readback() {
    let test = TestExpiry::new(false).await;
    let mut winner = test.repository.load(&test.session).await.unwrap().unwrap();
    winner.revision += 1;
    winner.phase = MultipartPhase::Aborted;
    test.probe.conflict(test.key(), winner.encode().unwrap(), 0);
    let before = test.probe.counts();
    assert_eq!(
        test.executor
            .refresh_multipart(&test.session, true)
            .await
            .unwrap(),
        winner
    );
    assert_eq!(test.delta(before), (1, 1));
    test.retained_parts().await;
}

#[tokio::test]
async fn completion_winning_the_session_cas_is_never_overwritten_by_expiry() {
    let test = TestExpiry::new(false).await;
    let winner = test.completion_winner().await;
    test.probe.conflict(test.key(), winner.encode().unwrap(), 0);
    let before = test.probe.counts();
    assert!(matches!(
        test.executor.refresh_multipart(&test.session, true).await,
        Err(MultipartFailure::S3(S3ErrorCode::NoSuchUpload))
    ));
    assert_eq!(test.delta(before), (1, 1));
    test.retained_parts().await;
}

#[tokio::test]
async fn pending_pointer_recovery_retains_its_authority_and_generation_reads() {
    let test = TestExpiry::new(true).await;
    assert!(test
        .repository
        .load(&test.session)
        .await
        .unwrap()
        .unwrap()
        .pending
        .is_some());
    let before = test.probe.counts();
    assert_eq!(
        test.executor
            .refresh_multipart(&test.session, true)
            .await
            .unwrap()
            .phase,
        MultipartPhase::Aborted
    );
    assert_eq!(
        test.delta(before),
        (4, 2),
        "initial load plus pending authority, generation and pointer"
    );
    test.retained(MultipartPhase::Aborted).await;
}

#[tokio::test]
async fn independent_public_abort_keeps_its_fresh_load_for_stale_callers() {
    let test = TestExpiry::new(false).await;
    let before = test.probe.counts();
    assert_eq!(
        test.repository.abort(&test.session).await.unwrap().phase,
        MultipartPhase::Aborted
    );
    assert_eq!(test.delta(before), (1, 1));
    let before = test.probe.counts();
    assert_eq!(
        test.repository.abort(&test.session).await.unwrap().phase,
        MultipartPhase::Aborted
    );
    assert_eq!(test.delta(before), (1, 0));
    test.retained(MultipartPhase::Aborted).await;
}

#[tokio::test]
async fn foreign_request_bindings_never_reach_an_abort_mutation() {
    for identity in 0..3 {
        let test = TestExpiry::new(false).await;
        let mut foreign = test.session.clone();
        match identity {
            0 => foreign.object_key = b"another".to_vec(),
            1 => foreign.bucket_id = BucketId::new([8; 16]),
            _ => foreign.upload_id = [8; 16],
        }
        let before = test.probe.counts();
        assert!(matches!(
            test.executor.refresh_multipart(&foreign, true).await,
            Err(MultipartFailure::S3(
                S3ErrorCode::InvalidRequest | S3ErrorCode::NoSuchUpload
            ))
        ));
        assert_eq!(test.delta(before), (1, 0));
        test.retained(MultipartPhase::Open).await;
    }
    let test = TestExpiry::new(false).await;
    let before = test.probe.counts();
    assert!(matches!(
        test.executor
            .multipart_session(b"bucket", b"another", &test.session.upload_id, true)
            .await,
        Err(MultipartFailure::S3(S3ErrorCode::NoSuchUpload))
    ));
    assert_eq!(test.delta(before), (2, 0));
    test.retained(MultipartPhase::Open).await;
}

#[tokio::test]
async fn an_aborted_observation_with_a_foreign_identity_is_rejected_without_reload() {
    let test = TestExpiry::new(false).await;
    let mut winner = test.repository.load(&test.session).await.unwrap().unwrap();
    winner.revision += 1;
    winner.phase = MultipartPhase::Aborted;
    winner.object_key = b"another".to_vec();
    test.probe.conflict(test.key(), winner.encode().unwrap(), 0);
    let before = test.probe.counts();
    assert!(matches!(
        test.executor.refresh_multipart(&test.session, true).await,
        Err(MultipartFailure::S3(S3ErrorCode::InvalidRequest))
    ));
    assert_eq!(test.delta(before), (1, 1));
    test.retained_parts().await;
}

#[tokio::test]
async fn refresh_uses_the_current_expiry_instead_of_a_stale_callers_deadline() {
    let test = TestExpiry::new(false).await;
    let current = test.repository.load(&test.session).await.unwrap().unwrap();
    let mut extended = current.clone();
    extended.revision += 1;
    extended.expires_ms = u64::MAX;
    assert!(test.repository.exchange(&current, &extended).await.unwrap());
    let before = test.probe.counts();
    assert_eq!(
        test.executor
            .refresh_multipart(&test.session, true)
            .await
            .unwrap(),
        extended
    );
    assert_eq!(test.delta(before), (1, 0));
    test.retained(MultipartPhase::Open).await;
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::Ordering;

use crowdb_access_iceberg::{
    file::{FileContent, FileRepository},
    gc::{CandidatePhase, GcPhase, GcRepository, GcTask},
    key::FileId,
    record::StorageRecord,
};
use crowdb_access_server::iceberg::{mark_unreferenced_file_for_tests, FileS3ErrorCode};

#[path = "common/iceberg_file_delete.rs"]
mod fixture;
use fixture::{TestDeletion, DELETED_MS};

#[tokio::test]
async fn file_delete_only_marks_authority_and_replay_keeps_the_first_deadline() {
    let fixture = TestDeletion::new(false).await;
    fixture.store.deny_gc.store(true, Ordering::SeqCst);
    fixture.store.reset_counts();
    fixture.mark(DELETED_MS).await.unwrap();
    assert_eq!(fixture.store.counts(), (4, 1));
    assert_eq!(fixture.store.gc_requests.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.blocks.notifications.load(Ordering::SeqCst), 0);
    let StorageRecord::DeletedFile(deleted) = fixture.authority() else {
        panic!("logical deletion must remain the durable discovery fact");
    };
    assert_eq!(deleted.file, fixture.file);
    assert_eq!(deleted.deleted_ms, DELETED_MS);

    fixture.store.reset_counts();
    fixture.mark(DELETED_MS + 60_000).await.unwrap();
    assert_eq!(fixture.store.counts(), (2, 0));
    assert_eq!(fixture.store.gc_requests.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.authority(), StorageRecord::DeletedFile(deleted.clone()));

    fixture.store.deny_gc.store(false, Ordering::SeqCst);
    let gc = GcRepository::new(fixture.store.clone());
    gc.admit_multipart(fixture.context, DELETED_MS, fixture.limits)
        .await
        .unwrap();
    fixture.store.reset_counts();
    gc.claim_deleted_file(fixture.context, &deleted, DELETED_MS, fixture.limits)
        .await
        .unwrap();
    assert_eq!(fixture.store.counts(), (6, 2));
    eprintln!("file logical delete: 10 GET + 3 CAS -> 4 GET + 1 CAS; replay: 8 GET -> 2 GET; reference scan unchanged");
}

#[tokio::test]
async fn restart_discovers_only_deleted_file_and_observes_the_original_grace_period() {
    for native in [false, true] {
        let fixture = TestDeletion::new(native).await;
        fixture.mark(DELETED_MS).await.unwrap();
        assert!(fixture.candidate().await.is_none());
        let mut task = fixture.discover().await;
        let candidate = fixture.candidate().await.unwrap();
        assert_eq!(candidate.file, fixture.file);
        assert_eq!(candidate.not_before_ms, DELETED_MS + GcTask::PURGE_DELAY_MS);
        for _ in 0..30 {
            task = fixture
                .step_after_restart(&task, candidate.not_before_ms - 1)
                .await
                .unwrap();
            if task.phase == GcPhase::Complete {
                break;
            }
        }
        assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
        assert_eq!(fixture.candidate().await.unwrap(), candidate);
        assert!(matches!(fixture.authority(), StorageRecord::DeletedFile(_)));
        assert_eq!(fixture.blocks.notifications.load(Ordering::SeqCst), 0);
        let previous = task;
        let next_period_ms = DELETED_MS + 30 * 60_000;
        task = GcRepository::new(fixture.store.clone())
            .admit_multipart(fixture.context, next_period_ms, fixture.limits)
            .await
            .unwrap();
        assert_eq!(task.identity, previous.identity);
        assert_eq!(task.created_ms, previous.created_ms);
        assert_eq!(task.not_before_ms, previous.not_before_ms);
        assert_eq!(task.sweep_round, previous.sweep_round);
        for _ in 0..100 {
            task = fixture.step_after_restart(&task, next_period_ms).await.unwrap();
            if task.phase == GcPhase::Complete {
                break;
            }
        }
        assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
        assert_eq!(task.deleted, 1);
        assert_eq!(task.sweep_round, previous.sweep_round + 1);
        assert_eq!(fixture.blocks.notifications.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.blocks.accepted.load().len(), 1);
        assert_eq!(fixture.candidate().await.unwrap().phase, CandidatePhase::Complete);
        assert!(FileRepository::new(fixture.store.clone())
            .deleted(fixture.context, &fixture.file.location)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn immediate_reupload_preserves_the_old_cleanup_without_background_discovery() {
    for native in [false, true] {
        let fixture = TestDeletion::new(native).await;
        fixture.mark(DELETED_MS).await.unwrap();
        let files = FileRepository::new(fixture.store.clone());
        let mut replacement = fixture.file.clone();
        replacement.file = FileId::random();
        let FileContent::Chunks { root: Some(root) } = &mut replacement.content else {
            panic!("expected one physical range");
        };
        root.chunk.low = 8;
        fixture.store.reset_counts();
        files.publish(fixture.context, &replacement).await.unwrap();
        assert_eq!(fixture.store.counts(), (9, 5));
        let candidate = fixture.candidate().await.unwrap();
        assert_eq!(candidate.file, fixture.file);
        assert_eq!(candidate.not_before_ms, DELETED_MS + GcTask::PURGE_DELAY_MS);
        let task = fixture.discover().await;
        fixture.finish(task).await;
        assert_eq!(
            files.load(fixture.context, &replacement.location).await.unwrap(),
            Some(replacement)
        );
        assert_eq!(fixture.blocks.notifications.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.blocks.accepted.load()[0].chunk.low, 7);
    }
}

#[tokio::test]
async fn rejected_or_unknown_logical_marks_return_error_and_replay_from_authority() {
    let fixture = TestDeletion::new(false).await;
    fixture.store.mark_failure.store(1, Ordering::SeqCst);
    assert!(fixture.mark(DELETED_MS).await.is_err());
    assert_eq!(
        fixture.authority(),
        StorageRecord::File(Box::new(fixture.file.clone()))
    );
    assert_eq!(fixture.blocks.notifications.load(Ordering::SeqCst), 0);

    fixture.store.mark_failure.store(2, Ordering::SeqCst);
    assert!(fixture.mark(DELETED_MS + 1).await.is_err());
    let StorageRecord::DeletedFile(deleted) = fixture.authority() else {
        panic!("applied mark with lost reply must remain discoverable");
    };
    assert_eq!(deleted.deleted_ms, DELETED_MS + 1);
    fixture.mark(DELETED_MS + 60_000).await.unwrap();
    assert_eq!(fixture.authority(), StorageRecord::DeletedFile(deleted));
    assert_eq!(fixture.blocks.notifications.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_or_unknown_chunk_notification_keeps_the_candidate_and_file_evidence() {
    let fixture = TestDeletion::new(false).await;
    fixture.mark(DELETED_MS).await.unwrap();
    let mut task = fixture.discover().await;
    fixture.blocks.denied.store(true, Ordering::SeqCst);
    for _ in 0..40 {
        match fixture
            .step_after_restart(&task, DELETED_MS + GcTask::PURGE_DELAY_MS)
            .await
        {
            Ok(next) => task = next,
            Err(_) => break,
        }
    }
    assert_eq!(fixture.blocks.notifications.load(Ordering::SeqCst), 1);
    assert!(fixture.blocks.accepted.load().is_empty());
    assert_ne!(task.phase, GcPhase::Complete);
    assert_ne!(fixture.candidate().await.unwrap().phase, CandidatePhase::Complete);
    assert!(matches!(fixture.authority(), StorageRecord::DeletedFile(_)));

    fixture.blocks.denied.store(false, Ordering::SeqCst);
    fixture.blocks.lose_reply.store(true, Ordering::SeqCst);
    assert!(fixture
        .step_after_restart(&task, DELETED_MS + GcTask::PURGE_DELAY_MS)
        .await
        .is_err());
    assert_eq!(fixture.blocks.accepted.load().len(), 1);
    assert_ne!(fixture.candidate().await.unwrap().phase, CandidatePhase::Complete);
    assert!(matches!(fixture.authority(), StorageRecord::DeletedFile(_)));
    fixture.finish(task).await;
    assert_eq!(fixture.blocks.accepted.load().len(), 2);
    assert_eq!(
        fixture.blocks.accepted.load()[0],
        fixture.blocks.accepted.load()[1]
    );
}

#[tokio::test]
async fn uncertain_or_retained_reference_inventory_cannot_mark_a_live_file() {
    let fixture = TestDeletion::new(false).await;
    let files = FileRepository::new(fixture.store.clone());
    let result =
        mark_unreferenced_file_for_tests(&files, fixture.context, &fixture.file.location, DELETED_MS, None)
            .await;
    assert_eq!(result, Err(FileS3ErrorCode::SlowDown));
    let retained = [fixture.file.location.clone()];
    let result = mark_unreferenced_file_for_tests(
        &files,
        fixture.context,
        &fixture.file.location,
        DELETED_MS,
        Some(&retained),
    )
    .await;
    assert_eq!(result, Err(FileS3ErrorCode::Conflict));
    assert_eq!(
        fixture.authority(),
        StorageRecord::File(Box::new(fixture.file.clone()))
    );
    assert_eq!(fixture.blocks.notifications.load(Ordering::SeqCst), 0);
}

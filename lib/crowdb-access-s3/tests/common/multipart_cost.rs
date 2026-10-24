// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    part, repository_with_probe, session, CompletionPart, MetadataKey, MultipartPhase,
    MultipartRepositoryError, TenantId,
};

fn tenant() -> TenantId {
    TenantId::new(b"tenant".to_vec()).unwrap()
}

fn completion() -> [CompletionPart; 1] {
    [CompletionPart {
        number: 1,
        etag: "09".repeat(16),
    }]
}

#[tokio::test]
async fn normal_part_and_completion_have_no_post_write_metadata_reload() {
    let (repository, probe, _) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    let before = probe.counts();
    repository
        .put_stream_part(&session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (2, 4));
    repository
        .put_stream_part(&session, &part(), 111)
        .await
        .unwrap()
        .unwrap();
    let replay = probe.counts();
    assert_eq!((replay.0 - after.0, replay.1 - after.1), (2, 0));
    let frozen = repository
        .freeze_completion(&session, &completion(), 120)
        .await
        .unwrap()
        .unwrap();
    let freeze = probe.counts();
    assert_eq!((freeze.0 - replay.0, freeze.1 - replay.1), (3, 1));
    assert_eq!(
        repository.publish_completion(&frozen).await.unwrap().phase,
        MultipartPhase::Published
    );
    let published = probe.counts();
    assert_eq!((published.0 - freeze.0, published.1 - freeze.1), (4, 2));
}

#[tokio::test]
async fn abort_accepts_the_returned_terminal_record_without_get() {
    let (repository, probe, _) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    let mut aborted = session.clone();
    aborted.revision += 1;
    aborted.phase = MultipartPhase::Aborted;
    probe.conflict(
        MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id),
        aborted.encode().unwrap(),
        0,
    );
    let before = probe.counts();
    assert_eq!(repository.abort(&session).await.unwrap(), aborted);
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
}

#[tokio::test]
async fn completion_reuses_a_concurrent_identical_freeze_observation() {
    let (repository, probe, store) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let open = repository.load(&session).await.unwrap().unwrap();
    let frozen = repository
        .freeze_completion(&session, &completion(), 120)
        .await
        .unwrap()
        .unwrap();
    let key = MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id);
    assert!(store
        .compare_exchange(key.clone(), frozen.encode().unwrap(), open.encode().unwrap())
        .await
        .unwrap());
    probe.conflict(key, frozen.encode().unwrap(), 0);
    assert_eq!(
        repository
            .freeze_completion(&session, &completion(), 120)
            .await
            .unwrap(),
        Some(frozen)
    );
}

#[tokio::test]
async fn publication_finish_reuses_the_published_observation() {
    let (repository, probe, _) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let frozen = repository
        .freeze_completion(&session, &completion(), 120)
        .await
        .unwrap()
        .unwrap();
    let mut published = frozen.clone();
    published.revision += 1;
    published.phase = MultipartPhase::Published;
    probe.conflict(
        MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id),
        published.encode().unwrap(),
        0,
    );
    assert_eq!(repository.publish_completion(&frozen).await.unwrap(), published);
}

#[tokio::test]
async fn object_conflict_returns_without_get_or_overwriting_a_competitor() {
    for predecessor in [None, Some(b"old".to_vec())] {
        let (repository, probe, store) = repository_with_probe().await;
        let session = session();
        repository.begin(&session).await.unwrap();
        repository.put_stream_part(&session, &part(), 110).await.unwrap();
        let key = MetadataKey::object(&tenant(), session.bucket_id, &session.object_key).unwrap();
        if let Some(value) = predecessor {
            store.put_if_absent(key.clone(), value).await.unwrap();
        }
        let frozen = repository
            .freeze_completion(&session, &completion(), 120)
            .await
            .unwrap()
            .unwrap();
        probe.conflict(key, b"concurrent replacement".to_vec(), 0);
        assert!(matches!(
            repository.publish_completion(&frozen).await,
            Err(MultipartRepositoryError::Conflict)
        ));
    }
}

#[tokio::test]
async fn clearing_reservation_reuses_the_returned_upload_without_get() {
    let (repository, probe, _) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    let mut settled = session.clone();
    settled.revision = 3;
    settled.part_count = 1;
    settled.staged_bytes = part().length;
    probe.conflict(
        MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id),
        settled.encode().unwrap(),
        1,
    );
    let before = probe.counts();
    let saved = repository
        .put_stream_part(&session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.number, 1);
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (2, 4));
}

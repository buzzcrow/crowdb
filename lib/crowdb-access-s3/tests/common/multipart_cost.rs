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
    assert_eq!((freeze.0 - replay.0, freeze.1 - replay.1), (2, 1));
    assert_eq!(
        repository
            .publish_completion(&frozen)
            .await
            .unwrap()
            .logical_length,
        part().length
    );
    let published = probe.counts();
    assert_eq!((published.0 - freeze.0, published.1 - freeze.1), (3, 1));
}

#[tokio::test]
async fn upload_identity_admission_reads_only_the_bound_authority() {
    let (repository, probe, _) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    for (object, upload, expected) in [
        (
            session.object_key.as_slice(),
            session.upload_id,
            Some(session.clone()),
        ),
        (b"another".as_slice(), session.upload_id, None),
        (session.object_key.as_slice(), [8; 16], None),
    ] {
        let before = probe.counts();
        assert_eq!(
            repository
                .load_identity(session.bucket_id, object, &upload)
                .await
                .unwrap(),
            expected
        );
        let after = probe.counts();
        assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    }
}

#[tokio::test]
async fn a_foreign_listing_index_does_not_gate_direct_upload_requests() {
    let (repository, probe, store) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    let mut other = session.clone();
    other.upload_id = [8; 16];
    repository.begin(&other).await.unwrap();
    let index = MetadataKey::multipart_upload_index(
        &tenant(),
        session.bucket_id,
        &session.object_key,
        &session.upload_id,
    )
    .unwrap();
    let authority = MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id);
    let foreign = MetadataKey::multipart_session(&tenant(), other.bucket_id, &other.upload_id);
    assert!(store.compare_exchange(index, authority, foreign).await.unwrap());
    let before = probe.counts();
    assert_eq!(
        repository
            .load_identity(session.bucket_id, &session.object_key, &session.upload_id)
            .await
            .unwrap(),
        Some(session.clone())
    );
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    assert!(matches!(
        repository
            .list_uploads(session.bucket_id, b"", None, None, 10, 110)
            .await,
        Err(MultipartRepositoryError::Conflict)
    ));
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
async fn object_publication_never_writes_a_success_receipt() {
    let (repository, probe, _) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let frozen = repository
        .freeze_completion(&session, &completion(), 120)
        .await
        .unwrap()
        .unwrap();
    let before = probe.counts();
    repository.publish_completion(&frozen).await.unwrap();
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (3, 1));
    assert_eq!(repository.load(&session).await.unwrap(), Some(frozen));
}

#[tokio::test]
async fn object_publication_overwrites_any_previous_value_without_loading_it() {
    for predecessor in [None, Some(b"old".to_vec())] {
        let (repository, probe, store) = repository_with_probe().await;
        let session = session();
        repository.begin(&session).await.unwrap();
        repository.put_stream_part(&session, &part(), 110).await.unwrap();
        let key = MetadataKey::object(&tenant(), session.bucket_id, &session.object_key).unwrap();
        if let Some(value) = predecessor {
            store.put(key.clone(), value).await.unwrap();
        }
        let frozen = repository
            .freeze_completion(&session, &completion(), 120)
            .await
            .unwrap()
            .unwrap();
        store
            .put(key.clone(), b"concurrent replacement".to_vec())
            .await
            .unwrap();
        let before = probe.counts();
        let result = repository.publish_completion(&frozen).await.unwrap();
        let after = probe.counts();
        assert_eq!((after.0 - before.0, after.1 - before.1), (3, 1));
        let object = super::ObjectRecord::decode(&store.get(key).await.unwrap().unwrap().value).unwrap();
        assert_eq!(object.etag, result.etag);
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

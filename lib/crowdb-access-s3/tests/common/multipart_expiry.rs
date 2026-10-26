// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    part, repository_with_probe, session, unknown::assert_unknown, CompletionPart, MetadataKey,
    MultipartPhase, TenantId,
};

fn tenant() -> TenantId {
    TenantId::new(b"tenant".to_vec()).unwrap()
}

#[tokio::test]
async fn expiry_reuses_the_scanned_authority_without_an_abort_reload() {
    let (repository, probe, _) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let before = probe.counts();
    let page = repository
        .expire_page(session.bucket_id, None, 250, 1)
        .await
        .unwrap();
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    assert_eq!(page.expired, 1);
    assert!(page.next.is_none());
    assert_eq!(
        repository.load(&session).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
    assert!(repository
        .part_generation(&session, 1, 1)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn expiry_uses_a_returned_abort_winner_without_querying_it_again() {
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
    let page = repository
        .expire_page(session.bucket_id, None, 250, 1)
        .await
        .unwrap();
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    assert_eq!(page.expired, 1);
}

#[tokio::test]
async fn expiry_cannot_abort_a_concurrent_completion_that_wins_the_same_session_key() {
    let (repository, probe, store) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let open = repository.load(&session).await.unwrap().unwrap();
    let frozen = repository
        .freeze_completion(
            &session,
            &[CompletionPart {
                number: 1,
                etag: "09".repeat(16),
            }],
            120,
        )
        .await
        .unwrap()
        .unwrap();
    let key = MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id);
    assert!(store
        .compare_exchange(key.clone(), frozen.encode().unwrap(), open.encode().unwrap())
        .await
        .unwrap());
    probe.conflict(key, frozen.encode().unwrap(), 0);
    let before = probe.counts();
    let page = repository
        .expire_page(session.bucket_id, None, 250, 1)
        .await
        .unwrap();
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    assert_eq!(page.expired, 0);
}

#[tokio::test]
async fn unknown_expiry_does_not_query_and_rescan_preserves_all_part_evidence() {
    let (repository, probe, _) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let before = probe.counts();
    probe.lose_after(0);
    assert_unknown(repository.expire_page(session.bucket_id, None, 250, 1).await);
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    let page = repository
        .expire_page(session.bucket_id, None, 250, 1)
        .await
        .unwrap();
    let replay = probe.counts();
    assert_eq!((replay.0 - after.0, replay.1 - after.1), (1, 0));
    assert_eq!(page.expired, 0);
    assert!(repository
        .part_generation(&session, 1, 1)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn expiry_still_settles_a_pending_part_before_terminal_transition() {
    let (repository, probe, _) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    probe.lose_after(2);
    assert_unknown(repository.put_stream_part(&session, &part(), 110).await);
    assert!(repository
        .load(&session)
        .await
        .unwrap()
        .unwrap()
        .pending
        .is_some());
    assert_eq!(
        repository
            .expire_page(session.bucket_id, None, 250, 1)
            .await
            .unwrap()
            .expired,
        1
    );
    let current = repository.load(&session).await.unwrap().unwrap();
    assert_eq!(current.phase, MultipartPhase::Aborted);
    assert!(current.pending.is_none());
    assert_eq!(
        repository.part(&session, 1).await.unwrap().unwrap().length,
        part().length
    );
    assert!(repository
        .part_generation(&session, 1, 1)
        .await
        .unwrap()
        .is_some());
}

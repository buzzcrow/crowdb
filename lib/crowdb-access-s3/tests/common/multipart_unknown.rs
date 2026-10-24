// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{part, repository_with_probe, session, CompletionPart, MultipartRepositoryError, TenantId};
use crowdb_access_s3::metadata::{MetadataStoreError, MultipartPhase, MultipartRepository};
use crowdb_chunk_kv_client::ClientError;

pub fn assert_unknown<T: std::fmt::Debug>(result: Result<T, MultipartRepositoryError>) {
    match result {
        Err(MultipartRepositoryError::Store(MetadataStoreError::Client(ClientError::Transport(_)))) => {}
        other => panic!("lost mutation reply must remain unknown: {other:?}"),
    }
}

fn completion() -> [CompletionPart; 1] {
    [CompletionPart {
        number: 1,
        etag: "09".repeat(16),
    }]
}

#[tokio::test]
async fn initiation_does_not_confirm_either_lost_mutation() {
    for offset in 0..2 {
        let (repository, probe, store) = repository_with_probe().await;
        let session = session();
        probe.lose_after(offset);
        assert_unknown(repository.begin(&session).await);
        assert_eq!(probe.counts(), (0, offset + 1));
        let restarted = MultipartRepository::new(store, TenantId::new(b"tenant".to_vec()).unwrap());
        assert_eq!(restarted.begin(&session).await.unwrap(), session);
        assert!(restarted
            .list_parts(&session, 0, 10)
            .await
            .unwrap()
            .parts
            .is_empty());
    }
}

#[tokio::test]
async fn each_lost_part_mutation_returns_without_confirmation_and_full_part_retry_works() {
    for offset in 0..4 {
        let (repository, probe, store) = repository_with_probe().await;
        let session = session();
        repository.begin(&session).await.unwrap();
        let before = probe.counts();
        probe.lose_after(offset);
        assert_unknown(repository.put_stream_part(&session, &part(), 110).await);
        let after = probe.counts();
        assert_eq!((after.0 - before.0, after.1 - before.1), (2, offset + 1));
        let restarted = MultipartRepository::new(store, TenantId::new(b"tenant".to_vec()).unwrap());
        let saved = restarted
            .put_stream_part(&session, &part(), 110)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.raw_md5, part().raw_md5);
        assert_eq!(restarted.part(&session, 1).await.unwrap(), Some(saved));
        let current = restarted.load(&session).await.unwrap().unwrap();
        assert!(current.pending.is_none());
        assert_eq!((current.part_count, current.staged_bytes), (1, 5));
    }
}

#[tokio::test]
async fn completion_unknown_is_explicit_and_cross_instance_retry_uses_the_same_selection() {
    for offset in 0..3 {
        let (repository, probe, store) = repository_with_probe().await;
        let session = session();
        repository.begin(&session).await.unwrap();
        repository.put_stream_part(&session, &part(), 110).await.unwrap();
        let before = probe.counts();
        let frozen = if offset == 0 {
            probe.lose_after(0);
            assert_unknown(repository.freeze_completion(&session, &completion(), 120).await);
            let after = probe.counts();
            assert_eq!((after.0 - before.0, after.1 - before.1), (3, 1));
            None
        } else {
            Some(
                repository
                    .freeze_completion(&session, &completion(), 120)
                    .await
                    .unwrap()
                    .unwrap(),
            )
        };
        if let Some(frozen) = frozen {
            let before = probe.counts();
            probe.lose_after(offset - 1);
            assert_unknown(repository.publish_completion(&frozen).await);
            let after = probe.counts();
            assert_eq!((after.0 - before.0, after.1 - before.1), (4, offset));
        }
        let restarted = MultipartRepository::new(store, TenantId::new(b"tenant".to_vec()).unwrap());
        let frozen = restarted
            .freeze_completion(&session, &completion(), 120)
            .await
            .unwrap()
            .unwrap();
        let published = restarted.publish_completion(&frozen).await.unwrap();
        assert_eq!(published.phase, MultipartPhase::Published);
        assert_eq!(
            published.completion_request_digest,
            frozen.completion_request_digest
        );
        assert_eq!(published.selection.as_ref().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn lost_abort_does_not_confirm_and_later_retry_preserves_part_evidence() {
    let (repository, probe, store) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let before = probe.counts();
    probe.lose_after(0);
    assert_unknown(repository.abort(&session).await);
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    let restarted = MultipartRepository::new(store, TenantId::new(b"tenant".to_vec()).unwrap());
    assert_eq!(
        restarted.abort(&session).await.unwrap().phase,
        MultipartPhase::Aborted
    );
    assert_eq!(restarted.part(&session, 1).await.unwrap().unwrap().length, 5);
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_s3::{
    execution::{MultipartFailure, S3Executor},
    metadata::{ChunkKvMetadataStore, MultipartPhase, MultipartRepository, MultipartSessionRecord},
    storage::S3StorageClients,
    S3ErrorCode,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};

use super::retrieval_storage::TestReadStorage;
use super::{actor::TestProbe, part, repository_with_probe, session, CompletionPart, MetadataKey, TenantId};

struct TestCompletion {
    repository: MultipartRepository,
    store: Arc<ChunkKvMetadataStore>,
    probe: Arc<TestProbe>,
    chunks: Arc<TestReadStorage>,
    executor: S3Executor,
    session: MultipartSessionRecord,
}

impl TestCompletion {
    async fn new() -> Self {
        let (repository, probe, store) = repository_with_probe().await;
        let chunks = Arc::new(TestReadStorage { queries: 0.into() });
        let executor = S3Executor::new(
            S3StorageClients {
                control: Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new()))),
                metadata: store.clone(),
                chunks: Arc::new(chunks.client()),
            },
            TenantId::new(b"tenant".to_vec()).unwrap(),
        );
        let mut session = session();
        session.expires_ms = u64::MAX;
        repository.begin(&session).await.unwrap();
        repository
            .put_stream_part(&session, &part(), 110)
            .await
            .unwrap()
            .unwrap();
        Self {
            repository,
            store,
            probe,
            chunks,
            executor,
            session,
        }
    }

    async fn complete(&self) -> Result<MultipartSessionRecord, MultipartFailure> {
        self.executor
            .complete_multipart(&self.session, &selection())
            .await
    }

    async fn assert_published(&self) {
        let key = MetadataKey::object(
            &TenantId::new(b"tenant".to_vec()).unwrap(),
            self.session.bucket_id,
            &self.session.object_key,
        )
        .unwrap();
        let stored = self.store.get(key).await.unwrap().unwrap();
        let record = super::ObjectRecord::decode(&stored.value).unwrap();
        assert_eq!(record.bucket_id, self.session.bucket_id);
        assert_eq!(record.key, self.session.object_key);
        assert_eq!(record.logical_length, part().length);
        assert_eq!(
            bincode::deserialize::<Vec<crowdb_protocol::chunkdb::rpc::Location>>(&record.data_reference)
                .unwrap(),
            part().locations
        );
        assert_eq!(self.chunks.queries.load(Ordering::SeqCst), 0);
    }
}

fn selection() -> [CompletionPart; 1] {
    [CompletionPart {
        number: 1,
        etag: "09".repeat(16),
    }]
}

#[tokio::test]
async fn executor_completion_reuses_its_admitted_and_frozen_session() {
    let fixture = TestCompletion::new().await;
    let before = fixture.probe.counts();
    assert_eq!(fixture.complete().await.unwrap().phase, MultipartPhase::Published);
    let after = fixture.probe.counts();
    eprintln!(
        "S3 executor Complete: {} GET + {} mutations",
        after.0 - before.0,
        after.1 - before.1
    );
    assert_eq!((after.0 - before.0, after.1 - before.1), (6, 3));
    fixture.assert_published().await;
}

#[tokio::test]
async fn executor_completion_reuses_an_already_frozen_session_and_exact_published_replay() {
    let fixture = TestCompletion::new().await;
    let frozen = fixture
        .repository
        .freeze_completion(&fixture.session, &selection(), 120)
        .await
        .unwrap()
        .unwrap();
    let before = fixture.probe.counts();
    let published = fixture.complete().await.unwrap();
    let after = fixture.probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (4, 2));
    assert_eq!(published.selection, frozen.selection);
    assert_eq!(published.publication_ms, frozen.publication_ms);
    let before = fixture.probe.counts();
    assert_eq!(fixture.complete().await.unwrap(), published);
    let after = fixture.probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    let mut changed = selection();
    changed[0].etag = "08".repeat(16);
    let before = fixture.probe.counts();
    assert!(matches!(
        fixture
            .executor
            .complete_multipart(&fixture.session, &changed)
            .await,
        Err(MultipartFailure::S3(S3ErrorCode::NoSuchUpload))
    ));
    let after = fixture.probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    fixture.assert_published().await;
}

#[tokio::test]
async fn executor_completion_keeps_the_object_predecessor_conflict_without_a_confirmation_get() {
    let fixture = TestCompletion::new().await;
    let key = MetadataKey::object(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        fixture.session.bucket_id,
        &fixture.session.object_key,
    )
    .unwrap();
    fixture.probe.conflict(key, b"competing generation".to_vec(), 0);
    let before = fixture.probe.counts();
    assert!(matches!(
        fixture.complete().await,
        Err(MultipartFailure::S3(S3ErrorCode::NoSuchUpload))
    ));
    let after = fixture.probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (6, 2));
    assert_eq!(
        fixture
            .repository
            .load(&fixture.session)
            .await
            .unwrap()
            .unwrap()
            .phase,
        MultipartPhase::Publishing
    );
}

#[tokio::test]
async fn executor_completion_stops_each_unknown_write_and_fresh_calls_recover_the_frozen_selection() {
    for (offset, expected) in [(0, (3, 1)), (1, (6, 2)), (2, (6, 3))] {
        let fixture = TestCompletion::new().await;
        fixture.probe.lose_after(offset);
        let before = fixture.probe.counts();
        assert!(matches!(
            fixture.complete().await,
            Err(MultipartFailure::Uncertain)
        ));
        let after = fixture.probe.counts();
        assert_eq!((after.0 - before.0, after.1 - before.1), expected);
        let frozen = fixture.repository.load(&fixture.session).await.unwrap().unwrap();
        assert!(matches!(
            frozen.phase,
            MultipartPhase::Publishing | MultipartPhase::Published
        ));
        let published = fixture.complete().await.unwrap();
        assert_eq!(published.selection, frozen.selection);
        assert_eq!(published.publication_ms, frozen.publication_ms);
        fixture.assert_published().await;
    }
}

#[tokio::test]
async fn concurrent_part_replacement_cannot_publish_a_different_generation_for_the_requested_etag() {
    for _ in 0..20 {
        let fixture = TestCompletion::new().await;
        let mut replacement = part();
        replacement.locations[0].offset += 39;
        replacement.raw_md5 = [8; 16];
        let (completed, replaced) = tokio::join!(
            fixture.complete(),
            fixture
                .repository
                .put_stream_part(&fixture.session, &replacement, 115),
        );
        if completed.is_ok() {
            assert!(replaced.is_err());
            fixture.assert_published().await;
        } else {
            assert!(matches!(
                completed,
                Err(MultipartFailure::S3(
                    S3ErrorCode::InvalidPart | S3ErrorCode::ServiceUnavailable
                ))
            ));
            assert_eq!(replaced.unwrap().unwrap().raw_md5, replacement.raw_md5);
            let key = MetadataKey::object(
                &TenantId::new(b"tenant".to_vec()).unwrap(),
                fixture.session.bucket_id,
                &fixture.session.object_key,
            )
            .unwrap();
            assert!(fixture.store.get(key).await.unwrap().is_none());
        }
    }
}

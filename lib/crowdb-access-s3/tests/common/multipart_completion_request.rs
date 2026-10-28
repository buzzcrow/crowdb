// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_s3::{
    execution::{MultipartFailure, S3Executor},
    metadata::{ChunkKvMetadataStore, MultipartPhase, MultipartRepository, MultipartSessionRecord},
    storage::S3StorageClients,
    S3ErrorCode,
};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};

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
                control: Arc::new(PaxosKvClient::new(ClientConfig::new(Vec::new()))),
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

    async fn complete(&self) -> Result<crowdb_access_s3::metadata::MultipartCompletion, MultipartFailure> {
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

async fn replace_with_competing_object(fixture: &TestCompletion) {
    let key = MetadataKey::object(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        fixture.session.bucket_id,
        &fixture.session.object_key,
    )
    .unwrap();
    let observed = fixture.store.get(key.clone()).await.unwrap().unwrap();
    let mut competing = super::ObjectRecord::decode(&observed.value).unwrap();
    let mut locations = part().locations;
    locations[0].offset += 1_000;
    competing.data_reference = bincode::serialize(&locations).unwrap();
    competing.content_type = "application/competing".into();
    fixture.store.put(key, competing.encode().unwrap()).await.unwrap();
}

#[tokio::test]
async fn completion_retry_overwrites_a_later_put_after_an_unknown_object_publication() {
    let fixture = TestCompletion::new().await;
    fixture.probe.lose_after(1);
    assert!(matches!(
        fixture.complete().await,
        Err(MultipartFailure::Uncertain)
    ));
    replace_with_competing_object(&fixture).await;
    fixture.complete().await.unwrap();
    fixture.assert_published().await;
}

#[tokio::test]
async fn completion_retry_overwrites_a_later_put_after_confirmed_publication() {
    let fixture = TestCompletion::new().await;
    fixture.complete().await.unwrap();
    replace_with_competing_object(&fixture).await;
    fixture.complete().await.unwrap();
    fixture.assert_published().await;
}

#[tokio::test]
async fn bound_upload_rejects_a_different_valid_part_subset_before_writing() {
    let fixture = TestCompletion::new().await;
    let mut second = part();
    second.number = 2;
    second.raw_md5 = [8; 16];
    second.locations[0].offset += 39;
    fixture
        .repository
        .put_stream_part(&fixture.session, &second, 115)
        .await
        .unwrap()
        .unwrap();
    fixture.complete().await.unwrap();
    let before = fixture.probe.counts();
    assert!(matches!(
        fixture
            .executor
            .complete_multipart(
                &fixture.session,
                &[CompletionPart {
                    number: 2,
                    etag: "08".repeat(16),
                }]
            )
            .await,
        Err(MultipartFailure::S3(S3ErrorCode::NoSuchUpload))
    ));
    let after = fixture.probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    fixture.assert_published().await;
}

#[tokio::test]
async fn executor_completion_reuses_its_admitted_and_frozen_session() {
    let fixture = TestCompletion::new().await;
    let before = fixture.probe.counts();
    assert!(fixture.complete().await.unwrap().etag.ends_with("-1"));
    let after = fixture.probe.counts();
    eprintln!(
        "S3 executor Complete: {} GET + {} mutations",
        after.0 - before.0,
        after.1 - before.1
    );
    assert_eq!((after.0 - before.0, after.1 - before.1), (4, 2));
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
    assert_eq!((after.0 - before.0, after.1 - before.1), (3, 1));
    assert_eq!(
        fixture.repository.load(&fixture.session).await.unwrap().unwrap(),
        frozen
    );
    let before = fixture.probe.counts();
    assert_eq!(fixture.complete().await.unwrap(), published);
    let after = fixture.probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (3, 1));
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
    assert_eq!((after.0 - before.0, after.1 - before.1), (3, 0));
    fixture.assert_published().await;
}

#[tokio::test]
async fn executor_completion_overwrites_an_existing_object_without_loading_it() {
    let fixture = TestCompletion::new().await;
    let key = MetadataKey::object(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        fixture.session.bucket_id,
        &fixture.session.object_key,
    )
    .unwrap();
    fixture
        .store
        .put(key, b"competing generation".to_vec())
        .await
        .unwrap();
    let before = fixture.probe.counts();
    fixture.complete().await.unwrap();
    let after = fixture.probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (4, 2));
    fixture.assert_published().await;
}

#[tokio::test]
async fn executor_completion_stops_each_unknown_write_and_fresh_calls_recover_the_frozen_selection() {
    for (offset, expected) in [(0, (2, 1)), (1, (4, 2))] {
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
        assert_eq!(frozen.phase, MultipartPhase::Publishing);
        let published = fixture.complete().await.unwrap();
        assert!(published.etag.ends_with("-1"));
        assert_eq!(
            fixture.repository.load(&fixture.session).await.unwrap().unwrap(),
            frozen
        );
        fixture.assert_published().await;
    }
}

#[tokio::test]
async fn unknown_before_apply_stops_without_get_and_fresh_request_can_retry() {
    for (offset, expected) in [(0, (2, 1)), (1, (4, 2))] {
        let fixture = TestCompletion::new().await;
        fixture.probe.fail_before(offset);
        let before = fixture.probe.counts();
        assert!(matches!(
            fixture.complete().await,
            Err(MultipartFailure::Uncertain)
        ));
        let after = fixture.probe.counts();
        assert_eq!((after.0 - before.0, after.1 - before.1), expected);
        let key = MetadataKey::object(
            &TenantId::new(b"tenant".to_vec()).unwrap(),
            fixture.session.bucket_id,
            &fixture.session.object_key,
        )
        .unwrap();
        assert!(fixture.store.get(key).await.unwrap().is_none());
        fixture.complete().await.unwrap();
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

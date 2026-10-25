// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_s3::{
    execution::{MultipartFailure, S3Executor},
    metadata::{
        BucketId, BucketNameRecord, ChunkKvMetadataStore, CompletionPart, MetadataKey, MetadataStoreError,
        MultipartPartRecord, MultipartPhase, MultipartRepository, MultipartRepositoryError,
        MultipartSessionRecord, TenantId, UserMetadata,
    },
    storage::S3StorageClients,
};
use crowdb_chunk_kv_client::ClientError;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::{chunkdb::rpc::Location, common::ChunkId};

use super::{actor::TestProbe, retrieval_storage::TestReadStorage};

pub struct TestExpiry {
    pub repository: MultipartRepository,
    pub store: Arc<ChunkKvMetadataStore>,
    pub probe: Arc<TestProbe>,
    pub executor: S3Executor,
    pub session: MultipartSessionRecord,
    chunks: Arc<TestReadStorage>,
}

impl TestExpiry {
    pub async fn new(pending: bool) -> Self {
        let (repository, probe, store) = super::actor::repository_with_probe().await;
        let chunks = Arc::new(TestReadStorage { queries: 0.into() });
        let executor = executor(store.clone(), &chunks);
        let session = session();
        let bucket = BucketNameRecord {
            tenant: tenant(),
            name: b"bucket".to_vec(),
            bucket_id: session.bucket_id,
            tombstone: false,
        };
        store
            .put(
                MetadataKey::bucket_name(&tenant(), b"bucket").unwrap(),
                bucket.encode(),
            )
            .await
            .unwrap();
        repository.begin(&session).await.unwrap();
        if pending {
            probe.lose_after(2);
            assert_unknown(&repository.put_stream_part(&session, &part(), 110).await);
        } else {
            repository
                .put_stream_part(&session, &part(), 110)
                .await
                .unwrap()
                .unwrap();
        }
        Self {
            repository,
            store,
            probe,
            executor,
            session,
            chunks,
        }
    }

    pub fn key(&self) -> Vec<u8> {
        MetadataKey::multipart_session(&tenant(), self.session.bucket_id, &self.session.upload_id)
    }

    pub fn delta(&self, before: (usize, usize)) -> (usize, usize) {
        let after = self.probe.counts();
        (after.0 - before.0, after.1 - before.1)
    }

    pub fn restart(&self) -> S3Executor {
        executor(self.store.clone(), &self.chunks)
    }

    pub async fn retained(&self, phase: MultipartPhase) {
        let current = self.repository.load(&self.session).await.unwrap().unwrap();
        assert_eq!(current.phase, phase);
        assert!(current.pending.is_none());
        self.retained_parts().await;
    }

    pub async fn retained_parts(&self) {
        let pointer = self.repository.part(&self.session, 1).await.unwrap().unwrap();
        let generation = self
            .repository
            .part_generation(&self.session, 1, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pointer, generation);
        assert_eq!(pointer.length, part().length);
        assert_eq!(pointer.raw_md5, part().raw_md5);
        assert_eq!(pointer.locations, part().locations);
        assert_eq!(self.chunks.queries.load(Ordering::Relaxed), 0);
    }

    pub async fn completion_winner(&self) -> MultipartSessionRecord {
        let open = self.repository.load(&self.session).await.unwrap().unwrap();
        let frozen = self
            .repository
            .freeze_completion(&self.session, &selection(), 120)
            .await
            .unwrap()
            .unwrap();
        assert!(self
            .store
            .compare_exchange(self.key(), frozen.encode().unwrap(), open.encode().unwrap())
            .await
            .unwrap());
        frozen
    }
}

fn executor(store: Arc<ChunkKvMetadataStore>, chunks: &Arc<TestReadStorage>) -> S3Executor {
    S3Executor::new(
        S3StorageClients {
            control: Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new()))),
            metadata: store,
            chunks: Arc::new(chunks.client()),
        },
        tenant(),
    )
}

pub fn assert_unknown<T: std::fmt::Debug>(result: &Result<T, MultipartRepositoryError>) {
    assert!(
        matches!(
            result,
            Err(MultipartRepositoryError::Store(MetadataStoreError::Client(
                ClientError::Transport(_)
            )))
        ),
        "lost mutation reply must remain unknown: {result:?}"
    );
}

pub fn assert_uncertain<T: std::fmt::Debug>(result: &Result<T, MultipartFailure>) {
    assert!(
        matches!(result, Err(MultipartFailure::Uncertain)),
        "unknown expiry: {result:?}"
    );
}

pub fn tenant() -> TenantId {
    TenantId::new(b"tenant".to_vec()).unwrap()
}

fn selection() -> [CompletionPart; 1] {
    [CompletionPart {
        number: 1,
        etag: "09".repeat(16),
    }]
}

fn session() -> MultipartSessionRecord {
    let record = MultipartSessionRecord {
        bucket_id: BucketId::new([3; 16]),
        object_key: b"object".to_vec(),
        upload_id: [7; 16],
        revision: 1,
        phase: MultipartPhase::Open,
        created_ms: 100,
        expires_ms: 200,
        content_type: "application/octet-stream".into(),
        attributes: UserMetadata::from_headers(&hyper::HeaderMap::new())
            .unwrap()
            .encode()
            .unwrap(),
        max_parts: 10,
        max_part_bytes: 100,
        max_object_bytes: 500,
        max_staged_bytes: 1_000,
        part_count: 0,
        staged_bytes: 0,
        pending: None,
        selection: None,
        legacy_observation: None,
    };
    // Pending recovery and its exact mutation cut points retain the historical v4 resource.
    let mut bytes = record.encode().unwrap();
    bytes[4] = 4;
    MultipartSessionRecord::decode(&bytes, record.bucket_id, &record.object_key, &record.upload_id).unwrap()
}

fn part() -> MultipartPartRecord {
    MultipartPartRecord {
        bucket_id: BucketId::new([3; 16]),
        upload_id: [7; 16],
        number: 1,
        revision: 1,
        modified_ms: 0,
        length: 5,
        raw_md5: [9; 16],
        locations: vec![Location {
            chunk_id: Some(ChunkId { high: 1, low: 2 }),
            offset: 10,
            length: 39,
            logical_offset: 0,
            logical_length: 5,
        }],
    }
}

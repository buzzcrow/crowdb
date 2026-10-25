// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crowdb_access_s3::{
    bucket::{create_bucket, BucketIdGenerator},
    metadata::{
        BucketId, ChunkKvMetadataStore, MetadataKey, MultipartExpirySweep, MultipartPartRecord,
        MultipartPhase, MultipartRepository, MultipartSessionRecord, TenantId,
    },
};

use crate::{actor::TestProbe, part, repository_with_probe, session};

pub(super) const CREATED: u64 = 100;
pub(super) const WEEK: u64 = 7 * 24 * 60 * 60 * 1_000;
pub(super) const NOW: u64 = CREATED + WEEK + 1;

pub(super) struct TestBucketIds(AtomicUsize);

impl BucketIdGenerator for TestBucketIds {
    fn next_id(&self) -> BucketId {
        let value = u8::try_from(self.0.fetch_add(1, Ordering::Relaxed)).unwrap();
        BucketId::new([128 + value; 16])
    }
}

pub(super) struct TestExpiry {
    pub repository: MultipartRepository,
    pub probe: Arc<TestProbe>,
    pub store: Arc<ChunkKvMetadataStore>,
    pub tenant: TenantId,
    ids: TestBucketIds,
}

impl TestExpiry {
    pub(super) async fn new() -> Self {
        Self::with_tenant(TenantId::new(b"tenant".to_vec()).unwrap()).await
    }

    pub(super) async fn with_tenant(tenant: TenantId) -> Self {
        let (_, probe, store) = repository_with_probe().await;
        Self {
            repository: MultipartRepository::new(store.clone(), tenant.clone()),
            probe,
            store,
            tenant,
            ids: TestBucketIds(AtomicUsize::new(0)),
        }
    }

    pub(super) async fn bucket(&self, name: &[u8]) -> BucketId {
        create_bucket(&self.store, &self.ids, &self.tenant, name)
            .await
            .unwrap()
    }

    pub(super) fn upload(bucket: BucketId, id: u8) -> MultipartSessionRecord {
        let mut value = session();
        value.bucket_id = bucket;
        value.upload_id = [id; 16];
        value.object_key = vec![id];
        value.created_ms = CREATED;
        value.expires_ms = CREATED + WEEK;
        value
    }

    pub(super) async fn begin_part(&self, upload: &MultipartSessionRecord) -> MultipartPartRecord {
        self.repository.begin(upload).await.unwrap();
        let mut value = part();
        value.bucket_id = upload.bucket_id;
        value.upload_id = upload.upload_id;
        self.repository
            .put_stream_part(upload, &value, CREATED + 1)
            .await
            .unwrap()
            .unwrap()
    }

    pub(super) async fn evidence(&self, upload: &MultipartSessionRecord) -> Vec<(Vec<u8>, Vec<u8>)> {
        let keys = [
            MetadataKey::multipart_upload_index(
                &self.tenant,
                upload.bucket_id,
                &upload.object_key,
                &upload.upload_id,
            )
            .unwrap(),
            MetadataKey::multipart_part(&self.tenant, upload.bucket_id, &upload.upload_id, 1).unwrap(),
            MetadataKey::multipart_part_generation(&self.tenant, upload.bucket_id, &upload.upload_id, 1, 1)
                .unwrap(),
        ];
        let mut values = Vec::new();
        for key in keys {
            let value = self.store.get(key.clone()).await.unwrap().unwrap();
            values.push((key, value.value));
        }
        values
    }

    pub(super) async fn assert_phase(&self, upload: &MultipartSessionRecord, phase: MultipartPhase) {
        assert_eq!(self.repository.load(upload).await.unwrap().unwrap().phase, phase);
    }

    pub(super) async fn assert_evidence(&self, expected: &[(Vec<u8>, Vec<u8>)]) {
        for (key, value) in expected {
            assert_eq!(self.store.get(key.clone()).await.unwrap().unwrap().value, *value);
        }
    }

    pub(super) fn tenant_prefix(&self) -> Vec<u8> {
        let mut key = MetadataKey::object_prefix(&self.tenant, BucketId::new([0; 16]));
        key.truncate(key.len() - 17);
        key
    }

    pub(super) fn bucket_prefix(&self, bucket: BucketId) -> Vec<u8> {
        let mut key = MetadataKey::object_prefix(&self.tenant, bucket);
        key.pop();
        key
    }

    pub(super) async fn seed_retained_parts(&self, upload: &MultipartSessionRecord) {
        for number in 1..=5_000 {
            let mut value = part();
            value.bucket_id = upload.bucket_id;
            value.upload_id = upload.upload_id;
            value.number = number;
            let keys = [
                MetadataKey::multipart_part(&self.tenant, upload.bucket_id, &upload.upload_id, number)
                    .unwrap(),
                MetadataKey::multipart_part_generation(
                    &self.tenant,
                    upload.bucket_id,
                    &upload.upload_id,
                    number,
                    1,
                )
                .unwrap(),
            ];
            for key in keys {
                self.store
                    .put_if_absent(key, value.encode().unwrap())
                    .await
                    .unwrap();
            }
        }
    }

    pub(super) fn sweep(&self) -> MultipartExpirySweep {
        MultipartExpirySweep::new(self.store.clone(), self.tenant.clone())
    }
}

pub(super) async fn finish_round(sweep: &mut MultipartExpirySweep, now: u64, limit: usize) -> usize {
    let mut expired = 0;
    for _ in 0..limit {
        let step = sweep.step(now).await.unwrap();
        expired += step.expired;
        if !step.more {
            return expired;
        }
    }
    panic!("one expiry round exceeded its bounded work window count");
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::bucket_client;

use async_trait::async_trait;
use crowdb_access_s3::{
    bucket::{BucketError, BucketListRequest, BucketListSigner},
    metadata::{
        BucketId, BucketNameRecord, ChunkKvMetadataStore, MetadataKey, MultipartPartRecord, MultipartPhase,
        MultipartSessionRecord, TenantId,
    },
};
use crowdb_chunk_kv_client::{ChunkKvTransport, Result};
use crowdb_protocol::{chunk_kv::*, chunkdb::rpc::Location};
use std::sync::Arc;

struct TestTransport(Vec<RpcValue>);
type TestMetadataRow = (&'static str, Vec<u8>, Vec<u8>);

#[async_trait]
impl ChunkKvTransport for TestTransport {
    async fn point(&self, _: &str, _: &PointRequest) -> Result<ChunkKvResponse> {
        panic!("listing must only scan");
    }

    async fn seek(&self, _: &str, _: &SeekRequest) -> Result<ChunkKvResponse> {
        panic!("listing must only scan");
    }

    async fn scan(&self, _: &str, request: &ScanRequest) -> Result<ChunkKvResponse> {
        let mut items: Vec<_> = self
            .0
            .iter()
            .filter(|item| {
                request.start.as_ref().map_or(true, |start| item.key >= *start)
                    && request.end.as_ref().map_or(true, |end| item.key < *end)
                    && request
                        .continuation
                        .as_ref()
                        .map_or(true, |cursor| item.key > cursor.last_key)
            })
            .cloned()
            .collect();
        let more = items.len() > request.limit as usize;
        items.truncate(request.limit as usize);
        let continuation = more.then(|| ScanContinuation {
            direction: request.direction,
            last_key: items.last().unwrap().key.clone(),
            partition_id: request.routing.partition_id,
            owner_epoch: request.routing.owner_epoch,
            map_revision: request.routing.map_revision,
        });
        Ok(ChunkKvResponse {
            map_revision: 1,
            journal_position: None,
            result: Ok(OperationResult::Scan { items, continuation }),
        })
    }
}

pub(super) fn session() -> MultipartSessionRecord {
    let mut bucket = [0x11; 16];
    bucket[0] = 1;
    MultipartSessionRecord {
        bucket_id: BucketId::new(bucket),
        object_key: b"multipart\0object".to_vec(),
        upload_id: [7; 16],
        revision: 1,
        phase: MultipartPhase::Open,
        created_ms: 100,
        expires_ms: 200,
        content_type: "application/octet-stream".into(),
        attributes: Vec::new(),
        max_parts: 10,
        max_part_bytes: 100,
        max_object_bytes: 500,
        max_staged_bytes: 1000,
        part_count: 0,
        staged_bytes: 0,
        pending: None,
        selection: None,
        legacy_observation: None,
    }
}

pub(super) fn multipart_rows(tenant: &TenantId) -> Vec<TestMetadataRow> {
    let session = session();
    let session_key = MetadataKey::multipart_session(tenant, session.bucket_id, &session.upload_id);
    let session_bytes = session.encode().unwrap();
    assert_eq!(
        MultipartSessionRecord::decode(
            &session_bytes,
            session.bucket_id,
            &session.object_key,
            &session.upload_id
        )
        .unwrap(),
        session
    );
    let part = MultipartPartRecord {
        bucket_id: session.bucket_id,
        upload_id: session.upload_id,
        number: 1,
        revision: 1,
        modified_ms: 150,
        length: 5,
        raw_md5: [9; 16],
        locations: vec![Location {
            chunk_id: Some(crowdb_protocol::common::ChunkId { high: 1, low: 2 }),
            offset: 10,
            length: 39,
            logical_offset: 0,
            logical_length: 5,
        }],
    };
    let part_bytes = part.encode().unwrap();
    assert_eq!(
        MultipartPartRecord::decode(&part_bytes, part.bucket_id, &part.upload_id, 1).unwrap(),
        part
    );
    vec![
        ("session", session_key.clone(), session_bytes),
        (
            "part",
            MetadataKey::multipart_part(tenant, part.bucket_id, &part.upload_id, 1).unwrap(),
            part_bytes.clone(),
        ),
        (
            "part-generation",
            MetadataKey::multipart_part_generation(tenant, part.bucket_id, &part.upload_id, 1, 1).unwrap(),
            part_bytes,
        ),
        (
            "upload-index",
            MetadataKey::multipart_upload_index(
                tenant,
                session.bucket_id,
                &session.object_key,
                &session.upload_id,
            )
            .unwrap(),
            session_key,
        ),
    ]
}

pub(super) async fn store(
    tenant: &TenantId,
    extra_key: Vec<u8>,
    extra_value: Vec<u8>,
) -> ChunkKvMetadataStore {
    let mapping = BucketNameRecord {
        tenant: tenant.clone(),
        name: b"expected".to_vec(),
        bucket_id: session().bucket_id,
        tombstone: false,
    };
    let mut items = vec![
        RpcValue {
            key: MetadataKey::bucket_name(tenant, &mapping.name).unwrap(),
            value: mapping.encode(),
            revision: 1,
        },
        RpcValue {
            key: extra_key,
            value: extra_value,
            revision: 2,
        },
    ];
    items.sort_by(|a, b| a.key.cmp(&b.key));
    let client = Arc::new(bucket_client::client(Arc::new(TestTransport(items))));
    client.refresh_catalog().await.unwrap();
    ChunkKvMetadataStore::new(client)
}

pub(super) async fn paginated(
    store: &ChunkKvMetadataStore,
    tenant: &TenantId,
) -> std::result::Result<Vec<Vec<u8>>, BucketError> {
    BucketListSigner::new(b"secret".to_vec())?
        .list(
            store,
            tenant,
            &BucketListRequest {
                prefix: b"",
                continuation: None,
                max_buckets: 100,
                max_scan_items: 100,
                max_scan_bytes: 65536,
                now: 150,
                ttl: 30,
            },
        )
        .await
        .map(|page| page.buckets.into_iter().map(|bucket| bucket.name).collect())
}

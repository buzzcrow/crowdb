// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use crowdb_access_s3::metadata::{BucketId, ChunkKvMetadataStore, ObjectRecord, TenantId};
use crowdb_access_s3::object;
use crowdb_access_s3::publication::{publish, PublicationRequest};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, Result,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, ChunkKvResponse, Id128, KeyRange, OperationResult, OwnerDescriptor,
    PartitionArtifact, PointOperation, PointRequest,
};
use crowdb_protocol::chunk_stream::StreamName;

struct Catalog(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>);

#[async_trait]
impl ChunkKvRangeCatalogSource for Catalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        Ok((self.0.clone(), self.1.clone()))
    }
}

#[derive(Default)]
struct Transport(Mutex<Vec<PointOperation>>);

#[async_trait]
impl ChunkKvTransport for Transport {
    async fn point(&self, _: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        let applied = !matches!(request.operation, PointOperation::Delete { .. });
        self.0
            .lock()
            .expect("test transport lock")
            .push(request.operation.clone());
        Ok(ChunkKvResponse {
            map_revision: 1,
            journal_position: None,
            result: Ok(OperationResult::Mutation {
                applied,
                revision: applied.then_some(1),
                observed: None,
            }),
        })
    }

    async fn seek(&self, _: &str, _: &crowdb_protocol::chunk_kv::SeekRequest) -> Result<ChunkKvResponse> {
        unreachable!()
    }
    async fn scan(&self, _: &str, _: &crowdb_protocol::chunk_kv::ScanRequest) -> Result<ChunkKvResponse> {
        unreachable!()
    }
}

#[tokio::test]
async fn missing_object_delete_is_one_idempotent_point_delete() {
    let transport = Arc::new(Transport::default());
    let client = Arc::new(client(transport.clone()));
    client.refresh_catalog().await.expect("catalog");
    let tenant = TenantId::new(b"tenant".to_vec()).expect("tenant");
    object::delete(
        &ChunkKvMetadataStore::new(client),
        &tenant,
        BucketId::new([3; 16]),
        b"missing",
    )
    .await
    .expect("idempotent delete");
    let operations = transport.0.lock().expect("test transport lock");
    assert!(matches!(operations.as_slice(), [PointOperation::Delete { .. }]));
}

fn client(transport: Arc<Transport>) -> ChunkKvClient {
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id: Id128 { high: 1, low: 1 },
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 1,
                rpc_endpoint: "owner".into(),
            },
            owner_epoch: 1,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 1,
                stream_name: StreamName { high: 1, low: 1 },
            },
            transition_id: None,
        }],
        checksum: [0; 32],
    };
    page.seal().expect("catalog page");
    let mut head = ChunkKvRangeCatalogHead {
        generation: 1,
        previous_generation: None,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().expect("catalog head");
    ChunkKvClient::new(
        ClientConfig {
            operation_timeout: Duration::from_secs(1),
            retry_backoff: Duration::from_millis(1),
            ..ClientConfig::default()
        },
        Arc::new(Catalog(head, vec![page])),
        transport,
    )
    .expect("client")
}

#[tokio::test]
async fn direct_publication_uses_only_put_operations() {
    let transport = Arc::new(Transport::default());
    let client = Arc::new(client(transport.clone()));
    client.refresh_catalog().await.expect("catalog");
    let tenant = TenantId::new(b"tenant".to_vec()).expect("tenant");
    let generation = ObjectRecord {
        bucket_id: BucketId::new([1; 16]),
        key: b"key".to_vec(),
        logical_length: 3,
        checksum: b"sum".to_vec(),
        etag: "etag".into(),
        created_at_ms: 1,
        modified_at_ms: 1,
        content_type: "application/octet-stream".into(),
        attributes: Vec::new(),
        data_reference: b"ref".to_vec(),
        data_length: 3,
    };
    publish(
        &ChunkKvMetadataStore::new(client),
        &mut PublicationRequest {
            tenant,
            object: generation,
        },
    )
    .await
    .expect("publish");
    let operations = transport.0.lock().expect("test transport lock");
    assert_eq!(operations.len(), 1);
    assert!(operations
        .iter()
        .all(|operation| matches!(operation, PointOperation::Put { .. })));
}

#[tokio::test]
async fn retry_publication_is_one_overwrite() {
    let transport = Arc::new(Transport::default());
    let client = Arc::new(client(transport.clone()));
    client.refresh_catalog().await.expect("catalog");
    let tenant = TenantId::new(b"tenant".to_vec()).expect("tenant");
    let generation = ObjectRecord {
        bucket_id: BucketId::new([2; 16]),
        key: b"key".to_vec(),
        logical_length: 3,
        checksum: b"sum".to_vec(),
        etag: "etag".into(),
        created_at_ms: 1,
        modified_at_ms: 1,
        content_type: "application/octet-stream".into(),
        attributes: Vec::new(),
        data_reference: b"ref".to_vec(),
        data_length: 3,
    };
    publish(
        &ChunkKvMetadataStore::new(client),
        &mut PublicationRequest {
            tenant,
            object: generation,
        },
    )
    .await
    .expect("publish");
    let operations = transport.0.lock().expect("test transport lock");
    assert!(matches!(operations.as_slice(), [PointOperation::Put { .. }]));
}

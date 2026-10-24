// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/bucket_client.rs"]
mod bucket_client;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use crowdb_access_s3::{
    bucket::head_bucket,
    metadata::{BucketId, BucketNameRecord, ChunkKvMetadataStore, MetadataKey, ObjectRecord, TenantId},
    object,
    publication::{publish_versioned, PublicationRequest},
};
use crowdb_chunk_kv_client::{ChunkKvTransport, Result};
use crowdb_protocol::chunk_kv::*;

#[derive(Default)]
struct TestState {
    operations: Vec<PointOperation>,
    values: HashMap<Vec<u8>, RpcValue>,
    revision: u64,
}

#[derive(Default)]
struct TestTransport(Mutex<TestState>);

#[async_trait]
impl ChunkKvTransport for TestTransport {
    async fn point(&self, _: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        let mut state = self.0.lock().unwrap();
        state.operations.push(request.operation.clone());
        let result = match &request.operation {
            PointOperation::Get { key } => OperationResult::Value(state.values.get(key).cloned()),
            PointOperation::Put { key, value } => {
                state.revision += 1;
                let revision = state.revision;
                state.values.insert(
                    key.clone(),
                    RpcValue {
                        key: key.clone(),
                        value: value.clone(),
                        revision,
                    },
                );
                OperationResult::Mutation {
                    applied: true,
                    revision: Some(revision),
                    observed: None,
                }
            }
            PointOperation::Delete { key } => {
                let applied = state.values.remove(key).is_some();
                state.revision += 1;
                OperationResult::Mutation {
                    applied,
                    revision: applied.then_some(state.revision),
                    observed: None,
                }
            }
            operation => panic!("unexpected extra conditional metadata operation: {operation:?}"),
        };
        Ok(ChunkKvResponse {
            map_revision: 1,
            journal_position: None,
            result: Ok(result),
        })
    }

    async fn seek(&self, _: &str, _: &SeekRequest) -> Result<ChunkKvResponse> {
        panic!("unexpected seek")
    }
    async fn scan(&self, _: &str, _: &ScanRequest) -> Result<ChunkKvResponse> {
        panic!("unexpected scan")
    }
}

async fn setup() -> (ChunkKvMetadataStore, Arc<TestTransport>, TenantId) {
    let transport = Arc::new(TestTransport::default());
    let client = Arc::new(bucket_client::client(transport.clone()));
    client.refresh_catalog().await.unwrap();
    let store = ChunkKvMetadataStore::new(client);
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let mapping = BucketNameRecord {
        tenant: tenant.clone(),
        name: b"bucket".to_vec(),
        bucket_id: BucketId::new([3; 16]),
        tombstone: false,
    };
    store
        .put(
            MetadataKey::bucket_name(&tenant, &mapping.name).unwrap(),
            mapping.encode(),
        )
        .await
        .unwrap();
    transport.0.lock().unwrap().operations.clear();
    (store, transport, tenant)
}

fn record(marker: u8) -> ObjectRecord {
    ObjectRecord {
        bucket_id: BucketId::new([3; 16]),
        key: b"key".to_vec(),
        logical_length: 3,
        checksum: vec![marker; 3],
        etag: marker.to_string(),
        created_at_ms: 1,
        modified_at_ms: 1,
        content_type: "application/octet-stream".into(),
        attributes: Vec::new(),
        data_reference: vec![marker; 3],
        data_length: 3,
    }
}

#[tokio::test]
async fn missing_object_delete_is_one_idempotent_point_delete() {
    let (store, transport, tenant) = setup().await;
    object::delete(&store, &tenant, BucketId::new([3; 16]), b"missing")
        .await
        .unwrap();
    assert!(matches!(
        transport.0.lock().unwrap().operations.as_slice(),
        [PointOperation::Delete { .. }]
    ));
}

#[tokio::test]
async fn direct_publication_returns_its_mutation_revision_with_only_one_put() {
    let (store, transport, tenant) = setup().await;
    let request = PublicationRequest {
        tenant,
        object: record(1),
    };
    let revision = publish_versioned(&store, &request).await.unwrap();
    let state = transport.0.lock().unwrap();
    assert!(matches!(
        state.operations.as_slice(),
        [PointOperation::Put { .. }]
    ));
    let key = MetadataKey::object(&request.tenant, request.object.bucket_id, &request.object.key).unwrap();
    let retained = state.values.get(&key).unwrap();
    assert_eq!(retained.revision, revision);
    assert_eq!(ObjectRecord::decode(&retained.value).unwrap(), request.object);
}

#[tokio::test]
async fn same_content_retry_after_an_intervening_put_is_one_unconditional_overwrite() {
    let (store, transport, tenant) = setup().await;
    let bucket_key = MetadataKey::bucket_name(&tenant, b"bucket").unwrap();
    let object_key = MetadataKey::object(&tenant, BucketId::new([3; 16]), b"key").unwrap();
    let mut revisions = Vec::new();
    for marker in [1, 2, 1] {
        let bucket = head_bucket(&store, &tenant, b"bucket").await.unwrap().unwrap();
        let request = PublicationRequest {
            tenant: tenant.clone(),
            object: record(marker),
        };
        assert_eq!(bucket, request.object.bucket_id);
        revisions.push(publish_versioned(&store, &request).await.unwrap());
    }
    let state = transport.0.lock().unwrap();
    assert_eq!(state.operations.len(), 6);
    for pair in state.operations.chunks_exact(2) {
        assert!(matches!(&pair[0], PointOperation::Get { key } if key == &bucket_key));
        assert!(matches!(&pair[1], PointOperation::Put { key, .. } if key == &object_key));
    }
    assert!(revisions.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(
        ObjectRecord::decode(&state.values.get(&object_key).unwrap().value).unwrap(),
        record(1)
    );
}

#[tokio::test]
async fn object_delete_including_bucket_resolution_is_one_get_and_one_delete_per_request() {
    let (store, transport, tenant) = setup().await;
    publish_versioned(
        &store,
        &PublicationRequest {
            tenant: tenant.clone(),
            object: record(1),
        },
    )
    .await
    .unwrap();
    transport.0.lock().unwrap().operations.clear();
    for _ in 0..2 {
        let bucket = head_bucket(&store, &tenant, b"bucket").await.unwrap().unwrap();
        object::delete(&store, &tenant, bucket, b"key").await.unwrap();
    }
    let state = transport.0.lock().unwrap();
    assert_eq!(state.operations.len(), 4);
    for pair in state.operations.chunks_exact(2) {
        assert!(matches!(
            pair,
            [PointOperation::Get { .. }, PointOperation::Delete { .. }]
        ));
    }
    let object_key = MetadataKey::object(&tenant, BucketId::new([3; 16]), b"key").unwrap();
    assert!(!state.values.contains_key(&object_key));
}

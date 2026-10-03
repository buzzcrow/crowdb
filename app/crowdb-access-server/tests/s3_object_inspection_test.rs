// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#![cfg(feature = "s3")]

use async_trait::async_trait;
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_access_s3::auth::{AuthError, RawAuthRequest, RequestAuthenticator};
use crowdb_access_s3::metadata::{
    BucketId, BucketNameRecord, ChunkKvMetadataStore, MetadataKey, ObjectRecord, TenantId,
};
use crowdb_access_s3::metrics::S3Metrics;
use crowdb_access_s3::route::S3Route;
use crowdb_access_server::s3::{
    serve, ObjectInspector, S3Dispatcher, S3Operations, S3OperationsFuture, OBJECT_LOCATIONS_PATH,
};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, Result,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, ChunkKvResponse, Id128, KeyRange, OperationResult, OwnerDescriptor,
    PartitionArtifact, PointOperation, PointRequest, RpcValue,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;
use hyper::body::Incoming;
use hyper::Request;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct Catalog(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>);
#[async_trait]
impl ChunkKvRangeCatalogSource for Catalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        Ok((self.0.clone(), self.1.clone()))
    }
}
struct TestTransport {
    bucket_key: Vec<u8>,
    bucket: Vec<u8>,
    object_key: Vec<u8>,
    object: Vec<u8>,
    revision: AtomicU64,
    calls: AtomicUsize,
    deleted: AtomicUsize,
}
#[async_trait]
impl ChunkKvTransport for TestTransport {
    async fn point(&self, _: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let PointOperation::Get { key } = &request.operation else {
            panic!("inspection mutated metadata");
        };
        let value = if key == &self.bucket_key {
            Some(self.bucket.clone())
        } else if key == &self.object_key && self.deleted.load(Ordering::Relaxed) == 0 {
            Some(self.object.clone())
        } else {
            None
        };
        Ok(ChunkKvResponse {
            map_revision: 1,
            journal_position: None,
            result: Ok(OperationResult::Value(value.map(|value| RpcValue {
                key: key.clone(),
                value,
                revision: self.revision.load(Ordering::Relaxed),
            }))),
        })
    }
    async fn seek(&self, _: &str, _: &crowdb_protocol::chunk_kv::SeekRequest) -> Result<ChunkKvResponse> {
        panic!("inspection scanned metadata");
    }
    async fn scan(&self, _: &str, _: &crowdb_protocol::chunk_kv::ScanRequest) -> Result<ChunkKvResponse> {
        panic!("inspection scanned metadata");
    }
}
struct NeverPayload;
#[async_trait]
impl RequestAuthenticator for NeverPayload {
    async fn authenticate(&self, _: RawAuthRequest<'_>) -> std::result::Result<(), AuthError> {
        panic!("admin inspection entered general S3 authentication");
    }
}
impl S3Operations for NeverPayload {
    fn execute(
        self: Arc<Self>,
        _: S3Route,
        _: Request<Incoming>,
        _: String,
        _: String,
    ) -> S3OperationsFuture {
        panic!("admin inspection dispatched object payload operations");
    }
}
fn client(transport: Arc<TestTransport>) -> ChunkKvClient {
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
                tail_overlay: None,
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

async fn inspection_fixture() -> (Arc<TestTransport>, S3Dispatcher) {
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let bucket_id = BucketId::new([1; 16]);
    let locations: Vec<_> = (0..23)
        .map(|index| Location {
            chunk_id: Some(ChunkId {
                high: u64::MAX,
                low: index,
            }),
            offset: 9_007_199_254_740_993,
            length: 8,
            logical_offset: index * 8,
            logical_length: 8,
        })
        .collect();
    let object = ObjectRecord {
        bucket_id,
        key: "folder/中文.bin".as_bytes().to_vec(),
        logical_length: 184,
        checksum: vec![1; 16],
        etag: "etag".into(),
        created_at_ms: 1,
        modified_at_ms: 1,
        content_type: "application/octet-stream".into(),
        attributes: vec![],
        data_reference: bincode::serialize(&locations).unwrap(),
        data_length: 184,
    };
    let transport = Arc::new(TestTransport {
        bucket_key: MetadataKey::bucket_name(&tenant, b"bucket").unwrap(),
        bucket: BucketNameRecord {
            tenant: tenant.clone(),
            name: b"bucket".to_vec(),
            bucket_id,
            tombstone: false,
        }
        .encode(),
        object_key: MetadataKey::object(&tenant, bucket_id, &object.key).unwrap(),
        object: object.encode().unwrap(),
        revision: AtomicU64::new(100),
        calls: AtomicUsize::new(0),
        deleted: AtomicUsize::new(0),
    });
    let client = Arc::new(client(transport.clone()));
    client.refresh_catalog().await.unwrap();
    let authentication =
        BearerAuthenticator::new(&"A".repeat(32), &"B".repeat(32), &"C".repeat(32), &"D".repeat(32)).unwrap();
    let inspector = ObjectInspector::new(
        authentication,
        Arc::new(ChunkKvMetadataStore::new(client)),
        tenant,
        vec![42; 32],
    )
    .unwrap();
    let dispatcher = S3Dispatcher::new(
        Arc::new(NeverPayload),
        Arc::new(NeverPayload),
        Arc::new(S3Metrics::default()),
        "host".into(),
        false,
    )
    .with_object_inspector(Some(inspector));
    (transport, dispatcher)
}

#[tokio::test]
async fn privileged_http_inspection_reads_two_metadata_keys_and_rejects_stale_generations() {
    let (transport, dispatcher) = inspection_fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}{}",
        listener.local_addr().unwrap(),
        OBJECT_LOCATIONS_PATH
    );
    let server = tokio::spawn(serve(listener, Arc::new(dispatcher), std::future::pending()));
    let http = reqwest::Client::new();
    let query = [("bucket", "bucket"), ("key", "folder/中文.bin")];
    let response = http.get(&endpoint).query(&query).send().await.unwrap();
    assert_eq!(response.status().as_u16(), 401);
    let response = http
        .get(&endpoint)
        .query(&query)
        .bearer_auth("A".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 403);
    assert_eq!(transport.calls.load(Ordering::Relaxed), 0);
    let response = http
        .get(&endpoint)
        .query(&query)
        .bearer_auth("C".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let first: Value = response.json().await.unwrap();
    assert_eq!(first["locations"].as_array().unwrap().len(), 20);
    assert_eq!(first["locations"][0]["offset"], "9007199254740993");
    assert_eq!(transport.calls.load(Ordering::Relaxed), 2);
    let cursor = first["next_cursor"].as_str().unwrap();
    let response = http
        .get(&endpoint)
        .query(&query)
        .query(&[("cursor", cursor)])
        .bearer_auth("C".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let last: Value = response.json().await.unwrap();
    assert_eq!(last["locations"][0]["index"], "20");
    assert!(last["next_cursor"].is_null());
    transport.revision.store(101, Ordering::Relaxed);
    let response = http
        .get(&endpoint)
        .query(&query)
        .query(&[("cursor", cursor)])
        .bearer_auth("C".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 409);
    transport.deleted.store(1, Ordering::Relaxed);
    let response = http
        .get(&endpoint)
        .query(&query)
        .bearer_auth("C".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 404);
    let response = http
        .get(&endpoint)
        .query(&query)
        .query(&[("limit", "101")])
        .bearer_auth("C".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 400);
    server.abort();
    let _ = server.await;
}

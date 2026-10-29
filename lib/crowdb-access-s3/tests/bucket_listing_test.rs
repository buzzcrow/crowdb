// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/bucket_client.rs"]
mod bucket_client;

use async_trait::async_trait;
use crowdb_access_s3::{
    bucket::{BucketListRequest, BucketListSigner},
    metadata::{BucketId, BucketNameRecord, ChunkKvMetadataStore, MetadataKey, TenantId},
};
use crowdb_chunk_kv_client::{ChunkKvTransport, Result};
use crowdb_protocol::chunk_kv::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct TestTransport {
    items: Vec<RpcValue>,
    scans: AtomicUsize,
}

#[async_trait]
impl ChunkKvTransport for TestTransport {
    async fn point(&self, _: &str, _: &PointRequest) -> Result<ChunkKvResponse> {
        panic!("listing must not perform point reads")
    }

    async fn seek(&self, _: &str, _: &SeekRequest) -> Result<ChunkKvResponse> {
        panic!("listing must use its bounded scan")
    }

    async fn scan(&self, _: &str, request: &ScanRequest) -> Result<ChunkKvResponse> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        let mut items: Vec<_> = self
            .items
            .iter()
            .filter(|value| {
                request.start.as_ref().map_or(true, |start| value.key >= *start)
                    && request.end.as_ref().map_or(true, |end| value.key < *end)
                    && request
                        .continuation
                        .as_ref()
                        .map_or(true, |cursor| value.key > cursor.last_key)
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

async fn store() -> (ChunkKvMetadataStore, Arc<TestTransport>, TenantId) {
    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let items = [b"a0", b"a1", b"a2", b"b1"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            let record = BucketNameRecord {
                tenant: tenant.clone(),
                name: name.to_vec(),
                bucket_id: BucketId::new([u8::try_from(index).unwrap() + 1; 16]),
                tombstone: index == 0,
            };
            RpcValue {
                key: MetadataKey::bucket_name(&tenant, name).unwrap(),
                value: record.encode(),
                revision: 1,
            }
        })
        .collect();
    let transport = Arc::new(TestTransport {
        items,
        scans: AtomicUsize::new(0),
    });
    let client = Arc::new(bucket_client::client(transport.clone()));
    client.refresh_catalog().await.unwrap();
    (ChunkKvMetadataStore::new(client), transport, tenant)
}

fn options(continuation: Option<&str>) -> BucketListRequest<'_> {
    BucketListRequest {
        prefix: b"a",
        continuation,
        max_buckets: 1,
        max_scan_items: 2,
        max_scan_bytes: 4096,
        now: 10,
        ttl: 30,
    }
}

#[tokio::test]
async fn prefix_pages_skip_tombstones_and_resume_without_duplicates_or_false_continuation() {
    let (store, transport, tenant) = store().await;
    let signer = BucketListSigner::new(b"secret".to_vec()).unwrap();
    let first = signer.list(&store, &tenant, &options(None)).await.unwrap();
    assert_eq!(
        first
            .buckets
            .iter()
            .map(|record| record.name.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a1"]
    );
    let second = signer
        .list(&store, &tenant, &options(first.continuation.as_deref()))
        .await
        .unwrap();
    assert_eq!(
        second
            .buckets
            .iter()
            .map(|record| record.name.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a2"]
    );
    assert_eq!(second.continuation, None);
    assert_eq!(transport.scans.load(Ordering::Relaxed), 2);
    let mut empty = options(None);
    empty.prefix = b"unmatched";
    assert!(signer
        .list(&store, &tenant, &empty)
        .await
        .unwrap()
        .buckets
        .is_empty());
}

#[tokio::test]
async fn scope_expiry_tampering_and_invalid_limits_fail_before_metadata_access() {
    let (store, transport, tenant) = store().await;
    let signer = BucketListSigner::new(b"secret".to_vec()).unwrap();
    let first = signer.list(&store, &tenant, &options(None)).await.unwrap();
    let token = first.continuation.unwrap();
    let mut request = options(Some(&token));
    request.prefix = b"b";
    assert!(signer.list(&store, &tenant, &request).await.is_err());
    request = options(Some(&token));
    request.now = 41;
    assert!(signer.list(&store, &tenant, &request).await.is_err());
    let other = TenantId::new(b"other".to_vec()).unwrap();
    assert!(signer.list(&store, &other, &options(Some(&token))).await.is_err());
    let wrong_signer = BucketListSigner::new(b"other secret".to_vec()).unwrap();
    assert!(wrong_signer
        .list(&store, &tenant, &options(Some(&token)))
        .await
        .is_err());
    let corrupt = format!("{token}A");
    assert!(signer
        .list(&store, &tenant, &options(Some(&corrupt)))
        .await
        .is_err());
    request = options(None);
    request.max_buckets = 0;
    assert!(signer.list(&store, &tenant, &request).await.is_err());
    assert_eq!(transport.scans.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn a_window_containing_only_tombstones_still_exposes_its_resume_position() {
    let (store, _, tenant) = store().await;
    let signer = BucketListSigner::new(b"secret".to_vec()).unwrap();
    let mut request = options(None);
    request.max_scan_items = 1;
    let empty = signer.list(&store, &tenant, &request).await.unwrap();
    assert!(empty.buckets.is_empty());
    assert!(empty.continuation.is_some());
    request.continuation = empty.continuation.as_deref();
    let next = signer.list(&store, &tenant, &request).await.unwrap();
    assert_eq!(next.buckets[0].name, b"a1");
}

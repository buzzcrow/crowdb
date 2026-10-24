// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/bucket_client.rs"]
mod bucket_client;

use async_trait::async_trait;
use crowdb_access_s3::{
    bucket::{create_bucket, BucketIdGenerator},
    metadata::{BucketId, BucketNameRecord, ChunkKvMetadataStore, MetadataKey, TenantId},
};
use crowdb_chunk_kv_client::{ChunkKvTransport, Result};
use crowdb_protocol::chunk_kv::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

enum TestReply {
    Conflict(Option<BucketNameRecord>),
    Applied,
    Unknown,
}
struct TestTransport {
    replies: Vec<TestReply>,
    calls: AtomicUsize,
}

#[async_trait]
impl ChunkKvTransport for TestTransport {
    async fn point(&self, _: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        let index = self.calls.fetch_add(1, Ordering::Relaxed);
        let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
        assert_eq!(
            request.operation.key(),
            MetadataKey::bucket_name(&tenant, b"bucket").unwrap()
        );
        let expected = index.checked_sub(1).and_then(|i| match &self.replies[i] {
            TestReply::Conflict(record) => record.as_ref(),
            _ => panic!("operation retried after success or unknown response"),
        });
        let bytes = match (&request.operation, expected) {
            (PointOperation::PutIfAbsent { value, .. }, None) => value,
            (
                PointOperation::CompareExchange {
                    condition: RpcCompareCondition::Value(actual),
                    value,
                    ..
                },
                Some(expected),
            ) => {
                assert_eq!(
                    actual,
                    &expected.encode(),
                    "must retry against the latest returned observation"
                );
                value
            }
            _ => panic!("unexpected extra RPC or stale base: {:?}", request.operation),
        };
        let candidate = BucketNameRecord::decode(bytes).unwrap();
        assert!(!candidate.tombstone);
        assert_eq!(
            candidate.bucket_id,
            BucketId::new([u8::try_from(index + 1).unwrap(); 16])
        );
        let result = match self.replies.get(index).expect("unexpected extra RPC") {
            TestReply::Conflict(record) => OperationResult::Mutation {
                applied: false,
                revision: None,
                observed: record.as_ref().map(|record| RpcValue {
                    key: request.operation.key().to_vec(),
                    value: record.encode(),
                    revision: index as u64 + 1,
                }),
            },
            TestReply::Applied => OperationResult::Mutation {
                applied: true,
                revision: Some(100),
                observed: None,
            },
            TestReply::Unknown => OperationResult::Value(None),
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

#[derive(Default)]
struct TestIds(AtomicUsize);
impl BucketIdGenerator for TestIds {
    fn next_id(&self) -> BucketId {
        BucketId::new([u8::try_from(self.0.fetch_add(1, Ordering::Relaxed) + 1).unwrap(); 16])
    }
}

fn record(id: u8, tombstone: bool) -> BucketNameRecord {
    BucketNameRecord {
        tenant: TenantId::new(b"tenant".to_vec()).unwrap(),
        name: b"bucket".to_vec(),
        bucket_id: BucketId::new([id; 16]),
        tombstone,
    }
}

async fn run(replies: Vec<TestReply>, expected: Option<BucketId>) {
    let count = replies.len();
    let transport = Arc::new(TestTransport {
        replies,
        calls: AtomicUsize::new(0),
    });
    let client = Arc::new(bucket_client::client(transport.clone()));
    client.refresh_catalog().await.unwrap();
    let result = create_bucket(
        &ChunkKvMetadataStore::new(client),
        &TestIds::default(),
        &record(64, true).tenant,
        b"bucket",
    )
    .await;
    match expected {
        Some(id) => assert_eq!(result.unwrap(), id),
        None => assert!(result.is_err()),
    }
    assert_eq!(transport.calls.load(Ordering::Relaxed), count);
}

#[tokio::test]
async fn new_or_existing_bucket_needs_one_conditional_rpc() {
    run(vec![TestReply::Applied], Some(BucketId::new([1; 16]))).await;
    run(
        vec![TestReply::Conflict(Some(record(64, false)))],
        Some(BucketId::new([64; 16])),
    )
    .await;
}

#[tokio::test]
async fn every_tombstone_conflict_reuses_observed_without_get_or_repeated_create() {
    let mut replies: Vec<_> = (64..68)
        .map(|id| TestReply::Conflict(Some(record(id, true))))
        .collect();
    replies.push(TestReply::Applied);
    run(replies, Some(BucketId::new([5; 16]))).await;
}

#[tokio::test]
async fn concurrent_creator_wins_without_an_extra_rpc() {
    run(
        vec![
            TestReply::Conflict(Some(record(64, true))),
            TestReply::Conflict(Some(record(65, false))),
        ],
        Some(BucketId::new([65; 16])),
    )
    .await;
}

#[tokio::test]
async fn missing_mapping_retries_create_and_never_reuses_an_old_bucket_identity() {
    run(
        vec![
            TestReply::Conflict(Some(record(64, true))),
            TestReply::Conflict(None),
            TestReply::Applied,
        ],
        Some(BucketId::new([3; 16])),
    )
    .await;
}

#[tokio::test]
async fn conflict_budget_and_unknown_response_do_not_trigger_confirmation_reads() {
    run(
        (64..72)
            .map(|id| TestReply::Conflict(Some(record(id, true))))
            .collect(),
        None,
    )
    .await;
    run(
        vec![TestReply::Conflict(Some(record(64, true))), TestReply::Unknown],
        None,
    )
    .await;
}

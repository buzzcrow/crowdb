// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/storage.rs"]
mod storage;

use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use crowdb_access_s3::metadata::TenantId;
use crowdb_chunk_client::SmallWritePolicy;
use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_sdk_native::s3::S3Client;
use crowdb_sdk_native::{Error, OperationContext};

fn context(id: u64) -> OperationContext {
    OperationContext::new(
        NonZeroU64::new(id).unwrap(),
        Instant::now() + Duration::from_secs(1),
    )
}

#[tokio::test]
async fn native_namespace_uses_shared_storage_without_a_listener_or_credentials() {
    let runtime = storage::runtime().await;
    let first = S3Client::new(runtime.clone(), TenantId::new(b"first".to_vec()).unwrap()).unwrap();
    let second = S3Client::new(runtime.clone(), TenantId::new(b"second".to_vec()).unwrap()).unwrap();
    let bucket = first.create_bucket(context(1), b"bucket").await.unwrap();
    assert_eq!(first.create_bucket(context(2), b"bucket").await.unwrap(), bucket);
    assert_eq!(first.head_bucket(context(3), b"bucket").await.unwrap(), bucket);
    assert!(matches!(
        second.head_bucket(context(4), b"bucket").await,
        Err(Error::S3(crowdb_access_s3::S3ErrorCode::NoSuchBucket))
    ));
    assert_eq!(first.list_buckets(context(5), 100).await.unwrap().len(), 1);
    drop(first);
    assert!(second.list_buckets(context(6), 100).await.unwrap().is_empty());
    assert_eq!(runtime.resources().status().requests, 0);
    runtime.shutdown().await.unwrap();
    assert!(matches!(
        second.list_buckets(context(7), 100).await,
        Err(Error::Shutdown)
    ));
}

#[tokio::test]
async fn writer_groups_keep_chunk_purposes_separate_and_share_admission() {
    let runtime = storage::runtime().await;
    let same = runtime
        .with_writer_policy(&SmallWritePolicy::new(ChunkType::S3))
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(runtime.resources(), same.resources()));
    assert_eq!(runtime.writer_group_count(), 1);
    let dataset = runtime
        .with_writer_policy(&SmallWritePolicy::new(ChunkType::Dataset))
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(runtime.resources(), dataset.resources()));
    assert_eq!(runtime.writer_group_count(), 2);
    assert!(matches!(
        S3Client::new(dataset, TenantId::new(b"tenant".to_vec()).unwrap()),
        Err(Error::Configuration(_))
    ));
    let view = runtime.topology().unwrap();
    assert_eq!(view.catalog().generation(), 1);
    assert_eq!(view.catalog().route(b"key").unwrap().owner.instance_id, 1);
    assert!(view.disks.is_none());
    assert!(view.data_transport_json.is_none());
    runtime.shutdown().await.unwrap();
    assert_eq!(view.catalog().generation(), 1);
    assert!(matches!(
        same.with_writer_policy(&SmallWritePolicy::new(ChunkType::S3)),
        Err(Error::Shutdown)
    ));
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{metadata, write_storage};
use std::num::NonZeroU64;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_access_s3::metadata::TenantId;
use crowdb_sdk_native::s3::{GetOptions, PutObjectInfo, PutOptions, S3Client, S3ClientConfig, S3WritePolicy};
use crowdb_sdk_native::{ClientRuntime, OperationContext};
use write_storage::TestWriteStorage;

pub fn context(id: u64) -> OperationContext {
    OperationContext::new(
        NonZeroU64::new(id).unwrap(),
        Instant::now() + Duration::from_secs(5),
    )
}

pub async fn fixture(loss: Option<Arc<AtomicBool>>) -> (ClientRuntime, S3Client, Arc<TestWriteStorage>) {
    let metadata = match loss {
        Some(loss) => metadata::client_with_loss(loss).await,
        None => metadata::client().await,
    };
    fixture_with_metadata(metadata).await
}

pub async fn fixture_with_metadata(
    metadata: Arc<crowdb_chunk_kv_client::ChunkKvClient>,
) -> (ClientRuntime, S3Client, Arc<TestWriteStorage>) {
    let backend = TestWriteStorage::new();
    let runtime = backend.runtime(metadata);
    let mut config = S3ClientConfig::from(TenantId::new(b"tenant".to_vec()).unwrap());
    let mut policy = S3WritePolicy::basic(0);
    let large = Arc::make_mut(&mut policy.large_write.client);
    large.large_mirror_copies = Some(1);
    large.max_chunk_size = 2 << 20;
    large.large_parallel_strip_writes = 1;
    config.write_policy = Some(policy);
    let client = S3Client::new(runtime.clone(), config).unwrap();
    client.create_bucket(context(1), b"bucket").await.unwrap();
    (runtime, client, backend)
}

pub fn options(length: usize) -> PutOptions {
    PutOptions {
        object: PutObjectInfo {
            content_length: Some(length as u64),
            ..PutObjectInfo::default()
        },
        ..PutOptions::default()
    }
}

pub async fn read(client: &S3Client, key: &[u8]) -> Vec<u8> {
    let mut get = client
        .get_object(context(900), b"bucket", key, &GetOptions::default())
        .await
        .unwrap();
    let mut output = Vec::new();
    while let Some(lease) = get.body.next().await.unwrap() {
        output.extend_from_slice(lease.as_slice());
    }
    output
}

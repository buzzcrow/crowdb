// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable bucket-name lifecycle on the deliberately slow control path.

use crate::metadata::{
    BucketId, BucketNameRecord, ChunkKvMetadataStore, MetadataKey, MetadataRecordError, MetadataStoreError,
    PutIfAbsentOutcome, TenantId,
};

const LIST_MAX_BYTES: usize = 4 * 1024 * 1024;

pub trait BucketIdGenerator: Send + Sync {
    fn next_id(&self) -> BucketId;
}

pub struct RandomBucketIdGenerator;

impl BucketIdGenerator for RandomBucketIdGenerator {
    fn next_id(&self) -> BucketId {
        BucketId::new(*uuid::Uuid::new_v4().as_bytes())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteBucketResult {
    Deleted,
    Missing,
    NotEmpty,
    Conflict,
}

#[derive(Debug, thiserror::Error)]
pub enum BucketError {
    #[error("bucket metadata key is invalid: {0}")]
    Key(#[from] crate::metadata::MetadataKeyError),
    #[error("bucket metadata is invalid: {0}")]
    Record(#[from] MetadataRecordError),
    #[error("bucket metadata storage failed: {0}")]
    Store(#[from] MetadataStoreError),
}

/// Creates a bucket or returns its already-active immutable ID.
///
/// # Errors
///
/// Returns metadata validation, storage, or repeated conflict failures.
pub async fn create_bucket(
    store: &ChunkKvMetadataStore,
    generator: &dyn BucketIdGenerator,
    tenant: &TenantId,
    name: &[u8],
) -> Result<BucketId, BucketError> {
    let key = MetadataKey::bucket_name(tenant, name)?;
    for _ in 0..8 {
        let record = BucketNameRecord {
            tenant: tenant.clone(),
            name: name.to_vec(),
            bucket_id: generator.next_id(),
            tombstone: false,
        };
        match store.put_if_absent(key.clone(), record.encode()).await? {
            PutIfAbsentOutcome::Inserted { .. } => return Ok(record.bucket_id),
            PutIfAbsentOutcome::Existing(existing) => {
                let observed = BucketNameRecord::decode(&existing.value)?;
                if !observed.tombstone {
                    return Ok(observed.bucket_id);
                }
                if store
                    .compare_exchange(key.clone(), existing.value, record.encode())
                    .await?
                {
                    return Ok(record.bucket_id);
                }
            }
        }
    }
    Err(MetadataStoreError::Result("repeated bucket-name conflict").into())
}

/// Resolves one active bucket mapping with one point read.
///
/// # Errors
///
/// Returns metadata validation or storage failures.
pub async fn head_bucket(
    store: &ChunkKvMetadataStore,
    tenant: &TenantId,
    name: &[u8],
) -> Result<Option<BucketId>, BucketError> {
    let value = store.get(MetadataKey::bucket_name(tenant, name)?).await?;
    value
        .map(|value| BucketNameRecord::decode(&value.value))
        .transpose()
        .map(|record| {
            record
                .filter(|record| !record.tombstone)
                .map(|record| record.bucket_id)
        })
        .map_err(Into::into)
}

/// Lists active bucket mappings in binary name order.
///
/// # Errors
///
/// Returns metadata validation or storage failures.
pub async fn list_buckets(
    store: &ChunkKvMetadataStore,
    tenant: &TenantId,
    max_items: usize,
) -> Result<Vec<BucketNameRecord>, BucketError> {
    let values = store
        .scan(
            MetadataKey::bucket_name_prefix(tenant),
            MetadataKey::bucket_name_end(tenant),
            max_items,
            LIST_MAX_BYTES,
        )
        .await?;
    values
        .into_iter()
        .map(|value| BucketNameRecord::decode(&value.value))
        .filter_map(|record| match record {
            Ok(record) if !record.tombstone => Some(Ok(record)),
            Ok(_) => None,
            Err(error) => Some(Err(error.into())),
        })
        .collect()
}

/// Tombstones an empty bucket using its observed mapping bytes as the fence.
///
/// # Errors
///
/// Returns metadata validation or storage failures.
pub async fn delete_bucket(
    store: &ChunkKvMetadataStore,
    tenant: &TenantId,
    name: &[u8],
) -> Result<DeleteBucketResult, BucketError> {
    let key = MetadataKey::bucket_name(tenant, name)?;
    let Some(value) = store.get(key.clone()).await? else {
        return Ok(DeleteBucketResult::Missing);
    };
    let mut record = BucketNameRecord::decode(&value.value)?;
    if record.tombstone {
        return Ok(DeleteBucketResult::Missing);
    }
    if store.has_visible_object(tenant, record.bucket_id).await? {
        return Ok(DeleteBucketResult::NotEmpty);
    }
    record.tombstone = true;
    if store
        .replace_bucket_mapping(key, value.value, record.encode())
        .await?
    {
        Ok(DeleteBucketResult::Deleted)
    } else {
        Ok(DeleteBucketResult::Conflict)
    }
}

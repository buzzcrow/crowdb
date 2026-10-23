// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Trusted S3 execution shared by HTTP parsing and native business handles.

mod namespace;
mod put;
mod write;
pub use namespace::{DeleteObjectFailure, DeleteObjectResult, DeleteObjectStream, ObjectListRequest};
pub use put::{PreparedPut, PutObjectInfo};
pub use write::{ObjectWriter, S3WritePolicy};

use std::sync::Arc;

use crate::bucket::{self, BucketError, DeleteBucketResult, RandomBucketIdGenerator};
use crate::condition::{ObjectConditions, ReadCondition};
use crate::error::S3ErrorCode;
use crate::metadata::{BucketId, BucketNameRecord, ObjectRecord, TenantId};
use crate::object::{self, ObjectMetadataError, SelectedObject};
use crate::retrieval::{self, ObjectHeaders, PreparedGet, RetrievalError};
use crate::storage::S3StorageClients;

/// Namespace and storage owners only; credential verification belongs to the
/// frontend. Cloned executors retain exactly the same connection groups.
#[derive(Clone)]
pub struct S3Executor {
    pub(crate) storage: S3StorageClients,
    tenant: TenantId,
}

impl S3Executor {
    #[must_use]
    pub fn new(storage: S3StorageClients, tenant: TenantId) -> Self {
        Self { storage, tenant }
    }

    #[must_use]
    pub fn chunks(&self) -> &Arc<crowdb_chunk_client::ChunkIoClient> {
        &self.storage.chunks
    }

    #[must_use]
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// # Errors
    /// Returns namespace validation or routed metadata failures.
    pub async fn create_bucket(&self, name: &[u8]) -> Result<BucketId, S3ErrorCode> {
        bucket::create_bucket(
            &self.storage.metadata,
            &RandomBucketIdGenerator,
            &self.tenant,
            name,
        )
        .await
        .map_err(|error| bucket_error(&error))
    }

    /// # Errors
    /// Returns a missing bucket or routed metadata failure.
    pub async fn resolve_bucket(&self, name: &[u8]) -> Result<BucketId, S3ErrorCode> {
        bucket::head_bucket(&self.storage.metadata, &self.tenant, name)
            .await
            .map_err(|error| bucket_error(&error))?
            .ok_or(S3ErrorCode::NoSuchBucket)
    }

    /// # Errors
    /// Returns namespace validation or bounded metadata scan failure.
    pub async fn list_buckets(&self, limit: usize) -> Result<Vec<BucketNameRecord>, S3ErrorCode> {
        if !(1..=1000).contains(&limit) {
            return Err(S3ErrorCode::InvalidRequest);
        }
        bucket::list_buckets(&self.storage.metadata, &self.tenant, limit)
            .await
            .map_err(|error| bucket_error(&error))
    }

    /// # Errors
    /// Returns missing/nonempty bucket, conflict or metadata failure.
    pub async fn delete_bucket(&self, name: &[u8]) -> Result<(), S3ErrorCode> {
        match bucket::delete_bucket(&self.storage.metadata, &self.tenant, name)
            .await
            .map_err(|error| bucket_error(&error))?
        {
            DeleteBucketResult::Deleted => Ok(()),
            DeleteBucketResult::Missing => Err(S3ErrorCode::NoSuchBucket),
            DeleteBucketResult::NotEmpty => Err(S3ErrorCode::BucketNotEmpty),
            DeleteBucketResult::Conflict => Err(S3ErrorCode::ServiceUnavailable),
        }
    }

    /// Resolves one immutable object generation. Subsequent reads retain it.
    /// # Errors
    /// Returns missing bucket/key, invalid metadata or routed storage failure.
    pub async fn object(&self, bucket: &[u8], key: &[u8]) -> Result<ObjectRecord, S3ErrorCode> {
        Ok(self.selected_object(bucket, key).await?.record)
    }

    /// # Errors
    /// Resolves one record and revision without an additional metadata read.
    pub async fn selected_object(&self, bucket: &[u8], key: &[u8]) -> Result<SelectedObject, S3ErrorCode> {
        let bucket_id = self.resolve_bucket(bucket).await?;
        object::head_versioned(&self.storage.metadata, &self.tenant, bucket_id, key)
            .await
            .map_err(|error| object_error(&error))?
            .ok_or(S3ErrorCode::NoSuchKey)
    }

    /// # Errors
    /// Returns namespace or metadata mutation failure. Physical reclamation is deferred.
    pub async fn delete_object(&self, bucket: &[u8], key: &[u8]) -> Result<(), S3ErrorCode> {
        self.delete_object_typed(bucket, key)
            .await
            .map_err(|error| error.code())
    }

    /// # Errors
    /// Preserves a possibly applied metadata mutation after a lost transport reply.
    pub async fn delete_object_typed(&self, bucket: &[u8], key: &[u8]) -> Result<(), DeleteObjectFailure> {
        let bucket_id = self
            .resolve_bucket(bucket)
            .await
            .map_err(DeleteObjectFailure::Error)?;
        object::delete(&self.storage.metadata, &self.tenant, bucket_id, key)
            .await
            .map_err(|error| namespace::delete_failure(&error))
    }

    /// # Errors
    /// Returns the same conditional outcome for HTTP and native callers.
    pub fn prepare_head(
        &self,
        record: &ObjectRecord,
        conditions: &(impl ReadCondition + ?Sized),
    ) -> Result<ObjectHeaders, RetrievalError> {
        retrieval::prepare_head(record, conditions)
    }

    /// # Errors
    /// Rejects invalid ranges/conditions without replacing the selected generation.
    pub fn prepare_get(
        &self,
        record: &ObjectRecord,
        range: Option<&str>,
        conditions: &ObjectConditions<'_>,
    ) -> Result<PreparedGet, RetrievalError> {
        retrieval::prepare_get(&self.storage.chunks, record, range, conditions)
    }
}

pub(crate) fn bucket_error(error: &BucketError) -> S3ErrorCode {
    match error {
        BucketError::Key(_) => S3ErrorCode::InvalidRequest,
        BucketError::Record(_) => S3ErrorCode::InternalError,
        BucketError::Store(_) => S3ErrorCode::ServiceUnavailable,
    }
}

pub(crate) fn object_error(error: &ObjectMetadataError) -> S3ErrorCode {
    match error {
        ObjectMetadataError::Key(_)
        | ObjectMetadataError::Continuation(_)
        | ObjectMetadataError::InvalidListLimit => S3ErrorCode::InvalidRequest,
        ObjectMetadataError::Record(_) => S3ErrorCode::InternalError,
        ObjectMetadataError::Store(_) => S3ErrorCode::ServiceUnavailable,
    }
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! S3 business handles over the shared trusted runtime, without HTTP authentication.

use crowdb_access_s3::execution::S3Executor;
use crowdb_access_s3::metadata::{BucketId, BucketNameRecord, ChunkKvMetadataStore};
use crowdb_access_s3::storage::S3StorageClients;
use crowdb_protocol::chunkdb::rpc::ChunkType;

use crate::{ClientRuntime, Error, OperationContext, Result};
use std::sync::Arc;

mod body;
mod config;
mod locality;
mod namespace;
mod object;
mod read;
mod write;
pub use crowdb_access_s3::execution::{DeleteObjectFailure, DeleteObjectResult};
pub use namespace::{DeleteObjects, ListObject, ListObjects, ListOptions};

pub use body::ObjectBody;
pub use config::S3ClientConfig;
pub use crowdb_access_s3::condition::{EtagSelection, ReadPreconditions};
pub use crowdb_access_s3::execution::{PutObjectInfo, S3WritePolicy};
pub use crowdb_access_s3::metadata::UserMetadata;
pub use crowdb_access_s3::range::ByteRange;
pub use locality::{
    ChunkLayoutObservation, FragmentPlacement, FragmentRole, LocalityOptions, ObjectExtent, ObjectLocality,
    StripPlacement,
};
pub use object::{ObjectMetadata, ResolvedObject};
pub use read::{GetObject, GetOptions};
pub use write::{PutObject, PutOptions};

#[derive(Clone)]
pub struct S3Client {
    runtime: ClientRuntime,
    execution: S3Executor,
    signer: Arc<crowdb_access_s3::continuation::ContinuationTokenSigner>,
    write_policy: S3WritePolicy,
}

impl S3Client {
    /// Creates a tenant handle using the runtime's existing storage owners.
    /// # Errors
    /// Rejects closed runtime or a writer group for another concrete chunk type.
    pub fn new(runtime: ClientRuntime, config: impl Into<S3ClientConfig>) -> Result<Self> {
        let config = config.into();
        if config.continuation_key.as_ref().is_some_and(Vec::is_empty) {
            return Err(Error::Configuration(
                "S3 continuation key must be nonempty".into(),
            ));
        }
        if config
            .small_write
            .as_ref()
            .is_some_and(|policy| policy.chunk_type != ChunkType::S3)
        {
            return Err(Error::Configuration("S3 requires an S3 writer policy".into()));
        }
        let runtime = match &config.small_write {
            Some(policy) => runtime.with_writer_policy(policy)?,
            None => runtime,
        };
        runtime.resources.check_open()?;
        if runtime.storage.chunk_type != ChunkType::S3 {
            return Err(Error::Configuration("S3 requires an S3 writer group".into()));
        }
        let write_policy = config.write_policy.unwrap_or_else(|| {
            S3WritePolicy::basic(
                runtime
                    .storage
                    .policy
                    .object_limit
                    .min(16 * crowdb_protocol::frame::MAX_FRAME_PAYLOAD_BYTES),
            )
        });
        if write_policy.small_object_limit > runtime.storage.policy.object_limit
            || write_policy.large_write.client.chunk_type != ChunkType::S3
        {
            return Err(Error::Configuration(
                "S3 write policy exceeds its writer group".into(),
            ));
        }
        write_policy
            .large_write
            .client
            .validate()
            .map_err(|error| Error::Configuration(error.to_string()))?;
        let storage = S3StorageClients {
            control: Arc::clone(&runtime.storage.control),
            metadata: Arc::new(ChunkKvMetadataStore::new(Arc::clone(&runtime.storage.metadata))),
            chunks: Arc::clone(&runtime.storage.chunks),
        };
        let signer = crowdb_access_s3::continuation::ContinuationTokenSigner::new(
            config
                .continuation_key
                .unwrap_or_else(|| runtime.resources.continuation_key.clone()),
        )
        .map_err(|_| Error::Configuration("S3 continuation key must be nonempty".into()))?;
        Ok(Self {
            execution: S3Executor::new(storage, config.tenant),
            signer: Arc::new(signer),
            runtime,
            write_policy,
        })
    }

    /// # Errors
    /// Returns admission, deadline, cancellation or S3 namespace/storage failures.
    pub async fn create_bucket(&self, context: OperationContext, name: &[u8]) -> Result<BucketId> {
        let request = self.runtime.resources.admit(context)?;
        request
            .run_mutation(async { self.execution.create_bucket(name).await.map_err(Error::S3) })
            .await
    }

    /// # Errors
    /// Returns admission, deadline, cancellation or a missing bucket.
    pub async fn head_bucket(&self, context: OperationContext, name: &[u8]) -> Result<BucketId> {
        let request = self.runtime.resources.admit(context)?;
        request
            .run(async { self.execution.resolve_bucket(name).await.map_err(Error::S3) })
            .await
    }

    /// # Errors
    /// Returns admission or bounded metadata scan failure.
    pub async fn list_buckets(
        &self,
        context: OperationContext,
        limit: usize,
    ) -> Result<Vec<BucketNameRecord>> {
        let request = self.runtime.resources.admit(context)?;
        request
            .run(async { self.execution.list_buckets(limit).await.map_err(Error::S3) })
            .await
    }

    /// # Errors
    /// Returns nonempty/missing bucket or a storage/admission failure.
    pub async fn delete_bucket(&self, context: OperationContext, name: &[u8]) -> Result<()> {
        let request = self.runtime.resources.admit(context)?;
        request
            .run_mutation(async { self.execution.delete_bucket(name).await.map_err(Error::S3) })
            .await
    }

    /// Captures one published object identity for inspection or a later read.
    /// # Errors
    /// Returns admission or missing/corrupt object metadata.
    pub async fn resolve_object(
        &self,
        context: OperationContext,
        bucket: &[u8],
        key: &[u8],
    ) -> Result<ResolvedObject> {
        let request = self.runtime.resources.admit(context)?;
        request.run(self.select_object(bucket, key)).await
    }

    /// # Errors
    /// Returns namespace, admission or routed metadata mutation failure.
    pub async fn delete_object(&self, context: OperationContext, bucket: &[u8], key: &[u8]) -> Result<()> {
        let id = context.request_id.get();
        let request = self.runtime.resources.admit(context)?;
        request
            .run_mutation(async {
                self.execution
                    .delete_object_typed(bucket, key)
                    .await
                    .map_err(|error| match error {
                        DeleteObjectFailure::Error(code) => Error::S3(code),
                        DeleteObjectFailure::Uncertain => Error::UncertainMutation {
                            request_id: id,
                            reason: crate::error::MutationInterruption::Transport,
                        },
                    })
            })
            .await
    }
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Trusted Dataset access over the shared native runtime and existing authority.

mod composition;
mod control;
mod error;
mod query;
mod read;
mod upload;

pub use composition::{CompositionCleanup, CompositionStatus};
pub use control::DatasetControl;
pub use error::FailureKind;
pub use read::{BatchBody, BatchField, BatchHeader, BatchRequest, FieldBuffer, FieldState, ReadBatch};

use crate::{ClientRuntime, Result};
use crowdb_access_dataset::{
    ChunkKvDatasetStore, ChunkPayloadStorage, DatasetAuthority, DatasetIdentity, DatasetUploads,
    UploadedChunkReader,
};
use crowdb_chunk_client::SmallWritePolicy;
use crowdb_protocol::chunkdb::rpc::ChunkType;
use std::sync::Arc;

#[derive(Clone)]
pub struct DatasetClient {
    runtime: ClientRuntime,
    authority: Arc<DatasetAuthority>,
    uploads: Arc<DatasetUploads>,
    identity: DatasetIdentity,
}

impl DatasetClient {
    /// Attaches a Dataset writer group without another runtime or transport.
    /// # Errors
    /// Returns invalid identity/policy, closed runtime or writer group capacity.
    pub fn new(runtime: &ClientRuntime, identity: DatasetIdentity) -> Result<Self> {
        identity.key()?;
        let mut policy = SmallWritePolicy::new(ChunkType::Dataset);
        policy.object_limit = crowdb_access_dataset::MAX_UPLOAD_PART_BYTES;
        policy.max_batch_bytes = policy.object_limit;
        policy.max_pipelines = 8;
        policy.memory_budget = 128 * 1024 * 1024;
        Self::with_writer_policy(runtime, identity, &policy)
    }

    /// Attaches an explicitly tuned Dataset writer group to the shared runtime.
    /// # Errors
    /// Rejects other chunk purposes, undersized part limits or invalid policy.
    pub fn with_writer_policy(
        runtime: &ClientRuntime,
        identity: DatasetIdentity,
        policy: &SmallWritePolicy,
    ) -> Result<Self> {
        identity.key()?;
        if policy.chunk_type != ChunkType::Dataset
            || policy.object_limit < crowdb_access_dataset::MAX_UPLOAD_PART_BYTES
        {
            return Err(crate::Error::Configuration(
                "Dataset writer must admit complete upload parts".into(),
            ));
        }
        let runtime = runtime.with_writer_policy(policy)?;
        let store = Arc::new(ChunkKvDatasetStore::new(Arc::clone(&runtime.storage.metadata)));
        let uploads = Arc::new(DatasetUploads::new(
            store.clone(),
            Arc::new(ChunkPayloadStorage::new((*runtime.storage.chunks).clone())),
        ));
        let authority = Arc::new(DatasetAuthority::with_chunk_reader(
            store,
            Arc::new(UploadedChunkReader::new((*uploads).clone())),
        ));
        Ok(Self {
            runtime,
            authority,
            uploads,
            identity,
        })
    }

    #[must_use]
    pub fn identity(&self) -> &DatasetIdentity {
        &self.identity
    }

    #[must_use]
    pub fn runtime(&self) -> &ClientRuntime {
        &self.runtime
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn from_services_for_tests(
        runtime: ClientRuntime,
        identity: DatasetIdentity,
        authority: Arc<DatasetAuthority>,
        uploads: Arc<DatasetUploads>,
    ) -> Self {
        Self {
            runtime,
            authority,
            uploads,
            identity,
        }
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| time.as_secs())
}

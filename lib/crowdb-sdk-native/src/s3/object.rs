// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_access_s3::metadata::{BucketId, ObjectRecord, TenantId, UserMetadata};
use crowdb_access_s3::S3ErrorCode;
use crowdb_chunk_kv_client::ChunkKvClient;

use super::S3Client;
use crate::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    pub bucket_id: BucketId,
    pub key: Vec<u8>,
    pub revision: u64,
    pub logical_length: u64,
    pub etag: String,
    pub checksum: Vec<u8>,
    pub created_at_ms: u64,
    pub modified_at_ms: u64,
    pub content_type: String,
    pub user_metadata: UserMetadata,
}

/// An immutable metadata selection bound to its tenant and storage authority.
/// Execution does not look the mutable object name up again.
#[derive(Clone)]
pub struct ResolvedObject {
    pub(super) record: Arc<ObjectRecord>,
    metadata: ObjectMetadata,
    tenant: TenantId,
    authority: Arc<ChunkKvClient>,
}

impl ResolvedObject {
    #[must_use]
    pub fn metadata(&self) -> &ObjectMetadata {
        &self.metadata
    }
}

impl S3Client {
    pub(super) async fn select_object(&self, bucket: &[u8], key: &[u8]) -> Result<ResolvedObject> {
        let selected = self
            .execution
            .selected_object(bucket, key)
            .await
            .map_err(Error::S3)?;
        self.bind_object(selected)
    }

    pub(super) fn bind_object(
        &self,
        selected: crowdb_access_s3::object::SelectedObject,
    ) -> Result<ResolvedObject> {
        let record = selected.record;
        let user_metadata =
            UserMetadata::decode(&record.attributes).map_err(|_| Error::S3(S3ErrorCode::InternalError))?;
        let metadata = ObjectMetadata {
            bucket_id: record.bucket_id,
            key: record.key.clone(),
            revision: selected.revision,
            logical_length: record.logical_length,
            etag: record.etag.clone(),
            checksum: record.checksum.clone(),
            created_at_ms: record.created_at_ms,
            modified_at_ms: record.modified_at_ms,
            content_type: record.content_type.clone(),
            user_metadata,
        };
        Ok(ResolvedObject {
            record: Arc::new(record),
            metadata,
            tenant: self.execution.tenant().clone(),
            authority: Arc::clone(&self.runtime.storage.metadata),
        })
    }

    pub(super) fn validate_selection(&self, selected: &ResolvedObject) -> Result<()> {
        if selected.tenant != *self.execution.tenant()
            || !Arc::ptr_eq(&selected.authority, &self.runtime.storage.metadata)
        {
            return Err(Error::Configuration(
                "object selection belongs to another tenant or storage authority".into(),
            ));
        }
        Ok(())
    }
}

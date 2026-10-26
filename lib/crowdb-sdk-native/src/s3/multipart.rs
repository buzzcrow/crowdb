// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Native multipart handles remain bound to their tenant and metadata authority.

mod listing;
mod part;
pub use listing::{ListUploads, PartPage, UploadsPage};
pub use part::{PartMetadata, UploadPart};

use super::{CompletionPart, CopyMetadata, S3Client};
use crate::{Error, OperationContext, Result};
use crowdb_access_s3::execution::MultipartFailure;
use crowdb_access_s3::metadata::{MultipartSessionRecord, TenantId};
use crowdb_chunk_kv_client::ChunkKvClient;
use std::sync::Arc;

#[derive(Clone)]
pub struct MultipartUpload {
    session: MultipartSessionRecord,
    tenant: TenantId,
    authority: Arc<ChunkKvClient>,
}

impl MultipartUpload {
    #[must_use]
    pub fn upload_id(&self) -> [u8; 16] {
        self.session.upload_id
    }
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.session.object_key
    }
    #[must_use]
    pub fn created_at_ms(&self) -> u64 {
        self.session.created_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedMultipart {
    pub upload_id: [u8; 16],
    pub key: Vec<u8>,
    pub etag: String,
    pub logical_length: u64,
}

impl S3Client {
    /// # Errors
    /// Returns namespace, metadata validation or uncertain session creation.
    pub async fn begin_multipart(
        &self,
        context: OperationContext,
        bucket: &[u8],
        key: &[u8],
        metadata: CopyMetadata,
    ) -> Result<MultipartUpload> {
        let id = context.request_id.get();
        let request = self.runtime.resources.admit(context)?;
        let session = request
            .run_mutation(async {
                self.execution
                    .begin_multipart(bucket, key, metadata)
                    .await
                    .map_err(|error| map_error(error, id))
            })
            .await?;
        Ok(self.bind_upload(session))
    }

    /// Reattaches an upload after reconnecting without trusting caller state.
    /// # Errors
    /// Returns missing/expired upload or unavailable metadata.
    pub async fn resolve_multipart(
        &self,
        context: OperationContext,
        bucket: &[u8],
        key: &[u8],
        upload_id: &[u8; 16],
    ) -> Result<MultipartUpload> {
        let id = context.request_id.get();
        let request = self.runtime.resources.admit(context)?;
        let session = request
            .run(async {
                self.execution
                    .multipart_session(bucket, key, upload_id, false)
                    .await
                    .map_err(|error| map_error(error, id))
            })
            .await?;
        Ok(self.bind_upload(session))
    }

    /// Binds exact part generations and publishes one unconditional overwrite.
    /// # Errors
    /// Returns part validation/conflict or uncertain completion; a retry with the
    /// same selection repeats the metadata overwrite without replaying payloads.
    pub async fn complete_multipart(
        &self,
        context: OperationContext,
        upload: &MultipartUpload,
        parts: &[CompletionPart],
    ) -> Result<CompletedMultipart> {
        self.validate_upload(upload)?;
        let id = context.request_id.get();
        let request = self.runtime.resources.admit(context)?;
        let session = request
            .run_mutation(async {
                Box::pin(self.execution.complete_multipart(&upload.session, parts))
                    .await
                    .map_err(|error| map_error(error, id))
            })
            .await?;
        Ok(CompletedMultipart {
            upload_id: session.upload_id,
            key: session.object_key,
            logical_length: session.logical_length,
            etag: session.etag,
        })
    }

    /// # Errors
    /// Returns foreign identity, closed/conflicting upload or uncertain abort.
    pub async fn abort_multipart(&self, context: OperationContext, upload: &MultipartUpload) -> Result<()> {
        self.validate_upload(upload)?;
        let id = context.request_id.get();
        let request = self.runtime.resources.admit(context)?;
        request
            .run_mutation(async {
                self.execution
                    .abort_multipart(&upload.session)
                    .await
                    .map_err(|error| map_error(error, id))
            })
            .await
    }

    fn bind_upload(&self, session: MultipartSessionRecord) -> MultipartUpload {
        MultipartUpload {
            session,
            tenant: self.execution.tenant().clone(),
            authority: self.runtime.storage.metadata.clone(),
        }
    }

    fn validate_upload(&self, upload: &MultipartUpload) -> Result<()> {
        if upload.tenant != *self.execution.tenant()
            || !Arc::ptr_eq(&upload.authority, &self.runtime.storage.metadata)
        {
            return Err(Error::Configuration(
                "upload belongs to another tenant or storage authority".into(),
            ));
        }
        Ok(())
    }
}

pub(super) fn map_error(error: MultipartFailure, request_id: u64) -> Error {
    match error {
        MultipartFailure::S3(code) => Error::S3(code),
        MultipartFailure::Uncertain => Error::UncertainMutation {
            request_id,
            reason: crate::error::MutationInterruption::Transport,
        },
    }
}

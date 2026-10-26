// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::pin::Pin;

use super::{object_error, S3Executor};
use crate::continuation::ContinuationTokenSigner;
use crate::delete::DeleteSelection;
use crate::error::S3ErrorCode;
use crate::metadata::{BucketId, MetadataStoreError};
use crate::object::{self, ListObjectsV2Page, ListObjectsV2Request, ObjectMetadataError};

pub struct ObjectListRequest<'a> {
    pub bucket: BucketId,
    pub prefix: &'a [u8],
    pub delimiter: Option<&'a [u8]>,
    pub start_after: Option<&'a [u8]>,
    pub continuation_token: Option<&'a str>,
    pub max_keys: usize,
    pub max_scan_items: usize,
    pub max_scan_bytes: usize,
    pub now_unix_seconds: u64,
    pub token_ttl_seconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteObjectFailure {
    Error(S3ErrorCode),
    Uncertain,
}

impl DeleteObjectFailure {
    #[must_use]
    pub fn code(&self) -> S3ErrorCode {
        match self {
            Self::Error(code) => *code,
            Self::Uncertain => S3ErrorCode::ServiceUnavailable,
        }
    }
}

pub type DeleteObjectResult = (Vec<u8>, Result<(), DeleteObjectFailure>);
pub type DeleteObjectStream = Pin<Box<dyn futures::Stream<Item = DeleteObjectResult> + Send>>;

pub(super) fn delete_failure(error: &ObjectMetadataError) -> DeleteObjectFailure {
    if matches!(
        error,
        ObjectMetadataError::Store(MetadataStoreError::Client(
            crowdb_chunk_kv_client::ClientError::Deadline | crowdb_chunk_kv_client::ClientError::Transport(_)
        ))
    ) {
        DeleteObjectFailure::Uncertain
    } else {
        DeleteObjectFailure::Error(object_error(error))
    }
}

impl S3Executor {
    /// # Errors
    /// Returns bounded scan, continuation-scope or metadata validation failures.
    pub async fn list_objects(
        &self,
        signer: &ContinuationTokenSigner,
        request: &ObjectListRequest<'_>,
    ) -> Result<ListObjectsV2Page, S3ErrorCode> {
        object::list_v2(
            &self.storage.metadata,
            signer,
            &ListObjectsV2Request {
                tenant: &self.tenant,
                bucket: request.bucket,
                prefix: request.prefix,
                delimiter: request.delimiter,
                start_after: request.start_after,
                continuation_token: request.continuation_token,
                max_keys: request.max_keys,
                max_scan_items: request.max_scan_items,
                max_scan_bytes: request.max_scan_bytes,
                now_unix_seconds: request.now_unix_seconds,
                token_ttl_seconds: request.token_ttl_seconds,
            },
        )
        .await
        .map_err(|error| object_error(&error))
    }

    /// Validates the complete selection before any mutation, then yields one
    /// independently completed deletion at a time in input order.
    /// # Errors
    /// Rejects invalid selection bounds before polling any deletion.
    pub fn delete_objects(
        &self,
        bucket: BucketId,
        selection: &DeleteSelection,
    ) -> Result<DeleteObjectStream, S3ErrorCode> {
        let metadata = self.storage.metadata.clone();
        let tenant = self.tenant.clone();
        let stream = selection.stream_typed(move |key| {
            let metadata = metadata.clone();
            let tenant = tenant.clone();
            async move {
                object::delete(&metadata, &tenant, bucket, &key)
                    .await
                    .map_err(|error| delete_failure(&error))
            }
        })?;
        Ok(Box::pin(stream))
    }
}

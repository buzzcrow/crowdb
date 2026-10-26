// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_access_s3::delete::DeleteSelection;
use crowdb_access_s3::execution::{DeleteObjectResult, DeleteObjectStream, ObjectListRequest};
use crowdb_access_s3::S3ErrorCode;
use futures::StreamExt;

use super::S3Client;
use crate::error::MutationInterruption;
use crate::runtime::RequestLease;
use crate::{Error, OperationContext, Result};

#[derive(Clone, Debug)]
pub struct ListOptions {
    pub prefix: Vec<u8>,
    pub delimiter: Option<Vec<u8>>,
    pub start_after: Option<Vec<u8>>,
    pub continuation: Option<String>,
    pub limit: usize,
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            prefix: Vec::new(),
            delimiter: None,
            start_after: None,
            continuation: None,
            limit: 1000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListObject {
    pub key: Vec<u8>,
    pub logical_length: u64,
    pub etag: String,
    pub modified_at_ms: u64,
}

pub struct ListObjects {
    pub objects: Vec<ListObject>,
    pub common_prefixes: Vec<Vec<u8>>,
    pub next_continuation: Option<String>,
}

impl S3Client {
    /// Reads one bounded, non-snapshot page with scoped continuation.
    /// # Errors
    /// Returns namespace, admission, deadline or invalid continuation failures.
    pub async fn list_objects(
        &self,
        context: OperationContext,
        bucket: &[u8],
        options: &ListOptions,
    ) -> Result<ListObjects> {
        let request = self.runtime.resources.admit(context)?;
        request
            .run(async {
                if options.limit > 1000 {
                    return Err(Error::S3(S3ErrorCode::InvalidRequest));
                }
                let bucket = self.execution.resolve_bucket(bucket).await.map_err(Error::S3)?;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|error| Error::Configuration(error.to_string()))?
                    .as_secs();
                let page = self
                    .execution
                    .list_objects(
                        &self.signer,
                        &ObjectListRequest {
                            bucket,
                            prefix: &options.prefix,
                            delimiter: options.delimiter.as_deref(),
                            start_after: options.start_after.as_deref(),
                            continuation_token: options.continuation.as_deref(),
                            max_keys: options.limit,
                            max_scan_items: 1001,
                            max_scan_bytes: 4 << 20,
                            now_unix_seconds: now,
                            token_ttl_seconds: 900,
                        },
                    )
                    .await
                    .map_err(Error::S3)?;
                Ok(ListObjects {
                    objects: page
                        .objects
                        .into_iter()
                        .map(|record| ListObject {
                            key: record.key,
                            logical_length: record.logical_length,
                            etag: record.etag,
                            modified_at_ms: record.modified_at_ms,
                        })
                        .collect(),
                    common_prefixes: page.common_prefixes,
                    next_continuation: page.next_continuation_token,
                })
            })
            .await
    }

    /// Validates every key before starting ordered independent deletions.
    /// Dropping the returned body stops the remaining selection.
    /// # Errors
    /// Returns invalid selection, namespace or operation admission failures.
    pub async fn delete_objects(
        &self,
        context: OperationContext,
        bucket: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> Result<DeleteObjects> {
        let request = self.runtime.resources.admit(context)?;
        let bucket = request
            .run(async { self.execution.resolve_bucket(bucket).await.map_err(Error::S3) })
            .await?;
        let selection = DeleteSelection { keys, quiet: false };
        let stream = self
            .execution
            .delete_objects(bucket, &selection)
            .map_err(Error::S3)?;
        Ok(DeleteObjects {
            stream: Some(stream),
            request: Some(request),
            keys: selection.keys,
            cursor: 0,
            pending: false,
            failure: None,
        })
    }
}

pub struct DeleteObjects {
    stream: Option<DeleteObjectStream>,
    request: Option<RequestLease>,
    keys: Vec<Vec<u8>>,
    cursor: usize,
    pending: bool,
    failure: Option<Error>,
}

impl DeleteObjects {
    #[must_use]
    pub fn pending_key(&self) -> Option<&[u8]> {
        self.pending
            .then(|| self.keys.get(self.cursor))
            .flatten()
            .map(Vec::as_slice)
    }

    /// Produces one independent result. Cancellation while a mutation is pending
    /// is reported as uncertain; completed results are never replayed or rolled back.
    /// # Errors
    /// Returns sticky operation cancellation/deadline/uncertainty or stream failure.
    pub async fn next(&mut self) -> Result<Option<DeleteObjectResult>> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.cursor == self.keys.len() {
            return Ok(None);
        }
        let request = self.request.as_ref().ok_or(Error::Cancelled)?;
        let stream = self.stream.as_mut().ok_or(Error::Cancelled)?;
        let already_pending = self.pending;
        self.pending = true;
        let result = request.run_mutation(async { Ok(stream.next().await) }).await;
        match result {
            Ok(Some(result)) => {
                self.cursor += 1;
                self.pending = false;
                if self.cursor == self.keys.len() {
                    self.stream = None;
                    self.request = None;
                }
                Ok(Some(result))
            }
            result => {
                let mut error = result.err().unwrap_or(Error::S3(S3ErrorCode::InternalError));
                if already_pending {
                    let reason = match &error {
                        Error::Cancelled => Some(MutationInterruption::Cancelled),
                        Error::Deadline => Some(MutationInterruption::Deadline),
                        Error::Shutdown => Some(MutationInterruption::Shutdown),
                        _ => None,
                    };
                    if let Some(reason) = reason {
                        error = Error::UncertainMutation {
                            request_id: request.context().request_id.get(),
                            reason,
                        };
                    }
                }
                self.failure = Some(error.clone());
                if matches!(error, Error::Cancelled | Error::Deadline | Error::Shutdown) {
                    self.pending = false;
                }
                self.stream = None;
                self.request = None;
                Err(error)
            }
        }
    }
}

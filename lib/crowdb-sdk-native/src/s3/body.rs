// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_access_s3::retrieval::{GetStreamError, VerifiedGetStream};
use crowdb_chunk_client::ReadError;
use crowdb_rpc_ffi::OwnedPayload;

use super::ByteRange;
use crate::buffer::BufferLease;
use crate::error::ReadFailureKind;
use crate::runtime::RequestLease;
use crate::{Error, Result, RuntimeResources};

/// Pull-based logical object delivery. Dropping a pending pull retains its
/// pending payload for a subsequent pull. Dropping the body cancels production.
pub struct ObjectBody {
    stream: Option<VerifiedGetStream>,
    pending: Option<OwnedPayload>,
    request: Option<RequestLease>,
    resources: Arc<RuntimeResources>,
    range: ByteRange,
    offset: u64,
    sequence: u64,
    complete: bool,
    failure: Option<Error>,
}

impl ObjectBody {
    pub(super) fn new(
        stream: Option<VerifiedGetStream>,
        request: RequestLease,
        resources: Arc<RuntimeResources>,
        range: ByteRange,
    ) -> Self {
        Self {
            stream,
            pending: None,
            request: Some(request),
            resources,
            range,
            offset: range.start,
            sequence: 0,
            complete: false,
            failure: None,
        }
    }

    /// Successful EOF means all applicable length and integrity checks passed.
    /// Failures remain visible on every later pull.
    /// # Errors
    /// Returns cancellation, deadline, capacity or typed verified-read failures.
    pub async fn next(&mut self) -> Result<Option<BufferLease>> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.complete {
            return Ok(None);
        }
        match self.pull().await {
            Ok(Some(lease)) => Ok(Some(lease)),
            Ok(None) => {
                self.complete = true;
                self.stream = None;
                self.request = None;
                Ok(None)
            }
            Err(error) => {
                self.fail(error.clone());
                Err(error)
            }
        }
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    pub fn cancel(&mut self) {
        if !self.complete && self.failure.is_none() {
            self.fail(Error::Cancelled);
        }
    }

    fn fail(&mut self, error: Error) {
        self.stream = None;
        self.pending = None;
        self.request = None;
        self.failure = Some(error);
    }

    async fn pull(&mut self) -> Result<Option<BufferLease>> {
        let request = self.request.as_ref().ok_or(Error::Cancelled)?;
        if self.pending.is_none() {
            let stream = self
                .stream
                .as_mut()
                .ok_or(Error::S3(crowdb_access_s3::S3ErrorCode::InternalError))?;
            self.pending = request
                .run(async { stream.next_payload().await.map_err(read_error) })
                .await?;
            if self.pending.is_none() {
                if self.offset != self.range.end {
                    return Err(Error::Length {
                        expected: self.range.end - self.range.start,
                        delivered: self.offset - self.range.start,
                    });
                }
                return Ok(None);
            }
        }
        let payload = self.pending.as_ref().ok_or(Error::BufferRange)?.clone();
        let id = request.context().request_id.get();
        let lease = request
            .run(
                self.resources
                    .retain_payload(payload, id, id, self.sequence, self.offset),
            )
            .await?;
        self.pending = None;
        self.offset = self
            .offset
            .checked_add(lease.metadata().length as u64)
            .ok_or(Error::BufferRange)?;
        self.sequence = self.sequence.checked_add(1).ok_or(Error::BufferRange)?;
        Ok(Some(lease))
    }
}

fn read_error(error: GetStreamError) -> Error {
    match error {
        GetStreamError::Integrity => Error::Integrity,
        GetStreamError::Length { expected, delivered } => Error::Length { expected, delivered },
        GetStreamError::Chunk(error) => {
            let kind = match &*error {
                ReadError::InvalidLocations(_) => ReadFailureKind::InvalidLocations,
                ReadError::InvalidRange { .. } => ReadFailureKind::InvalidRange,
                ReadError::ChunkDeleted(_) => ReadFailureKind::Deleted,
                ReadError::NotYetAvailable(_) => ReadFailureKind::NotAvailable,
                ReadError::LayoutExpired => ReadFailureKind::LayoutExpired,
                ReadError::DataLoss(_) => ReadFailureKind::DataLoss,
                ReadError::Metadata(_) => ReadFailureKind::Metadata,
                ReadError::DiskIo(_) => ReadFailureKind::DiskIo,
                ReadError::EcDecode(_) => ReadFailureKind::EcDecode,
                ReadError::FailedRange { .. } => ReadFailureKind::FailedRange,
            };
            Error::Read {
                kind,
                detail: error.to_string(),
            }
        }
    }
}

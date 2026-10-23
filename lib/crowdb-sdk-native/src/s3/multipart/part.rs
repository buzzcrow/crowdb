// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{map_error, MultipartUpload};
use crate::buffer::{BufferLease, FrozenObjectBuffer};
use crate::s3::{ByteRange, PutObject, ReadPreconditions, ResolvedObject, S3Client};
use crate::{Error, OperationContext, Result};
use crowdb_access_s3::condition::{ConditionOutcome, ReadCondition};
use crowdb_access_s3::metadata::MultipartPartRecord;
use crowdb_access_s3::S3ErrorCode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartMetadata {
    pub number: u16,
    pub generation: u64,
    pub length: u64,
    pub etag: String,
    pub checksum: [u8; 16],
    pub modified_at_ms: u64,
}

impl From<MultipartPartRecord> for PartMetadata {
    fn from(part: MultipartPartRecord) -> Self {
        Self {
            number: part.number,
            generation: part.revision,
            length: part.length,
            etag: crowdb_access_s3::integrity::raw_md5_etag(part.raw_md5),
            checksum: part.raw_md5,
            modified_at_ms: part.modified_ms,
        }
    }
}

pub struct UploadPart {
    input: PutObject,
}

impl UploadPart {
    #[must_use]
    pub fn accepted_bytes(&self) -> u64 {
        self.input.accepted_bytes()
    }

    /// # Errors
    /// Returns the same bounded input/cancellation rules as a single-part PUT.
    pub async fn write(&mut self, buffer: BufferLease) -> Result<()> {
        self.input.write(buffer).await
    }

    /// # Errors
    /// Rejects invalid frame boundaries or returns storage/cancellation failure.
    pub async fn write_framed(&mut self, buffer: FrozenObjectBuffer) -> Result<()> {
        self.input.write_framed(buffer).await
    }

    /// # Errors
    /// Returns durability, digest/length, session fence or uncertain part failure.
    pub async fn finish(self) -> Result<PartMetadata> {
        self.input.finish_part().await.map(Into::into)
    }
}

impl S3Client {
    /// Copies a retained source generation through the shared multipart engine.
    /// # Errors
    /// Returns foreign identity, invalid condition/range/part or publication uncertainty.
    pub async fn copy_part_resolved(
        &self,
        context: OperationContext,
        upload: &MultipartUpload,
        number: u16,
        source: &ResolvedObject,
        range: Option<ByteRange>,
        conditions: &ReadPreconditions,
    ) -> Result<PartMetadata> {
        self.validate_upload(upload)?;
        self.validate_selection(source)?;
        if conditions.evaluate(&source.record) != ConditionOutcome::Proceed {
            return Err(Error::S3(S3ErrorCode::PreconditionFailed));
        }
        if range.is_some_and(|range| range.start >= range.end || range.end > source.metadata().logical_length)
        {
            return Err(Error::S3(S3ErrorCode::InvalidRange));
        }
        let range = range.map(|range| format!("bytes={}-{}", range.start, range.end - 1));
        let id = context.request_id.get();
        let request = self.runtime.resources.admit(context)?;
        request
            .run_mutation(async {
                Box::pin(self.execution.copy_part_record(
                    &source.record,
                    &upload.session,
                    number,
                    range.as_deref(),
                    &self.write_policy,
                ))
                .await
                .map(Into::into)
                .map_err(|error| map_error(error, id))
            })
            .await
    }

    /// # Errors
    /// Returns invalid upload/part selection, admission or writer preparation failure.
    pub async fn begin_upload_part(
        &self,
        context: OperationContext,
        upload: &MultipartUpload,
        number: u16,
        length: u64,
        expected_md5: Option<[u8; 16]>,
    ) -> Result<UploadPart> {
        self.validate_upload(upload)?;
        let id = context.request_id.get();
        let request = self.runtime.resources.admit(context)?;
        let part = request
            .run(async {
                self.execution
                    .prepare_part(&upload.session, number, length, &self.write_policy)
                    .await
                    .map_err(|error| map_error(error, id))
            })
            .await?;
        Ok(UploadPart {
            input: PutObject::from_part(self.clone(), request, part, length, expected_md5),
        })
    }
}

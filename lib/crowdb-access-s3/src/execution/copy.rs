// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded verified copies over a captured source generation.

use super::{ObjectWriter, PutObjectInfo, S3Executor, S3WritePolicy};
use crate::condition::ObjectConditions;
use crate::integrity::SinglePartIntegrity;
use crate::metadata::{BucketId, ObjectRecord, UserMetadata};
use crate::object::SelectedObject;
use crate::retrieval::{self, RetrievalError, VerifiedGetStream};
use crate::streaming::PutOutcome;
use crate::S3ErrorCode;
use crowdb_chunk_client::{ChunkIoWriter, ProtoLocation};

#[derive(Clone, Debug)]
pub struct CopyMetadata {
    pub content_type: String,
    pub user_metadata: UserMetadata,
}

pub enum CopyFailure {
    S3(S3ErrorCode),
    Publication(PutOutcome),
}

pub struct CompletedCopy {
    pub etag: String,
    pub checksum: Vec<u8>,
    pub locations: Vec<ProtoLocation>,
}

impl S3Executor {
    /// Copies one captured source and publishes only after verified durable data.
    /// # Errors
    /// Returns source validation, storage failure or uncertain publication.
    pub async fn copy_object_record(
        &self,
        source: &ObjectRecord,
        destination: BucketId,
        key: &[u8],
        replace: Option<CopyMetadata>,
        policy: &S3WritePolicy,
    ) -> Result<SelectedObject, CopyFailure> {
        crate::copy::validate_object(source, destination, key, replace.is_some()).map_err(CopyFailure::S3)?;
        let info = match replace {
            Some(replace) => PutObjectInfo {
                content_length: Some(source.logical_length),
                content_type: replace.content_type,
                user_metadata: replace.user_metadata,
            },
            None => PutObjectInfo {
                content_length: Some(source.logical_length),
                content_type: source.content_type.clone(),
                user_metadata: UserMetadata::decode(&source.attributes).map_err(CopyFailure::S3)?,
            },
        };
        let mut prepared =
            retrieval::prepare_get(&self.storage.chunks, source, None, &ObjectConditions::default())
                .map_err(|error| CopyFailure::S3(retrieval_error(&error)))?;
        let mut session = self
            .prepare_put_in_bucket(destination, key, info, policy)
            .await
            .map_err(CopyFailure::S3)?;
        let result = feed_copy(&mut prepared.stream, session.writer_mut(), source.logical_length).await;
        let (etag, checksum) = match result {
            Ok(result) => result,
            Err(error) => {
                session.abort().await;
                return Err(CopyFailure::S3(error));
            }
        };
        session
            .finish(etag, checksum)
            .await
            .map_err(CopyFailure::Publication)
    }

    /// Prepares verified copied locations for a multipart part publication.
    /// # Errors
    /// Returns range, checksum, writer or durable completion failure.
    pub async fn stream_copy(
        &self,
        source: &ObjectRecord,
        range: Option<&str>,
        key: &[u8],
        policy: &S3WritePolicy,
    ) -> Result<CompletedCopy, S3ErrorCode> {
        let mut prepared =
            retrieval::prepare_get(&self.storage.chunks, source, range, &ObjectConditions::default())
                .map_err(|error| retrieval_error(&error))?;
        let mut writer = self
            .prepare_writer(Some(prepared.headers.content_length), key, policy)
            .await?;
        let result = feed_copy(&mut prepared.stream, &mut writer, prepared.headers.content_length).await;
        let (etag, checksum) = match result {
            Ok(result) => result,
            Err(error) => {
                let _ = writer.on_error().await;
                return Err(error);
            }
        };
        let Ok(locations) = writer.on_finish().await else {
            let _ = writer.on_error().await;
            return Err(S3ErrorCode::ServiceUnavailable);
        };
        Ok(CompletedCopy {
            etag,
            checksum,
            locations,
        })
    }
}

async fn feed_copy(
    stream: &mut Option<VerifiedGetStream>,
    writer: &mut ObjectWriter,
    length: u64,
) -> Result<(String, Vec<u8>), S3ErrorCode> {
    let mut integrity = SinglePartIntegrity::default();
    let mut actual = 0u64;
    if let Some(stream) = stream {
        while let Some(bytes) = stream
            .next_verified()
            .await
            .map_err(|_| S3ErrorCode::ServiceUnavailable)?
        {
            actual = actual
                .checked_add(bytes.len() as u64)
                .ok_or(S3ErrorCode::InternalError)?;
            integrity.update(&bytes);
            while !writer.require_data() {
                writer.wait_for_capacity().await;
            }
            writer
                .on_data(bytes)
                .await
                .map_err(|_| S3ErrorCode::ServiceUnavailable)?;
        }
    }
    if actual != length {
        return Err(S3ErrorCode::InternalError);
    }
    Ok(integrity.finish())
}

fn retrieval_error(error: &RetrievalError) -> S3ErrorCode {
    match error {
        RetrievalError::Range(_) => S3ErrorCode::InvalidRange,
        RetrievalError::PreconditionFailed | RetrievalError::NotModified => S3ErrorCode::PreconditionFailed,
        _ => S3ErrorCode::ServiceUnavailable,
    }
}

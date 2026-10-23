// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{mutation_error, unix_millis, MultipartFailure};
use crate::condition::ObjectConditions;
use crate::execution::{ObjectWriter, S3Executor, S3WritePolicy};
use crate::metadata::ObjectRecord;
use crate::metadata::{MultipartPartRecord, MultipartPhase, MultipartSessionRecord};
use crate::S3ErrorCode;
use crowdb_chunk_client::{ChunkIoWriter, ProtoLocation};

pub struct PreparedPart {
    writer: ObjectWriter,
    executor: S3Executor,
    session: Box<MultipartSessionRecord>,
    number: u16,
    length: u64,
}

impl S3Executor {
    /// Verifies a captured source and publishes one copied part generation.
    /// # Errors
    /// Returns invalid ranges/parts, source corruption or uncertain publication.
    pub async fn copy_part_record(
        &self,
        source: &ObjectRecord,
        session: &MultipartSessionRecord,
        number: u16,
        range: Option<&str>,
        policy: &S3WritePolicy,
    ) -> Result<MultipartPartRecord, MultipartFailure> {
        let session = self.refresh_multipart(session, false).await?;
        if let Some(range) = range {
            crate::copy::validate_range(range, source.logical_length).map_err(MultipartFailure::S3)?;
        }
        let prepared =
            crate::retrieval::prepare_get(self.chunks(), source, range, &ObjectConditions::default())
                .map_err(|_| MultipartFailure::S3(S3ErrorCode::InvalidRange))?;
        let length = prepared.headers.content_length;
        validate_part(&session, number, length)?;
        let key = self.part_writer_key(&session, number);
        let copied = self
            .stream_copy(source, range, &key, policy)
            .await
            .map_err(MultipartFailure::S3)?;
        self.save_completed_part(&session, number, length, &copied.checksum, copied.locations)
            .await
    }

    /// # Errors
    /// Rejects closed uploads, invalid part selection or writer preparation failure.
    pub async fn prepare_part(
        &self,
        session: &MultipartSessionRecord,
        number: u16,
        length: u64,
        policy: &S3WritePolicy,
    ) -> Result<PreparedPart, MultipartFailure> {
        let session = self.refresh_multipart(session, false).await?;
        validate_part(&session, number, length)?;
        let key = self.part_writer_key(&session, number);
        let writer = self
            .prepare_writer(Some(length), &key, policy)
            .await
            .map_err(MultipartFailure::S3)?;
        Ok(PreparedPart {
            writer,
            executor: self.clone(),
            session: Box::new(session.clone()),
            number,
            length,
        })
    }

    fn part_writer_key(&self, session: &MultipartSessionRecord, number: u16) -> Vec<u8> {
        let mut key = self.tenant().as_bytes().to_vec();
        key.extend_from_slice(session.bucket_id.as_bytes());
        key.extend_from_slice(&session.upload_id);
        key.extend_from_slice(&number.to_be_bytes());
        key
    }

    /// Saves completed locations through the same generation fence as uploads.
    /// # Errors
    /// Returns invalid digest/length, a closed upload or an unconfirmed mutation.
    pub async fn save_completed_part(
        &self,
        session: &MultipartSessionRecord,
        number: u16,
        length: u64,
        checksum: &[u8],
        locations: Vec<ProtoLocation>,
    ) -> Result<MultipartPartRecord, MultipartFailure> {
        let raw_md5 = checksum
            .try_into()
            .map_err(|_| MultipartFailure::S3(S3ErrorCode::InvalidDigest))?;
        let actual = locations
            .iter()
            .try_fold(0u64, |total, location| total.checked_add(location.logical_length));
        if actual != Some(length) {
            return Err(MultipartFailure::S3(S3ErrorCode::InvalidRequest));
        }
        let now = unix_millis();
        let part = MultipartPartRecord {
            bucket_id: session.bucket_id,
            upload_id: session.upload_id,
            number,
            revision: 1,
            modified_ms: now,
            length,
            raw_md5,
            locations,
        };
        self.multipart_repository()
            .put_stream_part(session, &part, now)
            .await
            .map_err(mutation_error)?
            .ok_or(MultipartFailure::S3(S3ErrorCode::SlowDown))
    }
}

fn validate_part(session: &MultipartSessionRecord, number: u16, length: u64) -> Result<(), MultipartFailure> {
    if number == 0 || number > session.max_parts || length > session.max_part_bytes {
        return Err(MultipartFailure::S3(S3ErrorCode::InvalidRequest));
    }
    if session.phase != MultipartPhase::Open || session.expires_ms <= unix_millis() {
        return Err(MultipartFailure::S3(S3ErrorCode::NoSuchUpload));
    }
    Ok(())
}

impl PreparedPart {
    pub fn writer_mut(&mut self) -> &mut ObjectWriter {
        &mut self.writer
    }

    #[must_use]
    pub fn is_large(&self) -> bool {
        self.writer.is_large()
    }

    pub async fn abort(&mut self) {
        let _ = self.writer.on_error().await;
    }

    /// # Errors
    /// Returns durability, length/digest or uncertain part publication failure.
    pub async fn finish(mut self, checksum: Vec<u8>) -> Result<MultipartPartRecord, MultipartFailure> {
        let Ok(locations) = self.writer.on_finish().await else {
            self.abort().await;
            return Err(MultipartFailure::S3(S3ErrorCode::ServiceUnavailable));
        };
        self.executor
            .save_completed_part(&self.session, self.number, self.length, &checksum, locations)
            .await
    }
}

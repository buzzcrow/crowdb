// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded native PUT input, with publication only after durable completion.

use super::{ObjectMetadata, PutObjectInfo, S3Client};
use crate::buffer::{BufferLease, FrozenObjectBuffer};
use crate::runtime::RequestLease;
use crate::{Error, OperationContext, Result};
use crowdb_access_s3::execution::PreparedPart;
mod session;
use crowdb_access_s3::integrity::SinglePartIntegrity;
use crowdb_access_s3::streaming::{PutErrorCode, PutOutcome};
use crowdb_access_s3::S3ErrorCode;
use crowdb_chunk_client::ChunkIoWriter;
use crowdb_protocol::frame::MAX_FRAME_PAYLOAD_BYTES;
use session::{UploadResult, UploadSession};

#[derive(Default)]
pub struct PutOptions {
    pub object: PutObjectInfo,
    pub expected_md5: Option<[u8; 16]>,
}

pub struct PutObject {
    client: S3Client,
    request: Option<RequestLease>,
    session: Option<UploadSession>,
    integrity: SinglePartIntegrity,
    expected_md5: Option<[u8; 16]>,
    expected_length: Option<u64>,
    length: u64,
    final_frame: bool,
    failure: Option<Error>,
}

impl S3Client {
    /// Creates a pull-driven writer; no future message is needed to flush it.
    /// # Errors
    /// Returns admission, namespace or writer preparation failure.
    pub async fn begin_put(
        &self,
        context: OperationContext,
        bucket: &[u8],
        key: &[u8],
        options: PutOptions,
    ) -> Result<PutObject> {
        let request = self.runtime.resources.admit(context)?;
        let expected_length = options.object.content_length;
        let session = request
            .run(async {
                self.execution
                    .prepare_put(bucket, key, options.object, &self.write_policy)
                    .await
                    .map_err(Error::S3)
            })
            .await?;
        Ok(PutObject {
            client: self.clone(),
            request: Some(request),
            session: Some(UploadSession::Object(session)),
            integrity: SinglePartIntegrity::default(),
            expected_md5: options.expected_md5,
            expected_length,
            length: 0,
            final_frame: false,
            failure: None,
        })
    }

    /// Retains ordinary input using its actual allocation capacity.
    /// # Errors
    /// Returns capacity, write, integrity or uncertain publication failure.
    pub async fn put_vec(
        &self,
        context: OperationContext,
        bucket: &[u8],
        key: &[u8],
        data: Vec<u8>,
        mut options: PutOptions,
    ) -> Result<ObjectMetadata> {
        let id = context.request_id.get();
        options.object.content_length.get_or_insert(data.len() as u64);
        let mut put = self.begin_put(context, bucket, key, options).await?;
        let lease = self.runtime.resources.retain_vec(data, id, id, 0, 0)?;
        for start in (0..lease.as_slice().len()).step_by(1 << 20) {
            put.write(lease.slice(start..(start + (1 << 20)).min(lease.as_slice().len()))?)
                .await?;
        }
        drop(lease);
        put.finish().await
    }
}

impl PutObject {
    pub(super) fn from_part(
        client: S3Client,
        request: RequestLease,
        session: PreparedPart,
        length: u64,
        expected_md5: Option<[u8; 16]>,
    ) -> Self {
        Self {
            client,
            request: Some(request),
            session: Some(UploadSession::Part(session)),
            integrity: SinglePartIntegrity::default(),
            expected_md5,
            expected_length: Some(length),
            length: 0,
            final_frame: false,
            failure: None,
        }
    }

    #[must_use]
    pub fn accepted_bytes(&self) -> u64 {
        self.length
    }

    /// Accepts one owned input and applies the existing bounded storage adapter.
    /// Cancellation or dropping this pending future terminates the writer.
    /// Each call accepts at most 1 MiB; larger ordinary owners can be sliced.
    /// # Errors
    /// Returns length, cancellation or durable storage failure.
    pub async fn write(&mut self, buffer: BufferLease) -> Result<()> {
        if !buffer.belongs_to(&self.client.runtime.resources) {
            return Err(Error::Configuration(
                "input lease belongs to another runtime".into(),
            ));
        }
        if buffer.as_slice().len() > 1 << 20 {
            return Err(Error::BufferRange);
        }
        let next = self.check_input(buffer.as_slice().len() as u64)?;
        if buffer.as_slice().is_empty() {
            return Ok(());
        }
        let bytes = buffer.into_bytes();
        let (request, mut session) = self.take_active()?;
        let result = request
            .run(async {
                let writer = session.writer_mut();
                while !writer.require_data() && !writer.input_complete() {
                    writer.wait_for_capacity().await;
                }
                writer
                    .on_data(bytes.clone())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                Ok(())
            })
            .await;
        self.restore(result, request, session)?;
        self.integrity.update(&bytes);
        self.length = next;
        Ok(())
    }

    /// Submits final registered frame storage without copying its payload.
    /// A small object must arrive in one frozen owner. Large inputs use full
    /// payload frames except for their final owner; raw/framed boundaries align.
    /// # Errors
    /// Rejects an interior partial frame or returns cancellation/storage failure.
    pub async fn write_framed(&mut self, buffer: FrozenObjectBuffer) -> Result<()> {
        if !buffer.belongs_to(&self.client.runtime.resources) {
            return Err(Error::Configuration(
                "input producer belongs to another runtime".into(),
            ));
        }
        let length = buffer.length();
        let next = self.check_input(length)?;
        if self.length % MAX_FRAME_PAYLOAD_BYTES as u64 != 0
            || self.session.as_ref().is_some_and(|session| !session.is_large())
                && (self.length != 0 || self.expected_length != Some(length))
        {
            return Err(Error::BufferRange);
        }
        let payload = buffer.payload_views();
        let (request, mut session) = self.take_active()?;
        let result = request
            .run(async {
                let writer = session.writer_mut();
                while !writer.require_data() && !writer.input_complete() {
                    writer.wait_for_capacity().await;
                }
                writer
                    .on_framed_data(Box::new(buffer.owner))
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                Ok(())
            })
            .await;
        self.restore(result, request, session)?;
        for bytes in &payload {
            self.integrity.update(bytes);
        }
        self.final_frame = length % MAX_FRAME_PAYLOAD_BYTES as u64 != 0;
        self.length = next;
        Ok(())
    }

    /// Returns success only after data durability and object publication.
    /// # Errors
    /// Returns incomplete input, checksum failure or an uncertain publication.
    pub async fn finish(self) -> Result<ObjectMetadata> {
        let client = self.client.clone();
        let selected = self.finish_upload().await?.object()?;
        Ok(client.bind_object(selected)?.metadata().clone())
    }

    pub(super) async fn finish_part(self) -> Result<crowdb_access_s3::metadata::MultipartPartRecord> {
        self.finish_upload().await?.part()
    }

    async fn finish_upload(mut self) -> Result<UploadResult> {
        let (request, mut session) = self.take_active()?;
        if let Some(expected) = self.expected_length.filter(|expected| *expected != self.length) {
            session.abort().await;
            return Err(Error::Length {
                expected,
                delivered: self.length,
            });
        }
        let (etag, checksum) = self.integrity.finish();
        if self
            .expected_md5
            .is_some_and(|expected| expected.as_slice() != checksum)
        {
            session.abort().await;
            return Err(Error::Integrity);
        }
        let id = request.context().request_id.get();
        request.run_mutation(session.finish(etag, checksum, id)).await
    }

    fn check_input(&self, count: u64) -> Result<u64> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let next = self.length.checked_add(count).ok_or(Error::BufferRange)?;
        if self.final_frame || next > 5 << 40 || self.expected_length.is_some_and(|expected| next > expected)
        {
            return Err(Error::BufferRange);
        }
        Ok(next)
    }

    fn take_active(&mut self) -> Result<(RequestLease, UploadSession)> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        self.failure = Some(Error::Cancelled);
        Ok((
            self.request.take().ok_or(Error::Cancelled)?,
            self.session.take().ok_or(Error::Cancelled)?,
        ))
    }

    fn restore(&mut self, result: Result<()>, request: RequestLease, session: UploadSession) -> Result<()> {
        match result {
            Ok(()) => {
                self.request = Some(request);
                self.session = Some(session);
                self.failure = None;
                Ok(())
            }
            Err(error) => {
                self.failure = Some(error.clone());
                Err(error)
            }
        }
    }
}

pub(super) fn map_outcome(outcome: PutOutcome, request_id: u64) -> Error {
    match outcome {
        PutOutcome::Timeout => Error::UncertainMutation {
            request_id,
            reason: crate::error::MutationInterruption::Transport,
        },
        PutOutcome::Error { code, message } => match code {
            PutErrorCode::BodyRead => Error::S3(S3ErrorCode::InvalidRequest),
            PutErrorCode::InvalidDigest
            | PutErrorCode::BadDigest
            | PutErrorCode::InvalidPayloadDigest
            | PutErrorCode::PayloadMismatch => Error::Integrity,
            _ => Error::Storage(message),
        },
        PutOutcome::Success => Error::Storage("unexpected successful publication failure".into()),
    }
}

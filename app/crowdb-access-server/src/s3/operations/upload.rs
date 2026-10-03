// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One S3 PUT or `UploadPart` body through the shared object write flow.

use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_access_s3::integrity::{validate_completed_digests, IntegrityError};
use crowdb_access_s3::metrics::S3Metrics;
use crowdb_access_s3::native_buffer::NativeBodyReceiver;
use crowdb_access_s3::streaming::{PutErrorCode, PutOutcome};
use crowdb_chunk_client::FramedWriteBuffer;
use crowdb_protocol::frame::FrameMagic;
use http_body_util::BodyExt;
use hyper::body::{Bytes, Incoming};
use tokio::sync::mpsc;

use crate::upload_flow::body_encoding::{UploadBody, UploadEncodingError};
use crate::upload_flow::digest_pipe::DigestPipe;
use crate::upload_flow::{drive_transfer, write_buffers, OfferStatus, UploadBuffer, WriteFlow};

use super::ObjectWriter;

const TARGET_BUFFER_BYTES: usize = 1024 * 1024;

fn failed(code: PutErrorCode, message: &impl ToString) -> PutOutcome {
    PutOutcome::Error {
        code,
        message: message.to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_object_body(
    body: &mut UploadBody<Incoming>,
    writer: &mut ObjectWriter,
    native_receiver: Option<&NativeBodyReceiver>,
    declared_length: Option<u64>,
    expected_content_md5: Option<&str>,
    expected_payload_sha256: Option<&str>,
    held_buffers: usize,
    metrics: Option<&S3Metrics>,
) -> Result<(String, Vec<u8>), PutOutcome> {
    let (sender, receiver) = mpsc::channel(held_buffers);
    let progress = AtomicU64::new(0);
    let queued_peak = AtomicU64::new(0);
    let flow = WriteFlow::new(sender, &progress, &queued_peak);
    let mut digest = DigestPipe::start(expected_payload_sha256.is_some());
    let receive = receive_body(body, native_receiver, declared_length, flow, &mut digest, metrics);
    let write = write_buffers(writer, receiver, &progress, |error| {
        failed(PutErrorCode::ChunkWrite, &error)
    });
    let transfer = drive_transfer(receive, write, &progress).await;
    if let Some(error) = body.failure() {
        return Err(failed(
            if matches!(error, UploadEncodingError::Checksum) {
                PutErrorCode::BadDigest
            } else {
                PutErrorCode::BodyRead
            },
            &error,
        ));
    }
    let (length, _) = transfer?;
    if declared_length.is_some_and(|expected| expected != length) {
        return Err(failed(
            PutErrorCode::BodyRead,
            &"body length differs from Content-Length",
        ));
    }
    let digests = digest
        .finish()
        .await
        .map_err(|()| failed(PutErrorCode::ChunkWrite, &"digest worker failed"))?;
    validate_completed_digests(
        digests.md5,
        digests.sha256,
        expected_content_md5,
        expected_payload_sha256,
    )
    .map_err(|error| match error {
        IntegrityError::InvalidDigest => failed(PutErrorCode::InvalidDigest, &error),
        IntegrityError::Mismatch => failed(PutErrorCode::BadDigest, &error),
        IntegrityError::InvalidPayloadDigest => failed(PutErrorCode::InvalidPayloadDigest, &error),
        IntegrityError::PayloadMismatch => failed(PutErrorCode::PayloadMismatch, &error),
    })
}

async fn receive_body(
    body: &mut UploadBody<Incoming>,
    native_receiver: Option<&NativeBodyReceiver>,
    declared_length: Option<u64>,
    flow: WriteFlow<'_>,
    digest: &mut DigestPipe,
    metrics: Option<&S3Metrics>,
) -> Result<u64, PutOutcome> {
    let target = usize::try_from(declared_length.unwrap_or(TARGET_BUFFER_BYTES as u64))
        .unwrap_or(TARGET_BUFFER_BYTES)
        .clamp(1, TARGET_BUFFER_BYTES);
    let mut length = 0u64;
    let mut pending = Vec::with_capacity(if native_receiver.is_some() { 0 } else { target });
    let mut digest_pending = Vec::with_capacity(16);
    loop {
        let Some(frame) = body.frame().await else { break };
        let mut bytes = frame
            .map_err(|error| failed(PutErrorCode::BodyRead, &error))?
            .into_data()
            .map_err(|_| failed(PutErrorCode::BodyRead, &"unexpected non-data body frame"))?;
        if bytes.is_empty() {
            continue;
        }
        length = length
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| failed(PutErrorCode::BodyRead, &"body length overflow"))?;
        if declared_length.is_some_and(|expected| length > expected) {
            return Err(failed(PutErrorCode::BodyRead, &"body exceeds Content-Length"));
        }
        if let Some(metrics) = metrics {
            metrics.record_checksum_bytes(bytes.len());
        }
        if let Some(receiver) = native_receiver {
            digest_pending.push(bytes);
            if let Some(owner) = receiver.take_ready_owner() {
                handoff_owner(owner, &flow, digest, std::mem::take(&mut digest_pending)).await?;
            }
        } else {
            while !bytes.is_empty() {
                let count = (target - pending.len()).min(bytes.len());
                let piece = bytes.split_to(count);
                pending.extend_from_slice(&piece);
                digest_pending.push(piece);
                if pending.len() == target {
                    let owner = Bytes::from(std::mem::replace(&mut pending, Vec::with_capacity(target)));
                    handoff(
                        &flow,
                        digest,
                        UploadBuffer::Data(owner),
                        std::mem::take(&mut digest_pending),
                    )
                    .await?;
                }
            }
        }
    }
    if let Some(receiver) = native_receiver {
        if let Some(owner) = receiver
            .finish_owner_when_ready()
            .await
            .map_err(|error| failed(PutErrorCode::BodyRead, &error))?
        {
            handoff_owner(owner, &flow, digest, std::mem::take(&mut digest_pending)).await?;
        }
    }
    if !pending.is_empty() {
        handoff(
            &flow,
            digest,
            UploadBuffer::Data(Bytes::from(pending)),
            std::mem::take(&mut digest_pending),
        )
        .await?;
    }
    Ok(length)
}

async fn handoff_owner(
    mut owner: crowdb_access_s3::native_buffer::NativeFramedOwner,
    flow: &WriteFlow<'_>,
    digest: &mut DigestPipe,
    payload: Vec<Bytes>,
) -> Result<(), PutOutcome> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        });
    owner
        .prepare_frames(FrameMagic::RepoLargeV1, now_ms)
        .map_err(|error| failed(PutErrorCode::ChunkWrite, &error))?;
    handoff(flow, digest, UploadBuffer::Framed(Box::new(owner)), payload).await
}

async fn handoff(
    flow: &WriteFlow<'_>,
    digest: &mut DigestPipe,
    buffer: UploadBuffer,
    payload: Vec<Bytes>,
) -> Result<(), PutOutcome> {
    let offer = flow
        .offer(buffer)
        .await
        .map_err(|()| failed(PutErrorCode::ChunkWrite, &"write flow closed"))?;
    digest
        .enqueue(payload)
        .map_err(|()| failed(PutErrorCode::ChunkWrite, &"digest capacity exhausted"))?;
    if matches!(offer, OfferStatus::Pause) {
        flow.wait_ready()
            .await
            .map_err(|()| failed(PutErrorCode::ChunkWrite, &"write flow closed"))?;
    }
    Ok(())
}

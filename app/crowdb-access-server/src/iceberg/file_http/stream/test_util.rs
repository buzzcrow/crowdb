// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    drive_transfer, multipart, receive_body, write_buffers, DigestPipe, FileS3ErrorCode,
    FileTransferAdmission, FrameMagic, UploadBody, UploadFlowMetrics, WriteFlow, TARGET_BUFFER_BYTES,
};
use crate::iceberg::UploadFlowSnapshot;
use crowdb_chunk_client::ChunkIoWriter;
use hyper::body::{Body, Bytes};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc;

/// Drives the real bounded Iceberg fallback receive and write flow without a listener.
/// Storage completion and publication remain the caller's responsibility.
/// # Errors
/// Returns production admission, body, integrity, writer or flow failures.
pub async fn write_iceberg_fallback_body_for_tests<B: Body<Data = Bytes> + Unpin>(
    body: &mut UploadBody<B>,
    writer: &mut impl ChunkIoWriter,
    admission: &FileTransferAdmission,
    held_buffers: usize,
    expected_sha256: Option<[u8; 32]>,
) -> Result<(u64, [u8; 16], UploadFlowSnapshot), FileS3ErrorCode> {
    if held_buffers == 0 {
        return Err(FileS3ErrorCode::InvalidRequest);
    }
    body.defer_md5();
    let length = body.decoded_length();
    let target = usize::try_from(length.unwrap_or(TARGET_BUFFER_BYTES as u64))
        .unwrap_or(TARGET_BUFFER_BYTES)
        .clamp(1, TARGET_BUFFER_BYTES);
    let (sender, receiver) = mpsc::channel(held_buffers);
    let progress = AtomicU64::new(0);
    let peak = AtomicU64::new(0);
    let metrics = Arc::new(UploadFlowMetrics::default());
    let mut observation = metrics.start();
    let mut digest = DigestPipe::start(expected_sha256.is_some());
    let receive = receive_body(
        body,
        &mut digest,
        admission,
        None,
        length,
        target,
        FrameMagic::RepoLargeV2,
        WriteFlow::new(sender, &progress, &peak),
        &mut observation,
    );
    let write = write_buffers(writer, receiver, &progress, |_| FileS3ErrorCode::SlowDown);
    let result = drive_transfer(receive, write, &progress).await;
    observation.queued_buffers_peak(peak.load(Ordering::Relaxed));
    let (length, written) = match result {
        Ok(value) => value,
        Err(error) => {
            observation.complete(false);
            let _ = digest.finish().await;
            return Err(error);
        }
    };
    observation.writer_feeds(written.feeds, written.feed_time);
    observation.writer_capacity_waits(written.capacity_waits, written.capacity_wait_time);
    let digests = digest
        .finish()
        .await
        .map_err(|()| FileS3ErrorCode::InternalError)?;
    observation.digest_process(digests.process_time);
    body.verify_deferred_md5(digests.md5)
        .map_err(multipart::encoding_error)?;
    if expected_sha256.is_some_and(|expected| digests.sha256 != Some(expected)) {
        return Err(FileS3ErrorCode::InvalidRequest);
    }
    observation.complete(true);
    drop(observation);
    Ok((length, digests.md5, metrics.snapshot()))
}

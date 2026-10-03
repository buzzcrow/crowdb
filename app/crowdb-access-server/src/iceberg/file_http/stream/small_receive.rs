use crate::upload_flow::body_encoding::UploadBody;
// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::Future;
use std::task::Poll;
use std::time::Instant;

use crowdb_access_iceberg::storage::IcebergFileWriter;
use crowdb_access_s3::native_buffer::NativeBodyReceiver;
use crowdb_chunk_client::FramedWriteBuffer;
use crowdb_protocol::frame::FrameMagic;
use http_body_util::BodyExt;
use hyper::body::Incoming;

use super::{
    admission_error, multipart, DigestPipe, FileS3ErrorCode, FileTransferAdmission, UploadObservation,
};
use crate::upload_flow::WriteStats;

#[allow(clippy::too_many_arguments)]
pub(super) async fn receive(
    body: &mut UploadBody<Incoming>,
    writer: &mut IcebergFileWriter,
    digest: &mut DigestPipe,
    admission: &FileTransferAdmission,
    receiver: &NativeBodyReceiver,
    declared_length: Option<u64>,
    observation: &mut UploadObservation,
) -> Result<(u64, WriteStats), FileS3ErrorCode> {
    let mut length = 0u64;
    let mut ready = None;
    loop {
        let started = Instant::now();
        let mut frame = std::pin::pin!(body.frame());
        let mut waited = None;
        let next = std::future::poll_fn(|cx| match frame.as_mut().poll(cx) {
            Poll::Pending => {
                waited.get_or_insert_with(Instant::now);
                Poll::Pending
            }
            Poll::Ready(value) => Poll::Ready(value),
        })
        .await;
        if let Some(waited) = waited {
            observation.body_wait(waited.elapsed());
        }
        observation.body_poll(started.elapsed(), next.is_some());
        let Some(frame) = next else {
            break;
        };
        let bytes = frame
            .map_err(multipart::encoding_error)?
            .into_data()
            .map_err(|_| FileS3ErrorCode::InvalidRequest)?;
        if bytes.is_empty() {
            continue;
        }
        observation.payload(bytes.len());
        length = length
            .checked_add(bytes.len() as u64)
            .ok_or(FileS3ErrorCode::EntityTooLarge)?;
        admission.check_bytes(length, length).map_err(admission_error)?;
        if declared_length.is_some_and(|declared| length > declared) {
            return Err(FileS3ErrorCode::InvalidRequest);
        }
        let started = Instant::now();
        digest
            .process_inline(&bytes)
            .map_err(|()| FileS3ErrorCode::InternalError)?;
        observation.digest_enqueue(started.elapsed());
        if let Some(owner) = receiver.take_ready_owner() {
            if ready.is_some() {
                return Err(FileS3ErrorCode::InternalError);
            }
            ready = Some(owner);
        }
    }
    if declared_length != Some(length) {
        return Err(FileS3ErrorCode::InvalidRequest);
    }
    let mut owner = match ready {
        Some(owner) => owner,
        None => receiver
            .finish_owner_when_ready()
            .await
            .map_err(|_| FileS3ErrorCode::InvalidRequest)?
            .ok_or(FileS3ErrorCode::InvalidRequest)?,
    };
    let started = Instant::now();
    owner
        .prepare_frames(FrameMagic::RepoSmallV1, super::super::now_ms()?)
        .map_err(|_| FileS3ErrorCode::InternalError)?;
    observation.frame_prepare(owner.frame_count(), started.elapsed());
    let started = Instant::now();
    writer
        .on_framed_data(Box::new(owner))
        .await
        .map_err(|_| FileS3ErrorCode::SlowDown)?;
    Ok((
        length,
        WriteStats {
            feeds: 1,
            feed_time: started.elapsed(),
            ..WriteStats::default()
        },
    ))
}

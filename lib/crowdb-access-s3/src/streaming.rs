// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::poll_fn;
use std::pin::Pin;

use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;

use crowdb_chunk_client::ChunkIoWriter;
use crowdb_chunk_kv_client::ClientError;
use hyper::body::{Body, Bytes};

use crate::integrity::SinglePartIntegrity;
use crate::metadata::{ChunkKvMetadataStore, MetadataStoreError, ObjectRecord};
use crate::native_buffer::NativeBodyReceiver;
use crate::publication::{publish, PublicationError, PublicationRequest};

#[derive(Debug, thiserror::Error)]
pub enum StreamingMetadataError {
    #[error("chunk location encoding failed")]
    LocationEncoding,
    #[error("S3 object publication failed: {0}")]
    Publication(#[from] PublicationError),
}

/// Stable, definite S3 PUT failure code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutErrorCode {
    BodyRead,
    ChunkWrite,
    LocationEncoding,
    MetadataEncoding,
    InvalidKey,
    KvRejected,
    InvalidDigest,
    BadDigest,
    InvalidPayloadDigest,
    PayloadMismatch,
}

/// Result of the final chunk-to-object publication step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PutOutcome {
    Success,
    Error {
        code: PutErrorCode,
        message: String,
    },
    /// The final KV mutation may have applied; do not synchronously delete chunks.
    Timeout,
}

/// Data eligible for cleanup after a definite metadata-publication failure.
///
/// A shared range is distinct from an owned chunk: deleting the whole chunk
/// for a small object could erase neighboring objects. Its physical deletion
/// therefore awaits the qualified range-delete contract.
#[derive(Clone, Debug, PartialEq)]
pub enum FailedPublicationTarget {
    DedicatedChunk(ChunkId),
    SharedRange(Location),
}

/// Streams one HTTP body into a chunk writer without polling the next frame
/// until the current frame is accepted. This makes writer capacity backpressure
/// reach Hyper and bounds retained request data.
///
/// # Errors
///
/// Returns a coded PUT error when body polling or chunk writing fails.
pub async fn write_body<B, W>(body: &mut B, writer: &mut W) -> Result<(), PutOutcome>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
    W: ChunkIoWriter,
{
    loop {
        if writer.input_complete() {
            return Ok(());
        }
        // Do not ask Hyper for another frame while its stable shared pipeline
        // is full. This leaves the socket unread and applies natural TCP flow
        // control instead of accumulating handler-side body buffers.
        while !writer.require_data() {
            writer.wait_for_capacity().await;
        }
        let frame = poll_fn(|context| Pin::new(&mut *body).poll_frame(context)).await;
        let Some(frame) = frame else {
            return Ok(());
        };
        let frame = frame.map_err(|error| put_error(PutErrorCode::BodyRead, error))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        writer
            .on_data(data)
            .await
            .map_err(|error| put_error(PutErrorCode::ChunkWrite, error))?;
    }
}

/// Streams one body while calculating the single-part logical checksum without
/// collecting its bytes. The caller must persist the returned ETag/checksum in
/// the final one-Put metadata record.
///
/// # Errors
///
/// Returns a coded body-read or chunk-writer error.
pub async fn write_body_with_integrity<B, W>(
    body: &mut B,
    writer: &mut W,
) -> Result<(String, Vec<u8>), PutOutcome>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
    W: ChunkIoWriter,
{
    write_body_with_content_md5(body, writer, None).await
}

/// Streams one body and validates an optional `Content-MD5` before returning.
///
/// # Errors
///
/// Returns a coded read, writer, malformed-digest, or mismatch error.
pub async fn write_body_with_content_md5<B, W>(
    body: &mut B,
    writer: &mut W,
    expected_content_md5: Option<&str>,
) -> Result<(String, Vec<u8>), PutOutcome>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
    W: ChunkIoWriter,
{
    write_body_with_checksums(body, writer, expected_content_md5, None).await
}

/// Streams one body and validates optional MD5 and `SigV4` SHA-256 digests.
///
/// # Errors
///
/// Returns a coded read, writer, malformed-digest, or mismatch error.
pub async fn write_body_with_checksums<B, W>(
    body: &mut B,
    writer: &mut W,
    expected_content_md5: Option<&str>,
    expected_payload_sha256: Option<&str>,
) -> Result<(String, Vec<u8>), PutOutcome>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
    W: ChunkIoWriter,
{
    let mut integrity = SinglePartIntegrity::default();
    loop {
        if writer.input_complete() {
            return finish_integrity(integrity, expected_content_md5, expected_payload_sha256);
        }
        while !writer.require_data() {
            writer.wait_for_capacity().await;
        }
        let frame = poll_fn(|context| Pin::new(&mut *body).poll_frame(context)).await;
        let Some(frame) = frame else {
            return finish_integrity(integrity, expected_content_md5, expected_payload_sha256);
        };
        let frame = frame.map_err(|error| put_error(PutErrorCode::BodyRead, error))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        integrity.update(&data);
        writer
            .on_data(data)
            .await
            .map_err(|error| put_error(PutErrorCode::ChunkWrite, error))?;
    }
}

/// Streams one body through a native owner provider while calculating object
/// integrity over the socket-filled payload views. Full owners and the EOF
/// prefix are handed to the large writer without payload copies.
///
/// Header-buffer read-ahead falls back to the generic payload path until its
/// edge-view representation is available.
///
/// # Errors
///
/// Returns a coded read, native-owner, writer, or checksum error.
pub async fn write_native_body_with_checksums<B, W>(
    body: &mut B,
    writer: &mut W,
    receiver: &NativeBodyReceiver,
    expected_content_md5: Option<&str>,
    expected_payload_sha256: Option<&str>,
) -> Result<(String, Vec<u8>), PutOutcome>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
    W: ChunkIoWriter,
{
    let mut integrity = SinglePartIntegrity::default();
    loop {
        while !writer.require_data() {
            writer.wait_for_capacity().await;
        }
        let frame = poll_fn(|context| Pin::new(&mut *body).poll_frame(context)).await;
        let Some(frame) = frame else {
            if receiver.owner_handoff_active() {
                if let Some(owner) = receiver
                    .finish_owner()
                    .map_err(|error| put_error(PutErrorCode::BodyRead, error))?
                {
                    writer
                        .on_framed_data(Box::new(owner))
                        .await
                        .map_err(|error| put_error(PutErrorCode::ChunkWrite, error))?;
                }
            }
            return finish_integrity(integrity, expected_content_md5, expected_payload_sha256);
        };
        let frame = frame.map_err(|error| put_error(PutErrorCode::BodyRead, error))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        integrity.update(&data);
        if receiver.owner_handoff_active() {
            if let Some(owner) = receiver.take_ready_owner() {
                writer
                    .on_framed_data(Box::new(owner))
                    .await
                    .map_err(|error| put_error(PutErrorCode::ChunkWrite, error))?;
            }
        } else {
            writer
                .on_data(data)
                .await
                .map_err(|error| put_error(PutErrorCode::ChunkWrite, error))?;
        }
    }
}

fn finish_integrity(
    integrity: SinglePartIntegrity,
    expected_content_md5: Option<&str>,
    expected_payload_sha256: Option<&str>,
) -> Result<(String, Vec<u8>), PutOutcome> {
    integrity
        .finish_validated_checksums(expected_content_md5, expected_payload_sha256)
        .map_err(|error| match error {
            crate::integrity::IntegrityError::InvalidDigest => put_error(PutErrorCode::InvalidDigest, error),
            crate::integrity::IntegrityError::Mismatch => put_error(PutErrorCode::BadDigest, error),
            crate::integrity::IntegrityError::InvalidPayloadDigest => {
                put_error(PutErrorCode::InvalidPayloadDigest, error)
            }
            crate::integrity::IntegrityError::PayloadMismatch => {
                put_error(PutErrorCode::PayloadMismatch, error)
            }
        })
}

/// Best-effort cleanup of locations whose final metadata publication definitely failed.
#[async_trait::async_trait]
pub trait FailedPublicationCleanup: Send + Sync {
    /// Records or performs cleanup; its failure must not replace the PUT result.
    async fn cleanup(&self, targets: &[FailedPublicationTarget]);
}

/// Attaches completed writer locations to the metadata that one KV Put publishes.
///
/// # Errors
///
/// Returns an error when the opaque location representation cannot be encoded.
pub fn attach_completed_locations(
    object: &mut ObjectRecord,
    locations: &[Location],
) -> Result<(), StreamingMetadataError> {
    object.data_reference =
        bincode::serialize(locations).map_err(|_| StreamingMetadataError::LocationEncoding)?;
    Ok(())
}

/// Publishes exactly once after the chunk writer has returned completed locations.
///
/// # Errors
///
/// Returns location encoding or routed publication failure.
pub async fn publish_completed_locations(
    store: &ChunkKvMetadataStore,
    request: &mut PublicationRequest,
    locations: &[Location],
) -> PutOutcome {
    match attach_completed_locations(&mut request.object, locations) {
        Ok(()) => match publish(store, request).await {
            Ok(()) => PutOutcome::Success,
            Err(error) => classify_publication_error(error),
        },
        Err(error) => PutOutcome::Error {
            code: PutErrorCode::LocationEncoding,
            message: error.to_string(),
        },
    }
}

/// Streams, seals, and then publishes an S3 PUT in that order.
///
/// A body or chunk-writer error does not publish metadata. Writer abort errors
/// are intentionally secondary: data left after a chunk failure is garbage for
/// asynchronous reclamation.
pub async fn write_body_and_publish<B, W>(
    body: &mut B,
    writer: &mut W,
    store: &ChunkKvMetadataStore,
    request: &mut PublicationRequest,
) -> PutOutcome
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
    W: ChunkIoWriter,
{
    if let Err(outcome) = write_body(body, writer).await {
        let _ = writer.on_error().await;
        return outcome;
    }
    match writer.on_finish().await {
        Ok(locations) => publish_completed_locations(store, request, &locations).await,
        Err(error) => {
            let _ = writer.on_error().await;
            put_error(PutErrorCode::ChunkWrite, error)
        }
    }
}

/// Publishes completed locations and runs best-effort cleanup only after a definite error.
pub async fn publish_completed_locations_with_cleanup(
    store: &ChunkKvMetadataStore,
    request: &mut PublicationRequest,
    locations: &[Location],
    cleanup_targets: &[FailedPublicationTarget],
    cleanup: &dyn FailedPublicationCleanup,
) -> PutOutcome {
    let outcome = publish_completed_locations(store, request, locations).await;
    cleanup_after_definite_error(outcome, cleanup_targets, cleanup).await
}

/// Runs cleanup only for a definite publication error.
pub async fn cleanup_after_definite_error(
    outcome: PutOutcome,
    cleanup_targets: &[FailedPublicationTarget],
    cleanup: &dyn FailedPublicationCleanup,
) -> PutOutcome {
    if matches!(outcome, PutOutcome::Error { .. }) {
        cleanup.cleanup(cleanup_targets).await;
    }
    outcome
}

fn classify_publication_error(error: PublicationError) -> PutOutcome {
    match error {
        PublicationError::Store(MetadataStoreError::Client(
            ClientError::Deadline | ClientError::Transport(_),
        )) => PutOutcome::Timeout,
        PublicationError::Metadata(error) => PutOutcome::Error {
            code: PutErrorCode::MetadataEncoding,
            message: error.to_string(),
        },
        PublicationError::Key(error) => PutOutcome::Error {
            code: PutErrorCode::InvalidKey,
            message: error.to_string(),
        },
        PublicationError::Store(error) => PutOutcome::Error {
            code: PutErrorCode::KvRejected,
            message: error.to_string(),
        },
    }
}

fn put_error(code: PutErrorCode, error: impl std::fmt::Display) -> PutOutcome {
    PutOutcome::Error {
        code,
        message: error.to_string(),
    }
}

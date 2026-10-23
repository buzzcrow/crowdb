// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Metadata-consistent HEAD and pull-based GET preparation.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use crowdb_chunk_client::{ChunkIoClient, ChunkReadStream, ReadError};
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_rpc_ffi::OwnedPayload;
use hyper::body::Bytes;

use crate::condition::{ConditionOutcome, ReadCondition};
use crate::integrity::SinglePartIntegrity;
use crate::metadata::ObjectRecord;
use crate::range::{resolve_range, ByteRange, RangeError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectHeaders {
    pub content_length: u64,
    pub etag: String,
    pub checksum: Vec<u8>,
    pub last_modified: String,
    pub content_type: String,
    pub attributes: Vec<u8>,
    pub content_range: Option<String>,
}

pub struct PreparedGet {
    pub headers: ObjectHeaders,
    pub stream: Option<VerifiedGetStream>,
    pub partial: bool,
}

pub struct VerifiedGetStream {
    inner: Option<ChunkReadStream>,
    integrity: Option<SinglePartIntegrity>,
    expected_checksum: Vec<u8>,
    terminal: bool,
    failure: Option<GetStreamError>,
    expected_length: u64,
    delivered: u64,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum GetStreamError {
    #[error("chunk read failed: {0}")]
    Chunk(Arc<ReadError>),
    #[error("object checksum does not match its published metadata")]
    Integrity,
    #[error("object length mismatch: expected={expected} delivered={delivered}")]
    Length { expected: u64, delivered: u64 },
}

impl From<ReadError> for GetStreamError {
    fn from(error: ReadError) -> Self {
        Self::Chunk(Arc::new(error))
    }
}

impl VerifiedGetStream {
    /// Pulls one verified storage window. A full-object stream checks its
    /// persisted logical checksum before reporting end-of-stream.
    pub async fn next_chunk(&mut self) -> Option<Result<Bytes, GetStreamError>> {
        if self.terminal {
            return None;
        }
        self.next_verified().await.transpose()
    }

    /// Successful EOF includes length and applicable object integrity checks.
    /// A terminal failure remains visible on every subsequent pull.
    /// # Errors
    /// Returns storage, length or final checksum failure without successful EOF.
    pub async fn next_verified(&mut self) -> Result<Option<Bytes>, GetStreamError> {
        self.next_payload()
            .await
            .map(|data| data.map(OwnedPayload::into_bytes))
    }

    /// Pulls the same verified logical view with allocation ownership intact.
    /// # Errors
    /// Returns sticky storage, length or applicable final checksum failures.
    pub async fn next_payload(&mut self) -> Result<Option<OwnedPayload>, GetStreamError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.terminal {
            return Ok(None);
        }
        let next = match &mut self.inner {
            Some(inner) => inner.next_payload().await,
            None => None,
        };
        match next {
            Some(Ok(bytes)) => {
                self.delivered = self.delivered.saturating_add(bytes.len() as u64);
                if self.delivered > self.expected_length {
                    return self.fail(GetStreamError::Length {
                        expected: self.expected_length,
                        delivered: self.delivered,
                    });
                }
                if let Some(integrity) = &mut self.integrity {
                    integrity.update(&bytes);
                }
                Ok(Some(bytes))
            }
            Some(Err(error)) => self.fail(error.into()),
            None => {
                self.terminal = true;
                self.inner = None;
                if self.delivered != self.expected_length {
                    return self.fail(GetStreamError::Length {
                        expected: self.expected_length,
                        delivered: self.delivered,
                    });
                }
                if let Some(integrity) = self.integrity.take() {
                    let (_, checksum) = integrity.finish();
                    if checksum != self.expected_checksum {
                        return self.fail(GetStreamError::Integrity);
                    }
                }
                Ok(None)
            }
        }
    }

    fn fail(&mut self, error: GetStreamError) -> Result<Option<OwnedPayload>, GetStreamError> {
        self.terminal = true;
        self.inner = None;
        self.failure = Some(error.clone());
        Err(error)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RetrievalError {
    #[error("request precondition failed")]
    PreconditionFailed,
    #[error("object was not modified")]
    NotModified,
    #[error("invalid object range: {0:?}")]
    Range(RangeError),
    #[error("invalid object data reference")]
    DataReference,
    #[error("chunk reader rejected object metadata")]
    ChunkRead,
}

/// Builds HEAD attributes without contacting chunk storage.
///
/// # Errors
///
/// Returns a conditional outcome; all returned headers come from `record`.
pub fn prepare_head(
    record: &ObjectRecord,
    conditions: &(impl ReadCondition + ?Sized),
) -> Result<ObjectHeaders, RetrievalError> {
    check_conditions(record, conditions)?;
    Ok(headers(record, None))
}

/// Resolves a range and creates a lazy chunk stream without reading payload.
///
/// # Errors
///
/// Rejects conditions, ranges, encoded locations, or inconsistent metadata.
pub fn prepare_get(
    client: &ChunkIoClient,
    record: &ObjectRecord,
    range_header: Option<&str>,
    conditions: &(impl ReadCondition + ?Sized),
) -> Result<PreparedGet, RetrievalError> {
    check_conditions(record, conditions)?;
    let requested = resolve_range(range_header, record.logical_length).map_err(RetrievalError::Range)?;
    build_get(client, record, requested)
}

/// Prepares the same selected generation using a typed half-open range.
/// # Errors
/// Rejects conditions or intervals outside the published object length.
pub fn prepare_get_range(
    client: &ChunkIoClient,
    record: &ObjectRecord,
    requested: Option<ByteRange>,
    conditions: &(impl ReadCondition + ?Sized),
) -> Result<PreparedGet, RetrievalError> {
    check_conditions(record, conditions)?;
    if requested.is_some_and(|range| range.start >= range.end || range.end > record.logical_length) {
        return Err(RetrievalError::Range(RangeError::Unsatisfiable));
    }
    build_get(client, record, requested)
}

fn build_get(
    client: &ChunkIoClient,
    record: &ObjectRecord,
    requested: Option<ByteRange>,
) -> Result<PreparedGet, RetrievalError> {
    let interval = requested.unwrap_or(ByteRange {
        start: 0,
        end: record.logical_length,
    });
    let inner = if interval.start == interval.end {
        None
    } else {
        let locations: Vec<Location> =
            bincode::deserialize(&record.data_reference).map_err(|_| RetrievalError::DataReference)?;
        Some(
            client
                .read_range_stream(&locations, interval.start, interval.end)
                .map_err(|_| RetrievalError::ChunkRead)?,
        )
    };
    let stream = Some(VerifiedGetStream {
        inner,
        integrity: (requested.is_none()
            && !crate::integrity::is_multipart_checksum(&record.checksum, &record.etag))
        .then(SinglePartIntegrity::default),
        expected_checksum: record.checksum.clone(),
        terminal: false,
        failure: None,
        expected_length: interval.end - interval.start,
        delivered: 0,
    });
    Ok(PreparedGet {
        headers: headers(record, requested),
        stream,
        partial: requested.is_some(),
    })
}

fn check_conditions(
    record: &ObjectRecord,
    conditions: &(impl ReadCondition + ?Sized),
) -> Result<(), RetrievalError> {
    match conditions.evaluate(record) {
        ConditionOutcome::Proceed => Ok(()),
        ConditionOutcome::NotModified => Err(RetrievalError::NotModified),
        ConditionOutcome::PreconditionFailed => Err(RetrievalError::PreconditionFailed),
    }
}

fn headers(record: &ObjectRecord, range: Option<ByteRange>) -> ObjectHeaders {
    let (content_length, content_range) = range.map_or((record.logical_length, None), |range| {
        (
            range.end - range.start,
            Some(format!(
                "bytes {}-{}/{}",
                range.start,
                range.end - 1,
                record.logical_length
            )),
        )
    });
    ObjectHeaders {
        content_length,
        etag: record.etag.clone(),
        checksum: record.checksum.clone(),
        last_modified: httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_millis(record.modified_at_ms)),
        content_type: record.content_type.clone(),
        attributes: record.attributes.clone(),
        content_range,
    }
}

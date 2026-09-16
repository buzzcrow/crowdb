// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Metadata-consistent HEAD and pull-based GET preparation.

use std::time::{Duration, UNIX_EPOCH};

use crowdb_chunk_client::{ChunkIoClient, ChunkReadStream, ReadError};
use crowdb_protocol::chunkdb::rpc::Location;
use hyper::body::Bytes;

use crate::condition::{evaluate, ConditionOutcome, ObjectConditions};
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
    pub content_range: Option<String>,
}

pub struct PreparedGet {
    pub headers: ObjectHeaders,
    pub stream: Option<VerifiedGetStream>,
    pub partial: bool,
}

pub struct VerifiedGetStream {
    inner: ChunkReadStream,
    integrity: Option<SinglePartIntegrity>,
    expected_checksum: Vec<u8>,
    terminal: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum GetStreamError {
    #[error("chunk read failed: {0}")]
    Chunk(#[from] ReadError),
    #[error("object checksum does not match its published metadata")]
    Integrity,
}

impl VerifiedGetStream {
    /// Pulls one verified storage window. A full-object stream checks its
    /// persisted logical checksum before reporting end-of-stream.
    pub async fn next_chunk(&mut self) -> Option<Result<Bytes, GetStreamError>> {
        if self.terminal {
            return None;
        }
        match self.inner.next_chunk().await {
            Some(Ok(bytes)) => {
                if let Some(integrity) = &mut self.integrity {
                    integrity.update(&bytes);
                }
                Some(Ok(bytes))
            }
            Some(Err(error)) => {
                self.terminal = true;
                Some(Err(error.into()))
            }
            None => {
                self.terminal = true;
                let integrity = self.integrity.take()?;
                let (_, checksum) = integrity.finish();
                (checksum != self.expected_checksum).then_some(Err(GetStreamError::Integrity))
            }
        }
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
    conditions: &ObjectConditions<'_>,
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
    conditions: &ObjectConditions<'_>,
) -> Result<PreparedGet, RetrievalError> {
    check_conditions(record, conditions)?;
    let requested = resolve_range(range_header, record.logical_length).map_err(RetrievalError::Range)?;
    let interval = requested.unwrap_or(ByteRange {
        start: 0,
        end: record.logical_length,
    });
    let stream = if interval.start == interval.end {
        None
    } else {
        let locations: Vec<Location> =
            bincode::deserialize(&record.data_reference).map_err(|_| RetrievalError::DataReference)?;
        Some(VerifiedGetStream {
            inner: client
                .read_range_stream(&locations, interval.start, interval.end)
                .map_err(|_| RetrievalError::ChunkRead)?,
            integrity: requested.is_none().then(SinglePartIntegrity::default),
            expected_checksum: record.checksum.clone(),
            terminal: false,
        })
    };
    Ok(PreparedGet {
        headers: headers(record, requested),
        stream,
        partial: requested.is_some(),
    })
}

fn check_conditions(record: &ObjectRecord, conditions: &ObjectConditions<'_>) -> Result<(), RetrievalError> {
    match evaluate(record, conditions) {
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
        content_range,
    }
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Error types for the chunk data path.

use thiserror::Error;

/// Error type for chunk IO operations.
#[derive(Debug, Error)]
pub enum IoError {
    #[error("chunk allocation failed: {0}")]
    AllocationFailed(String),
    #[error("disk write failed: {0}")]
    WriteFailed(String),
    #[error("disk read failed: {0}")]
    ReadFailed(String),
    #[error("chunk not found: {0}")]
    ChunkNotFound(String),
    #[error("chunk metadata conflict: {0}")]
    MetadataConflict(String),
    #[error("source read failed: {0}")]
    SourceRead(String),
    #[error("invalid disk IO topology: {0}")]
    Topology(String),
    #[error("EC encode failed: {0}")]
    EcEncodeFailed(String),
    #[error("memory budget exhausted")]
    MemoryBudgetExhausted,
    #[error("object size {size} exceeds small-object limit {limit}")]
    ObjectTooLarge { size: usize, limit: usize },
    #[error("object size mismatch: declared {declared} bytes, received {actual}")]
    ObjectSizeMismatch { declared: usize, actual: usize },
    #[error("writer already finished")]
    Finished,
    #[error("internal error: {0}")]
    Internal(String),
}

/// Error returned by object and range reads.
#[derive(Debug, Error)]
pub enum ReadError {
    #[error("invalid object locations: {0}")]
    InvalidLocations(String),
    #[error("invalid logical range [{start}, {end}) for object length {object_length}")]
    InvalidRange {
        start: u64,
        end: u64,
        object_length: u64,
    },
    #[error("chunk was deleted: {0}")]
    ChunkDeleted(String),
    #[error("requested bytes are not yet durably available: {0}")]
    NotYetAvailable(String),
    #[error("chunk layout expired before its reads completed")]
    LayoutExpired,
    #[error("unrecoverable strip data: {0}")]
    DataLoss(String),
    #[error("chunk metadata read failed: {0}")]
    Metadata(String),
    #[error("disk read failed: {0}")]
    DiskIo(String),
    #[error("EC reconstruction failed: {0}")]
    EcDecode(String),
    #[error("object bytes [{start}, {end}) could not be read: {message}")]
    FailedRange { start: u64, end: u64, message: String },
}

/// Result alias for object and range reads.
pub type ReadResult<T> = std::result::Result<T, ReadError>;

/// Result alias.
pub type Result<T> = std::result::Result<T, IoError>;

impl From<crowdb_chunkdb_client::ChunkdbClientError> for IoError {
    fn from(e: crowdb_chunkdb_client::ChunkdbClientError) -> Self {
        match e {
            crowdb_chunkdb_client::ChunkdbClientError::NotFound(message) => Self::ChunkNotFound(message),
            crowdb_chunkdb_client::ChunkdbClientError::Aborted(message) => Self::MetadataConflict(message),
            other => Self::AllocationFailed(other.to_string()),
        }
    }
}

impl From<crowdb_diskio_client::DiskioError> for IoError {
    fn from(e: crowdb_diskio_client::DiskioError) -> Self {
        Self::WriteFailed(e.to_string())
    }
}

impl From<crowdb_common::ec::EcError> for IoError {
    fn from(e: crowdb_common::ec::EcError) -> Self {
        Self::EcEncodeFailed(e.to_string())
    }
}

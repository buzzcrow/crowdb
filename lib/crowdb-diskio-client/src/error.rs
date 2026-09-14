// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Typed semantic `DiskIO` failures.

use thiserror::Error;

/// Semantic `DiskIO` operation failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DiskioError {
    #[error("invalid DiskIO input: {0}")]
    InvalidInput(String),
    #[error("DiskIO topology unavailable: {0}")]
    TopologyUnavailable(String),
    #[error("inconsistent DiskIO topology: {0}")]
    TopologyInconsistent(String),
    #[error("DiskIO transport unavailable: {0}")]
    TransportUnavailable(String),
    #[error("DiskIO queue rejected the operation: {0}")]
    Backpressure(String),
    #[error("permanent disk failure: {0}")]
    DiskFailure(String),
    #[error("DiskIO reported a partial write")]
    PartialWrite,
    #[error("DiskIO durability failed: {0}")]
    DurabilityFailure(String),
    #[error("DiskIO write outcome is ambiguous: {0}")]
    AmbiguousWrite(String),
    #[error("DiskIO operation deadline elapsed")]
    DeadlineExceeded,
    #[error("invalid DiskIO response: {0}")]
    Protocol(String),
}

impl DiskioError {
    #[must_use]
    pub fn is_retryable_read(&self) -> bool {
        matches!(
            self,
            Self::TransportUnavailable(_) | Self::Backpressure(_) | Self::DeadlineExceeded
        )
    }
}

pub type DiskioResult<T> = Result<T, DiskioError>;

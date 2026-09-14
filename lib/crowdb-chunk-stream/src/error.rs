// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StreamError {
    #[error("invalid stream request: {0}")]
    InvalidRequest(String),
    #[error("stream admission is full")]
    Backpressure,
    #[error("writer epoch is stale")]
    StaleWriter,
    #[error("append definitely did not commit: {0}")]
    DefinitelyNotCommitted(String),
    #[error("append outcome is being resolved")]
    ResolvingAmbiguousAppend,
    #[error("stream writes are stalled pending reopen")]
    WriteStalled,
    #[error("stream read is unavailable: {0}")]
    ReadUnavailable(String),
    #[error("stream metadata or data is corrupt: {0}")]
    Corruption(String),
    #[error("internal stream invariant failed: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, StreamError>;

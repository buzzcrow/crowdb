// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use thiserror::Error;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChunkKvError {
    #[error("partition request is outside its owned range")]
    OutOfRange,
    #[error("partition ownership epoch is stale")]
    StaleEpoch,
    #[error("partition is overloaded")]
    Overloaded,
    #[error("partition is recovering")]
    Recovering,
    #[error("partition writes are stalled")]
    WriteStalled,
    #[error("partition is not serving: {0}")]
    NotServing(String),
    #[error("request identity conflicts with another logical operation")]
    RequestConflict,
    #[error("request result is older than the retained retry floor")]
    RequestExpired,
    #[error("invalid partition request: {0}")]
    InvalidRequest(String),
    #[error("journal frame is incomplete")]
    IncompleteFrame,
    #[error("journal is corrupt: {0}")]
    JournalCorruption(String),
    #[error("tree data is unavailable: {0}")]
    TreeUnavailable(String),
    #[error("tree data or metadata is corrupt: {0}")]
    TreeCorruption(String),
    #[error("tree apply completion is unknown")]
    ApplyStateUnknown,
    #[error("maintenance is degraded: {0}")]
    MaintenanceDegraded(String),
    #[error("split is retryable: {0}")]
    SplitRetry(String),
    #[error("partition is faulted: {0}")]
    Faulted(String),
    #[error("internal partition invariant failed: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, ChunkKvError>;

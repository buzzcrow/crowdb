// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Native scheduling and semantic failures, independent of HTTP encoding.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacityKind {
    Requests,
    Producers,
    HeldBuffers,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationInterruption {
    Cancelled,
    Deadline,
    Shutdown,
    Transport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadFailureKind {
    InvalidLocations,
    InvalidRange,
    Deleted,
    NotAvailable,
    LayoutExpired,
    DataLoss,
    Metadata,
    DiskIo,
    EcDecode,
    FailedRange,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologySource {
    Catalog,
    ChunkLayout,
    ChunkRoutes,
    DiskRoutes,
    Services,
    MetadataTransport,
    DataTransport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyFailureKind {
    Unavailable,
    Invalid,
    Stale,
    Fenced,
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum Error {
    #[error("invalid native client configuration: {0}")]
    Configuration(String),
    #[error("native runtime is shut down")]
    Shutdown,
    #[error("operation was cancelled")]
    Cancelled,
    #[error("operation deadline expired")]
    Deadline,
    #[error("native capacity exhausted: {0:?}")]
    Capacity(CapacityKind),
    #[error("native storage unavailable: {0}")]
    Storage(String),
    #[error("invalid buffer range or initialized length")]
    BufferRange,
    #[error("S3 operation failed: {0:?}")]
    S3(crowdb_access_s3::S3ErrorCode),
    #[error("object was not modified")]
    NotModified,
    #[error("object checksum does not match its published metadata")]
    Integrity,
    #[error("object length mismatch: expected={expected} delivered={delivered}")]
    Length { expected: u64, delivered: u64 },
    #[error("object read failed: {kind:?}: {detail}")]
    Read { kind: ReadFailureKind, detail: String },
    #[error("topology {origin:?} failed: {kind:?}: {detail}")]
    Topology {
        origin: TopologySource,
        kind: TopologyFailureKind,
        detail: String,
    },
    #[error("mutation outcome is uncertain: request={request_id} reason={reason:?}")]
    UncertainMutation {
        request_id: u64,
        reason: MutationInterruption,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

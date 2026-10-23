// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Physical prefix observations for an exclusively owned/excluded last chunk.
//! Ordinary object reads retain their acknowledged-cursor bound.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::frame::{FrameScanConfig, FrameScanError, FrameScanSummary, ScannedFrameExtent};
use thiserror::Error;

use crate::IoError;

mod mirrored;
mod prefix;
pub use mirrored::MirroredFrameReaders;
mod replicas;
pub use prefix::{observe_frame_prefix, replay_frame_prefix};
pub use replicas::{select_frame_prefix, FrameReplicaSelection};

pub const MAX_FRAME_OBSERVATION_REPLICAS: usize = 5;

/// Supplies exact physical windows from one stable layout observation. Callers
/// establish owner exclusion and layout/read deadlines before constructing it.
/// IO failures and short reads must not be replaced with fabricated zero bytes.
#[async_trait]
pub trait FrameWindowReader: Send + Sync {
    async fn read_window(&self, offset: u64, length: u32) -> crate::Result<Bytes>;
}

/// Receives bounded mapping batches only during replay of a validated source.
/// A failed replay must never publish its partial mappings as a stable root.
#[async_trait]
pub trait FrameExtentSink: Send {
    async fn append_extents(&mut self, extents: &[ScannedFrameExtent]) -> crate::Result<()>;
}

#[derive(Clone, Copy, Debug)]
pub struct FrameObservation {
    config: FrameScanConfig,
    window_bytes: u32,
    summary: FrameScanSummary,
    reads: u64,
    read_bytes: u64,
}

impl FrameObservation {
    #[must_use]
    pub const fn summary(&self) -> &FrameScanSummary {
        &self.summary
    }
    #[must_use]
    pub const fn reads(&self) -> u64 {
        self.reads
    }
    #[must_use]
    pub const fn read_bytes(&self) -> u64 {
        self.read_bytes
    }
}

#[derive(Debug, Error)]
pub enum FrameObservationError {
    #[error("invalid physical frame layout: {0}")]
    InvalidLayout(String),
    #[error("invalid physical prefix observation bounds or replica budget")]
    InvalidRequest,
    #[error("physical prefix observation deadline expired")]
    DeadlineExpired,
    #[error("physical read at {offset} failed: {source}")]
    Read { offset: u64, source: IoError },
    #[error("short physical read at {offset}: expected {expected} bytes, received {actual}")]
    ShortRead {
        offset: u64,
        expected: u32,
        actual: usize,
    },
    #[error(transparent)]
    InvalidPrefix(#[from] FrameScanError),
    #[error("mapping sink failed: {0}")]
    Sink(IoError),
    #[error("no healthy completed replica observation: {failures:?}")]
    NoHealthyReplica { failures: Vec<String> },
    #[error("valid replicas {first} and {second} conflict through complete boundary {physical_end}")]
    ConflictingPrefixes {
        first: usize,
        second: usize,
        physical_end: u64,
    },
    #[error("prefix comparison of replica {replica} failed: {source}")]
    Comparison {
        replica: usize,
        source: Box<FrameObservationError>,
    },
    #[error("replica observation task failed: {0}")]
    Task(String),
    #[error("replayed prefix differs from its validated observation")]
    ChangedPrefix,
}

pub type FrameObservationResult<T> = Result<T, FrameObservationError>;

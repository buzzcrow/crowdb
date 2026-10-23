// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded current-format prefix parsing shared by recovery and finalization.

use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{AppendGroupValidator, ChunkId, FrameError, MAX_FRAME_BYTES};

mod complete;
mod input;

pub const MAX_FRAME_SCAN_INPUT_BYTES: usize = 1024 * 1024;
pub const MAX_FRAME_SCAN_EXTENTS: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameScanMode {
    Ordinary,
    Grouped,
}

#[derive(Clone, Copy, Debug)]
pub struct FrameScanConfig {
    pub chunk_id: ChunkId,
    pub mode: FrameScanMode,
    pub physical_start: u64,
    pub physical_limit: u64,
    pub required_end: u64,
    pub logical_start: u64,
    pub max_append_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameScanStop {
    Running,
    Failed,
    Unwritten,
    IncompleteAtCapacity,
    ExactCapacity,
}

/// One complete grouped append, or one canonical ordinary frame. Empty ordinary
/// frames can have zero logical length; Stream groups are always nonempty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScannedFrameExtent {
    pub physical_offset: u64,
    pub physical_length: u64,
    pub logical_offset: u64,
    pub logical_length: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameScanSummary {
    pub observed_end: u64,
    pub physical_end: u64,
    pub logical_end: u64,
    pub complete_extents: u64,
    pub frames_parsed: u64,
    /// SHA-256 of the exact encoded complete prefix, excluding incomplete groups.
    pub digest: [u8; 32],
    pub stop: FrameScanStop,
}

#[derive(Debug)]
pub struct FrameScanBatch {
    pub consumed: usize,
    pub extents: Vec<ScannedFrameExtent>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameScanError {
    #[error("frame observation has already failed")]
    FailedObservation,
    #[error("invalid frame scan bounds or batch budget")]
    InvalidBounds,
    #[error("frame scan input is not contiguous")]
    OutOfOrder,
    #[error("invalid frame at physical offset {offset}: {source}")]
    Frame { offset: u64, source: FrameError },
    #[error("complete prefix {observed} does not cover required boundary {required}")]
    RequiredPrefix { required: u64, observed: u64 },
    #[error("required checkpoint {required} is inside a grouped append ending at {observed}")]
    CheckpointBoundary { required: u64, observed: u64 },
    #[error("observation stopped at {observed} before allocated limit {limit}")]
    IncompleteObservation { observed: u64, limit: u64 },
}

struct PendingGroup {
    validator: AppendGroupValidator,
    physical_start: u64,
    logical_length: u64,
    hash: Sha256,
}

/// Keeps only one bounded frame, constant group/hash state and scalar progress.
/// A caller must finish the observation before treating its summary as healthy.
pub struct FramePrefixScanner {
    config: FrameScanConfig,
    input_end: u64,
    physical_end: u64,
    logical_end: u64,
    complete_extents: u64,
    frames_parsed: u64,
    buffer: Vec<u8>,
    group: Option<PendingGroup>,
    hash: Sha256,
    stop: FrameScanStop,
}

impl FramePrefixScanner {
    #[must_use]
    pub const fn stop(&self) -> FrameScanStop {
        self.stop
    }

    /// # Errors
    /// Rejects inverted physical bounds, an out-of-range checkpoint or empty limits.
    pub fn new(config: FrameScanConfig) -> Result<Self, FrameScanError> {
        if config.physical_start > config.required_end
            || config.required_end > config.physical_limit
            || config.max_append_bytes == 0
        {
            return Err(FrameScanError::InvalidBounds);
        }
        Ok(Self {
            input_end: config.physical_start,
            physical_end: config.physical_start,
            logical_end: config.logical_start,
            complete_extents: 0,
            frames_parsed: 0,
            buffer: Vec::with_capacity(MAX_FRAME_BYTES),
            group: None,
            hash: Sha256::new(),
            stop: FrameScanStop::Running,
            config,
        })
    }

    #[must_use]
    pub fn summary(&self) -> FrameScanSummary {
        FrameScanSummary {
            observed_end: self.input_end,
            physical_end: self.physical_end,
            logical_end: self.logical_end,
            complete_extents: self.complete_extents,
            frames_parsed: self.frames_parsed,
            digest: self.hash.clone().finalize().into(),
            stop: self.stop,
        }
    }

    /// Ends an exact allocated-range observation. Short IO is an unavailable
    /// observation, never a valid shorter prefix. An incomplete group is omitted.
    /// # Errors
    /// Rejects premature observation completion and an uncovered required prefix.
    pub fn finish(&mut self) -> Result<FrameScanSummary, FrameScanError> {
        if self.stop == FrameScanStop::Failed {
            return Err(FrameScanError::FailedObservation);
        }
        let result = self.finish_observation();
        if result.is_err() {
            self.stop = FrameScanStop::Failed;
        }
        result
    }

    fn finish_observation(&mut self) -> Result<FrameScanSummary, FrameScanError> {
        if self.stop == FrameScanStop::Running {
            if self.input_end != self.config.physical_limit {
                return Err(FrameScanError::IncompleteObservation {
                    observed: self.input_end,
                    limit: self.config.physical_limit,
                });
            }
            self.stop = if self.buffer.is_empty() && self.group.is_none() {
                FrameScanStop::ExactCapacity
            } else {
                FrameScanStop::IncompleteAtCapacity
            };
        }
        self.check_required()?;
        self.group = None;
        self.buffer.clear();
        Ok(self.summary())
    }

    fn check_required(&self) -> Result<(), FrameScanError> {
        if self.physical_end < self.config.required_end {
            return Err(FrameScanError::RequiredPrefix {
                required: self.config.required_end,
                observed: self.physical_end,
            });
        }
        Ok(())
    }
}

fn frame_error(offset: u64, source: FrameError) -> FrameScanError {
    FrameScanError::Frame { offset, source }
}

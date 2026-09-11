// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Epoch-fenced range-partitioned KV state machine over chunk storage.

mod error;
mod manager;
mod metrics;
mod partition;
mod types;

#[cfg(feature = "test-util")]
pub mod memory;

pub use error::{ChunkKvError, Result};
pub use manager::PartitionManager;
pub use metrics::{PartitionMetrics, PartitionMetricsSnapshot};
pub use partition::{
    decode_frame, encode_frame, CrowdbPartitionTree, DecodedFrame, FrameDecode, MutationResponse, Partition,
    PartitionConfig, PartitionJournal, PartitionSnapshot, PartitionTree, ScanEntry, ScanPage,
    StreamPartitionJournal, MAX_FRAME_BYTES,
};
pub use types::{
    canonical_operation_digest, Checkpoint, CompareCondition, JournalPosition, MutationOperation,
    MutationResult, PartitionId, PartitionLifecycle, PartitionRange, PreparedChildArtifact, RequestId,
    SplitAbortProof, SplitArtifact, SplitChild, SplitCommitProof, SplitPlan, TransitionId, ValueRevision,
    WalRecord,
};

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Epoch-fenced range-partitioned KV state machine over chunk storage.

mod error;
mod frame;
mod journal;
mod partition;
mod tree;
mod types;

#[cfg(feature = "test-util")]
pub mod memory;

pub use error::{ChunkKvError, Result};
pub use frame::{decode_frame, encode_frame, DecodedFrame, FrameDecode, MAX_FRAME_BYTES};
pub use journal::{PartitionJournal, StreamPartitionJournal};
pub use partition::{MutationResponse, Partition, PartitionConfig, PartitionSnapshot};
pub use tree::{CrowdbPartitionTree, PartitionTree};
pub use types::{
    canonical_operation_digest, Checkpoint, CompareCondition, JournalPosition, MutationOperation,
    MutationResult, PartitionId, PartitionLifecycle, PartitionRange, RequestId, ValueRevision, WalRecord,
};

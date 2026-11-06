// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Safe Rust adapter over the crowdb-tree C ABI (`c_api.h`, PT8).
//!
//! Wraps the opaque `ct_*` handles in RAII types, translates owned `ct_buf`
//! buffers into `Vec<u8>` (freeing them via `ct_free_buf`), maps `ct_status`
//! into `Result`, and offers an async facade (`AsyncCrowdbtree`). `get`/`flush`/
//! `snapshot`/`scan` drive the engine's io_uring reactor directly (no OS
//! thread hop, Phase 3); the remaining methods (no async C API twin exists
//! for them yet) are called via the synchronous `Crowdbtree` handle.

pub mod admission;
pub mod async_tree;
pub mod backend_release;
pub mod batch;
pub mod chunk;
pub mod config;
pub mod cpp_global_metrics;
pub mod crc;
pub mod error;
pub mod immutable_snapshot;
pub mod maintenance;
mod owner_budget;
pub mod page;
pub mod reactor;
pub mod runtime;
pub mod scan;
pub mod snapshot;
pub mod stats;
pub(crate) mod sys;
pub mod tree;
pub mod tuning;
pub use tuning::TreeTuningOptions;
pub mod uring;
pub mod write_handle;

pub use admission::{ArmedApply, PreparedApply};
pub use async_tree::{AsyncCrowdbtree, GetOutcome, OwnedGetOutcome, PinnedGetOutcome, ScanOutcome};
pub use backend_release::BackendReleaseService;
pub use batch::{BatchOp, ExtOp, OwnedAllocation, OwnedOp};
pub use chunk::{
    ChunkPageStoreOptions, ChunkPageStoreStats, ChunkRootCatalog, ChunkRpcDiskRoute, ChunkRpcRoute,
    ChunkRpcTransportOptions, ChunkRuntimeOptions, ChunkTransport, RootCatalogObject, RootCatalogStore,
};
#[cfg(feature = "chunk-rpc")]
pub use chunk::{ChunkRpcRouteResolver, OwnedChunkRpcDiskRoute, OwnedChunkRpcTransportOptions};
pub use config::{
    Compression, Config, KeyRange, PageStore, PageStoreBackend, SyncMode, TreeBindingConfig,
    TreeEngineConfig, TreeHostConfig, TreeResourceConfig, TreeRuntimeConfig,
};
pub use cpp_global_metrics::{cpp_global_metrics_max_name_len, flush_cpp_global_metrics};
pub use crc::{crc32c, crc32c_update};
pub use error::CtError;
pub use immutable_snapshot::ImmutableTreeSnapshot;
pub use maintenance::{CheckpointReport, FlushPhase, FlushReport};
pub use reactor::PinnedValue;
pub use runtime::{
    TreeBindingOptions, TreeExecutorSnapshot, TreeResourceLimits, TreeRuntime, TreeRuntimeOptions,
    TreeRuntimeSnapshot,
};
pub use scan::{ScanDirection, ScanEntry, ScanOptions, ViewEntry};
pub use snapshot::{
    OwnedSnapshotChunk, SnapshotChunk, SnapshotExportSession, SnapshotImportSession, SnapshotMetadata,
};
pub use stats::{
    BackingStatsSnapshot, MergeGcStats, RangeRebuildStats, Stats, TreeFlushDispatchStats, TreeHistogramStats,
    TreeLogicalStats, TreeProgressSnapshot, TreeResourceStats, TreeStatsSnapshot, TreeSummary,
    TreeWorkCollector, TreeWorkCounters, TreeWorkHistograms, TreeWorkSnapshot, TreeWorkStage,
    TreeWorkStageAge, TreeWorkStageStats, TREE_WORK_CAUSE_NAMES, TREE_WORK_COUNTER_NAMES,
    TREE_WORK_STAGE_NAMES,
};
pub use tree::{
    ct_add_log_stderr, ct_flush_logging, ct_init_logging, ct_init_test_logging, ct_shutdown_logging,
    Crowdbtree, InstalledSplit, PreparedSplitOverlay, SplitOverlayLimits,
};
pub use uring::{Uring, UringFile};
pub use write_handle::WriteHandle;

pub use async_tree::{CompactionOptions, CompactionReport};

pub use stats::{TreeWaitSnapshot, TreeWaitStats, TREE_WAIT_CLASS_NAMES};

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkClientConfig` — shared configuration for the chunk data path.
//!
//! Replaces `WriterConfig`. All writers (`LargeObjectWriter`,
//! `LargeAsyncObjectWriter`, `SmallObjectWriter`, `WriterPool`,
//! `ChunkPrefetch`, `ChunkWriter`, `EcStripWriter`) read the fields
//! they need from this single config.

use std::time::Duration;

use crowdb_common::ec::EcScheme;

use crate::IoError;

/// Bounded aggregation and elasticity policy for shared small writes.
#[derive(Debug, Clone)]
pub struct SmallWritePolicy {
    pub object_limit: usize,
    pub memory_budget: usize,
    pub queue_capacity: usize,
    pub min_pipelines: usize,
    pub max_pipelines: usize,
    pub max_batch_bytes: usize,
    pub max_batch_objects: usize,
    /// Reports a batch that remains in flight beyond this interval. The
    /// watchdog does not delay queue draining or cancel durability work.
    pub batch_watchdog: Duration,
    pub scale_out_queue_bytes: usize,
    pub scale_out_queue_objects: usize,
    /// Compatibility setting retained for callers; queue state alone decides scale-in.
    pub scale_in_delay: Duration,
    pub control_interval: Duration,
    /// Compatibility setting retained for callers; it is not a scaling signal.
    pub cooldown: Duration,
    pub chunk_capacity: u64,
    /// Mirror strips attached per allocation/prefetch RPC. Default 4.
    pub small_strip_prefetch_count: u32,
    pub mirror_copies: u32,
    pub conversion_enabled: bool,
    pub conversion_data_num: usize,
    pub conversion_code_num: usize,
    pub writer_lease: Duration,
    pub failed_disk_ttl: Duration,
    pub repair_attempts_per_replica: usize,
}

impl Default for SmallWritePolicy {
    fn default() -> Self {
        const MIB: usize = 1024 * 1024;
        Self {
            object_limit: MIB,
            memory_budget: 96 * MIB,
            queue_capacity: 1_024,
            min_pipelines: 1,
            max_pipelines: 32,
            max_batch_bytes: MIB,
            max_batch_objects: 1_024,
            batch_watchdog: Duration::from_millis(500),
            scale_out_queue_bytes: 4 * MIB,
            scale_out_queue_objects: 128,
            scale_in_delay: Duration::from_secs(30),
            control_interval: Duration::from_millis(10),
            cooldown: Duration::from_millis(100),
            chunk_capacity: 1024 * 1024 * 1024,
            small_strip_prefetch_count: 4,
            mirror_copies: 3,
            conversion_enabled: true,
            conversion_data_num: 8,
            conversion_code_num: 4,
            writer_lease: Duration::from_secs(30),
            failed_disk_ttl: Duration::from_secs(60),
            repair_attempts_per_replica: 3,
        }
    }
}

impl SmallWritePolicy {
    pub fn validate(&self) -> Result<(), IoError> {
        const HARD_LIMIT: usize = 1024 * 1024;
        if self.object_limit == 0 || self.object_limit > HARD_LIMIT {
            return Err(IoError::Internal(
                "small object limit must be in 1..=1 MiB".into(),
            ));
        }
        let shadow_budget = self.max_pipelines.saturating_mul(HARD_LIMIT);
        let conversion_budget = if self.conversion_enabled {
            self.conversion_data_num
                .saturating_add(self.conversion_code_num)
                .saturating_mul(HARD_LIMIT)
        } else {
            0
        };
        if self.memory_budget
            < self
                .object_limit
                .saturating_add(shadow_budget)
                .saturating_add(conversion_budget)
        {
            return Err(IoError::Internal(
                "small write memory budget must cover pipeline shadows, one conversion group, and one object"
                    .into(),
            ));
        }
        if self.memory_budget > u32::MAX as usize {
            return Err(IoError::Internal(
                "small write memory budget must fit Tokio semaphore permits".into(),
            ));
        }
        if self.queue_capacity == 0
            || self.min_pipelines == 0
            || self.min_pipelines > self.max_pipelines
            || self.max_batch_bytes == 0
            || self.max_batch_objects == 0
            || self.scale_out_queue_bytes == 0
            || self.scale_out_queue_bytes > self.memory_budget
            || self.scale_out_queue_objects == 0
            || self.scale_out_queue_objects > self.queue_capacity
            || self.chunk_capacity < self.object_limit as u64
            || self.small_strip_prefetch_count == 0
            || self.mirror_copies == 0
            || self.conversion_data_num == 0
            || self.conversion_code_num == 0
            || self.batch_watchdog.is_zero()
            || self.control_interval.is_zero()
            || self.writer_lease.is_zero()
            || self.failed_disk_ttl.is_zero()
            || self.repair_attempts_per_replica == 0
        {
            return Err(IoError::Internal("invalid small write policy".into()));
        }
        Ok(())
    }
}

/// Configuration for the chunk data path. Shared by all writers.
#[derive(Debug, Clone)]
pub struct ChunkClientConfig {
    // ── write path ──────────────────────────────────────────────
    /// Fetch read granularity / block size (bytes). Default 1 MB.
    pub read_buffer_size: usize,
    /// Max un-written data in fetch cache (bytes). Default 4 MB.
    pub max_cached_buffer: usize,

    // ── large write ─────────────────────────────────────────────
    /// Max chunk size before rotation (bytes). Default 1 GB.
    pub max_chunk_size: u64,
    /// Strips allocated ahead of the write cursor. Default 2.
    pub prefetch_strips_per_chunk: usize,
    /// Maximum completed-strip parity/finalization tasks in flight. Default 2.
    pub parity_depth: usize,
    /// Chunks allocated ahead. Default 1.
    pub chunk_preparation_depth: usize,
    /// Placement-safe replacement attempts for one failed EC segment.
    pub large_write_repair_attempts: usize,

    // ── memory ──────────────────────────────────────────────────
    /// Memory budget for `WriterPool` (bytes). Default 0 = unlimited
    /// (caller sets per-pool).
    pub memory_budget: usize,
}

impl Default for ChunkClientConfig {
    fn default() -> Self {
        const MB: usize = 1024 * 1024;
        const GB: usize = 1024 * 1024 * 1024;
        Self {
            read_buffer_size: MB,
            max_cached_buffer: 4 * MB,
            max_chunk_size: GB as u64,
            prefetch_strips_per_chunk: 2,
            parity_depth: 2,
            chunk_preparation_depth: 1,
            large_write_repair_attempts: 3,
            memory_budget: 0,
        }
    }
}

impl ChunkClientConfig {
    /// Validate config fields. Returns `Err` on invalid combinations.
    pub fn validate(&self) -> Result<(), IoError> {
        if self.read_buffer_size == 0 {
            return Err(IoError::Internal("read_buffer_size must be > 0".into()));
        }
        if self.max_cached_buffer < self.read_buffer_size {
            return Err(IoError::Internal(
                "max_cached_buffer must be >= read_buffer_size".into(),
            ));
        }
        if self.max_chunk_size == 0 {
            return Err(IoError::Internal("max_chunk_size must be > 0".into()));
        }
        if self.prefetch_strips_per_chunk == 0 {
            return Err(IoError::Internal("prefetch_strips_per_chunk must be > 0".into()));
        }
        if self.parity_depth == 0 {
            return Err(IoError::Internal("parity_depth must be > 0".into()));
        }
        if self.chunk_preparation_depth == 0 {
            return Err(IoError::Internal("chunk_preparation_depth must be > 0".into()));
        }
        if self.large_write_repair_attempts == 0 {
            return Err(IoError::Internal(
                "large_write_repair_attempts must be > 0".into(),
            ));
        }
        Ok(())
    }

    /// Per-writer memory footprint for `WriterPool` budgeting.
    /// Formula: max_cached_buffer + 1 block (fetch) + parity_depth *
    /// total_blocks * block.
    pub fn per_writer_memory(&self, ec_scheme: &EcScheme) -> usize {
        let block = self.read_buffer_size;
        self.max_cached_buffer + block + self.parity_depth * ec_scheme.total_blocks() * block
    }
}

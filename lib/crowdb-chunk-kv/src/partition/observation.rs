// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Metadata and counters from the currently opened storage handles.

use super::Partition;
use crate::Result;

#[derive(Debug)]
pub struct TreeObservation {
    pub checkpoint_manifest: u64,
    pub checkpoint_applied_seq: u64,
    pub runtime: Option<crowdb_tree_ffi::Stats>,
    pub summary: Option<crowdb_tree_ffi::TreeSummary>,
    pub maintenance: crate::PartitionMetricsSnapshot,
}

impl Partition {
    /// Reads a bounded structural page from this partition's actual tree.
    ///
    /// # Errors
    /// Returns a storage error or a changed-tree observation.
    pub fn inspect_page(
        &self,
        path: &[u32],
        version: Option<u64>,
    ) -> Result<crowdb_tree_ffi::page::PageInspection> {
        self.tree.inspect_page(path, version)
    }

    /// Samples existing metadata and counters without flushing or reading pages.
    ///
    /// # Errors
    /// Returns a storage error when the opened checkpoint are unavailable.
    pub fn observe_tree(&self) -> Result<TreeObservation> {
        let (checkpoint_manifest, checkpoint_applied_seq) = self.tree.checkpoint_state()?;
        Ok(TreeObservation {
            checkpoint_manifest,
            checkpoint_applied_seq,
            runtime: self.tree.runtime_stats(),
            summary: self.tree.tree_summary(),
            maintenance: self.metrics.snapshot(),
        })
    }

    /// Copies a bounded journal extent index from its published manifest.
    ///
    /// # Errors
    /// Returns a stale-generation or invalid-continuation error.
    pub fn observe_journal(
        &self,
        generation: Option<u64>,
        offset: usize,
    ) -> Result<Option<crowdb_chunk_stream::StreamMetadataObservation>> {
        self.journal.observe_metadata(generation, offset)
    }
}

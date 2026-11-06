// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! An admitted source prefix ready for a non-waiting route cutover.

use crate::{ChunkKvError, Result};
use async_trait::async_trait;
use std::sync::Arc;

/// Complete mutation work admitted ahead of the cutover control message.
#[derive(Clone, Copy, Debug)]
pub struct SplitOverlayLimits {
    pub max_records: usize,
    pub max_bytes: u64,
}

/// The immutable native capture retained until background publication finishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstalledSplit {
    pub generation: u64,
    pub frontier: u64,
}

/// Owns all preparation needed before publishing one inherited source prefix.
#[async_trait]
pub trait PreparedSplitOverlay: Send + Sync {
    /// Prepare the destination lease after replay covers the registered start.
    ///
    /// # Errors
    /// A changed destination or expired observer leaves the child uninstalled.
    async fn prepare(&self, replayed_prefix: u64) -> Result<()>;

    /// Commit the exact completed parent frontier without waiting or allocating.
    ///
    /// # Errors
    /// A stale or exhausted preparation leaves the destination unchanged.
    fn try_install(&self, frontier: u64) -> Result<InstalledSplit>;

    /// Returns the sole committed frontier, including after an idempotent retry.
    fn committed_frontier(&self) -> Option<u64>;
}

pub(super) struct NativePreparedOverlay(pub(super) Arc<crowdb_tree_ffi::PreparedSplitOverlay>);

#[async_trait]
impl PreparedSplitOverlay for NativePreparedOverlay {
    async fn prepare(&self, replayed_prefix: u64) -> Result<()> {
        let prepared = self.0.clone();
        tokio::task::spawn_blocking(move || prepared.prepare(replayed_prefix))
            .await
            .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
            .map_err(super::map_tree_read_error)
    }

    fn try_install(&self, frontier: u64) -> Result<InstalledSplit> {
        let installed = self.0.try_install(frontier).map_err(super::map_tree_read_error)?;
        Ok(InstalledSplit {
            generation: installed.generation,
            frontier: installed.frontier,
        })
    }

    fn committed_frontier(&self) -> Option<u64> {
        self.0.committed_frontier()
    }
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! WAL publication is a background readiness proof, separate from route installation.

use crate::{ChunkKvError, Partition, Result};

impl Partition {
    /// Publish the parent journal prefix required by a split recovery reader.
    /// This control-plane operation does not occupy the partition mutation worker.
    /// The stream's own ordered checkpoint can still delay its later appends.
    ///
    /// # Errors
    /// Returns publication failures or a journal that does not cover the fixed end.
    pub async fn publish_split_journal_prefix(&self, minimum_end: u64) -> Result<u64> {
        let published_end = self.journal.checkpoint().await?;
        if published_end < minimum_end {
            return Err(ChunkKvError::JournalCorruption(
                "published parent WAL does not cover the installed split prefix".into(),
            ));
        }
        Ok(published_end)
    }
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recovery reconciled with publication that advances while artifacts reopen.

use super::{ChunkKvRangeCatalogError, ChunkKvRangeCatalogPublisher};
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage};
use std::{fmt::Display, future::Future, time::Instant};

impl ChunkKvRangeCatalogPublisher {
    /// Recover a published catalog, discarding obsolete recovery results only
    /// when a newer authoritative generation proves publication advanced.
    ///
    /// # Errors
    /// Returns unchanged-generation recovery errors, catalog read errors, or
    /// generation conflicts when publication does not settle by the deadline.
    pub async fn recover_current<T, E: Display, F, R>(
        &self,
        deadline: Instant,
        mut recover: F,
    ) -> Result<Option<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>, T)>, ChunkKvRangeCatalogError>
    where
        F: FnMut(Vec<ChunkKvRangeCatalogPage>) -> R,
        R: Future<Output = Result<T, E>>,
    {
        let mut current = self.load_current().await?;
        while let Some((head, pages)) = current {
            let result = recover(pages.clone()).await;
            let latest = self.load_current().await?;
            if latest
                .as_ref()
                .is_some_and(|(next, _)| next.generation > head.generation)
            {
                if Instant::now() >= deadline {
                    return Err(ChunkKvRangeCatalogError::GenerationConflict);
                }
                current = latest;
                continue;
            }
            if latest.as_ref().map(|(next, _)| next) != Some(&head) {
                return Err(ChunkKvRangeCatalogError::GenerationConflict);
            }
            return result
                .map(|recovered| Some((head, pages, recovered)))
                .map_err(|error| ChunkKvRangeCatalogError::Unavailable(error.to_string()));
        }
        Ok(None)
    }
}

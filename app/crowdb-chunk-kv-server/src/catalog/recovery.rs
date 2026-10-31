// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recovery reconciled with publication that advances while artifacts reopen.

use super::{ChunkKvRangeCatalogError, ChunkKvRangeCatalogPublisher};
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage};
use std::{fmt::Display, future::Future, time::Instant};

impl ChunkKvRangeCatalogPublisher {
    /// Recovers this instance's assignments against the current published head.
    /// A successful replay remains usable when only remote assignments change;
    /// every local entry must still match exactly, including its epoch and artifact.
    /// # Errors
    /// Returns catalog or artifact recovery failures under the supplied deadline.
    pub async fn recover_current_for_instance<T, E: Display, F, R>(
        &self,
        instance_id: u64,
        deadline: Instant,
        recover: F,
    ) -> Result<Option<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>, T)>, ChunkKvRangeCatalogError>
    where
        F: FnMut(Vec<ChunkKvRangeCatalogPage>) -> R,
        R: Future<Output = Result<T, E>>,
    {
        self.recover_matching(deadline, Some(instance_id), recover).await
    }

    /// Recover a published catalog, discarding obsolete recovery results only
    /// when a newer authoritative generation proves publication advanced.
    ///
    /// # Errors
    /// Returns unchanged-generation recovery errors, catalog read errors, or
    /// generation conflicts when publication does not settle by the deadline.
    pub async fn recover_current<T, E: Display, F, R>(
        &self,
        deadline: Instant,
        recover: F,
    ) -> Result<Option<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>, T)>, ChunkKvRangeCatalogError>
    where
        F: FnMut(Vec<ChunkKvRangeCatalogPage>) -> R,
        R: Future<Output = Result<T, E>>,
    {
        self.recover_matching(deadline, None, recover).await
    }

    async fn recover_matching<T, E: Display, F, R>(
        &self,
        deadline: Instant,
        instance_id: Option<u64>,
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
                if result.is_ok()
                    && instance_id.is_some_and(|instance| {
                        let (_, next_pages) = latest.as_ref().expect("newer catalog exists");
                        local_assignments_match(&pages, next_pages, instance)
                    })
                {
                    let (head, pages) = latest.expect("newer catalog exists");
                    return result
                        .map(|recovered| Some((head, pages, recovered)))
                        .map_err(|error| ChunkKvRangeCatalogError::Unavailable(error.to_string()));
                }
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

fn local_assignments_match(
    before: &[ChunkKvRangeCatalogPage],
    after: &[ChunkKvRangeCatalogPage],
    instance_id: u64,
) -> bool {
    let local = |entry: &&crowdb_protocol::chunk_kv::ChunkKvRangeCatalogEntry| {
        crate::server::recoverable_local_entry(entry, instance_id)
    };
    before
        .iter()
        .flat_map(|page| &page.entries)
        .filter(local)
        .eq(after.iter().flat_map(|page| &page.entries).filter(local))
}

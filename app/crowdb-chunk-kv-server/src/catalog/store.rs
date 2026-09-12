// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use crowdb_protocol::chunk_kv::{ChunkKvProtocolError, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage};
use thiserror::Error;
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeadWriteOutcome {
    Committed,
    DefinitelyNotCommitted,
    Ambiguous,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChunkKvRangeCatalogError {
    #[error(transparent)]
    Invalid(#[from] ChunkKvProtocolError),
    #[error("catalog storage is unavailable: {0}")]
    Unavailable(String),
    #[error("catalog generation conflicts with the current head")]
    GenerationConflict,
    #[error("catalog head write definitely did not commit")]
    HeadNotCommitted,
    #[error("catalog head write outcome remains ambiguous")]
    AmbiguousHead,
    #[error("catalog references an absent or changed immutable page")]
    MissingPage,
    #[error("catalog transition is not ready for publication")]
    TransitionNotReady,
    #[error("catalog transition does not match the current generation")]
    TransitionConflict,
    #[error("catalog generation cannot advance")]
    GenerationOverflow,
}

#[async_trait]
pub trait ChunkKvRangeCatalogStore: Send + Sync {
    async fn put_page(&self, page: ChunkKvRangeCatalogPage) -> Result<(), ChunkKvRangeCatalogError>;
    async fn get_page(
        &self,
        generation: u64,
        page_index: u64,
    ) -> Result<Option<ChunkKvRangeCatalogPage>, ChunkKvRangeCatalogError>;
    async fn put_head(
        &self,
        head: ChunkKvRangeCatalogHead,
    ) -> Result<HeadWriteOutcome, ChunkKvRangeCatalogError>;
    async fn get_head(&self) -> Result<Option<ChunkKvRangeCatalogHead>, ChunkKvRangeCatalogError>;
}

pub struct ChunkKvRangeCatalogPublisher {
    store: Arc<dyn ChunkKvRangeCatalogStore>,
}

impl ChunkKvRangeCatalogPublisher {
    #[must_use]
    pub fn new(store: Arc<dyn ChunkKvRangeCatalogStore>) -> Self {
        Self { store }
    }

    /// Loads and validates the currently published generation.
    ///
    /// # Errors
    ///
    /// Returns an error when the head or any referenced immutable page is
    /// unavailable, changed, or invalid.
    pub async fn load_current(
        &self,
    ) -> Result<Option<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)>, ChunkKvRangeCatalogError>
    {
        let Some(head) = self.store.get_head().await? else {
            return Ok(None);
        };
        let pages = self.load_pages(&head).await?;
        head.validate_pages(&pages)?;
        Ok(Some((head, pages)))
    }

    /// Publishes one fully validated immutable generation, pages before head.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid/regressing input, storage failure, a
    /// definite absent head write, or an ambiguous outcome not proven by reread.
    pub async fn publish(
        &self,
        head: ChunkKvRangeCatalogHead,
        pages: Vec<ChunkKvRangeCatalogPage>,
    ) -> Result<(), ChunkKvRangeCatalogError> {
        head.validate_pages(&pages)?;
        let current = self.store.get_head().await?;
        if current.as_ref() == Some(&head) {
            return Ok(());
        }
        match current.as_ref() {
            Some(previous) => {
                let previous_pages = self.load_pages(previous).await?;
                head.validate_successor(&pages, previous, &previous_pages)?;
                if head.previous_generation != Some(previous.generation) {
                    return Err(ChunkKvRangeCatalogError::GenerationConflict);
                }
            }
            None if head.previous_generation.is_some() || head.generation != 1 => {
                return Err(ChunkKvRangeCatalogError::GenerationConflict);
            }
            None => {}
        }

        for page in pages.iter().filter(|page| page.generation == head.generation) {
            self.store.put_page(page.clone()).await?;
        }
        let stored_pages = self.load_pages(&head).await?;
        head.validate_pages(&stored_pages)?;
        match self.store.put_head(head.clone()).await? {
            HeadWriteOutcome::Committed => Ok(()),
            HeadWriteOutcome::DefinitelyNotCommitted => Err(ChunkKvRangeCatalogError::HeadNotCommitted),
            HeadWriteOutcome::Ambiguous => {
                if self.store.get_head().await?.as_ref() == Some(&head) {
                    Ok(())
                } else {
                    Err(ChunkKvRangeCatalogError::AmbiguousHead)
                }
            }
        }
    }

    async fn load_pages(
        &self,
        head: &ChunkKvRangeCatalogHead,
    ) -> Result<Vec<ChunkKvRangeCatalogPage>, ChunkKvRangeCatalogError> {
        let mut pages = Vec::with_capacity(head.pages.len());
        for reference in &head.pages {
            let page = self
                .store
                .get_page(reference.page_generation, reference.page_index)
                .await?
                .ok_or(ChunkKvRangeCatalogError::MissingPage)?;
            pages.push(page);
        }
        Ok(pages)
    }
}

#[derive(Default)]
struct MemoryCatalogState {
    pages: HashMap<(u64, u64), ChunkKvRangeCatalogPage>,
    head: Option<ChunkKvRangeCatalogHead>,
    next_head_outcome: Option<(HeadWriteOutcome, bool)>,
    page_writes: u64,
    head_writes: u64,
}

#[derive(Default)]
pub struct MemoryChunkKvRangeCatalogStore {
    state: Mutex<MemoryCatalogState>,
}

impl MemoryChunkKvRangeCatalogStore {
    pub async fn set_next_head_outcome(&self, outcome: HeadWriteOutcome, commit: bool) {
        self.state.lock().await.next_head_outcome = Some((outcome, commit));
    }

    pub async fn write_counts(&self) -> (u64, u64) {
        let state = self.state.lock().await;
        (state.page_writes, state.head_writes)
    }
}

#[async_trait]
impl ChunkKvRangeCatalogStore for MemoryChunkKvRangeCatalogStore {
    async fn put_page(&self, page: ChunkKvRangeCatalogPage) -> Result<(), ChunkKvRangeCatalogError> {
        let mut state = self.state.lock().await;
        let key = (page.generation, page.page_index);
        if state.pages.get(&key).is_some_and(|current| current != &page) {
            return Err(ChunkKvRangeCatalogError::GenerationConflict);
        }
        state.pages.insert(key, page);
        state.page_writes += 1;
        Ok(())
    }

    async fn get_page(
        &self,
        generation: u64,
        page_index: u64,
    ) -> Result<Option<ChunkKvRangeCatalogPage>, ChunkKvRangeCatalogError> {
        Ok(self
            .state
            .lock()
            .await
            .pages
            .get(&(generation, page_index))
            .cloned())
    }

    async fn put_head(
        &self,
        head: ChunkKvRangeCatalogHead,
    ) -> Result<HeadWriteOutcome, ChunkKvRangeCatalogError> {
        let mut state = self.state.lock().await;
        state.head_writes += 1;
        let (outcome, commit) = state
            .next_head_outcome
            .take()
            .unwrap_or((HeadWriteOutcome::Committed, true));
        if commit {
            state.head = Some(head);
        }
        Ok(outcome)
    }

    async fn get_head(&self) -> Result<Option<ChunkKvRangeCatalogHead>, ChunkKvRangeCatalogError> {
        Ok(self.state.lock().await.head.clone())
    }
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use crowdb_protocol::chunk_kv::{CatalogHead, CatalogPage, ChunkKvProtocolError};
use thiserror::Error;
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeadWriteOutcome {
    Committed,
    DefinitelyNotCommitted,
    Ambiguous,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum CatalogError {
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
}

#[async_trait]
pub trait CatalogStore: Send + Sync {
    async fn put_page(&self, page: CatalogPage) -> Result<(), CatalogError>;
    async fn get_page(&self, generation: u64, page_index: u64) -> Result<Option<CatalogPage>, CatalogError>;
    async fn put_head(&self, head: CatalogHead) -> Result<HeadWriteOutcome, CatalogError>;
    async fn get_head(&self) -> Result<Option<CatalogHead>, CatalogError>;
}

pub struct CatalogPublisher {
    store: Arc<dyn CatalogStore>,
}

impl CatalogPublisher {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self { store }
    }

    /// Publishes one fully validated immutable generation, pages before head.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid/regressing input, storage failure, a
    /// definite absent head write, or an ambiguous outcome not proven by reread.
    pub async fn publish(&self, head: CatalogHead, pages: Vec<CatalogPage>) -> Result<(), CatalogError> {
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
                    return Err(CatalogError::GenerationConflict);
                }
            }
            None if head.previous_generation.is_some() || head.generation != 1 => {
                return Err(CatalogError::GenerationConflict);
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
            HeadWriteOutcome::DefinitelyNotCommitted => Err(CatalogError::HeadNotCommitted),
            HeadWriteOutcome::Ambiguous => {
                if self.store.get_head().await?.as_ref() == Some(&head) {
                    Ok(())
                } else {
                    Err(CatalogError::AmbiguousHead)
                }
            }
        }
    }

    async fn load_pages(&self, head: &CatalogHead) -> Result<Vec<CatalogPage>, CatalogError> {
        let mut pages = Vec::with_capacity(head.pages.len());
        for reference in &head.pages {
            let page = self
                .store
                .get_page(reference.page_generation, reference.page_index)
                .await?
                .ok_or(CatalogError::MissingPage)?;
            pages.push(page);
        }
        Ok(pages)
    }
}

#[derive(Default)]
struct MemoryCatalogState {
    pages: HashMap<(u64, u64), CatalogPage>,
    head: Option<CatalogHead>,
    next_head_outcome: Option<(HeadWriteOutcome, bool)>,
    page_writes: u64,
    head_writes: u64,
}

#[derive(Default)]
pub struct MemoryCatalogStore {
    state: Mutex<MemoryCatalogState>,
}

impl MemoryCatalogStore {
    pub async fn set_next_head_outcome(&self, outcome: HeadWriteOutcome, commit: bool) {
        self.state.lock().await.next_head_outcome = Some((outcome, commit));
    }

    pub async fn write_counts(&self) -> (u64, u64) {
        let state = self.state.lock().await;
        (state.page_writes, state.head_writes)
    }
}

#[async_trait]
impl CatalogStore for MemoryCatalogStore {
    async fn put_page(&self, page: CatalogPage) -> Result<(), CatalogError> {
        let mut state = self.state.lock().await;
        let key = (page.generation, page.page_index);
        if state.pages.get(&key).is_some_and(|current| current != &page) {
            return Err(CatalogError::GenerationConflict);
        }
        state.pages.insert(key, page);
        state.page_writes += 1;
        Ok(())
    }

    async fn get_page(&self, generation: u64, page_index: u64) -> Result<Option<CatalogPage>, CatalogError> {
        Ok(self
            .state
            .lock()
            .await
            .pages
            .get(&(generation, page_index))
            .cloned())
    }

    async fn put_head(&self, head: CatalogHead) -> Result<HeadWriteOutcome, CatalogError> {
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

    async fn get_head(&self) -> Result<Option<CatalogHead>, CatalogError> {
        Ok(self.state.lock().await.head.clone())
    }
}

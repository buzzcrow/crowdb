// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage};
use crowdb_protocol::key::{ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, TextKey};

use crowdb_kv_client::{CrowdbKvClient, GetOutcome, ReadMode};

use crate::{ClientError, Result};

#[async_trait]
pub trait ChunkKvRangeCatalogSource: Send + Sync {
    /// Loads one head and all referenced immutable pages.
    ///
    /// # Errors
    ///
    /// Returns an availability or decoding failure without changing the cache.
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)>;
}

/// Production catalog source reading immutable pages from group 0.
pub struct Group0ChunkKvRangeCatalogSource {
    kv: Arc<CrowdbKvClient>,
}

impl Group0ChunkKvRangeCatalogSource {
    #[must_use]
    pub fn from_shared(kv: Arc<CrowdbKvClient>) -> Self {
        Self { kv }
    }

    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        match self
            .kv
            .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
            .await
            .map_err(|error| ClientError::CatalogUnavailable(error.to_string()))?
        {
            GetOutcome::Found { value, .. } => Ok(value.to_vec()),
            GetOutcome::NotFound => Err(ClientError::CatalogUnavailable(format!(
                "group-0 catalog record is missing: {path}"
            ))),
        }
    }
}

#[async_trait]
impl ChunkKvRangeCatalogSource for Group0ChunkKvRangeCatalogSource {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        let head_path = ChunkKvRangeCatalogHeadKey.to_path();
        let head: ChunkKvRangeCatalogHead = serde_json::from_slice(&self.read(&head_path).await?)
            .map_err(|error| ClientError::InvalidCatalog(error.to_string()))?;
        let mut pages = Vec::with_capacity(head.pages.len());
        for page in &head.pages {
            let path = ChunkKvRangeCatalogPageKey {
                generation: page.page_generation,
                page_index: page.page_index,
            }
            .to_path();
            pages.push(
                serde_json::from_slice(&self.read(&path).await?)
                    .map_err(|error| ClientError::InvalidCatalog(error.to_string()))?,
            );
        }
        head.validate_pages(&pages)
            .map_err(|error| ClientError::InvalidCatalog(error.to_string()))?;
        Ok((head, pages))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkKvRangeCatalogMap {
    generation: u64,
    entries: Vec<ChunkKvRangeCatalogEntry>,
}

impl ChunkKvRangeCatalogMap {
    /// Builds a route map only from one fully valid catalog generation.
    ///
    /// # Errors
    ///
    /// Returns an error for holes, overlap, corruption, or identity regression.
    pub fn decode(head: &ChunkKvRangeCatalogHead, pages: &[ChunkKvRangeCatalogPage]) -> Result<Self> {
        head.validate_pages(pages)
            .map_err(|error| ClientError::InvalidCatalog(error.to_string()))?;
        Ok(Self {
            generation: head.generation,
            entries: pages
                .iter()
                .flat_map(|page| page.entries.iter().cloned())
                .collect(),
        })
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn entries(&self) -> &[ChunkKvRangeCatalogEntry] {
        &self.entries
    }

    #[must_use]
    pub fn route(&self, key: &[u8]) -> Option<&ChunkKvRangeCatalogEntry> {
        let upper = self
            .entries
            .partition_point(|entry| entry.range.start.as_slice() <= key);
        upper
            .checked_sub(1)
            .and_then(|index| self.entries.get(index))
            .filter(|entry| entry.range.contains(key))
    }
}

#[derive(Default)]
pub struct ChunkKvRangeCatalogCache {
    current: ArcSwapOption<ChunkKvRangeCatalogMap>,
}

impl ChunkKvRangeCatalogCache {
    #[must_use]
    pub fn load(&self) -> Option<Arc<ChunkKvRangeCatalogMap>> {
        self.current.load_full()
    }

    /// Installs a strictly newer valid generation, or accepts an exact repeat.
    ///
    /// # Errors
    ///
    /// Returns an error for a same-generation conflict or generation regression.
    pub fn install(&self, map: ChunkKvRangeCatalogMap) -> Result<()> {
        let candidate = Arc::new(map);
        if let Some(current) = self.current.load_full() {
            if current.as_ref() == candidate.as_ref() {
                return Ok(());
            }
            if candidate.generation <= current.generation {
                return Err(ClientError::InvalidCatalog(
                    "catalog generation did not advance".into(),
                ));
            }
        }
        self.current.rcu(|current| {
            if current
                .as_ref()
                .is_some_and(|current| current.generation >= candidate.generation)
            {
                current.clone()
            } else {
                Some(Arc::clone(&candidate))
            }
        });
        if self.current.load_full().as_ref() == Some(&candidate) {
            Ok(())
        } else {
            Err(ClientError::InvalidCatalog(
                "catalog install lost a newer-generation race".into(),
            ))
        }
    }
}

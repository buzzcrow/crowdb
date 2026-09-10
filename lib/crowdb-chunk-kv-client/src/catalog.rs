// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use crowdb_protocol::chunk_kv::{CatalogEntry, CatalogHead, CatalogPage};

use crate::{ClientError, Result};

#[async_trait]
pub trait CatalogSource: Send + Sync {
    /// Loads one head and all referenced immutable pages.
    ///
    /// # Errors
    ///
    /// Returns an availability or decoding failure without changing the cache.
    async fn load(&self) -> Result<(CatalogHead, Vec<CatalogPage>)>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogMap {
    generation: u64,
    entries: Vec<CatalogEntry>,
}

impl CatalogMap {
    /// Builds a route map only from one fully valid catalog generation.
    ///
    /// # Errors
    ///
    /// Returns an error for holes, overlap, corruption, or identity regression.
    pub fn decode(head: &CatalogHead, pages: &[CatalogPage]) -> Result<Self> {
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
    pub fn entries(&self) -> &[CatalogEntry] {
        &self.entries
    }

    #[must_use]
    pub fn route(&self, key: &[u8]) -> Option<&CatalogEntry> {
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
pub struct CatalogCache {
    current: ArcSwapOption<CatalogMap>,
}

impl CatalogCache {
    #[must_use]
    pub fn load(&self) -> Option<Arc<CatalogMap>> {
        self.current.load_full()
    }

    /// Installs a strictly newer valid generation, or accepts an exact repeat.
    ///
    /// # Errors
    ///
    /// Returns an error for a same-generation conflict or generation regression.
    pub fn install(&self, map: CatalogMap) -> Result<()> {
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

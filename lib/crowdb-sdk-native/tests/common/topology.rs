// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_chunk_kv_client::{ChunkKvRangeCatalogSource, ClientError, Result};
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub struct TestCatalogSource {
    pub generation: AtomicU64,
    pub unavailable: AtomicBool,
}

#[async_trait]
impl ChunkKvRangeCatalogSource for TestCatalogSource {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        if self.unavailable.load(Ordering::Acquire) {
            return Err(ClientError::CatalogUnavailable(
                "test authority unavailable".into(),
            ));
        }
        let generation = self.generation.load(Ordering::Acquire);
        Ok(super::metadata::catalog(generation, generation))
    }
}

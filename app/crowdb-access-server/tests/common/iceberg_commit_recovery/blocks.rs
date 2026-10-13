// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_iceberg::file::{ChunkRoot, FileBlockStore, FileIdentity, FileIoError, FileWriteIntent};
use crowdb_access_iceberg::key::OperationId;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub struct TestBlocks {
    store: Arc<dyn crowdb_access_iceberg::catalog::CatalogStore>,
    next: Arc<AtomicU64>,
    values: Arc<ArcSwap<BTreeMap<u64, Arc<Vec<u8>>>>>,
}

impl TestBlocks {
    pub fn new(store: Arc<crate::memory::TestStore>) -> Self {
        Self {
            store,
            next: Arc::new(AtomicU64::new(0)),
            values: Arc::new(ArcSwap::from_pointee(BTreeMap::new())),
        }
    }

    pub fn with_store(&self, store: Arc<dyn crowdb_access_iceberg::catalog::CatalogStore>) -> Self {
        Self {
            store,
            next: self.next.clone(),
            values: self.values.clone(),
        }
    }
}

#[async_trait]
impl FileBlockStore for TestBlocks {
    async fn put(&self, owner: FileIdentity, height: u8, bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        let index = self.next.fetch_add(1, Ordering::SeqCst) + 1;
        let root = ChunkRoot {
            chunk: crowdb_protocol::common::ChunkId {
                high: (crowdb_protocol::chunkdb::rpc::ChunkType::IcebergTable as u64) << 56,
                low: index,
            },
            offset: 0,
            physical_length: bytes.len() as u64 + 64,
            logical_offset: 0,
            logical_length: bytes.len() as u64,
            height,
            digest: Sha256::digest(bytes).into(),
        };
        FileWriteIntent {
            identity: OperationId::random(),
            owner,
            created_ms: crate::common::now_ms(),
            not_before_ms: 0,
            deleting: false,
            root: root.clone(),
        }
        .register(self.store.clone())
        .await
        .map_err(|_| FileIoError::Bounds)?;
        self.values.rcu(|values| {
            let mut next = (**values).clone();
            next.insert(index, Arc::new(bytes.to_vec()));
            next
        });
        Ok(root)
    }

    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        self.values
            .load()
            .get(&root.chunk.low)
            .map(|value| value.as_ref().clone())
            .ok_or(FileIoError::Bounds)
    }
}

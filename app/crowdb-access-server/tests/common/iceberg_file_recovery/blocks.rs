// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{catalog::CatalogStore, file::NativeFileBlocks};
use crowdb_chunk_client::{ChunkIoClient, SmallWritePolicy};
use crowdb_protocol::chunkdb::rpc::ChunkType;
use std::sync::Arc;

#[derive(Default)]
pub struct TestBlocks {
    allocator: Arc<crate::allocator::TestAllocator>,
    disk: Arc<crate::disk::TestDisk>,
}

impl TestBlocks {
    pub fn new(_store: Arc<crate::memory::TestStore>) -> Self {
        Self::default()
    }

    pub fn with_store(&self, store: Arc<dyn CatalogStore>) -> NativeFileBlocks {
        let mut policy = SmallWritePolicy::new(ChunkType::IcebergTable);
        policy.conversion_enabled = false;
        let client =
            ChunkIoClient::from_parts_with_small_policy(self.allocator.clone(), self.disk.clone(), policy)
                .unwrap();
        NativeFileBlocks::new(client, store)
    }
}

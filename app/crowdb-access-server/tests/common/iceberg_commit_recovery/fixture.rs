// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

pub use crate::memory::activate;

pub struct TestCommitFixture {
    pub directory: crowdb_test_harness::test_dirs::TestDir,
    pub blocks: Arc<crate::blocks::TestBlocks>,
    store: Arc<crate::memory::TestStore>,
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

impl TestCommitFixture {
    pub fn new() -> Self {
        let store = Arc::new(crate::memory::TestStore::default());
        Self {
            directory: crowdb_test_harness::test_dirs::TestDir::new("iceberg-commit-recovery").unwrap(),
            blocks: Arc::new(crate::blocks::TestBlocks::new(store.clone())),
            store,
        }
    }

    pub fn store(&self) -> Arc<crate::memory::TestStore> {
        self.store.clone()
    }
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_kv_client::{BatchOp, CrowdbKvClient, GetOutcome, ReadMode, Result, ScanOutcome, WriteOutcome};
use std::sync::Arc;

use crate::directory::DirectoryRecords;

pub(super) struct KvDirectoryRecords {
    kv: Arc<CrowdbKvClient>,
    store_id: u64,
    group_id: u64,
}

impl KvDirectoryRecords {
    pub(super) fn new(kv: Arc<CrowdbKvClient>, store_id: u64, group_id: u64) -> Self {
        Self {
            kv,
            store_id,
            group_id,
        }
    }
}

#[async_trait]
impl DirectoryRecords for KvDirectoryRecords {
    async fn get(&self, key: &[u8]) -> Result<GetOutcome> {
        self.kv
            .get(self.store_id, self.group_id, key, ReadMode::Linearizable, None)
            .await
    }
    async fn scan_keys(&self, prefix: &[u8], after: &[u8], limit: u32) -> Result<ScanOutcome> {
        self.kv
            .scan_bounded(
                self.store_id,
                self.group_id,
                prefix,
                after,
                &[],
                limit,
                true,
                None,
            )
            .await
    }
    async fn put_cas(&self, key: &[u8], value: &[u8], revision: u64) -> Result<WriteOutcome> {
        self.kv
            .put_cas(self.store_id, self.group_id, key, value, revision)
            .await
    }
    async fn batch_write_cas(&self, ops: &[BatchOp], key: &[u8], revision: u64) -> Result<WriteOutcome> {
        self.kv
            .batch_write_cas(self.store_id, self.group_id, ops, key, revision)
            .await
    }
}

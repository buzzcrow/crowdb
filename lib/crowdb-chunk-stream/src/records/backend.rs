// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_paxos_kv_client::{BatchOp, GetOutcome, Result, ScanOutcome, WriteOutcome};

#[async_trait]
pub(crate) trait RecordBackend: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<GetOutcome>;
    async fn scan(&self, prefix: &[u8], after: &[u8], limit: u32, reverse: bool) -> Result<ScanOutcome>;
    async fn scan_keys(&self, prefix: &[u8], after: &[u8], limit: u32) -> Result<ScanOutcome> {
        let mut records = self.scan(prefix, after, limit, false).await?;
        for (_, value) in &mut records.items {
            *value = bytes::Bytes::new();
        }
        Ok(records)
    }
    async fn batch(&self, ops: &[BatchOp], guard: &[u8], revision: u64) -> Result<WriteOutcome>;
}

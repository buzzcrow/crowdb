// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Conditional record boundary shared by production KV and deterministic storage.

use async_trait::async_trait;
use crowdb_kv_client::{BatchOp, GetOutcome, Result, ScanOutcome, WriteOutcome};

#[async_trait]
pub(crate) trait DirectoryRecords: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<GetOutcome>;
    async fn scan_keys(&self, prefix: &[u8], after: &[u8], limit: u32) -> Result<ScanOutcome>;
    async fn put_cas(&self, key: &[u8], value: &[u8], revision: u64) -> Result<WriteOutcome>;
    async fn batch_write_cas(&self, ops: &[BatchOp], key: &[u8], revision: u64) -> Result<WriteOutcome>;
}

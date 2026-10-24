// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::perf::{perf, PhaseTimer};
use crate::records::RecordBackend;
use async_trait::async_trait;
use crowdb_kv_client::{BatchOp, CrowdbKvClient, GetOutcome, ReadMode, Result, ScanOutcome, WriteOutcome};
use std::sync::Arc;

pub(super) struct KvRecordBackend {
    pub(super) kv: Arc<CrowdbKvClient>,
    pub(super) store: u64,
    pub(super) group: u64,
}
#[async_trait]
impl RecordBackend for KvRecordBackend {
    async fn get(&self, key: &[u8]) -> Result<GetOutcome> {
        let _timer = PhaseTimer::start(&perf().record_get);
        self.kv
            .get(self.store, self.group, key, ReadMode::Linearizable, None)
            .await
    }
    async fn scan(&self, prefix: &[u8], after: &[u8], limit: u32, reverse: bool) -> Result<ScanOutcome> {
        self.scan_records(prefix, after, limit, reverse, false).await
    }
    async fn scan_keys(&self, prefix: &[u8], after: &[u8], limit: u32) -> Result<ScanOutcome> {
        self.scan_records(prefix, after, limit, false, true).await
    }
    async fn batch(&self, ops: &[BatchOp], key: &[u8], revision: u64) -> Result<WriteOutcome> {
        let _timer = PhaseTimer::start(&perf().record_cas);
        self.kv
            .batch_write_cas(self.store, self.group, ops, key, revision)
            .await
    }
}

impl KvRecordBackend {
    async fn scan_records(
        &self,
        prefix: &[u8],
        after: &[u8],
        limit: u32,
        reverse: bool,
        keys_only: bool,
    ) -> Result<ScanOutcome> {
        let _timer = PhaseTimer::start(&perf().record_scan);
        let outcome = if reverse {
            self.kv
                .scan_reverse(
                    self.store,
                    self.group,
                    prefix,
                    after,
                    &[],
                    limit,
                    ReadMode::Linearizable,
                    None,
                    keys_only,
                    None,
                )
                .await
        } else {
            self.kv
                .scan(
                    self.store,
                    self.group,
                    prefix,
                    after,
                    &[],
                    limit,
                    ReadMode::Linearizable,
                    None,
                    keys_only,
                    None,
                )
                .await
        }?;
        if outcome.timed_out || outcome.items.len() != outcome.commit_slots.len() {
            return Err(crowdb_kv_client::Error::Server(
                "incomplete Stream chunk record scan".into(),
            ));
        }
        Ok(outcome)
    }
}

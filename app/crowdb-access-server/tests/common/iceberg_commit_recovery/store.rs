// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::memory::TestStore;
use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, StoreError},
    file::{MultipartPartScan, MultipartPartStore},
    gc::{GcScan, GcStore, GcSystemScan},
    namespace::NamespaceStore,
};
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage, MultiScanRequest};
use crowdb_protocol::chunk_kv::{ClientRequestId, RpcValue};

fn scan(store: &TestStore, request: MultiScanRequest) -> MultiScanPage {
    let snapshot = store.values.load_full();
    let mut candidates = snapshot.iter().filter(|(key, _)| {
        request.start.as_ref().map_or(true, |start| *key >= start)
            && request.end.as_ref().map_or(true, |end| *key < end)
            && request
                .continuation
                .as_ref()
                .map_or(true, |cursor| *key > &cursor.last_key)
    });
    let items: Vec<_> = candidates
        .by_ref()
        .take(request.max_items)
        .map(|(key, value)| RpcValue {
            key: key.clone(),
            value: value.bytes.clone(),
            revision: value.revision,
        })
        .collect();
    let continuation = candidates.next().map(|_| MultiScanContinuation {
        direction: request.direction,
        original_start: request.start,
        original_end: request.end,
        last_key: items.last().unwrap().key.clone(),
        catalog_generation: 1,
    });
    MultiScanPage {
        items,
        continuation,
        terminal_failure: None,
    }
}

#[async_trait]
impl MultipartPartStore for TestStore {
    async fn scan_multipart_parts(&self, request: MultipartPartScan) -> Result<MultiScanPage, StoreError> {
        Ok(scan(self, request.request()?))
    }
}

#[async_trait]
impl GcStore for TestStore {
    async fn scan_gc(&self, request: GcScan) -> Result<MultiScanPage, StoreError> {
        Ok(scan(self, request.request()?))
    }

    async fn scan_gc_system(&self, request: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        Ok(scan(self, request.request()?))
    }

    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.delete_mapping(key, expected, identity).await
    }
}

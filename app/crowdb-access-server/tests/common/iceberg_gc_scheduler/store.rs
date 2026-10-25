// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    gc::{GcLocalScan, GcScan, GcStore, GcSystemScan},
};
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage};
use crowdb_protocol::chunk_kv::{ClientRequestId, RpcValue};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

#[path = "../iceberg_file_delete/store.rs"]
mod inner;

#[derive(Default)]
pub struct TestStore {
    pub inner: inner::TestStore,
    pub fault_key: ArcSwap<Vec<u8>>,
    pub write_failure: AtomicUsize,
    pub read_failure: AtomicUsize,
    pub pause_write: AtomicBool,
    pub pause_scope: AtomicUsize,
    pub entered: Notify,
    pub resume: Notify,
    pub invalid_page: AtomicBool,
    pub requests: AtomicUsize,
}

impl TestStore {
    pub fn fault_at(&self, key: Vec<u8>) {
        self.fault_key.store(Arc::new(key));
    }
}

#[async_trait]
impl CatalogStore for TestStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        if self.fault_key.load().as_slice() == key && self.read_failure.swap(0, Ordering::SeqCst) != 0 {
            return Err(StoreError::Response);
        }
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let targeted = self.fault_key.load().as_slice() == key;
        if targeted && self.pause_write.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        let failure = if targeted {
            self.write_failure.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if failure == 1 {
            return Err(StoreError::Budget);
        }
        let outcome = self
            .inner
            .compare_exchange(key, expected, value, identity)
            .await?;
        if failure == 2 {
            Err(StoreError::Response)
        } else {
            Ok(outcome)
        }
    }
}

#[async_trait]
impl GcStore for TestStore {
    async fn scan_gc_local(&self, scan: GcLocalScan) -> Result<MultiScanPage, StoreError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let request = scan.request(1)?;
        let start = request.start.as_ref().unwrap();
        if self.pause_scope.load(Ordering::SeqCst) == usize::from(*start.last().unwrap()) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        let snapshot = self.inner.values.load_full();
        let mut rows = snapshot.iter().filter(|(key, _)| {
            *key >= start
                && request.end.as_ref().map_or(true, |end| *key < end)
                && request
                    .continuation
                    .as_ref()
                    .map_or(true, |token| *key > &token.last_key)
        });
        let mut items = Vec::new();
        let mut bytes = 0;
        let mut more = false;
        for (key, value) in rows.by_ref() {
            let size = key.len() + value.bytes.len();
            if items.len() == request.max_items || bytes + size > request.max_bytes {
                if items.is_empty() {
                    return Err(StoreError::Response);
                }
                more = true;
                break;
            }
            items.push(RpcValue {
                key: key.clone(),
                value: value.bytes.clone(),
                revision: value.revision,
            });
            bytes += size;
        }
        if !items.is_empty() && self.invalid_page.swap(false, Ordering::SeqCst) {
            items[0].revision = 0;
        }
        let page = MultiScanPage {
            continuation: more.then(|| MultiScanContinuation {
                direction: request.direction,
                original_start: request.start,
                original_end: request.end,
                last_key: items.last().unwrap().key.clone(),
                catalog_generation: 1,
            }),
            items,
            terminal_failure: None,
        };
        scan.validate_page(&page)?;
        Ok(page)
    }
    async fn scan_gc(&self, scan: GcScan) -> Result<MultiScanPage, StoreError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_gc(scan).await
    }
    async fn scan_gc_system(&self, scan: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_gc_system(scan).await
    }
    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_gc_record(key, expected, identity).await
    }
}

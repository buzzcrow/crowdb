// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    gc::{GcScan, GcStore, GcSystemScan},
    key::{CatalogScope, IcebergKey, StorageKey},
    record::StorageRecord,
};
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage, MultiScanRequest};
use crowdb_protocol::chunk_kv::{ClientRequestId, RpcValue};

#[derive(Default)]
pub struct TestStore {
    pub values: ArcSwap<BTreeMap<Vec<u8>, StoredValue>>,
    pub reads: AtomicUsize,
    pub exchanges: AtomicUsize,
    pub gc_requests: AtomicUsize,
    pub deny_gc: AtomicBool,
    pub mark_failure: AtomicUsize,
}

impl TestStore {
    pub fn reset_counts(&self) {
        self.reads.store(0, Ordering::SeqCst);
        self.exchanges.store(0, Ordering::SeqCst);
        self.gc_requests.store(0, Ordering::SeqCst);
    }

    pub fn counts(&self) -> (usize, usize) {
        (
            self.reads.load(Ordering::SeqCst),
            self.exchanges.load(Ordering::SeqCst),
        )
    }

    fn admit_key(&self, key: &[u8]) -> Result<(), StoreError> {
        if matches!(
            IcebergKey::decode(key),
            Ok(IcebergKey::Catalog {
                scope: CatalogScope::GcTask
                    | CatalogScope::GcClaim
                    | CatalogScope::GcCandidate
                    | CatalogScope::GcRetirement,
                ..
            })
        ) {
            self.gc_requests.fetch_add(1, Ordering::SeqCst);
            if self.deny_gc.load(Ordering::SeqCst) {
                return Err(StoreError::Budget);
            }
        }
        Ok(())
    }

    fn scan(&self, request: MultiScanRequest) -> MultiScanPage {
        let snapshot = self.values.load_full();
        let mut candidates = snapshot.iter().filter(|(key, _)| {
            request.start.as_ref().map_or(true, |start| *key >= start)
                && request.end.as_ref().map_or(true, |end| *key < end)
        });
        let mut items = Vec::new();
        let mut bytes = 0;
        let mut more = false;
        for (key, value) in candidates.by_ref() {
            let size = key.len() + value.bytes.len();
            if items.len() == request.max_items || bytes + size > request.max_bytes {
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
        MultiScanPage {
            continuation: more.then(|| MultiScanContinuation {
                direction: request.direction,
                original_start: request.start,
                original_end: request.end,
                last_key: items.last().unwrap().key.clone(),
                catalog_generation: 1,
            }),
            items,
            terminal_failure: None,
        }
    }
}

#[async_trait]
impl CatalogStore for TestStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.admit_key(key)?;
        Ok(self.values.load().get(key).cloned())
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        identity.validate().unwrap();
        self.exchanges.fetch_add(1, Ordering::SeqCst);
        self.admit_key(key)?;
        let marking = matches!(
            StorageKey::decode(key).and_then(|key| StorageRecord::decode(&key, value)),
            Ok(StorageRecord::DeletedFile(_))
        );
        let failure = if marking {
            self.mark_failure.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if failure == 1 {
            return Err(StoreError::Budget);
        }
        loop {
            let current = self.values.load_full();
            let previous = current.get(key);
            if previous.map(|value| value.bytes.as_slice()) != expected {
                return Ok(CasOutcome::Conflict(previous.cloned()));
            }
            let revision = previous.map_or(1, |value| value.revision + 1);
            let mut next = (*current).clone();
            next.insert(
                key.to_vec(),
                StoredValue {
                    bytes: value.to_vec(),
                    revision,
                },
            );
            let observed = self.values.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&current, &observed) {
                return if failure == 2 {
                    Err(StoreError::Response)
                } else {
                    Ok(CasOutcome::Applied(revision))
                };
            }
        }
    }
}

#[async_trait]
impl GcStore for TestStore {
    async fn scan_gc(&self, scan: GcScan) -> Result<MultiScanPage, StoreError> {
        self.gc_requests.fetch_add(1, Ordering::SeqCst);
        if self.deny_gc.load(Ordering::SeqCst) {
            return Err(StoreError::Budget);
        }
        Ok(self.scan(scan.request()?))
    }

    async fn scan_gc_system(&self, scan: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        Ok(self.scan(scan.request()?))
    }

    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        identity.validate().unwrap();
        loop {
            let current = self.values.load_full();
            let previous = current.get(key);
            if previous.map(|value| value.bytes.as_slice()) != Some(expected) {
                return Ok(CasOutcome::Conflict(previous.cloned()));
            }
            let revision = previous.unwrap().revision + 1;
            let mut next = (*current).clone();
            next.remove(key);
            let observed = self.values.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&current, &observed) {
                return Ok(CasOutcome::Applied(revision));
            }
        }
    }
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    key::{IcebergKey, SystemScope},
    namespace::{ChildScan, NamespaceStore},
    operation::mutation_identity,
    record::StorageRecord,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use crate::common::TestStore;

pub struct TestInterruption {
    trigger: Vec<u8>,
    key: Vec<u8>,
    before: Vec<u8>,
    after: Vec<u8>,
}

impl TestInterruption {
    pub fn new(store: &TestStore, trigger: Vec<u8>, key: &IcebergKey, next: &StorageRecord) -> Self {
        let key = key.encode().unwrap();
        Self {
            trigger,
            before: store.values.load().get(&key).unwrap().bytes.clone(),
            key,
            after: next.encode().unwrap(),
        }
    }
}

pub struct TestPropertyAdmissionStore {
    pub inner: Arc<TestStore>,
    interruption: Option<TestInterruption>,
    fired: AtomicBool,
    reads: AtomicUsize,
    roots: AtomicUsize,
    exchanges: AtomicUsize,
}

impl TestPropertyAdmissionStore {
    pub fn new(inner: Arc<TestStore>, interruption: Option<TestInterruption>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            interruption,
            fired: AtomicBool::new(false),
            reads: AtomicUsize::new(0),
            roots: AtomicUsize::new(0),
            exchanges: AtomicUsize::new(0),
        })
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.reads.load(Ordering::Relaxed),
            self.roots.load(Ordering::Relaxed),
            self.exchanges.load(Ordering::Relaxed),
        )
    }
}

#[async_trait]
impl CatalogStore for TestPropertyAdmissionStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if matches!(
            IcebergKey::decode(key),
            Ok(IcebergKey::System {
                scope: SystemScope::ActiveRoot,
                ..
            })
        ) {
            self.roots.fetch_add(1, Ordering::Relaxed);
        }
        let selected = self.inner.get(key).await?;
        if let Some(interruption) = &self.interruption {
            if key == interruption.trigger && !self.fired.swap(true, Ordering::Relaxed) {
                // A rival writes after this read; its RPCs are excluded from the caller's counts.
                assert!(matches!(
                    self.inner
                        .compare_exchange(
                            &interruption.key,
                            Some(&interruption.before),
                            &interruption.after,
                            mutation_identity(
                                &interruption.key,
                                Some(&interruption.before),
                                &interruption.after,
                            ),
                        )
                        .await?,
                    CasOutcome::Applied(_)
                ));
            }
        }
        Ok(selected)
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.exchanges.fetch_add(1, Ordering::Relaxed);
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl NamespaceStore for TestPropertyAdmissionStore {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_children(scan).await
    }

    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete_mapping(key, expected, identity).await
    }
}

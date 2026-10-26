// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    namespace::{ChildScan, NamespaceAuthority, NamespaceStore},
    operation::mutation_identity,
    record::StorageRecord,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::common::TestStore;

pub struct TestPropertyRaceStore {
    pub inner: Arc<TestStore>,
    pub reads: AtomicUsize,
    pub attempts: AtomicUsize,
    key: Vec<u8>,
    updates: Vec<NamespaceAuthority>,
}

impl TestPropertyRaceStore {
    pub fn new(inner: Arc<TestStore>, key: Vec<u8>, updates: Vec<NamespaceAuthority>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            key,
            updates,
            reads: AtomicUsize::new(0),
            attempts: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl CatalogStore for TestPropertyRaceStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        if key != self.key {
            return self.inner.compare_exchange(key, expected, value, identity).await;
        }
        let index = self.attempts.fetch_add(1, Ordering::Relaxed);
        let current = self.inner.values.load().get(key).cloned();
        assert_eq!(
            current.as_ref().map(|current| current.bytes.as_slice()),
            expected,
            "the next CAS must use the returned authority"
        );
        if let Some(update) = self.updates.get(index) {
            let bytes = StorageRecord::NamespaceAuthority(Box::new(update.clone())).encode()?;
            assert!(matches!(
                self.inner
                    .compare_exchange(key, expected, &bytes, mutation_identity(key, expected, &bytes))
                    .await?,
                CasOutcome::Applied(_)
            ));
            return Ok(CasOutcome::Conflict(self.inner.values.load().get(key).cloned()));
        }
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl NamespaceStore for TestPropertyRaceStore {
    async fn scan_children(&self, request: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_children(request).await
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

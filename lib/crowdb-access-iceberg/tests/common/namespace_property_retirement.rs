// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    key::IcebergKey,
    namespace::{ChildScan, NamespaceStore},
    record::StorageRecord,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use tokio::sync::Notify;

use crate::common::TestStore;

pub struct TestPropertyRetirementStore {
    inner: Arc<TestStore>,
    key: Vec<u8>,
    pause: AtomicBool,
    reads: AtomicUsize,
    exchanges: AtomicUsize,
    guard_hits: AtomicUsize,
    retired_observations: AtomicUsize,
    authority_readbacks: AtomicUsize,
    fault_hits: AtomicUsize,
    replied: AtomicBool,
    after_reply: AtomicUsize,
    pub entered: Notify,
    pub release: Notify,
    pub fail_before: AtomicBool,
    pub lose_reply: AtomicBool,
}

impl TestPropertyRetirementStore {
    pub fn new(inner: Arc<TestStore>, key: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            key,
            pause: AtomicBool::new(true),
            reads: AtomicUsize::new(0),
            exchanges: AtomicUsize::new(0),
            guard_hits: AtomicUsize::new(0),
            retired_observations: AtomicUsize::new(0),
            replied: AtomicBool::new(false),
            authority_readbacks: AtomicUsize::new(0),
            fault_hits: AtomicUsize::new(0),
            after_reply: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
            fail_before: AtomicBool::new(false),
            lose_reply: AtomicBool::new(false),
        })
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.reads.load(Ordering::SeqCst),
            self.exchanges.load(Ordering::SeqCst),
            self.after_reply.load(Ordering::SeqCst),
        )
    }

    pub fn guard_hits(&self) -> usize {
        self.guard_hits.load(Ordering::SeqCst)
    }

    pub fn retired_observations(&self) -> usize {
        self.retired_observations.load(Ordering::SeqCst)
    }

    pub fn authority_readbacks(&self) -> usize {
        self.authority_readbacks.load(Ordering::SeqCst)
    }

    pub fn fault_hits(&self) -> usize {
        self.fault_hits.load(Ordering::SeqCst)
    }

    fn fault(&self) -> StoreError {
        self.fault_hits.fetch_add(1, Ordering::SeqCst);
        self.replied.store(true, Ordering::SeqCst);
        StoreError::Response
    }

    fn entered_call(&self) {
        if self.replied.load(Ordering::SeqCst) {
            self.after_reply.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn pause_property_cas(&self, key: &[u8], expected: Option<&[u8]>, value: &[u8]) {
        assert_eq!(key, self.key);
        let key = IcebergKey::decode(key).unwrap();
        let StorageRecord::NamespaceAuthority(before) =
            StorageRecord::decode(&key, expected.unwrap()).unwrap()
        else {
            panic!("property CAS must expect a namespace authority");
        };
        let StorageRecord::NamespaceAuthority(next) = StorageRecord::decode(&key, value).unwrap() else {
            panic!("property CAS must write a namespace authority");
        };
        assert_eq!(next.property_revision, before.property_revision + 1);
        assert_ne!(next.properties, before.properties);
        self.guard_hits.fetch_add(1, Ordering::SeqCst);
        if self.pause.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

#[async_trait]
impl CatalogStore for TestPropertyRetirementStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.entered_call();
        if key == self.key && self.replied.load(Ordering::SeqCst) {
            self.authority_readbacks.fetch_add(1, Ordering::SeqCst);
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.entered_call();
        self.exchanges.fetch_add(1, Ordering::SeqCst);
        let watched = key == self.key;
        if watched {
            self.pause_property_cas(key, expected, value).await;
            if self.fail_before.swap(false, Ordering::SeqCst) {
                return Err(self.fault());
            }
        }
        let result = self
            .inner
            .compare_exchange(key, expected, value, identity)
            .await?;
        if watched {
            if matches!(&result, CasOutcome::Conflict(Some(observed)) if matches!(IcebergKey::decode(key).and_then(|key| StorageRecord::decode(&key, &observed.bytes)), Ok(StorageRecord::RetiredNamespace { .. })))
            {
                self.retired_observations.fetch_add(1, Ordering::SeqCst);
            }
            self.replied.store(true, Ordering::SeqCst);
            if self.lose_reply.swap(false, Ordering::SeqCst) {
                return Err(self.fault());
            }
        }
        Ok(result)
    }
}

#[async_trait]
impl NamespaceStore for TestPropertyRetirementStore {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.entered_call();
        self.inner.scan_children(scan).await
    }

    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.entered_call();
        self.inner.delete_mapping(key, expected, identity).await
    }
}

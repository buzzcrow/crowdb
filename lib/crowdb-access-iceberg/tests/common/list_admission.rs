// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogAuthority, CatalogStore, StoreError, StoredValue},
    commit::{TableCreationRequest, TableCreator},
    key::{CatalogScope, IcebergKey, OperationId, SystemScope},
    namespace::{ChildScan, NamespaceAuthority, NamespaceStore},
    operation::{mutation_identity, RequestIdentity},
    record::StorageRecord,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use crate::{blocks::TestBlocks, common::TestStore, fixture::TestNamespace};

pub struct TestListFixture {
    pub fixture: TestNamespace,
    pub leaf: NamespaceAuthority,
}

impl TestListFixture {
    pub async fn new(depth: usize, native: bool) -> Self {
        let fixture = TestNamespace::new().await;
        if native {
            fixture
                .put(
                    IcebergKey::Catalog {
                        catalog: fixture.context.catalog,
                        scope: CatalogScope::Authority,
                        suffix: Vec::new(),
                    },
                    StorageRecord::Authority(
                        CatalogAuthority::new_native(fixture.context.catalog, "native".into()).unwrap(),
                    ),
                )
                .await;
        }
        let names = if depth == 1 {
            ["default", "", ""]
        } else {
            ["root", "branch", "leaf"]
        };
        let mut parent = None;
        let mut leaf = None;
        for level in 1..=depth {
            let authority = fixture.authority(parent, &names[..level]);
            fixture.publish(&authority).await;
            parent = Some(authority.namespace);
            leaf = Some(authority);
        }
        let leaf = leaf.unwrap();
        let creator = TableCreator::new(fixture.store.clone(), Arc::new(TestBlocks::default()));
        for name in ["a", "b", "c"] {
            let mut components: Vec<_> = leaf.identifier.components().iter().map(String::as_str).collect();
            components.push(name);
            fixture
                .publish(&fixture.authority(Some(leaf.namespace), &components))
                .await;
            let body = serde_json::to_vec(&serde_json::json!({
                "name": name,
                "schema": {"type": "struct", "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": true}
                ]}
            }))
            .unwrap();
            assert_eq!(
                creator
                    .create(&TableCreationRequest {
                        context: fixture.context,
                        identity: RequestIdentity {
                            operation: OperationId::random(),
                            issued_ms: 100,
                        },
                        principal: "listing-fixture".into(),
                        namespace: leaf.identifier.clone(),
                        body,
                        timestamp_ms: 1000,
                    })
                    .await
                    .unwrap()
                    .status,
                200
            );
        }
        Self { fixture, leaf }
    }
}

pub struct TestListMutation {
    key: Vec<u8>,
    before: Option<Vec<u8>>,
    after: Vec<u8>,
}

impl TestListMutation {
    pub fn new(store: &TestStore, key: &IcebergKey, next: &StorageRecord) -> Self {
        let key = key.encode().unwrap();
        Self {
            before: store.values.load().get(&key).map(|value| value.bytes.clone()),
            key,
            after: next.encode().unwrap(),
        }
    }
}

pub struct TestListStore {
    pub inner: Arc<TestStore>,
    after_scan: Vec<TestListMutation>,
    fired: AtomicBool,
    reads: AtomicUsize,
    roots: AtomicUsize,
    scans: AtomicUsize,
    exchanges: AtomicUsize,
}

impl TestListStore {
    pub fn new(inner: Arc<TestStore>, after_scan: Vec<TestListMutation>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            after_scan,
            fired: AtomicBool::new(false),
            reads: AtomicUsize::new(0),
            roots: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            exchanges: AtomicUsize::new(0),
        })
    }

    pub fn counts(&self) -> (usize, usize, usize, usize) {
        (
            self.reads.load(Ordering::Relaxed),
            self.roots.load(Ordering::Relaxed),
            self.scans.load(Ordering::Relaxed),
            self.exchanges.load(Ordering::Relaxed),
        )
    }
}

#[async_trait]
impl CatalogStore for TestListStore {
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
        self.inner.get(key).await
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
impl NamespaceStore for TestListStore {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        let page = self.inner.scan_children(scan).await?;
        if !self.fired.swap(true, Ordering::Relaxed) {
            for mutation in &self.after_scan {
                let expected = mutation.before.as_deref();
                assert!(matches!(
                    self.inner
                        .compare_exchange(
                            &mutation.key,
                            expected,
                            &mutation.after,
                            mutation_identity(&mutation.key, expected, &mutation.after),
                        )
                        .await?,
                    CasOutcome::Applied(_)
                ));
            }
        }
        Ok(page)
    }

    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.exchanges.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_mapping(key, expected, identity).await
    }
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogLifecycle, CatalogStore, RootState, StoreError, StoredValue},
    commit::{TableCreationRequest, TableCreator},
    key::{CatalogId, CatalogScope, IcebergKey, OperationId, StorageKey, SystemScope, TableStorageScope},
    namespace::{ChildScan, NamespaceStore},
    operation::{mutation_identity, RequestIdentity},
    record::StorageRecord,
    table::{
        storage_head_key, TableHead, TableLifecycle, TableLifecycleAction, TableLifecycleRequest,
        TableLifecycles, TableRepository,
    },
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

pub enum TestInterleave {
    Clear(bool),
    ClearAfterHead,
    Rename,
    Retire,
    Unknown(bool),
    Recreate,
}

pub struct TestDropInterleave {
    pub reads: Arc<crate::reads::TestDropReads>,
    pub head: TableHead,
    pub mode: TestInterleave,
    request: TableCreationRequest,
    blocks: Arc<crate::blocks::TestBlocks>,
    armed: AtomicBool,
    pub unknown: AtomicBool,
    pub reads_after_unknown: AtomicUsize,
    pub writes_after_unknown: AtomicUsize,
}

impl TestDropInterleave {
    pub async fn new(test: &crate::creation::TestCreation, mode: TestInterleave) -> Arc<Self> {
        let head = TableRepository::new(test.fixture.store.clone())
            .select(test.fixture.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap()
            .head;
        Arc::new(Self {
            reads: Arc::new(crate::reads::TestDropReads::new(test.fixture.store.clone())),
            head,
            mode,
            request: test.request.clone(),
            blocks: test.blocks.clone(),
            armed: AtomicBool::new(true),
            unknown: AtomicBool::new(false),
            reads_after_unknown: AtomicUsize::new(0),
            writes_after_unknown: AtomicUsize::new(0),
        })
    }

    async fn put(&self, key: StorageKey, after: StorageRecord) {
        let key = key.encode().unwrap();
        let before = self.reads.inner.values.load()[&key].bytes.clone();
        let after = after.encode().unwrap();
        assert!(matches!(
            self.reads
                .inner
                .compare_exchange(
                    &key,
                    Some(&before),
                    &after,
                    mutation_identity(&key, Some(&before), &after)
                )
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }

    async fn clear(&self, replacement: bool) {
        let key = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        };
        let StorageRecord::Active(mut root) = StorageRecord::decode(
            &key,
            &self.reads.inner.values.load()[&key.encode().unwrap()].bytes,
        )
        .unwrap() else {
            panic!()
        };
        if replacement {
            root.context = root.context.replacement(CatalogId::random()).unwrap();
        } else {
            root.state = RootState::Fencing;
        }
        self.put(StorageKey::Legacy(key), StorageRecord::Active(root))
            .await;
    }

    async fn retire(&self) {
        self.clear(true).await;
        let key = IcebergKey::Catalog {
            catalog: self.head.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        };
        let StorageRecord::Authority(mut authority) = StorageRecord::decode(
            &key,
            &self.reads.inner.values.load()[&key.encode().unwrap()].bytes,
        )
        .unwrap() else {
            panic!()
        };
        authority.lifecycle = CatalogLifecycle::Retired;
        self.put(StorageKey::Legacy(key), StorageRecord::Authority(authority))
            .await;
        self.put(
            storage_head_key(self.head.storage_location()),
            StorageRecord::RetiredTable(self.head.storage_location()),
        )
        .await;
    }

    async fn after_metadata(&self) {
        if !matches!(self.mode, TestInterleave::Clear(_) | TestInterleave::Rename)
            || !self.armed.swap(false, Ordering::SeqCst)
        {
            return;
        }
        if let TestInterleave::Clear(replacement) = self.mode {
            self.clear(replacement).await;
        } else {
            let outcome = TableLifecycles::new(self.reads.inner.clone())
                .execute(&TableLifecycleRequest {
                    context: self.request.context,
                    identity: RequestIdentity {
                        operation: OperationId::random(),
                        issued_ms: 100,
                    },
                    principal: "rival".into(),
                    namespace: self.request.namespace.clone(),
                    name: "events".into(),
                    action: TableLifecycleAction::Rename {
                        namespace: self.request.namespace.clone(),
                        name: "moved".into(),
                    },
                })
                .await
                .unwrap();
            assert_eq!(outcome.status, 204);
        }
    }
}

#[async_trait]
impl CatalogStore for TestDropInterleave {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        if self.unknown.load(Ordering::SeqCst) {
            self.reads_after_unknown.fetch_add(1, Ordering::SeqCst);
        }
        let result = self.reads.get(key).await?;
        if matches!(
            StorageKey::decode(key)?,
            StorageKey::Legacy(IcebergKey::Catalog {
                scope: CatalogScope::FileLocation | CatalogScope::File,
                ..
            })
        ) || matches!(StorageKey::decode(key)?, StorageKey::NativeTable(key) if key.scope == TableStorageScope::File)
        {
            self.after_metadata().await;
        }
        Ok(result)
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        if self.unknown.load(Ordering::SeqCst) {
            self.writes_after_unknown.fetch_add(1, Ordering::SeqCst);
        }
        let head = matches!(StorageKey::decode(key).and_then(|key| StorageRecord::decode(&key, value)), Ok(StorageRecord::TableHead(head)) if head.lifecycle == TableLifecycle::Tombstone);
        if head
            && matches!(self.mode, TestInterleave::Retire | TestInterleave::Unknown(_))
            && self.armed.swap(false, Ordering::SeqCst)
        {
            if let TestInterleave::Unknown(after) = self.mode {
                if after {
                    self.reads
                        .compare_exchange(key, expected, value, identity)
                        .await?;
                }
                self.unknown.store(true, Ordering::SeqCst);
                return Err(StoreError::Response);
            }
            self.retire().await;
        }
        let outcome = self
            .reads
            .compare_exchange(key, expected, value, identity)
            .await?;
        if head
            && matches!(self.mode, TestInterleave::ClearAfterHead)
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.clear(true).await;
        }
        Ok(outcome)
    }
}

#[async_trait]
impl NamespaceStore for TestDropInterleave {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.reads.scan_children(scan).await
    }
    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        if self.unknown.load(Ordering::SeqCst) {
            self.writes_after_unknown.fetch_add(1, Ordering::SeqCst);
        }
        if matches!(self.mode, TestInterleave::Recreate) && self.armed.swap(false, Ordering::SeqCst) {
            let mut request = self.request.clone();
            request.identity.operation = OperationId::random();
            assert_eq!(
                TableCreator::new(self.reads.inner.clone(), self.blocks.clone())
                    .create(&request)
                    .await
                    .unwrap()
                    .status,
                200
            );
        }
        self.reads.delete_mapping(key, expected, identity).await
    }
}

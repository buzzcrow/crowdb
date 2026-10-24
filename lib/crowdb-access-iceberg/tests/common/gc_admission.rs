// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{
        ActiveCatalogRecord, CasOutcome, CatalogContext, CatalogError, CatalogRepository, CatalogStore,
        ClearBounds, ManagementPrivilege, RootState, StoreError, StoredValue,
    },
    gc::{GcLimits, GcRepository, GcScan, GcStore, GcSystemScan, GcTask, GcTaskKind},
    key::{CatalogScope, IcebergKey, OperationId, SystemScope},
    operation::{
        mutation_identity, ManagementAction, ManagementOperation, ManagementRequest, RequestIdentity,
    },
    record::StorageRecord,
    table::{storage_head_key, TableLifecycle, TablePurgeTask},
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

use crate::{common::TestStore, metadata};

pub const NOW_MS: u64 = 1_000;

pub enum TestEntry {
    Retired(ManagementOperation),
    Purge(CatalogContext, TablePurgeTask),
}

#[derive(Clone, Copy)]
pub enum TestFault {
    Unknown = 1,
    Advanced,
    WrongContext,
    WrongKind,
    WrongHead,
    Empty,
    Rejected,
}

pub struct TestAdmissionStore {
    pub inner: Arc<TestStore>,
    fault: AtomicUsize,
}

impl TestAdmissionStore {
    pub fn arm(&self, fault: TestFault) {
        self.fault.store(fault as usize, Ordering::SeqCst);
    }

    pub fn reset_counts(&self) {
        self.inner.reads.store(0, Ordering::SeqCst);
        self.inner.cas_attempts.store(0, Ordering::SeqCst);
    }

    pub fn counts(&self) -> (usize, usize) {
        (
            self.inner.reads.load(Ordering::SeqCst),
            self.inner.cas_attempts.load(Ordering::SeqCst),
        )
    }
}

#[async_trait]
impl CatalogStore for TestAdmissionStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        bytes: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        let task_key = matches!(
            IcebergKey::decode(key),
            Ok(IcebergKey::Catalog {
                scope: CatalogScope::GcTask,
                ..
            })
        );
        let fault = if task_key && expected.is_none() {
            self.fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if fault == TestFault::Empty as usize || fault == TestFault::Rejected as usize {
            self.inner.cas_attempts.fetch_add(1, Ordering::SeqCst);
            return if fault == TestFault::Empty as usize {
                Ok(CasOutcome::Conflict(None))
            } else {
                Err(StoreError::Budget)
            };
        }
        let winner = conflicting_task(key, bytes, fault);
        let outcome = self
            .inner
            .compare_exchange(key, expected, winner.as_deref().unwrap_or(bytes), identity)
            .await?;
        if fault == TestFault::Unknown as usize {
            return Err(StoreError::Response);
        }
        Ok(winner.map_or(outcome, |bytes| {
            CasOutcome::Conflict(Some(StoredValue { bytes, revision: 2 }))
        }))
    }
}

fn conflicting_task(key: &[u8], bytes: &[u8], fault: usize) -> Option<Vec<u8>> {
    if fault < TestFault::Advanced as usize || fault > TestFault::WrongHead as usize {
        return None;
    }
    let key = IcebergKey::decode(key).unwrap();
    let StorageRecord::GcTask(mut task) = StorageRecord::decode(&key, bytes).unwrap() else {
        panic!("expected task");
    };
    if fault == TestFault::Advanced as usize {
        task.revision += 1;
        task.discovery_scope = 1;
        task.retry_at_ms = NOW_MS + 1_000;
    } else if fault == TestFault::WrongContext as usize {
        task.context.activation_epoch += 1;
    } else if fault == TestFault::WrongKind as usize {
        task.kind = if task.kind == GcTaskKind::RetiredCatalog {
            GcTaskKind::MultipartCleanup
        } else {
            GcTaskKind::RetiredCatalog
        };
        task.head = None;
    } else {
        task.head.as_mut().unwrap().operation_fence += 1;
    }
    Some(StorageRecord::GcTask(task).encode().unwrap())
}

#[async_trait]
impl GcStore for TestAdmissionStore {
    async fn scan_gc(&self, scan: GcScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_gc(scan).await
    }

    async fn scan_gc_system(&self, scan: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_gc_system(scan).await
    }

    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete_gc_record(key, expected, identity).await
    }
}

pub struct TestAdmission {
    pub store: Arc<TestAdmissionStore>,
    pub proposed: GcTask,
    pub entry: TestEntry,
}

impl TestAdmission {
    pub async fn new(retired: bool) -> Self {
        let inner = Arc::new(TestStore::default());
        let entry = if retired {
            retired_entry(inner.clone()).await
        } else {
            purge_entry(&inner).await
        };
        let proposed = match &entry {
            TestEntry::Retired(clear) => GcTask::plan(
                CatalogContext {
                    catalog: clear.request.confirmation.unwrap(),
                    activation_epoch: clear.request.expected_epoch,
                },
                clear.id(),
                None,
                NOW_MS,
                GcLimits::default(),
            )
            .unwrap(),
            TestEntry::Purge(context, marker) => GcTask::plan(
                *context,
                OperationId::from_bytes(marker.head.table.as_bytes()).unwrap(),
                Some(marker.head.clone()),
                marker.dropped_ms,
                GcLimits::default(),
            )
            .unwrap(),
        };
        let store = Arc::new(TestAdmissionStore {
            inner,
            fault: AtomicUsize::new(0),
        });
        store.reset_counts();
        Self {
            store,
            proposed,
            entry,
        }
    }

    pub async fn admit(&self) -> Result<GcTask, CatalogError> {
        self.admit_at(NOW_MS).await
    }

    pub async fn admit_at(&self, now_ms: u64) -> Result<GcTask, CatalogError> {
        let gc = GcRepository::new(self.store.clone());
        match &self.entry {
            TestEntry::Retired(clear) => gc.admit_retired(clear, now_ms, GcLimits::default()).await,
            TestEntry::Purge(context, marker) => {
                gc.admit_purge(*context, marker, now_ms, GcLimits::default())
                    .await
            }
        }
    }

    pub fn durable(&self) -> Option<GcTask> {
        let key = self.proposed.key();
        self.store
            .inner
            .values
            .load()
            .get(&key.encode().unwrap())
            .map(|value| {
                let StorageRecord::GcTask(task) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
                    panic!("expected task");
                };
                *task
            })
    }
}

async fn retired_entry(store: Arc<TestStore>) -> TestEntry {
    let catalog = CatalogRepository::new(
        store,
        ClearBounds {
            root_lease_ms: 0,
            request_ms: 10,
            delegated_access_ms: 0,
            clock_skew_ms: 1,
        },
    )
    .unwrap();
    let request = |action, epoch, identity| ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::from_bytes(&[identity; 16]).unwrap(),
            issued_ms: 100,
        },
        principal: "operator".into(),
        action,
        expected_epoch: epoch,
        display_name: "catalog".into(),
        confirmation: None,
        capabilities: None,
    };
    let initialized = catalog
        .execute(
            request(ManagementAction::Initialize, 0, 11),
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let mut clear = request(ManagementAction::Clear, 1, 22);
    clear.confirmation = Some(initialized.catalog);
    assert!(matches!(
        catalog
            .execute(clear.clone(), ManagementPrivilege::Clear, 101)
            .await,
        Err(CatalogError::Busy)
    ));
    let RootState::Published(transition) = catalog.status().await.unwrap().0.state else {
        panic!("expected published clear");
    };
    catalog
        .execute(
            clear.clone(),
            ManagementPrivilege::Clear,
            transition.complete_after_ms,
        )
        .await
        .unwrap();
    TestEntry::Retired(
        catalog
            .operation(clear.identity.operation)
            .await
            .unwrap()
            .unwrap(),
    )
}

async fn purge_entry(store: &TestStore) -> TestEntry {
    let mut head = metadata::head(b"{}", 1, None);
    head.lifecycle = TableLifecycle::Tombstone;
    head.pending_operation = Some(OperationId::random());
    let context = CatalogContext {
        catalog: head.catalog,
        activation_epoch: 1,
    };
    let marker = TablePurgeTask {
        activation_epoch: context.activation_epoch,
        head,
        dropped_ms: 100,
    };
    for (key, record) in [
        (
            IcebergKey::System {
                scope: SystemScope::ActiveRoot,
                suffix: Vec::new(),
            },
            StorageRecord::Active(ActiveCatalogRecord {
                context,
                state: RootState::Ready,
                operation: OperationId::random(),
            }),
        ),
        (
            marker.key(),
            StorageRecord::TablePurgeTask(Box::new(marker.clone())),
        ),
    ] {
        put(store, &key.encode().unwrap(), &record).await;
    }
    put(
        store,
        &storage_head_key(marker.head.storage_location()).encode().unwrap(),
        &StorageRecord::TableHead(Box::new(marker.head.clone())),
    )
    .await;
    TestEntry::Purge(context, marker)
}

async fn put(store: &TestStore, key: &[u8], record: &StorageRecord) {
    let bytes = record.encode().unwrap();
    store
        .compare_exchange(key, None, &bytes, mutation_identity(key, None, &bytes))
        .await
        .unwrap();
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    gc::{GcPhase, GcRepository, GcScan, GcStore, GcSystemScan, GcTask, GcTaskKind},
    key::{CatalogScope, IcebergKey},
    operation::mutation_identity,
    record::StorageRecord,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

use crate::{admission::TestAdmission, common::TestStore};

#[derive(Clone, Copy)]
pub enum TestFault {
    Applied,
    Unknown,
    Advanced,
    WrongContext,
    WrongKind,
    WrongHead,
    Rejected,
    Empty,
}

pub struct TestFailureStore {
    pub inner: Arc<TestStore>,
    fault: TestFault,
    after_cas: AtomicBool,
    pub reads: AtomicUsize,
    pub task_reads: AtomicUsize,
    pub following_reads: AtomicUsize,
    pub exchanges: AtomicUsize,
}

#[async_trait]
impl CatalogStore for TestFailureStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if self.after_cas.load(Ordering::Relaxed) {
            self.following_reads.fetch_add(1, Ordering::Relaxed);
        }
        if matches!(
            IcebergKey::decode(key),
            Ok(IcebergKey::Catalog {
                scope: CatalogScope::GcTask,
                ..
            })
        ) {
            self.task_reads.fetch_add(1, Ordering::Relaxed);
        }
        if matches!(
            IcebergKey::decode(key),
            Ok(IcebergKey::Catalog {
                scope: CatalogScope::Authority,
                ..
            })
        ) {
            return Err(StoreError::Response);
        }
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        bytes: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        let decoded = IcebergKey::decode(key).unwrap();
        assert!(matches!(
            decoded,
            IcebergKey::Catalog {
                scope: CatalogScope::GcTask,
                ..
            }
        ));
        self.exchanges.fetch_add(1, Ordering::Relaxed);
        self.after_cas.store(true, Ordering::Relaxed);
        if matches!(self.fault, TestFault::Rejected) {
            return Err(StoreError::Budget);
        }
        if matches!(self.fault, TestFault::Empty) {
            self.inner
                .delete_gc_record(key, expected.unwrap(), identity)
                .await?;
            return Ok(CasOutcome::Conflict(None));
        }
        let StorageRecord::GcTask(mut proposed) = StorageRecord::decode(&decoded, bytes).unwrap() else {
            panic!()
        };
        match self.fault {
            TestFault::Advanced => {
                proposed.revision += 1;
                proposed.paused = true;
            }
            TestFault::WrongContext => proposed.context.activation_epoch += 1,
            TestFault::WrongKind => {
                proposed.kind = if proposed.kind == GcTaskKind::RetiredCatalog {
                    GcTaskKind::MultipartCleanup
                } else {
                    GcTaskKind::RetiredCatalog
                };
                proposed.head = None;
            }
            TestFault::WrongHead => proposed.head.as_mut().unwrap().operation_fence += 1,
            _ => {}
        }
        let value = StorageRecord::GcTask(proposed).encode().unwrap();
        let result = self
            .inner
            .compare_exchange(key, expected, &value, identity)
            .await?;
        if matches!(self.fault, TestFault::Unknown) {
            return Err(StoreError::Response);
        }
        if matches!(self.fault, TestFault::Applied) {
            Ok(result)
        } else {
            Ok(CasOutcome::Conflict(self.inner.get(key).await?))
        }
    }
}

#[async_trait]
impl GcStore for TestFailureStore {
    async fn scan_gc(&self, _scan: GcScan) -> Result<MultiScanPage, StoreError> {
        Err(StoreError::Response)
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

pub struct TestFailure {
    pub store: Arc<TestFailureStore>,
    pub task: GcTask,
}

impl TestFailure {
    pub async fn new(retired: bool, fault: TestFault) -> Self {
        let fixture = TestAdmission::new(retired).await;
        let task = fixture.admit().await.unwrap();
        Self {
            task,
            store: Arc::new(TestFailureStore {
                inner: fixture.store.inner.clone(),
                fault,
                after_cas: AtomicBool::new(false),
                reads: AtomicUsize::new(0),
                task_reads: AtomicUsize::new(0),
                following_reads: AtomicUsize::new(0),
                exchanges: AtomicUsize::new(0),
            }),
        }
    }

    pub fn repository(&self) -> GcRepository {
        GcRepository::new(self.store.clone())
    }

    pub fn durable(&self) -> Option<GcTask> {
        let key = self.task.key();
        self.store
            .inner
            .values
            .load()
            .get(&key.encode().unwrap())
            .map(|value| {
                let StorageRecord::GcTask(task) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
                    panic!()
                };
                *task
            })
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.store.task_reads.load(Ordering::Relaxed),
            self.store.exchanges.load(Ordering::Relaxed),
            self.store.following_reads.load(Ordering::Relaxed),
        )
    }

    pub fn total_reads(&self) -> usize {
        self.store.reads.load(Ordering::Relaxed)
    }

    pub async fn install_retirement_owner(&mut self) -> GcTask {
        let mut task = self.task.advance().unwrap();
        task.phase = GcPhase::VerifyCleanup;
        GcRepository::new(self.store.inner.clone())
            .update(&self.task, &task)
            .await
            .unwrap();
        self.task = task;
        let mut owner = self.task.advance().unwrap();
        owner.phase = GcPhase::CleanupGc;
        let key = IcebergKey::Catalog {
            catalog: owner.context.catalog,
            scope: CatalogScope::GcRetirement,
            suffix: Vec::new(),
        }
        .encode()
        .unwrap();
        let bytes = StorageRecord::GcTask(Box::new(owner.clone())).encode().unwrap();
        self.store
            .inner
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
        owner
    }
}

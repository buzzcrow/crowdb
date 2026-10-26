// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    file::{canonical_file_key, ChunkRoot, FileBlockStore, FileIdentity, FileIoError, FileRecord},
    gc::{
        CandidatePhase, GcCandidate, GcLimits, GcPhase, GcRepository, GcScan, GcStore, GcSystemScan, GcTask,
        GcWorker, TreeReclaimCursor,
    },
    key::{CatalogScope, FileId, NamespaceId, OperationId, StorageKey},
    operation::mutation_identity,
    record::StorageRecord,
    table::{storage_head_key, TableHead, TableLifecycle, TablePurgeTask},
};
use crowdb_chunk_client::ReclaimOutcome;
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::{chunk_kv::ClientRequestId, chunkdb::rpc::Location};

use crate::common::{file::TestFile, TestStore};

pub const NOW: u64 = 2_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum TestStage {
    Delete = 1,
    Candidate = 2,
    Task = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum TestFault {
    UnknownBefore = 1,
    UnknownAfter = 2,
    ConflictSame = 3,
    ConflictAdvanced = 4,
}

pub struct TestInlineStore {
    pub inner: Arc<TestStore>,
    gets: AtomicUsize,
    scans: AtomicUsize,
    exchanges: AtomicUsize,
    deletes: AtomicUsize,
    fault: AtomicUsize,
    after_fault: AtomicBool,
    following_gets: AtomicUsize,
    following_task_gets: AtomicUsize,
}

#[async_trait]
impl CatalogStore for TestInlineStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        if self.after_fault.load(Ordering::Relaxed) {
            self.following_gets.fetch_add(1, Ordering::Relaxed);
            if Self::stage(key) == Some(TestStage::Task) {
                self.following_task_gets.fetch_add(1, Ordering::Relaxed);
            }
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
        self.exchanges.fetch_add(1, Ordering::Relaxed);
        let fault = Self::stage(key).map_or(0, |stage| self.take_fault(stage));
        if fault == TestFault::UnknownBefore as usize {
            return Err(StoreError::Response);
        }
        let advanced;
        let bytes = if fault == TestFault::ConflictAdvanced as usize {
            advanced = Self::advanced(key, bytes);
            advanced.as_slice()
        } else {
            bytes
        };
        let outcome = self
            .inner
            .compare_exchange(key, expected, bytes, identity)
            .await?;
        if fault == TestFault::UnknownAfter as usize {
            assert!(matches!(outcome, CasOutcome::Applied(_)));
            return Err(StoreError::Response);
        }
        if matches!(fault, 3 | 4) {
            return Ok(CasOutcome::Conflict(self.value(key)));
        }
        Ok(outcome)
    }
}

#[async_trait]
impl GcStore for TestInlineStore {
    async fn scan_gc(&self, scan: GcScan) -> Result<MultiScanPage, StoreError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.inner.scan_gc(scan).await
    }

    async fn scan_gc_system(&self, scan: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.inner.scan_gc_system(scan).await
    }

    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        let fault = self.take_fault(TestStage::Delete);
        if fault == TestFault::UnknownBefore as usize {
            return Err(StoreError::Response);
        }
        if fault == TestFault::ConflictAdvanced as usize {
            let decoded = StorageKey::decode(key).unwrap();
            let StorageRecord::File(mut replacement) = StorageRecord::decode(&decoded, expected).unwrap()
            else {
                panic!("expected file")
            };
            replacement.file = FileId::random();
            let bytes = StorageRecord::File(replacement).encode().unwrap();
            self.inner
                .compare_exchange(
                    key,
                    Some(expected),
                    &bytes,
                    mutation_identity(key, Some(expected), &bytes),
                )
                .await?;
            return Ok(CasOutcome::Conflict(self.value(key)));
        }
        let outcome = self.inner.delete_gc_record(key, expected, identity).await?;
        if fault == TestFault::UnknownAfter as usize {
            assert!(matches!(outcome, CasOutcome::Applied(_)));
            return Err(StoreError::Response);
        }
        if fault == TestFault::ConflictSame as usize {
            return Ok(CasOutcome::Conflict(None));
        }
        Ok(outcome)
    }
}

impl TestInlineStore {
    pub fn counts(&self) -> (usize, usize, usize, usize) {
        (
            self.gets.load(Ordering::Relaxed),
            self.scans.load(Ordering::Relaxed),
            self.exchanges.load(Ordering::Relaxed),
            self.deletes.load(Ordering::Relaxed),
        )
    }

    pub fn reset(&self) {
        for counter in [
            &self.gets,
            &self.scans,
            &self.exchanges,
            &self.deletes,
            &self.following_gets,
            &self.following_task_gets,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
        self.after_fault.store(false, Ordering::Relaxed);
    }

    pub fn arm(&self, stage: TestStage, fault: TestFault) {
        self.fault
            .store(stage as usize * 16 + fault as usize, Ordering::Relaxed);
    }

    pub fn fault_consumed(&self) -> bool {
        self.fault.load(Ordering::Relaxed) == 0 && self.after_fault.load(Ordering::Relaxed)
    }

    pub fn following_gets(&self) -> (usize, usize) {
        (
            self.following_gets.load(Ordering::Relaxed),
            self.following_task_gets.load(Ordering::Relaxed),
        )
    }

    fn take_fault(&self, stage: TestStage) -> usize {
        let value = self.fault.load(Ordering::Relaxed);
        if value / 16 != stage as usize {
            return 0;
        }
        self.after_fault.store(true, Ordering::Relaxed);
        self.fault.swap(0, Ordering::Relaxed) % 16
    }

    fn stage(key: &[u8]) -> Option<TestStage> {
        match StorageKey::decode(key).unwrap() {
            StorageKey::Legacy(crowdb_access_iceberg::key::IcebergKey::Catalog {
                scope: CatalogScope::GcCandidate,
                ..
            }) => Some(TestStage::Candidate),
            StorageKey::Legacy(crowdb_access_iceberg::key::IcebergKey::Catalog {
                scope: CatalogScope::GcTask,
                ..
            }) => Some(TestStage::Task),
            _ => None,
        }
    }

    fn value(&self, key: &[u8]) -> Option<StoredValue> {
        self.inner.values.load().get(key).cloned()
    }

    fn advanced(key: &[u8], bytes: &[u8]) -> Vec<u8> {
        let key = StorageKey::decode(key).unwrap();
        let mut record = StorageRecord::decode(&key, bytes).unwrap();
        match &mut record {
            StorageRecord::GcTask(task) => task.revision += 1,
            StorageRecord::GcCandidate(candidate) => candidate.revision += 1,
            _ => panic!("unexpected CAS"),
        }
        record.encode().unwrap()
    }
}

#[derive(Default)]
pub struct TestNoBlockIo(pub AtomicUsize);

#[async_trait]
impl FileBlockStore for TestNoBlockIo {
    async fn put(&self, _owner: FileIdentity, _height: u8, _bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(FileIoError::Bounds)
    }
    async fn read(&self, _root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(FileIoError::Bounds)
    }
    async fn reclaim(&self, _root: &ChunkRoot) -> Result<ReclaimOutcome, FileIoError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(FileIoError::Bounds)
    }
    async fn reclaim_location(&self, _location: &Location) -> Result<ReclaimOutcome, FileIoError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(FileIoError::Bounds)
    }
}

pub struct TestInline {
    pub store: Arc<TestInlineStore>,
    pub blocks: Arc<TestNoBlockIo>,
    pub task: GcTask,
    pub candidate: GcCandidate,
}

impl TestInline {
    pub async fn new() -> Self {
        let mut file = TestFile::new(TestStore::default()).await;
        file.table.storage_namespace = Some(NamespaceId::random());
        let record = file.record("metadata/inline.json", b"{}");
        let head = Self::tombstone_head(&file, &record);
        let mut task = GcTask::plan(
            file.context,
            OperationId::from_bytes(file.table.table.as_bytes()).unwrap(),
            Some(head.clone()),
            1,
            GcLimits::default(),
        )
        .unwrap();
        task.phase = GcPhase::Sweep;
        task.fenced = true;
        task.sweep_round = 1;
        let candidate = GcCandidate {
            task: task.identity,
            generation: head.generation,
            first_seen_ms: 100,
            not_before_ms: task.not_before_ms,
            revision: 1,
            phase: CandidatePhase::Retained,
            completed_round: 0,
            cursor: TreeReclaimCursor::new(&record).unwrap(),
            file: record,
            part: None,
            assembly: None,
            next_root: 0,
        };
        let marker = TablePurgeTask {
            activation_epoch: file.context.activation_epoch,
            head: head.clone(),
            dropped_ms: 1,
        };
        let mut fenced = head;
        fenced.lifecycle = TableLifecycle::Reclaiming;
        fenced.operation_fence += 1;
        fenced.pending_operation = Some(task.identity);
        let test = Self {
            store: Arc::new(TestInlineStore {
                inner: file.store,
                gets: AtomicUsize::new(0),
                scans: AtomicUsize::new(0),
                exchanges: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
                fault: AtomicUsize::new(0),
                after_fault: AtomicBool::new(false),
                following_gets: AtomicUsize::new(0),
                following_task_gets: AtomicUsize::new(0),
            }),
            blocks: Arc::new(TestNoBlockIo::default()),
            task,
            candidate,
        };
        for (key, record) in [
            (
                StorageKey::Legacy(test.task.key()),
                StorageRecord::GcTask(Box::new(test.task.clone())),
            ),
            (
                StorageKey::Legacy(marker.key()),
                StorageRecord::TablePurgeTask(Box::new(marker)),
            ),
            (
                storage_head_key(file.table),
                StorageRecord::TableHead(Box::new(fenced)),
            ),
            (
                canonical_file_key(&test.candidate.file.location),
                StorageRecord::File(Box::new(test.candidate.file.clone())),
            ),
            (
                StorageKey::Legacy(test.candidate.claim_key()),
                StorageRecord::GcCandidate(Box::new(test.candidate.clone())),
            ),
            (
                StorageKey::Legacy(test.candidate.key()),
                StorageRecord::GcCandidate(Box::new(test.candidate.clone())),
            ),
        ] {
            test.install(&key, &record).await;
        }
        test
    }

    fn tombstone_head(file: &TestFile, record: &FileRecord) -> TableHead {
        TableHead {
            catalog: file.context.catalog,
            table: file.table.table,
            namespace: NamespaceId::random(),
            name: "inline".into(),
            name_epoch: 1,
            lifecycle: TableLifecycle::Tombstone,
            generation: 7,
            metadata_file: record.file,
            metadata_location: record.location.clone(),
            metadata_digest: record.digest,
            commit_binding: None,
            format_version: 1,
            table_uuid: None,
            operation_fence: 2,
            pending_operation: Some(OperationId::random()),
        }
    }

    pub fn worker(&self) -> GcWorker {
        GcWorker::new(
            GcRepository::new(self.store.clone()),
            self.blocks.clone(),
            GcLimits::default(),
        )
        .unwrap()
    }

    pub async fn install(&self, key: &StorageKey, record: &StorageRecord) {
        let key = key.encode().unwrap();
        let before = self.store.inner.get(&key).await.unwrap();
        let bytes = record.encode().unwrap();
        assert!(matches!(
            self.store
                .inner
                .compare_exchange(
                    &key,
                    before.as_ref().map(|value| value.bytes.as_slice()),
                    &bytes,
                    mutation_identity(&key, before.as_ref().map(|value| value.bytes.as_slice()), &bytes)
                )
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }

    pub fn record(&self, key: &StorageKey) -> Option<StorageRecord> {
        self.store
            .inner
            .values
            .load()
            .get(&key.encode().unwrap())
            .map(|value| StorageRecord::decode(key, &value.bytes).unwrap())
    }

    pub fn file(&self) -> Option<FileRecord> {
        match self.record(&canonical_file_key(&self.candidate.file.location)) {
            Some(StorageRecord::File(record)) => Some(*record),
            None => None,
            other => panic!("{other:?}"),
        }
    }

    pub fn durable_candidate(&self) -> GcCandidate {
        let Some(StorageRecord::GcCandidate(candidate)) =
            self.record(&StorageKey::Legacy(self.candidate.key()))
        else {
            panic!("missing candidate")
        };
        *candidate
    }

    pub fn durable_task(&self) -> GcTask {
        let Some(StorageRecord::GcTask(task)) = self.record(&StorageKey::Legacy(self.task.key())) else {
            panic!("missing task")
        };
        *task
    }

    pub fn assert_no_block_io(&self) {
        assert_eq!(self.blocks.0.load(Ordering::Relaxed), 0);
    }

    pub async fn recover(&self) -> GcTask {
        let mut task = self.durable_task();
        for _ in 0..4 {
            let now = NOW
                .max(task.retry_at_ms)
                .max(task.not_before_ms)
                .max(self.durable_candidate().not_before_ms);
            task = self.worker().step(&task, now).await.unwrap();
            assert_eq!(task.deleted, 1);
            assert_eq!(task.reclaimed_bytes, self.candidate.file.length);
            if task.phase == GcPhase::Complete {
                self.assert_no_block_io();
                return task;
            }
        }
        panic!("recovery did not complete: {task:?}");
    }
}

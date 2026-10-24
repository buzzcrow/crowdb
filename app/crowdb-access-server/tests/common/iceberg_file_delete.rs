// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{ActiveCatalogRecord, CatalogAuthority, CatalogContext, CatalogStore, RootState},
    file::{
        canonical_file_key, ChunkRoot, ContentFormat, FileBlockStore, FileContent, FileIdentity, FileIoError,
        FileKind, FileRecord, FileRepository, TableLocation,
    },
    gc::{GcCandidate, GcLimits, GcPhase, GcRepository, GcTask, GcWorker},
    key::{CatalogId, CatalogScope, FileId, IcebergKey, NamespaceId, OperationId, SystemScope, TableId},
    operation::mutation_identity,
    record::StorageRecord,
};
use crowdb_access_server::iceberg::{mark_unreferenced_file_for_tests, FileS3ErrorCode};
use crowdb_chunk_client::ReclaimOutcome;
use crowdb_protocol::common::ChunkId;

#[path = "iceberg_file_delete/store.rs"]
mod store;
pub use store::TestStore;

pub const DELETED_MS: u64 = 2_000;

#[derive(Default)]
pub struct TestBlocks {
    pub notifications: AtomicUsize,
    pub denied: AtomicBool,
    pub lose_reply: AtomicBool,
    pub accepted: ArcSwap<Vec<ChunkRoot>>,
}

#[async_trait]
impl FileBlockStore for TestBlocks {
    async fn put(&self, _owner: FileIdentity, _height: u8, _bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        Err(FileIoError::Bounds)
    }

    async fn read(&self, _root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        Err(FileIoError::Bounds)
    }

    async fn reclaim(&self, root: &ChunkRoot) -> Result<ReclaimOutcome, FileIoError> {
        self.notifications.fetch_add(1, Ordering::SeqCst);
        if self.denied.load(Ordering::SeqCst) {
            return Err(FileIoError::Bounds);
        }
        self.accepted.rcu(|roots| {
            let mut next = (**roots).clone();
            next.push(root.clone());
            next
        });
        if self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(crowdb_chunk_client::IoError::Internal("lost DeleteRange reply".into()).into());
        }
        Ok(ReclaimOutcome::Accepted)
    }
}

pub struct TestDeletion {
    pub store: Arc<TestStore>,
    pub blocks: Arc<TestBlocks>,
    pub context: CatalogContext,
    pub file: FileRecord,
    pub limits: GcLimits,
}

impl TestDeletion {
    pub async fn new(native: bool) -> Self {
        let store = Arc::new(TestStore::default());
        let context = CatalogContext {
            catalog: CatalogId::random(),
            activation_epoch: 1,
        };
        let table = TableLocation {
            catalog: context.catalog,
            table: TableId::random(),
            storage_namespace: native.then(NamespaceId::random),
        };
        let root = ChunkRoot {
            chunk: ChunkId { high: 1, low: 7 },
            offset: 64,
            physical_length: 80,
            logical_offset: 0,
            logical_length: 16,
            height: 0,
            digest: [7; 32],
        };
        let file = FileRecord {
            file: FileId::random(),
            location: table.file("data/object.parquet").unwrap(),
            kind: FileKind::Data,
            format: ContentFormat::Parquet,
            length: 16,
            digest: root.digest,
            content: FileContent::Chunks { root: Some(root) },
            hint: None,
        };
        let authority = if native {
            CatalogAuthority::new_native(context.catalog, "current".into()).unwrap()
        } else {
            CatalogAuthority::new(context.catalog, "current".into()).unwrap()
        };
        for (key, record) in [
            (
                IcebergKey::System {
                    scope: SystemScope::ActiveRoot,
                    suffix: Vec::new(),
                },
                StorageRecord::Active(ActiveCatalogRecord {
                    context,
                    operation: OperationId::random(),
                    state: RootState::Ready,
                }),
            ),
            (
                IcebergKey::Catalog {
                    catalog: context.catalog,
                    scope: CatalogScope::Authority,
                    suffix: Vec::new(),
                },
                StorageRecord::Authority(authority),
            ),
        ] {
            let key = key.encode().unwrap();
            let bytes = record.encode().unwrap();
            store
                .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
                .await
                .unwrap();
        }
        FileRepository::new(store.clone())
            .publish(context, &file)
            .await
            .unwrap();
        Self {
            store,
            blocks: Arc::new(TestBlocks::default()),
            context,
            file,
            limits: GcLimits::default(),
        }
    }

    pub async fn mark(&self, now_ms: u64) -> Result<(), FileS3ErrorCode> {
        mark_unreferenced_file_for_tests(
            &FileRepository::new(self.store.clone()),
            self.context,
            &self.file.location,
            now_ms,
            Some(&[]),
        )
        .await
    }

    pub async fn candidate(&self) -> Option<GcCandidate> {
        let key = GcCandidate::deleted_key(&self.file);
        self.store
            .get(&key.encode().unwrap())
            .await
            .unwrap()
            .map(|value| {
                let StorageRecord::GcCandidate(candidate) =
                    StorageRecord::decode(&key, &value.bytes).unwrap()
                else {
                    panic!("expected a GC candidate");
                };
                *candidate
            })
    }

    pub async fn discover(&self) -> GcTask {
        let repository = GcRepository::new(self.store.clone());
        let mut task = repository
            .admit_multipart(self.context, DELETED_MS, self.limits)
            .await
            .unwrap();
        for _ in 0..20 {
            if self.candidate().await.is_some() {
                return task;
            }
            task = GcWorker::new(repository.clone(), self.blocks.clone(), self.limits)
                .unwrap()
                .step(&task, DELETED_MS)
                .await
                .unwrap();
        }
        panic!("DeletedFile was not discovered");
    }

    pub async fn step_after_restart(
        &self,
        task: &GcTask,
        now_ms: u64,
    ) -> Result<GcTask, crowdb_access_iceberg::gc::GcWorkError> {
        let repository = GcRepository::new(self.store.clone());
        let durable = repository
            .task(self.context.catalog, task.identity)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&durable, task);
        GcWorker::new(repository, self.blocks.clone(), self.limits)
            .unwrap()
            .step(&durable, now_ms)
            .await
    }

    pub async fn finish(&self, mut task: GcTask) {
        for _ in 0..100 {
            task = self
                .step_after_restart(&task, DELETED_MS + GcTask::PURGE_DELAY_MS)
                .await
                .unwrap();
            if task.phase == GcPhase::Complete {
                return;
            }
        }
        panic!("cleanup did not complete: {task:?}");
    }

    pub fn authority(&self) -> StorageRecord {
        let key = canonical_file_key(&self.file.location);
        let snapshot = self.store.values.load();
        StorageRecord::decode(&key, &snapshot.get(&key.encode().unwrap()).unwrap().bytes).unwrap()
    }
}

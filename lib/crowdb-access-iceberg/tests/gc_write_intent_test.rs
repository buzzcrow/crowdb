use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogLifecycle, CatalogStore, RootState},
    file::{
        ContentFormat, FileBlockStore, FileContent, FileIdentity, FileKind, FileRecord, FileRepository,
        FileWriteIntent,
    },
    gc::{GcLimits, GcPhase, GcRepository, GcTask, GcWorker},
    key::{CatalogId, CatalogScope, FileId, IcebergKey, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
};

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/gc_blocks.rs"]
mod gc_blocks;
#[path = "common/gc_terminal.rs"]
mod terminal;
mod common {
    pub mod store;
    pub use store::TestStore;
    #[allow(dead_code)]
    pub mod file;
    pub mod gc_store;
}

struct TestWrites {
    file: common::file::TestFile,
    blocks: Arc<gc_blocks::TestReclaimBlocks>,
    intent: FileWriteIntent,
    limits: GcLimits,
}

impl TestWrites {
    async fn new() -> Self {
        let file = common::file::TestFile::new(common::TestStore::default()).await;
        let blocks = Arc::new(gc_blocks::TestReclaimBlocks::default());
        let owner = FileIdentity {
            table: file.table,
            file: FileId::random(),
        };
        let root = blocks.put(owner, 0, b"orphan").await.unwrap();
        let intent = FileWriteIntent {
            identity: OperationId::random(),
            owner,
            root,
            created_ms: 10,
            not_before_ms: 0,
            deleting: false,
        };
        let limits = GcLimits {
            minimum_retention_ms: 10,
            ..GcLimits::default()
        };
        Self {
            file,
            blocks,
            intent,
            limits,
        }
    }

    async fn retire(&self) -> GcTask {
        let mut authority = CatalogAuthority::new(self.file.context.catalog, "old".into()).unwrap();
        authority.lifecycle = CatalogLifecycle::Retired;
        let key = IcebergKey::Catalog {
            catalog: self.file.context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        }
        .encode()
        .unwrap();
        let bytes = StorageRecord::Authority(authority).encode().unwrap();
        self.file
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
        self.file
            .root(
                self.file.context.replacement(CatalogId::random()).unwrap(),
                RootState::Ready,
            )
            .await;
        let task = GcTask::plan(self.file.context, OperationId::random(), None, 100, self.limits).unwrap();
        GcRepository::new(self.file.store.clone())
            .create(&task)
            .await
            .unwrap();
        task
    }

    async fn finish(&self, mut task: GcTask) -> GcTask {
        for _ in 0..150 {
            let repository = GcRepository::new(self.file.store.clone());
            let worker = GcWorker::new(repository.clone(), self.blocks.clone(), self.limits).unwrap();
            task = worker.run(&task, 1000.max(task.retry_at_ms)).await.unwrap();
            task = repository
                .task(task.context.catalog, task.identity)
                .await
                .unwrap()
                .unwrap();
            if matches!(task.phase, GcPhase::Complete | GcPhase::Quarantined) {
                return task;
            }
        }
        panic!("unfinished write GC: {task:?}");
    }
}

#[tokio::test]
async fn write_intent_roundtrips_and_binds_its_exact_owner_and_identity() {
    let fixture = TestWrites::new().await;
    let record = StorageRecord::FileWriteIntent(Box::new(fixture.intent.clone()));
    let bytes = record.encode().unwrap();
    assert_eq!(
        StorageRecord::decode(&fixture.intent.key(), &bytes).unwrap(),
        record
    );
    let mut wrong = fixture.intent.clone();
    wrong.owner.file = FileId::random();
    assert!(StorageRecord::decode(&wrong.key(), &bytes).is_err());
    assert!(StorageRecord::decode(&FileWriteIntent::fence_key(fixture.intent.owner), &bytes).is_err());
    wrong = fixture.intent.clone();
    wrong.deleting = true;
    assert!(wrong.validate().is_err());
}

#[tokio::test]
async fn uncertain_intent_reply_is_confirmed_before_authorizing_the_write() {
    let fixture = TestWrites::new().await;
    fixture.file.store.fail_after.store(
        fixture.file.store.writes.load(Ordering::Relaxed) + 1,
        Ordering::Relaxed,
    );
    fixture.intent.register(fixture.file.store.clone()).await.unwrap();
    fixture.intent.register(fixture.file.store.clone()).await.unwrap();
    assert!(fixture
        .file
        .store
        .get(&fixture.intent.key().encode().unwrap())
        .await
        .unwrap()
        .is_some());
    let task = fixture.retire().await;
    assert!(fixture.intent.register(fixture.file.store.clone()).await.is_err());
    fixture.finish(task).await;
}

#[tokio::test]
async fn unregistered_file_blocks_are_reclaimed_after_restart_and_lost_delete_reply() {
    let fixture = TestWrites::new().await;
    fixture.intent.register(fixture.file.store.clone()).await.unwrap();
    let task = fixture.retire().await;
    fixture.blocks.reply_loss.store(true, Ordering::Relaxed);
    let task = fixture.finish(task).await;
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.reclaimed_bytes, 0);
    assert!(fixture.blocks.blocks.values.load().is_empty());
    assert!(fixture
        .file
        .store
        .get(&fixture.intent.key().encode().unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(fixture
        .file
        .store
        .get(&FileWriteIntent::fence_key(fixture.intent.owner).encode().unwrap())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn shared_range_deferral_retains_intent_and_prevents_file_publication() {
    let fixture = TestWrites::new().await;
    fixture.intent.register(fixture.file.store.clone()).await.unwrap();
    let mut deleting = fixture.intent.clone();
    deleting.not_before_ms = 20;
    deleting.deleting = true;
    let key = FileWriteIntent::fence_key(deleting.owner).encode().unwrap();
    let bytes = StorageRecord::FileWriteIntent(Box::new(deleting))
        .encode()
        .unwrap();
    fixture
        .file
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    let record = FileRecord {
        file: fixture.intent.owner.file,
        location: fixture.file.table.file("data.parquet").unwrap(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: 6,
        digest: fixture.intent.root.digest,
        content: FileContent::Chunks {
            root: Some(fixture.intent.root.clone()),
        },
        hint: None,
    };
    assert!(FileRepository::new(fixture.file.store.clone())
        .publish(fixture.file.context, &record)
        .await
        .is_err());
    assert!(fixture.intent.register(fixture.file.store.clone()).await.is_err());
    let mut task = fixture.retire().await;
    fixture.blocks.deferred.store(true, Ordering::Relaxed);
    let worker = GcWorker::new(
        GcRepository::new(fixture.file.store.clone()),
        fixture.blocks.clone(),
        fixture.limits,
    )
    .unwrap();
    for _ in 0..100 {
        task = worker.step(&task, 1000.max(task.retry_at_ms)).await.unwrap();
        if task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Waiting);
    assert!(fixture
        .file
        .store
        .get(&fixture.intent.key().encode().unwrap())
        .await
        .unwrap()
        .is_some());
    assert_eq!(fixture.blocks.deletes.load(Ordering::Relaxed), 0);
    fixture.blocks.deferred.store(false, Ordering::Relaxed);
    assert_eq!(fixture.finish(task).await.phase, GcPhase::Complete);
}

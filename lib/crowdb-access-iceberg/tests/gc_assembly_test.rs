use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogLifecycle, CatalogStore, RootState},
    file::{FileBlockStore, FileTreeWriter, MultipartPhase, MultipartSession},
    gc::{GcLimits, GcPhase, GcRepository, GcStalledReason, GcTask, GcWorker},
    key::{CatalogId, CatalogScope, IcebergKey, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
};

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/gc_blocks.rs"]
mod gc_blocks;
#[path = "common/multipart.rs"]
mod multipart;
mod common {
    pub mod store;
    pub use store::TestStore;
    #[allow(dead_code)]
    pub mod file;
    pub mod gc_store;
}

struct TestAssembly {
    file: common::file::TestFile,
    blocks: Arc<gc_blocks::TestReclaimBlocks>,
    session: MultipartSession,
    task: GcTask,
    limits: GcLimits,
}

impl TestAssembly {
    async fn new(phase: MultipartPhase) -> Self {
        let file = common::file::TestFile::new(common::TestStore::default()).await;
        let blocks = Arc::new(gc_blocks::TestReclaimBlocks::default());
        let mut session = multipart::session();
        session.context = file.context;
        session.owner.table = file.table;
        session.location = file.table.file("assembly.parquet").unwrap();
        session.phase = phase;
        session.part_count = 1;
        session.staged_bytes = 600;
        session.limits.max_part_bytes = 600;
        let mut writer = FileTreeWriter::new(blocks.clone(), session.owner, 2).unwrap();
        writer.push(&vec![7; 600]).await.unwrap();
        let mut completion = multipart::completion(&session);
        completion.progress.writer = Some(writer.checkpoint().await.unwrap());
        completion.progress.completed_bytes = 600;
        completion.progress.next_part = 1;
        if phase != MultipartPhase::Aborted {
            completion.candidate = Some(writer.finish().await.unwrap());
            completion.publication = Some(completion.selection.clone());
        }
        if phase == MultipartPhase::Published {
            session.published = Some(session.owner.file);
        }
        session.completion = Some(completion);
        let mut authority = CatalogAuthority::new(file.context.catalog, "old".into()).unwrap();
        authority.lifecycle = CatalogLifecycle::Retired;
        let authority_key = IcebergKey::Catalog {
            catalog: file.context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        };
        for (key, record) in [
            (authority_key, StorageRecord::Authority(authority)),
            (
                session.key(),
                StorageRecord::MultipartSession(Box::new(session.clone())),
            ),
        ] {
            let key = key.encode().unwrap();
            let bytes = record.encode().unwrap();
            file.store
                .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
                .await
                .unwrap();
        }
        file.root(
            file.context.replacement(CatalogId::random()).unwrap(),
            RootState::Ready,
        )
        .await;
        let limits = GcLimits {
            minimum_retention_ms: 1,
            ..GcLimits::default()
        };
        let task = GcTask::plan(file.context, OperationId::random(), None, 1000, limits).unwrap();
        GcRepository::new(file.store.clone()).create(&task).await.unwrap();
        Self {
            file,
            blocks,
            session,
            task,
            limits,
        }
    }

    async fn run(&mut self) {
        for _ in 0..2000 {
            let repository = GcRepository::new(self.file.store.clone());
            let worker = GcWorker::new(repository.clone(), self.blocks.clone(), self.limits).unwrap();
            self.task = worker
                .run(&self.task, 1_000_000.max(self.task.retry_at_ms))
                .await
                .unwrap();
            self.task = repository
                .task(self.task.context.catalog, self.task.identity)
                .await
                .unwrap()
                .unwrap();
            if matches!(self.task.phase, GcPhase::Complete | GcPhase::Quarantined) {
                return;
            }
        }
        panic!("assembly did not terminate: {:?}", self.task);
    }
}

#[tokio::test]
async fn aborted_frontier_reclaims_each_tree_before_checkpoint_across_restarts_and_lost_reply() {
    let mut fixture = TestAssembly::new(MultipartPhase::Aborted).await;
    fixture.blocks.reply_loss.store(true, Ordering::Relaxed);
    fixture.run().await;
    assert_eq!(fixture.task.phase, GcPhase::Complete);
    assert!(fixture.blocks.blocks.values.load().is_empty());
    assert!(fixture
        .file
        .store
        .get(&fixture.session.key().encode().unwrap())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn conflicted_complete_tree_does_not_revisit_its_shared_checkpoint_frontier() {
    let mut fixture = TestAssembly::new(MultipartPhase::Conflicted).await;
    fixture.run().await;
    assert_eq!(fixture.task.phase, GcPhase::Complete);
    assert!(fixture.blocks.blocks.values.load().is_empty());
}

#[tokio::test]
async fn published_checkpoint_reclamation_preserves_the_assembled_file_tree() {
    use crowdb_access_iceberg::file::{ContentFormat, FileContent, FileKind, FileReader, FileRecord};
    let mut fixture = TestAssembly::new(MultipartPhase::Published).await;
    let before = fixture.blocks.blocks.values.load().len();
    let tree = fixture
        .session
        .completion
        .as_ref()
        .unwrap()
        .candidate
        .clone()
        .unwrap();
    fixture.run().await;
    assert_eq!(fixture.task.phase, GcPhase::Complete);
    assert_eq!(fixture.blocks.blocks.values.load().len(), before - 1);
    let record = FileRecord {
        file: fixture.session.owner.file,
        location: fixture.session.location.clone(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    let mut reader = FileReader::new(fixture.blocks, record, None, 64).unwrap();
    let mut actual = Vec::new();
    while let Some(bytes) = reader.next().await.unwrap() {
        actual.extend(bytes);
    }
    assert_eq!(actual, vec![7; 600]);
}

#[tokio::test]
async fn corrupted_checkpoint_is_quarantined_without_deleting_children_or_session() {
    let mut fixture = TestAssembly::new(MultipartPhase::Aborted).await;
    fixture.blocks.blocks.corrupt_reads.store(true, Ordering::Relaxed);
    fixture.run().await;
    assert_eq!(fixture.task.phase, GcPhase::Quarantined);
    assert_eq!(fixture.task.stalled, GcStalledReason::Corruption);
    assert_eq!(fixture.blocks.deletes.load(Ordering::Relaxed), 0);
    assert!(fixture
        .file
        .store
        .get(&fixture.session.key().encode().unwrap())
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn unsupported_checkpoint_ranges_remain_pending_without_accounting_completion() {
    let mut fixture = TestAssembly::new(MultipartPhase::Aborted).await;
    fixture.blocks.deferred.store(true, Ordering::Relaxed);
    let repository = GcRepository::new(fixture.file.store.clone());
    let worker = GcWorker::new(repository, fixture.blocks.clone(), fixture.limits).unwrap();
    for _ in 0..80 {
        fixture.task = worker
            .step(&fixture.task, 1_000_000.max(fixture.task.retry_at_ms))
            .await
            .unwrap();
        if fixture.task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert_eq!(fixture.task.phase, GcPhase::Waiting);
    assert_eq!(fixture.task.stalled, GcStalledReason::UnsupportedRange);
    assert_eq!(fixture.task.deleted, 0);
    assert_eq!(fixture.task.reclaimed_bytes, 0);
    let checkpoint = &fixture
        .session
        .completion
        .as_ref()
        .unwrap()
        .progress
        .writer
        .as_ref()
        .unwrap()
        .root;
    assert!(fixture.blocks.read(checkpoint).await.is_ok());
    fixture.blocks.deferred.store(false, Ordering::Relaxed);
    fixture.run().await;
    assert_eq!(fixture.task.phase, GcPhase::Complete);
}

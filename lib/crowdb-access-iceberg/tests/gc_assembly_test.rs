#[path = "common/legacy_checkpoint.rs"]
mod legacy_checkpoint;
use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogLifecycle, CatalogStore, RootState},
    file::{FileBlockStore, MultipartPhase, MultipartSession},
    gc::{GcLimits, GcPhase, GcRepository, GcStalledReason, GcTask, GcWorker},
    key::{CatalogId, CatalogScope, IcebergKey, OperationId},
    operation::{mutation_identity, PayloadStore},
    record::StorageRecord,
};

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/gc_blocks.rs"]
mod gc_blocks;
#[path = "common/multipart.rs"]
mod multipart;
#[path = "common/multipart_recovery_store.rs"]
mod scan;
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
        Self::new_with_catalog(phase, true).await
    }

    async fn new_with_catalog(phase: MultipartPhase, retire: bool) -> Self {
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
        let (checkpoint, tree) =
            legacy_checkpoint::TestLegacyCheckpoint::create(blocks.clone(), session.owner, &vec![7; 600], 2)
                .await;
        let mut completion = multipart::completion(&session);
        if !retire {
            completion.selection = PayloadStore::new(file.store.clone())
                .put(file.context.catalog, session.upload, b"selection")
                .await
                .unwrap();
            completion.progress.selection = completion.selection.digest;
        }
        completion.progress.writer = Some(checkpoint);
        completion.progress.completed_bytes = 600;
        completion.progress.next_part = 1;
        if !matches!(phase, MultipartPhase::Aborted | MultipartPhase::Completing) {
            completion.candidate = Some(tree);
            completion.publication = Some(completion.selection.clone());
        }
        if phase == MultipartPhase::Published {
            session.published = Some(session.owner.file);
        }
        session.completion = Some(completion);
        let mut authority = CatalogAuthority::new(file.context.catalog, "old".into()).unwrap();
        if retire {
            authority.lifecycle = CatalogLifecycle::Retired;
        }
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
        if retire {
            file.root(
                file.context.replacement(CatalogId::random()).unwrap(),
                RootState::Ready,
            )
            .await;
        }
        let limits = GcLimits {
            minimum_retention_ms: 1,
            ..GcLimits::default()
        };
        let repository = GcRepository::new(file.store.clone());
        let task = if retire {
            let task = GcTask::plan(file.context, OperationId::random(), None, 1000, limits).unwrap();
            repository.create(&task).await.unwrap();
            task
        } else {
            repository
                .admit_multipart(file.context, 1000, limits)
                .await
                .unwrap()
        };
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
async fn legacy_complete_progress_is_rejected_without_copy_and_expiry_preserves_gc_frontier() {
    use crowdb_access_iceberg::file::{
        MultipartRecovery, MultipartRepository, MultipartSelection, MultipartWorkError, SelectedPart,
    };
    let mut fixture = TestAssembly::new_with_catalog(MultipartPhase::Completing, false).await;
    let before = fixture.blocks.blocks.values.load_full();
    let repository = MultipartRepository::new(fixture.file.store.clone());
    let selection = MultipartSelection::new(vec![SelectedPart {
        number: 1,
        revision: 1,
        digest: [1; 32],
    }])
    .unwrap();
    let error = repository
        .assemble(&fixture.session, &selection, fixture.blocks.clone(), 1000, 2)
        .await
        .unwrap_err();
    assert!(matches!(error, MultipartWorkError::LegacyProgress));
    assert_eq!(*fixture.blocks.blocks.values.load_full(), *before);
    let report = MultipartRecovery::new(fixture.file.store.clone())
        .recover_page(fixture.session.context, None, fixture.session.expires_ms)
        .await
        .unwrap();
    assert_eq!(report.progressed, 1);
    let current = repository
        .load(fixture.session.context, fixture.session.upload)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.phase, MultipartPhase::Aborted);
    assert_eq!(current.completion, fixture.session.completion);
    assert_eq!(*fixture.blocks.blocks.values.load_full(), *before);
    fixture.run().await;
    assert_eq!(fixture.task.phase, GcPhase::Complete);
    assert!(fixture.blocks.blocks.values.load().is_empty());
}

#[tokio::test]
async fn valid_storage_digest_cannot_hide_invalid_legacy_frontier_coverage_owner_or_height() {
    use crowdb_access_iceberg::file::{FileDigest, FileWriterCheckpoint};
    for changed in 0..4 {
        let mut fixture = TestAssembly::new(MultipartPhase::Aborted).await;
        let completion = fixture.session.completion.as_ref().unwrap();
        let mut bytes = fixture
            .blocks
            .read(&completion.progress.writer.as_ref().unwrap().root)
            .await
            .unwrap();
        let digest_length = usize::from(u16::from_be_bytes(bytes[5..7].try_into().unwrap()));
        let mut directory = 7 + digest_length;
        while u32::from_be_bytes(bytes[directory..directory + 4].try_into().unwrap()) == 0 {
            directory += 4;
        }
        directory += 4;
        match changed {
            0 => {
                let mut digest = FileDigest::new(fixture.session.owner);
                digest.update(b"wrong length").unwrap();
                bytes[7..7 + digest_length].copy_from_slice(&digest.checkpoint());
            }
            1 => bytes.push(0),
            2 => bytes[directory + 53] += 1,
            _ => bytes[directory + 5] ^= 1,
        }
        let old = StorageRecord::MultipartSession(Box::new(fixture.session.clone()))
            .encode()
            .unwrap();
        let root = fixture
            .blocks
            .put(fixture.session.owner, 0, &bytes)
            .await
            .unwrap();
        fixture.session.completion.as_mut().unwrap().progress.writer = Some(FileWriterCheckpoint { root });
        let key = fixture.session.key().encode().unwrap();
        let next = StorageRecord::MultipartSession(Box::new(fixture.session.clone()))
            .encode()
            .unwrap();
        fixture
            .file
            .store
            .compare_exchange(
                &key,
                Some(&old),
                &next,
                mutation_identity(&key, Some(&old), &next),
            )
            .await
            .unwrap();
        fixture.run().await;
        assert_eq!(fixture.task.phase, GcPhase::Quarantined, "variant {changed}");
        assert_eq!(fixture.task.stalled, GcStalledReason::Corruption);
        assert_eq!(fixture.blocks.deletes.load(Ordering::Relaxed), 0);
        assert!(fixture.file.store.get(&key).await.unwrap().is_some());
    }
}

#[tokio::test]
async fn active_cleanup_reclaims_aborted_assembly_before_session_metadata() {
    let mut fixture = TestAssembly::new_with_catalog(MultipartPhase::Aborted, false).await;
    let key = fixture.session.key().encode().unwrap();
    let selection = fixture
        .session
        .completion
        .as_ref()
        .unwrap()
        .selection
        .page_key(0)
        .unwrap()
        .encode()
        .unwrap();
    fixture.run().await;
    assert_eq!(fixture.task.phase, GcPhase::Complete);
    assert!(fixture.file.store.get(&key).await.unwrap().is_none());
    assert!(fixture.file.store.get(&selection).await.unwrap().is_none());
    assert!(fixture.blocks.blocks.values.load().is_empty());
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

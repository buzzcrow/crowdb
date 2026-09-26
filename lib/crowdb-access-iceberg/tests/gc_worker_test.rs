use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogLifecycle, CatalogStore, RootState},
    file::{file_key, ContentFormat, FileContent, FileIdentity, FileKind, FileRepository, FileTreeWriter},
    gc::{GcLimits, GcPhase, GcRepository, GcStalledReason, GcTask, GcWorker},
    key::{CatalogId, CatalogScope, FileId, IcebergKey, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
};

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/gc_blocks.rs"]
mod gc_blocks;
mod common {
    pub mod store;
    pub use store::TestStore;
    #[allow(dead_code)]
    pub mod file;
    pub mod gc_store;
}

async fn fixture(
    retire: bool,
) -> (
    common::file::TestFile,
    Arc<gc_blocks::TestReclaimBlocks>,
    GcTask,
    GcLimits,
    FileId,
) {
    let fixture = common::file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(gc_blocks::TestReclaimBlocks::default());
    let owner = FileIdentity {
        table: fixture.table,
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 128).unwrap();
    writer.push(&vec![17; 4096]).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let file = crowdb_access_iceberg::file::FileRecord {
        file: owner.file,
        location: fixture.table.file("data/object.parquet").unwrap(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    FileRepository::new(fixture.store.clone())
        .publish(fixture.context, &file)
        .await
        .unwrap();
    let mut retired = CatalogAuthority::new(fixture.context.catalog, "retired".into()).unwrap();
    if retire {
        retired.lifecycle = CatalogLifecycle::Retired;
    }
    let key = IcebergKey::Catalog {
        catalog: fixture.context.catalog,
        scope: CatalogScope::Authority,
        suffix: Vec::new(),
    }
    .encode()
    .unwrap();
    let bytes = StorageRecord::Authority(retired).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    if retire {
        fixture
            .root(
                fixture.context.replacement(CatalogId::random()).unwrap(),
                RootState::Ready,
            )
            .await;
    }
    let limits = GcLimits {
        minimum_retention_ms: 10,
        ..GcLimits::default()
    };
    let task = GcTask::plan(fixture.context, OperationId::random(), None, 1000, limits).unwrap();
    GcRepository::new(fixture.store.clone())
        .create(&task)
        .await
        .unwrap();
    (fixture, blocks, task, limits, file.file)
}

#[tokio::test]
async fn retired_file_reclamation_survives_worker_restart_at_every_step() {
    let (fixture, blocks, mut task, limits, file) = fixture(true).await;
    for _ in 0..300 {
        let repository = GcRepository::new(fixture.store.clone());
        let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
        task = worker.step(&task, 2000_u64.max(task.retry_at_ms)).await.unwrap();
        task = repository
            .task(task.context.catalog, task.identity)
            .await
            .unwrap()
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 1);
    assert_eq!(task.reclaimed_bytes, 4096);
    assert!(blocks.blocks.values.load().is_empty());
    assert!(fixture
        .store
        .get(&file_key(fixture.context.catalog, file).encode().unwrap())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn retirement_adopts_a_purge_cursor_after_a_child_was_physically_deleted() {
    use crowdb_access_iceberg::{
        file::FileBlockStore,
        gc::{CandidatePhase, GcCandidate, ReclaimStep, TreeReclaimCursor},
        key::NamespaceId,
        table::{TableHead, TableLifecycle},
    };
    let (fixture, blocks, mut task, limits, file_id) = fixture(true).await;
    let repository = GcRepository::new(fixture.store.clone());
    let key = file_key(fixture.context.catalog, file_id);
    let stored = fixture.store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::File(file) = StorageRecord::decode(&key, &stored.bytes).unwrap() else {
        panic!()
    };
    let metadata = fixture.record("metadata/old.json", b"{}");
    let head = TableHead {
        catalog: fixture.context.catalog,
        table: fixture.table.table,
        namespace: NamespaceId::random(),
        name: "dropped".into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Tombstone,
        generation: 7,
        metadata_file: metadata.file,
        metadata_location: metadata.location,
        metadata_digest: metadata.digest,
        format_version: 1,
        table_uuid: None,
        operation_fence: 2,
        pending_operation: Some(OperationId::random()),
    };
    let mut old = GcTask::plan(fixture.context, OperationId::random(), Some(head), 500, limits).unwrap();
    old.paused = true;
    repository.create(&old).await.unwrap();
    let initial = GcCandidate {
        task: old.identity,
        generation: 7,
        first_seen_ms: 500,
        not_before_ms: 510,
        revision: 1,
        phase: CandidatePhase::Retained,
        completed_round: 0,
        cursor: TreeReclaimCursor::new(&file).unwrap(),
        file: *file,
    };
    repository.claim_candidate(&initial).await.unwrap();
    let mut interrupted = initial.clone();
    interrupted.phase = CandidatePhase::Deleting;
    interrupted.revision += 1;
    loop {
        match interrupted.cursor.next(blocks.as_ref()).await.unwrap() {
            ReclaimStep::Descended(cursor) => interrupted.cursor = cursor,
            ReclaimStep::Delete(cursor) => {
                interrupted.cursor = cursor;
                break;
            }
            ReclaimStep::Complete => panic!("expected a physical child"),
        }
    }
    repository.candidate(Some(&initial), &interrupted).await.unwrap();
    blocks
        .reclaim(interrupted.cursor.pending.as_ref().unwrap())
        .await
        .unwrap();
    let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
    for _ in 0..50 {
        task = worker.run(&task, 2000).await.unwrap();
        if task.stalled == GcStalledReason::Protected {
            break;
        }
    }
    assert_eq!(task.stalled, GcStalledReason::Protected);
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 1);
    repository.pause(&old, false).await.unwrap();
    let mut now = 10_000;
    for _ in 0..300 {
        now = now.max(task.retry_at_ms);
        task = GcWorker::new(repository.clone(), blocks.clone(), limits)
            .unwrap()
            .run(&task, now)
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 1);
    assert!(blocks.blocks.values.load().is_empty());
    let mut stale = interrupted.clone();
    stale.revision += 1;
    assert!(repository.candidate(Some(&interrupted), &stale).await.is_err());
    let current = repository.claim_candidate(&initial).await.unwrap();
    assert_eq!(current.task, task.identity);
    assert_eq!(current.key(), initial.key());
    assert_eq!(current.phase, CandidatePhase::Complete);
}

#[tokio::test]
async fn files_arriving_after_initial_discovery_are_rescanned_before_sweep() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let repository = GcRepository::new(fixture.store.clone());
    let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
    for _ in 0..10 {
        task = worker.step(&task, 2000).await.unwrap();
        if task.phase == GcPhase::Roots {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Roots);
    let file = fixture.record("metadata/late.json", b"{}");
    let key = file_key(fixture.context.catalog, file.file).encode().unwrap();
    let bytes = StorageRecord::File(Box::new(file)).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    let mut now = 3000;
    for _ in 0..300 {
        now = now.max(task.retry_at_ms);
        task = GcWorker::new(repository.clone(), blocks.clone(), limits)
            .unwrap()
            .run(&task, now)
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 2);
    assert_eq!(task.reclaimed_bytes, 4098);
    assert!(fixture.store.get(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn missing_file_claim_stops_sweep_before_any_physical_deletion() {
    use crowdb_access_iceberg::gc::{GcScan, GcStore};
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    for _ in 0..30 {
        task = worker.step(&task, 2000).await.unwrap();
        if task.phase == GcPhase::Sweep {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Sweep);
    let page = fixture
        .store
        .scan_gc(GcScan {
            catalog: task.context.catalog,
            scope: Some(CatalogScope::GcClaim),
            prefix: Vec::new(),
            after: Vec::new(),
            items: 1,
            bytes: 128 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    let claim = &page.items[0];
    fixture
        .store
        .delete_gc_record(
            &claim.key,
            &claim.value,
            mutation_identity(&claim.key, Some(&claim.value), &[]),
        )
        .await
        .unwrap();
    assert!(worker.step(&task, 3000).await.is_err());
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn lost_delete_reply_retries_the_durable_intent_without_rereading_deleted_bytes() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    blocks.reply_loss.store(true, Ordering::Relaxed);
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    let mut now = 2000;
    let mut failed = false;
    for _ in 0..300 {
        task = worker.run(&task, now.max(task.retry_at_ms)).await.unwrap();
        if task.stalled == GcStalledReason::Storage {
            failed = true;
            assert_eq!(task.deleted, 0);
            now = task.retry_at_ms;
        }
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert!(failed);
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 1);
    assert!(blocks.blocks.values.load().is_empty());
    assert_eq!(worker.status().active, 0);
}

#[tokio::test]
async fn repeated_corruption_quarantines_without_deleting_any_block() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    blocks.blocks.corrupt_reads.store(true, Ordering::Relaxed);
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    let mut now = 2000;
    for _ in 0..50 {
        task = worker.run(&task, now.max(task.retry_at_ms)).await.unwrap();
        now = now.max(task.retry_at_ms);
        if task.phase == GcPhase::Quarantined {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Quarantined);
    assert_eq!(task.stalled, GcStalledReason::Corruption);
    assert_eq!(task.attempts, u32::from(limits.corruption_attempts));
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert_eq!(worker.run(&task, now).await.unwrap(), task);
}

#[tokio::test]
async fn background_timeout_keeps_the_deletion_intent_and_releases_admission() {
    let (fixture, blocks, mut task, mut limits, _) = fixture(true).await;
    limits.step_ms = 10;
    blocks.delay_ms.store(1000, Ordering::Relaxed);
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    for _ in 0..100 {
        task = worker.run(&task, 2000_u64.max(task.retry_at_ms)).await.unwrap();
        if task.stalled == GcStalledReason::Storage {
            break;
        }
    }
    assert_eq!(task.stalled, GcStalledReason::Storage);
    assert_eq!(task.attempts, 1);
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert_eq!(worker.status().active, 0);
    blocks.delay_ms.store(0, Ordering::Relaxed);
    let now = task.retry_at_ms;
    for _ in 0..300 {
        task = worker.run(&task, now.max(task.retry_at_ms)).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 1);
}

#[tokio::test]
async fn unsupported_shared_ranges_keep_durable_work_and_never_claim_reclaimed_bytes() {
    let (fixture, blocks, mut task, limits, file) = fixture(true).await;
    blocks.deferred.store(true, Ordering::Relaxed);
    let repository = GcRepository::new(fixture.store.clone());
    let worker = GcWorker::new(repository, blocks.clone(), limits).unwrap();
    for _ in 0..100 {
        task = worker.step(&task, 2000_u64.max(task.retry_at_ms)).await.unwrap();
        if task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Waiting);
    assert_eq!(task.stalled, GcStalledReason::UnsupportedRange);
    assert_eq!(task.reclaimed_bytes, 0);
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert!(fixture
        .store
        .get(&file_key(fixture.context.catalog, file).encode().unwrap())
        .await
        .unwrap()
        .is_some());
    blocks.deferred.store(false, Ordering::Relaxed);
    for _ in 0..300 {
        task = worker.step(&task, 100_000).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.reclaimed_bytes, 4096);
}

#[tokio::test]
async fn deferred_rounds_do_not_count_completed_files_more_than_once() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let inline = fixture.record("metadata/orphan.json", b"{}");
    let key = file_key(fixture.context.catalog, inline.file).encode().unwrap();
    let bytes = StorageRecord::File(Box::new(inline)).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    blocks.deferred.store(true, Ordering::Relaxed);
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    for _ in 0..100 {
        task = worker.run(&task, 2000_u64.max(task.retry_at_ms)).await.unwrap();
        if task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Waiting);
    assert_eq!(task.deleted, 1);
    assert_eq!(task.reclaimed_bytes, 2);
    blocks.deferred.store(false, Ordering::Relaxed);
    for _ in 0..300 {
        task = worker.run(&task, 100_000).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 2);
    assert_eq!(task.reclaimed_bytes, 4098);
}

#[tokio::test]
async fn late_candidate_discovery_starts_a_fresh_retention_window() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    let discovered_ms = 1_000_000;
    for _ in 0..20 {
        task = worker.run(&task, discovered_ms).await.unwrap();
        if task.stalled == GcStalledReason::Retention {
            break;
        }
    }
    assert_eq!(task.retry_at_ms, discovered_ms + limits.minimum_retention_ms);
    assert_eq!(task.stalled, GcStalledReason::Retention);
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert_eq!(worker.run(&task, task.retry_at_ms - 1).await.unwrap(), task);
    let eligible_ms = task.retry_at_ms;
    for _ in 0..300 {
        task = worker.run(&task, eligible_ms).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 1);
}

#[tokio::test]
async fn purge_fences_new_readers_and_waits_for_the_existing_pin() {
    use crowdb_access_iceberg::{
        gc::{GcPin, ReaderPins},
        key::NamespaceId,
        table::{head_key, TableHead, TableLifecycle, TablePurgeTask},
    };
    let (fixture, blocks, _, limits, _) = fixture(false).await;
    let metadata = fixture.record("metadata/table.json", b"{}");
    FileRepository::new(fixture.store.clone())
        .publish(fixture.context, &metadata)
        .await
        .unwrap();
    let mut head = TableHead {
        catalog: fixture.context.catalog,
        table: fixture.table.table,
        namespace: NamespaceId::random(),
        name: "purged".into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Ready,
        generation: 1,
        metadata_file: metadata.file,
        metadata_location: metadata.location,
        metadata_digest: metadata.digest,
        format_version: 1,
        table_uuid: None,
        operation_fence: 1,
        pending_operation: None,
    };
    let key = head_key(head.catalog, head.table).encode().unwrap();
    let before = StorageRecord::TableHead(Box::new(head.clone())).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &before, mutation_identity(&key, None, &before))
        .await
        .unwrap();
    let pin = GcPin {
        context: fixture.context,
        identity: OperationId::random(),
        head: head.clone(),
        principal: "reader".into(),
        expires_ms: 5000,
        released: false,
        operator: false,
        protects_uploads: true,
    };
    let pins = ReaderPins::new(fixture.store.clone());
    pins.acquire(&pin).await.unwrap();
    head.lifecycle = TableLifecycle::Tombstone;
    head.operation_fence += 1;
    head.pending_operation = Some(OperationId::random());
    let after = StorageRecord::TableHead(Box::new(head.clone())).encode().unwrap();
    fixture
        .store
        .compare_exchange(
            &key,
            Some(&before),
            &after,
            mutation_identity(&key, Some(&before), &after),
        )
        .await
        .unwrap();
    let purge = TablePurgeTask {
        activation_epoch: fixture.context.activation_epoch,
        head: head.clone(),
    };
    let key = purge.key().encode().unwrap();
    let bytes = StorageRecord::TablePurgeTask(Box::new(purge)).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    let repository = GcRepository::new(fixture.store.clone());
    let mut task = GcTask::plan(fixture.context, OperationId::random(), Some(head), 1000, limits).unwrap();
    repository.create(&task).await.unwrap();
    let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
    for _ in 0..50 {
        task = worker.step(&task, 2000_u64.max(task.retry_at_ms)).await.unwrap();
        if task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert!(task.fenced);
    assert_eq!(task.stalled, GcStalledReason::Protected);
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    let mut newcomer = pin.clone();
    newcomer.identity = OperationId::random();
    assert!(pins.acquire(&newcomer).await.is_err());
    pins.release(&pin).await.unwrap();
    for _ in 0..300 {
        task = worker.step(&task, 10_000).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 2);
    assert!(!task.fenced);
    assert!(blocks.blocks.values.load().is_empty());
}

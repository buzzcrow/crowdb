use super::*;

#[tokio::test]
async fn retirement_adopts_a_purge_cursor_after_a_child_was_physically_deleted() {
    use crowdb_access_iceberg::{
        file::FileBlockStore,
        gc::{CandidatePhase, GcCandidate, ReclaimStep, TreeReclaimCursor},
    };
    let (fixture, blocks, mut task, limits, file_id) = fixture(true).await;
    let repository = GcRepository::new(fixture.store.clone());
    let key = file_key(fixture.context.catalog, file_id);
    let stored = fixture.store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::File(file) = StorageRecord::decode(&key, &stored.bytes).unwrap() else {
        panic!()
    };
    let head = tombstone_head(&fixture);
    let mut old = GcTask::plan(fixture.context, OperationId::random(), Some(head), 500, limits).unwrap();
    old.paused = true;
    repository.create(&old).await.unwrap();
    let initial = GcCandidate {
        assembly: None,
        next_root: 0,
        task: old.identity,
        generation: 7,
        first_seen_ms: 500,
        not_before_ms: 510,
        revision: 1,
        phase: CandidatePhase::Retained,
        completed_round: 0,
        cursor: TreeReclaimCursor::new(&file).unwrap(),
        file: *file,
        part: None,
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
    let mut adopted = false;
    for _ in 0..300 {
        now = now.max(task.retry_at_ms);
        task = GcWorker::new(repository.clone(), blocks.clone(), limits)
            .unwrap()
            .run(&task, now)
            .await
            .unwrap();
        if task.phase == GcPhase::VerifyCleanup {
            assert_adopted(&repository, &initial, &task).await;
            adopted = true;
        }
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
    assert!(adopted);
    assert!(repository.claim_candidate(&initial).await.is_err());
}

async fn assert_adopted(
    repository: &GcRepository,
    initial: &crowdb_access_iceberg::gc::GcCandidate,
    task: &GcTask,
) {
    let current = repository.claim_candidate(initial).await.unwrap();
    assert_eq!(current.task, task.identity);
    assert_eq!(current.key(), initial.key());
    assert_eq!(current.phase, crowdb_access_iceberg::gc::CandidatePhase::Complete);
}

fn tombstone_head(fixture: &common::file::TestFile) -> crowdb_access_iceberg::table::TableHead {
    let metadata = fixture.record("metadata/old.json", b"{}");
    crowdb_access_iceberg::table::TableHead {
        catalog: fixture.context.catalog,
        table: fixture.table.table,
        namespace: crowdb_access_iceberg::key::NamespaceId::random(),
        name: "dropped".into(),
        name_epoch: 1,
        lifecycle: crowdb_access_iceberg::table::TableLifecycle::Tombstone,
        generation: 7,
        metadata_file: metadata.file,
        metadata_location: metadata.location,
        metadata_digest: metadata.digest,
        format_version: 1,
        table_uuid: None,
        operation_fence: 2,
        pending_operation: Some(OperationId::random()),
    }
}

use super::*;
use crowdb_access_iceberg::gc::{GcPage, GcStalledReason};

async fn prepare(fixture: &TestWrites) -> (GcTask, GcTask) {
    fixture.intent.register(fixture.file.store.clone()).await.unwrap();
    let task = fixture.retire().await;
    let other = GcTask::plan(task.context, OperationId::random(), None, 101, fixture.limits).unwrap();
    let repository = GcRepository::new(fixture.file.store.clone());
    repository.create(&other).await.unwrap();
    repository
        .put_page(&GcPage {
            catalog: task.context.catalog,
            task: other.identity,
            kind: 0,
            sequence: 0,
            entries: vec![fixture.intent.key().encode().unwrap()],
        })
        .await
        .unwrap();
    (task, other)
}

async fn until(fixture: &TestWrites, mut task: GcTask, phase: GcPhase) -> GcTask {
    let worker = GcWorker::new(
        GcRepository::new(fixture.file.store.clone()),
        fixture.blocks.clone(),
        fixture.limits,
    )
    .unwrap();
    for _ in 0..100 {
        task = worker.step(&task, 1000.max(task.retry_at_ms)).await.unwrap();
        if task.phase == phase {
            return task;
        }
    }
    panic!("did not reach {phase:?}: {task:?}");
}

fn assert_terminal_records(fixture: &TestWrites, task: &GcTask) {
    let values = fixture.file.store.values.load();
    let mut scopes = Vec::new();
    for key in values.keys() {
        if let IcebergKey::Catalog { catalog, scope, .. } = IcebergKey::decode(key).unwrap() {
            assert_eq!(catalog, task.context.catalog);
            scopes.push(scope as u8);
        }
    }
    assert_eq!(
        scopes,
        vec![
            CatalogScope::Authority as u8,
            CatalogScope::GcTask as u8,
            CatalogScope::GcRetirement as u8
        ]
    );
}

#[tokio::test]
async fn final_verification_preserves_a_late_unfinished_write_and_refuses_terminal_cleanup() {
    let fixture = TestWrites::new().await;
    let (task, _) = prepare(&fixture).await;
    let task = until(&fixture, task, GcPhase::VerifyCleanup).await;
    let mut late = fixture.intent.clone();
    late.identity = OperationId::random();
    late.root = fixture.blocks.put(late.owner, 0, b"late").await.unwrap();
    let key = late.key().encode().unwrap();
    let bytes = StorageRecord::FileWriteIntent(Box::new(late.clone()))
        .encode()
        .unwrap();
    fixture
        .file
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    let worker = GcWorker::new(
        GcRepository::new(fixture.file.store.clone()),
        fixture.blocks.clone(),
        fixture.limits,
    )
    .unwrap();
    assert!(worker.step(&task, 2000).await.is_err());
    assert!(fixture.blocks.read(&late.root).await.is_ok());
    assert!(fixture.file.store.get(&key).await.unwrap().is_some());
}

#[tokio::test]
async fn final_cleanup_removes_gc_graph_and_old_owners_but_keeps_constant_retirement_receipts() {
    let fixture = TestWrites::new().await;
    let (task, other) = prepare(&fixture).await;
    let task = fixture.finish(task).await;
    assert_eq!(task.phase, GcPhase::Complete);
    assert_terminal_records(&fixture, &task);
    let repository = GcRepository::new(fixture.file.store.clone());
    assert!(repository.create(&other).await.is_err());
    assert!(repository.pause(&other, true).await.is_err());
    assert!(repository
        .task(task.context.catalog, other.identity)
        .await
        .unwrap()
        .is_none());
    assert!(fixture.intent.register(fixture.file.store.clone()).await.is_err());
}

#[tokio::test]
async fn retirement_marker_and_cleanup_progress_recover_lost_replies_without_recreating_old_owners() {
    for boundary in 1..=7 {
        let fixture = TestWrites::new().await;
        let (task, _) = prepare(&fixture).await;
        let task = until(&fixture, task, GcPhase::VerifyCleanup).await;
        fixture.file.store.fail_after.store(
            fixture.file.store.writes.load(Ordering::Relaxed) + boundary,
            Ordering::Relaxed,
        );
        fixture
            .file
            .store
            .gc_delete_reply_loss
            .store(true, Ordering::Relaxed);
        let task = fixture.finish(task).await;
        assert_eq!(task.phase, GcPhase::Complete, "boundary {boundary}");
        assert_terminal_records(&fixture, &task);
    }
}

#[tokio::test]
async fn paused_owner_stops_final_metadata_cleanup_until_explicit_resume() {
    let fixture = TestWrites::new().await;
    let (task, other) = prepare(&fixture).await;
    let repository = GcRepository::new(fixture.file.store.clone());
    let paused = repository.pause(&other, true).await.unwrap();
    let mut task = until(&fixture, task, GcPhase::VerifyCleanup).await;
    let worker = GcWorker::new(repository.clone(), fixture.blocks.clone(), fixture.limits).unwrap();
    task = worker.run(&task, 2000).await.unwrap();
    assert_eq!(task.phase, GcPhase::VerifyCleanup);
    assert_eq!(task.stalled, GcStalledReason::Protected);
    assert!(repository
        .task(task.context.catalog, other.identity)
        .await
        .unwrap()
        .is_some());
    repository.pause(&paused, false).await.unwrap();
    let task = fixture.finish(task).await;
    assert_eq!(task.phase, GcPhase::Complete);
    assert_terminal_records(&fixture, &task);
}

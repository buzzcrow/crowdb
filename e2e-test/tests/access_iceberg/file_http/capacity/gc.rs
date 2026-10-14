// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
pub(super) async fn seed_gc_workspace_task(
    stack: &TestNativeStorage,
    context: CatalogContext,
) -> (
    Arc<gc_capacity::TestGcWorkspace>,
    GcRepository,
    GcTask,
    FileLocation,
    GcLimits,
) {
    let store = stack.store().await;
    let table = TableLocation {
        storage_namespace: None,
        catalog: context.catalog,
        table: TableId::random(),
    };
    let metadata = FileRecord {
        file: FileId::random(),
        location: table.file("metadata/gc-candidate.json").unwrap(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: 2,
        digest: Sha256::digest(b"{}").into(),
        content: FileContent::select_inline(FileKind::Metadata, b"{}").unwrap(),
        hint: None,
    };
    FileRepository::new(store.clone())
        .publish(context, &metadata)
        .await
        .unwrap();
    let gc_location = metadata.location.clone();
    let head = TableHead {
        catalog: context.catalog,
        table: table.table,
        namespace: crowdb_access_iceberg::key::NamespaceId::random(),
        name: "gc-capacity".into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Tombstone,
        generation: 1,
        metadata_file: metadata.file,
        metadata_location: metadata.location,
        metadata_digest: metadata.digest,
        commit_binding: None,
        format_version: 1,
        table_uuid: None,
        operation_fence: 2,
        pending_operation: Some(OperationId::random()),
    };
    let key = head_key(context.catalog, table.table).encode().unwrap();
    let bytes = StorageRecord::TableHead(Box::new(head.clone())).encode().unwrap();
    store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    let marker = TablePurgeTask {
        activation_epoch: context.activation_epoch,
        head: head.clone(),
        dropped_ms: 0,
    };
    let key = marker.key().encode().unwrap();
    let bytes = StorageRecord::TablePurgeTask(Box::new(marker)).encode().unwrap();
    store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    let workspace = Arc::new(gc_capacity::TestGcWorkspace::new(store));
    let repository = GcRepository::new(workspace.clone());
    let limits = GcLimits {
        minimum_retention_ms: 1,
        ..GcLimits::default()
    };
    let task = GcTask::plan(context, OperationId::random(), Some(head), now_ms(), limits).unwrap();
    repository.create(&task).await.unwrap();
    (workspace, repository, task, gc_location, limits)
}

async fn run_gc_until_resource_stall(worker: &GcWorker, mut task: GcTask) -> GcTask {
    for _ in 0..20 {
        task = worker
            .run(&task, now_ms().max(task.retry_at_ms).max(task.not_before_ms))
            .await
            .unwrap();
        if task.stalled == GcStalledReason::Resource {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Discover);
    assert_eq!(task.stalled, GcStalledReason::Resource);
    assert_eq!(task.deleted, 0);
    task
}

async fn run_gc_until_complete(worker: &GcWorker, mut task: GcTask) -> GcTask {
    for _ in 0..300 {
        task = worker
            .run(&task, now_ms().max(task.retry_at_ms).max(task.not_before_ms))
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    task
}

pub(super) async fn assert_gc_workspace_stall(
    stack: &TestNativeStorage,
    client: &ChunkIoClient,
    workspace: &Arc<gc_capacity::TestGcWorkspace>,
    repository: &GcRepository,
    task: GcTask,
    file: &FileLocation,
    limits: GcLimits,
) -> GcTask {
    workspace.deny(true);
    let blocks = Arc::new(NativeFileBlocks::new(client.clone(), stack.store().await));
    let worker = GcWorker::new(repository.clone(), blocks, limits).unwrap();
    let stalled = run_gc_until_resource_stall(&worker, task).await;
    assert_eq!(
        worker.run(&stalled, stalled.retry_at_ms - 1).await.unwrap(),
        stalled
    );
    assert!(stack
        .store()
        .await
        .get(&location_key(file).encode().unwrap())
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        repository
            .task(stalled.context.catalog, stalled.identity)
            .await
            .unwrap(),
        Some(stalled.clone())
    );
    stalled
}

pub(super) async fn assert_gc_workspace_recovered(
    stack: &TestNativeStorage,
    client: &ChunkIoClient,
    workspace: &Arc<gc_capacity::TestGcWorkspace>,
    repository: GcRepository,
    task: GcTask,
    file: &FileLocation,
    limits: GcLimits,
) {
    workspace.deny(false);
    let blocks = Arc::new(NativeFileBlocks::new(client.clone(), stack.store().await));
    let worker = GcWorker::new(repository, blocks, limits).unwrap();
    let finished = run_gc_until_complete(&worker, task).await;
    assert!(finished.deleted >= 1);
    assert!(stack
        .store()
        .await
        .get(&location_key(file).encode().unwrap())
        .await
        .unwrap()
        .is_none());
}

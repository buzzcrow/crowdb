use std::sync::Arc;

use crowdb_access_iceberg::{
    catalog::{ActiveCatalogRecord, CatalogAuthority, CatalogContext, CatalogStore, RootState},
    file::{ContentFormat, FileContent, FileKind, FileRecord, FileRepository},
    gc::{GcLimits, GcPhase, GcRepository, GcStore, GcTask, GcWorker},
    key::{CatalogScope, FileId, IcebergKey, OperationId, SystemScope},
    operation::mutation_identity,
    record::StorageRecord,
    table::head_key,
};
use serde_json::json;
use sha2::{Digest, Sha256};

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/gc_graph.rs"]
mod graph;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/gc_write_proof.rs"]
mod write_proof;
mod common {
    pub mod store;
    pub use store::TestStore;
    pub mod gc_store;
}

async fn put(store: &common::TestStore, key: IcebergKey, record: StorageRecord) {
    let key = key.encode().unwrap();
    let bytes = record.encode().unwrap();
    store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
}

async fn fixture(version: u8, count: usize) -> (Arc<common::TestStore>, GcTask, Vec<FileId>) {
    fixture_graph(version, count, None).await
}

async fn fixture_graph(
    version: u8,
    count: usize,
    graph: Option<(serde_json::Value, Vec<FileRecord>)>,
) -> (Arc<common::TestStore>, GcTask, Vec<FileId>) {
    let store = Arc::new(common::TestStore::default());
    let context = CatalogContext {
        catalog: metadata::table().catalog,
        activation_epoch: 1,
    };
    put(
        &store,
        IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        },
        StorageRecord::Active(ActiveCatalogRecord {
            context,
            state: RootState::Ready,
            operation: OperationId::random(),
        }),
    )
    .await;
    put(
        &store,
        IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        },
        StorageRecord::Authority(CatalogAuthority::new(context.catalog, "test".into()).unwrap()),
    )
    .await;
    let repository = FileRepository::new(store.clone());
    let mut files = Vec::new();
    let mut logs = Vec::new();
    for index in 0..count {
        let location = metadata::table()
            .file(&format!("metadata/old-{index}.json"))
            .unwrap();
        let file = FileRecord {
            file: FileId::random(),
            location: location.clone(),
            kind: FileKind::Metadata,
            format: ContentFormat::Json,
            length: 2,
            digest: Sha256::digest(b"{}").into(),
            content: FileContent::select_inline(FileKind::Metadata, b"{}").unwrap(),
            hint: None,
        };
        repository.publish(context, &file).await.unwrap();
        files.push(file.file);
        logs.push(json!({"timestamp-ms": index, "metadata-file": location.to_string()}));
    }
    let (mut value, records) = graph.unwrap_or_else(|| (metadata::metadata(version), Vec::new()));
    for record in records {
        repository.publish(context, &record).await.unwrap();
        files.push(record.file);
    }
    value["metadata-log"] = json!(logs);
    let bytes = serde_json::to_vec(&value).unwrap();
    let head = metadata::head(
        &bytes,
        version,
        Some(value["table-uuid"].as_str().unwrap().parse().unwrap()),
    );
    let file = FileRecord {
        file: head.metadata_file,
        location: head.metadata_location.clone(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: bytes.len() as u64,
        digest: head.metadata_digest,
        content: FileContent::select_inline(FileKind::Metadata, &bytes).unwrap(),
        hint: None,
    };
    repository.publish(context, &file).await.unwrap();
    files.push(file.file);
    put(
        &store,
        head_key(head.catalog, head.table),
        StorageRecord::TableHead(Box::new(head.clone())),
    )
    .await;
    let task = GcTask::plan(
        context,
        OperationId::random(),
        Some(head),
        1,
        GcLimits {
            minimum_retention_ms: 1,
            ..GcLimits::default()
        },
    )
    .unwrap();
    GcRepository::new(store.clone()).create(&task).await.unwrap();
    (store, task, files)
}

#[tokio::test]
async fn proof_traverses_manifests_refs_statistics_and_live_dv_links() {
    for version in 1..=3 {
        let (document, records, deleted) = graph::graph(metadata::table(), version).await;
        let (store, task, files) = fixture_graph(version, 0, Some((document, records))).await;
        let task = finish(store.clone(), task).await;
        let repository = GcRepository::new(store);
        for file in files {
            assert_eq!(
                repository.proof_contains(&task, file).await.unwrap(),
                file != deleted
            );
        }
    }
}

async fn finish(store: Arc<common::TestStore>, mut task: GcTask) -> GcTask {
    for _ in 0..1000 {
        let repository = GcRepository::new(store.clone());
        let worker = GcWorker::new(
            repository.clone(),
            Arc::new(blocks::TestBlocks::default()),
            GcLimits {
                minimum_retention_ms: 1,
                ..GcLimits::default()
            },
        )
        .unwrap();
        task = worker.step(&task, 1_000_000).await.unwrap();
        task = repository
            .task(task.context.catalog, task.identity)
            .await
            .unwrap()
            .unwrap();
        if task.proof.complete {
            return task;
        }
    }
    panic!("proof failed to finish")
}

#[tokio::test]
async fn immutable_proof_survives_restart_and_finds_all_files_for_every_version() {
    for version in 1..=3 {
        let (store, task, files) = fixture(version, 48).await;
        let task = finish(store.clone(), task).await;
        assert_eq!(task.phase, GcPhase::Fence);
        assert_eq!(task.marked, files.len() as u64);
        assert_eq!(task.queue_read, task.queue_write);
        let repository = GcRepository::new(store);
        for file in files {
            assert!(repository.proof_contains(&task, file).await.unwrap());
        }
        assert!(!repository.proof_contains(&task, FileId::random()).await.unwrap());
    }
}

#[tokio::test]
async fn missing_proof_page_cannot_be_interpreted_as_an_unreachable_file() {
    let (store, task, _) = fixture(3, 4).await;
    let task = finish(store.clone(), task).await;
    let key = task
        .proof
        .root
        .as_ref()
        .unwrap()
        .page_key(0)
        .unwrap()
        .encode()
        .unwrap();
    let value = store.get(&key).await.unwrap().unwrap();
    store
        .delete_gc_record(
            &key,
            &value.bytes,
            mutation_identity(&key, Some(&value.bytes), &[]),
        )
        .await
        .unwrap();
    assert!(GcRepository::new(store)
        .proof_contains(&task, FileId::random())
        .await
        .is_err());
}

#[tokio::test]
async fn missing_pending_frame_does_not_complete_the_proof() {
    let (store, mut task, _) = fixture(2, 2).await;
    let worker = GcWorker::new(
        GcRepository::new(store.clone()),
        Arc::new(blocks::TestBlocks::default()),
        GcLimits::default(),
    )
    .unwrap();
    while task.phase != GcPhase::Mark {
        task = worker.step(&task, 1_000_000).await.unwrap();
    }
    let key = task
        .proof
        .pending
        .as_ref()
        .unwrap()
        .page_key(0)
        .unwrap()
        .encode()
        .unwrap();
    let value = store.get(&key).await.unwrap().unwrap();
    store
        .delete_gc_record(
            &key,
            &value.bytes,
            mutation_identity(&key, Some(&value.bytes), &[]),
        )
        .await
        .unwrap();
    assert!(worker.step(&task, 1_000_000).await.is_err());
    assert!(
        !GcRepository::new(store)
            .task(task.context.catalog, task.identity)
            .await
            .unwrap()
            .unwrap()
            .proof
            .complete
    );
}

#[tokio::test]
async fn live_sweep_keeps_proven_files_and_removes_only_an_unreferenced_file() {
    let (store, mut task, files) = fixture(3, 12).await;
    let orphan = FileRecord {
        file: FileId::random(),
        location: metadata::table().file("metadata/orphan.json").unwrap(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: 2,
        digest: Sha256::digest(b"{}").into(),
        content: FileContent::select_inline(FileKind::Metadata, b"{}").unwrap(),
        hint: None,
    };
    let file_repository = FileRepository::new(store.clone());
    file_repository.publish(task.context, &orphan).await.unwrap();
    let repository = GcRepository::new(store.clone());
    for step in 0..1000 {
        let worker = GcWorker::new(
            repository.clone(),
            Arc::new(blocks::TestBlocks::default()),
            GcLimits {
                minimum_retention_ms: 1,
                ..GcLimits::default()
            },
        )
        .unwrap();
        task = worker.step(&task, 1_000_000 + step * 10).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert!(!task.fenced);
    assert_eq!(task.deleted, 1);
    assert!(file_repository
        .load(task.context, &orphan.location)
        .await
        .unwrap()
        .is_none());
    for file in files {
        assert!(store
            .get(
                &crowdb_access_iceberg::file::file_key(task.context.catalog, file)
                    .encode()
                    .unwrap()
            )
            .await
            .unwrap()
            .is_some());
    }
}

#[tokio::test]
async fn request_protection_uses_persisted_bounds_and_rejects_fenced_heads() {
    let (store, mut task, _) = fixture(3, 0).await;
    let pins = crowdb_access_iceberg::gc::ReaderPins::new(store.clone());
    let key = IcebergKey::Catalog {
        catalog: task.context.catalog,
        scope: CatalogScope::Authority,
        suffix: Vec::new(),
    };
    let before = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::Authority(mut authority) = StorageRecord::decode(&key, &before.bytes).unwrap() else {
        panic!()
    };
    authority.admission_bounds.request_ms = 600_000;
    authority.admission_bounds.clock_skew_ms = 70_000;
    let bytes = StorageRecord::Authority(authority).encode().unwrap();
    let key = key.encode().unwrap();
    store
        .compare_exchange(
            &key,
            Some(&before.bytes),
            &bytes,
            mutation_identity(&key, Some(&before.bytes), &bytes),
        )
        .await
        .unwrap();
    assert_eq!(pins.request_expiry(task.context, 100).await.unwrap(), 670_100);
    assert!(pins.request_expiry(task.context, u64::MAX - 100).await.is_err());
    let pin = pins
        .protect_files(
            task.context,
            task.head.as_ref().unwrap().table,
            "reader",
            670_100,
            100,
        )
        .await
        .unwrap();
    assert!(pin.protects_uploads);
    pins.release(&pin).await.unwrap();
    task = finish(store.clone(), task).await;
    GcRepository::new(store).fence_table(&task).await.unwrap();
    assert!(pins
        .protect_files(
            task.context,
            task.head.as_ref().unwrap().table,
            "reader",
            670_100,
            100
        )
        .await
        .is_err());
}

#[tokio::test]
async fn late_credentials_cancel_sweep_and_release_the_table() {
    let (store, task, _) = fixture(3, 2).await;
    let mut task = finish(store.clone(), task).await;
    let pins = crowdb_access_iceberg::gc::ReaderPins::new(store.clone());
    pins.protect_files(
        task.context,
        task.head.as_ref().unwrap().table,
        "late-credentials",
        2_000_000,
        1_000_000,
    )
    .await
    .unwrap();
    let worker = GcWorker::new(
        GcRepository::new(store.clone()),
        Arc::new(blocks::TestBlocks::default()),
        GcLimits::default(),
    )
    .unwrap();
    for _ in 0..100 {
        task = worker.step(&task, 1_000_000).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(
        task.stalled,
        crowdb_access_iceberg::gc::GcStalledReason::Protected
    );
    assert!(!task.fenced);
    assert_eq!(task.deleted, 0);
    pins.protect_files(
        task.context,
        task.head.as_ref().unwrap().table,
        "new-request",
        2_000_000,
        1_000_000,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn proof_and_fence_resume_after_lost_durable_write_responses() {
    use std::sync::atomic::Ordering;
    for boundary in 1..=10 {
        let (store, mut task, files) = fixture(3, 3).await;
        let repository = GcRepository::new(store.clone());
        let worker = GcWorker::new(
            repository.clone(),
            Arc::new(blocks::TestBlocks::default()),
            GcLimits::default(),
        )
        .unwrap();
        while task.phase != GcPhase::Mark {
            task = worker.step(&task, 1_000_000).await.unwrap();
        }
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + boundary, Ordering::SeqCst);
        let _ = worker.step(&task, 1_000_000).await;
        store.fail_after.store(0, Ordering::SeqCst);
        task = repository
            .task(task.context.catalog, task.identity)
            .await
            .unwrap()
            .unwrap();
        task = finish(store.clone(), task).await;
        for file in files {
            assert!(repository.proof_contains(&task, file).await.unwrap());
        }
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        assert!(worker.step(&task, 1_000_000).await.is_err());
        store.fail_after.store(0, Ordering::SeqCst);
        task = worker.step(&task, 1_000_000).await.unwrap();
        assert!(task.fenced);
    }
}

#[tokio::test]
async fn historical_reader_root_is_retained_after_the_current_head_changes() {
    let (store, old, old_files) = fixture(3, 1).await;
    let old_head = old.head.as_ref().unwrap();
    let pin = crowdb_access_iceberg::gc::GcPin {
        context: old.context,
        identity: OperationId::random(),
        head: old_head.clone(),
        principal: "historical-reader".into(),
        expires_ms: 2_000_000,
        released: false,
        operator: false,
        protects_uploads: false,
    };
    crowdb_access_iceberg::gc::ReaderPins::new(store.clone())
        .acquire(&pin)
        .await
        .unwrap();
    let bytes = serde_json::to_vec(&metadata::metadata(3)).unwrap();
    let mut head = old_head.clone();
    head.generation += 1;
    head.operation_fence += 1;
    head.metadata_file = FileId::random();
    head.metadata_location = metadata::table().file("metadata/new.json").unwrap();
    head.metadata_digest = Sha256::digest(&bytes).into();
    let file = FileRecord {
        file: head.metadata_file,
        location: head.metadata_location.clone(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: bytes.len() as u64,
        digest: head.metadata_digest,
        content: FileContent::select_inline(FileKind::Metadata, &bytes).unwrap(),
        hint: None,
    };
    FileRepository::new(store.clone())
        .publish(old.context, &file)
        .await
        .unwrap();
    let key = head_key(head.catalog, head.table).encode().unwrap();
    let before = StorageRecord::TableHead(Box::new(old_head.clone()))
        .encode()
        .unwrap();
    let after = StorageRecord::TableHead(Box::new(head.clone())).encode().unwrap();
    store
        .compare_exchange(
            &key,
            Some(&before),
            &after,
            mutation_identity(&key, Some(&before), &after),
        )
        .await
        .unwrap();
    let task = GcTask::plan(
        old.context,
        OperationId::random(),
        Some(head),
        1,
        GcLimits::default(),
    )
    .unwrap();
    let repository = GcRepository::new(store.clone());
    repository.create(&task).await.unwrap();
    let task = finish(store, task).await;
    for file in old_files {
        assert!(repository.proof_contains(&task, file).await.unwrap());
    }
    assert!(repository.proof_contains(&task, file.file).await.unwrap());
}

#[tokio::test]
async fn publication_pin_prevents_sweep_while_location_publication_is_in_flight() {
    use std::sync::atomic::Ordering;
    let (store, mut task, _) = fixture(3, 0).await;
    let file = FileRecord {
        file: FileId::random(),
        location: metadata::table().file("metadata/in-flight.json").unwrap(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: 2,
        digest: Sha256::digest(b"{}").into(),
        content: FileContent::select_inline(FileKind::Metadata, b"{}").unwrap(),
        hint: None,
    };
    store.file_mapping_pause.store(true, Ordering::SeqCst);
    let publishing = tokio::spawn({
        let store = store.clone();
        let file = file.clone();
        let context = task.context;
        async move { FileRepository::new(store).publish(context, &file).await }
    });
    store.file_mapping_entered.notified().await;
    let worker = GcWorker::new(
        GcRepository::new(store.clone()),
        Arc::new(blocks::TestBlocks::default()),
        GcLimits {
            minimum_retention_ms: 1,
            ..GcLimits::default()
        },
    )
    .unwrap();
    for _ in 0..100 {
        task = worker.step(&task, 1_000_000).await.unwrap();
        if task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Waiting);
    assert_eq!(
        task.stalled,
        crowdb_access_iceberg::gc::GcStalledReason::Protected
    );
    assert_eq!(task.deleted, 0);
    store.file_mapping_release.notify_one();
    assert_eq!(publishing.await.unwrap().unwrap(), file);
}

#[tokio::test]
async fn live_sweep_recovers_a_lost_final_fence_release_response() {
    use crowdb_access_iceberg::gc::GcScan;
    use std::sync::atomic::Ordering;
    let (store, mut task, _) = fixture(3, 0).await;
    let worker = GcWorker::new(
        GcRepository::new(store.clone()),
        Arc::new(blocks::TestBlocks::default()),
        GcLimits {
            minimum_retention_ms: 1,
            ..GcLimits::default()
        },
    )
    .unwrap();
    let mut ready = false;
    for step in 0..100 {
        if task.phase == GcPhase::SweepWrites {
            let page = store
                .scan_gc(GcScan {
                    catalog: task.context.catalog,
                    scope: Some(CatalogScope::FileWriteIntent),
                    prefix: task.head.as_ref().unwrap().table.as_bytes().to_vec(),
                    after: task.scan_after.clone(),
                    items: 1,
                    bytes: 128 * 1024,
                })
                .await
                .unwrap();
            if page.items.is_empty() {
                ready = true;
                break;
            }
        }
        task = worker.step(&task, 1_000_000 + step * 10).await.unwrap();
    }
    assert!(ready);
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(worker.step(&task, 2_000_000).await.is_err());
    store.fail_after.store(0, Ordering::SeqCst);
    task = worker.step(&task, 2_000_000).await.unwrap();
    assert_eq!(task.phase, GcPhase::Complete);
    assert!(!task.fenced);
}

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/gc_blocks.rs"]
mod gc_blocks;
#[path = "common/gc_store.rs"]
mod gc_store;
#[path = "common/legacy_creation.rs"]
mod legacy;
#[path = "common/manifest_list.rs"]
#[allow(dead_code)]
mod list_fixture;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod namespaces;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod parquet;
#[path = "common/table_recovery.rs"]
mod recovery_store;
#[path = "common/snapshot_files.rs"]
#[allow(dead_code)]
mod snapshot;
#[path = "common/table_staging.rs"]
#[allow(dead_code)]
mod staging;

use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore},
    commit::{TableCreatePhase, TableRecoveryKind},
    file::{ContentFormat, FileContent, FileKind, FileRecord, FileRepository},
    gc::{GcLimits, GcPhase, GcRepository, GcTask, GcWorker},
    key::{IcebergKey, OperationId, SystemScope},
    namespace::{authority_key, NamespaceMutation},
    operation::{mutation_identity, PayloadStore},
    record::StorageRecord,
    table::{name_key, storage_head_key, TableMappingState, TableRepository},
};
use legacy::TestLegacyCreation;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn legacy_expiry_keeps_client_grace_then_retires_without_continuing_creation() {
    let test = TestLegacyCreation::new().await;
    let store = &test.staged.namespace.store;
    let page = test
        .recovery()
        .recover_page(test.operation.context, TableRecoveryKind::Create, None, 1999)
        .await
        .unwrap();
    assert_eq!((page.progressed, page.retained, page.failures.len()), (0, 1, 0));
    assert!(test.head().is_none());
    let writes = store.writes.load(Ordering::SeqCst);
    let page = test
        .recovery()
        .recover_page(test.operation.context, TableRecoveryKind::Create, None, 2000)
        .await
        .unwrap();
    assert_eq!((page.progressed, page.retained, page.failures.len()), (1, 0, 0));
    assert_eq!(store.writes.load(Ordering::SeqCst) - writes, 1);
    assert_eq!(test.staged.blocks.writes.load(Ordering::SeqCst), 0);
    assert_eq!(
        test.head(),
        Some(StorageRecord::RetiredTable(
            test.operation.candidate.storage_location()
        ))
    );
    assert!(!store
        .values
        .load()
        .contains_key(&test.operation.key().encode().unwrap()));
    let key = storage_head_key(test.operation.candidate.storage_location())
        .encode()
        .unwrap();
    let bytes = StorageRecord::TableHead(Box::new(test.operation.candidate.clone()))
        .encode()
        .unwrap();
    assert!(matches!(
        store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap(),
        CasOutcome::Conflict(Some(_))
    ));
}

#[tokio::test]
async fn legacy_reserved_work_is_cancelled_instead_of_publishing_missing_metadata() {
    let mut test = TestLegacyCreation::new().await;
    test.operation.phase = TableCreatePhase::Reserved;
    test.operation.stage = None;
    test.persist().await;
    let mapping = test.operation.mapping(TableMappingState::Reserved);
    let key = name_key(mapping.catalog, mapping.namespace, &mapping.name).unwrap();
    test.staged
        .namespace
        .put(key.clone(), StorageRecord::TableMapping(mapping))
        .await;
    let page = test
        .recovery()
        .recover_page(test.operation.context, TableRecoveryKind::Create, None, 1001)
        .await
        .unwrap();
    assert_eq!((page.progressed, page.failures.len()), (1, 0));
    assert!(!test
        .staged
        .namespace
        .store
        .values
        .load()
        .contains_key(&key.encode().unwrap()));
    assert!(matches!(test.head(), Some(StorageRecord::RetiredTable(_))));
    assert_eq!(test.staged.blocks.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_winning_legacy_head_only_settles_exact_resource_markers() {
    let mut test = TestLegacyCreation::new().await;
    test.operation.phase = TableCreatePhase::Publishing;
    test.operation.stage = None;
    test.persist().await;
    let store = &test.staged.namespace.store;
    let document = PayloadStore::new(store.clone())
        .get(&test.operation.document)
        .await
        .unwrap();
    FileRepository::new(store.clone())
        .publish(
            test.operation.context,
            &FileRecord {
                file: test.operation.candidate.metadata_file,
                location: test.operation.candidate.metadata_location.clone(),
                kind: FileKind::Metadata,
                format: ContentFormat::Json,
                length: document.len() as u64,
                digest: test.operation.candidate.metadata_digest,
                content: FileContent::select_inline(FileKind::Metadata, &document).unwrap(),
                hint: None,
            },
        )
        .await
        .unwrap();
    let head = storage_head_key(test.operation.candidate.storage_location())
        .encode()
        .unwrap();
    let bytes = StorageRecord::TableHead(Box::new(test.operation.candidate.clone()))
        .encode()
        .unwrap();
    store
        .compare_exchange(&head, None, &bytes, mutation_identity(&head, None, &bytes))
        .await
        .unwrap();
    let reserved = test.operation.mapping(TableMappingState::Reserved);
    test.staged
        .namespace
        .put(
            name_key(reserved.catalog, reserved.namespace, &reserved.name).unwrap(),
            StorageRecord::TableMapping(reserved),
        )
        .await;
    let writes = store.writes.load(Ordering::SeqCst);
    let page = test
        .recovery()
        .recover_page(test.operation.context, TableRecoveryKind::Create, None, 1001)
        .await
        .unwrap();
    assert_eq!((page.progressed, page.failures.len()), (1, 0));
    assert_eq!(store.writes.load(Ordering::SeqCst) - writes, 2);
    let selected = TableRepository::new(store.clone())
        .select(
            test.operation.context,
            test.operation.candidate.namespace,
            "events",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.head.pending_operation, None);
    assert_eq!(
        selected.head.metadata_location,
        test.operation.candidate.metadata_location
    );
    assert_eq!(test.staged.blocks.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn uncertain_retirement_stops_before_name_cleanup_and_retries_from_the_final_slot() {
    let mut test = TestLegacyCreation::new().await;
    test.operation.phase = TableCreatePhase::Reserved;
    test.operation.stage = None;
    test.persist().await;
    let mapping = test.operation.mapping(TableMappingState::Reserved);
    let key = name_key(mapping.catalog, mapping.namespace, &mapping.name).unwrap();
    test.staged
        .namespace
        .put(key.clone(), StorageRecord::TableMapping(mapping))
        .await;
    let store = &test.staged.namespace.store;
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(test
        .staged
        .creator()
        .resume(test.operation.context, test.operation.identity.operation)
        .await
        .is_err());
    assert!(store.values.load().contains_key(&key.encode().unwrap()));
    assert!(matches!(test.head(), Some(StorageRecord::RetiredTable(_))));
    store.fail_after.store(0, Ordering::SeqCst);
    store.gc_delete_reply_loss.store(true, Ordering::SeqCst);
    let page = test
        .recovery()
        .recover_page(test.operation.context, TableRecoveryKind::Create, None, 2000)
        .await
        .unwrap();
    assert_eq!(page.failures.len(), 1);
    assert!(!store.values.load().contains_key(&key.encode().unwrap()));
    let page = test
        .recovery()
        .recover_page(test.operation.context, TableRecoveryKind::Create, None, 2000)
        .await
        .unwrap();
    assert_eq!((page.progressed, page.retained, page.failures.len()), (0, 0, 0));
}

#[tokio::test]
async fn legacy_cleanup_does_not_clear_a_later_parent_marker_reusing_the_operation_id() {
    for reused in [false, true] {
        let mut test = TestLegacyCreation::new().await;
        test.operation.phase = TableCreatePhase::Admitted;
        test.operation.stage = None;
        let key = authority_key(test.operation.context.catalog, test.staged.parent.namespace);
        let mut captured = test.staged.parent.clone();
        captured.pending_operation = Some(test.operation.identity.operation);
        captured.mutation_revision += 1;
        let payloads = PayloadStore::new(test.staged.namespace.store.clone());
        let before = payloads
            .put(
                test.operation.context.catalog,
                test.operation.identity.operation,
                &StorageRecord::NamespaceAuthority(Box::new(test.staged.parent.clone()))
                    .encode()
                    .unwrap(),
            )
            .await
            .unwrap();
        let after = payloads
            .put(
                test.operation.context.catalog,
                test.operation.identity.operation,
                &StorageRecord::NamespaceAuthority(Box::new(captured.clone()))
                    .encode()
                    .unwrap(),
            )
            .await
            .unwrap();
        test.operation.admission = Some(NamespaceMutation {
            key: key.encode().unwrap(),
            before: Some(before),
            after,
        });
        test.persist().await;
        if reused {
            captured.mutation_revision += 2;
        }
        test.staged
            .namespace
            .put(key.clone(), StorageRecord::NamespaceAuthority(Box::new(captured)))
            .await;
        let page = test
            .recovery()
            .recover_page(test.operation.context, TableRecoveryKind::Create, None, 2000)
            .await
            .unwrap();
        assert_eq!((page.progressed, page.failures.len()), (1, 0));
        let value = test
            .staged
            .namespace
            .store
            .values
            .load()
            .get(&key.encode().unwrap())
            .unwrap()
            .bytes
            .clone();
        let StorageRecord::NamespaceAuthority(parent) = StorageRecord::decode(&key, &value).unwrap() else {
            panic!()
        };
        assert_eq!(parent.pending_operation.is_some(), reused);
    }
}

#[tokio::test]
async fn retired_catalog_gc_permanently_fences_legacy_create_heads_instead_of_waiting_for_phases() {
    for (native, head_won) in [(false, false), (false, true), (true, false), (true, true)] {
        let mut test = TestLegacyCreation::with_layout(native).await;
        test.operation.phase = TableCreatePhase::Publishing;
        test.operation.stage = None;
        test.persist().await;
        let store = &test.staged.namespace.store;
        let blocks = std::sync::Arc::new(gc_blocks::TestReclaimBlocks::default());
        test.publish_chunked_metadata(blocks.clone()).await;
        assert!(!blocks.blocks.values.load().is_empty());
        let key = storage_head_key(test.operation.candidate.storage_location());
        let head = StorageRecord::TableHead(Box::new(test.operation.candidate.clone()))
            .encode()
            .unwrap();
        if head_won {
            let key = key.encode().unwrap();
            store
                .compare_exchange(&key, None, &head, mutation_identity(&key, None, &head))
                .await
                .unwrap();
        }
        test.retire_catalog().await;
        let active = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        }
        .encode()
        .unwrap();
        let active_bytes = store.get(&active).await.unwrap().unwrap().bytes;
        let limits = GcLimits {
            minimum_retention_ms: 1,
            ..GcLimits::default()
        };
        let repository = GcRepository::new(store.clone());
        let mut task =
            GcTask::plan(test.operation.context, OperationId::random(), None, 1000, limits).unwrap();
        repository.create(&task).await.unwrap();
        store.retirement_reply_loss.store(true, Ordering::SeqCst);
        let mut lost_reply = false;
        for _ in 0..300 {
            let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
            let step = worker
                .step(
                    &task,
                    (10 * crowdb_access_iceberg::operation::RETRY_WINDOW_MS).max(task.retry_at_ms),
                )
                .await;
            if let Ok(next) = step {
                task = next;
            } else {
                assert!(!lost_reply, "unexpected second failure: {step:?}");
                assert!(matches!(test.head(), Some(StorageRecord::RetiredTable(_))));
                assert_eq!(blocks.deletes.load(Ordering::SeqCst), 0);
                assert!(store
                    .values
                    .load()
                    .contains_key(&test.operation.key().encode().unwrap()));
                lost_reply = true;
            }
            task = repository
                .task(task.context.catalog, task.identity)
                .await
                .unwrap()
                .unwrap();
            if task.phase == GcPhase::Complete {
                break;
            }
        }
        assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
        assert!(lost_reply);
        assert!(blocks.blocks.values.load().is_empty());
        assert_eq!(task.deleted, 1);
        assert_eq!(store.get(&active).await.unwrap().unwrap().bytes, active_bytes);
        assert!(!store
            .values
            .load()
            .contains_key(&test.operation.key().encode().unwrap()));
        assert!(matches!(test.head(), Some(StorageRecord::RetiredTable(_))));
        let key = key.encode().unwrap();
        assert!(matches!(
            store
                .compare_exchange(&key, None, &head, mutation_identity(&key, None, &head))
                .await
                .unwrap(),
            CasOutcome::Conflict(Some(_))
        ));
    }
}

#[tokio::test]
async fn legacy_cleanup_preserves_an_active_newer_head_and_unrelated_table_slot() {
    let mut test = TestLegacyCreation::new().await;
    test.operation.phase = TableCreatePhase::Publishing;
    test.operation.stage = None;
    test.persist().await;
    let mut current = test.operation.candidate.clone();
    current.generation += 1;
    current.pending_operation = None;
    let mut other = current.clone();
    other.table = crowdb_access_iceberg::key::TableId::random();
    other.metadata_location = crowdb_access_iceberg::file::TableLocation {
        table: other.table,
        ..current.storage_location()
    }
    .file("metadata/other.metadata.json")
    .unwrap();
    let store = &test.staged.namespace.store;
    let mut expected = Vec::new();
    for head in [current, other] {
        let key = storage_head_key(head.storage_location()).encode().unwrap();
        let bytes = StorageRecord::TableHead(Box::new(head)).encode().unwrap();
        store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
        expected.push((key, bytes));
    }
    let report = test
        .recovery()
        .recover_page(test.operation.context, TableRecoveryKind::Create, None, 2000)
        .await
        .unwrap();
    assert_eq!((report.progressed, report.failures.len()), (1, 0));
    for (key, bytes) in expected {
        assert_eq!(store.get(&key).await.unwrap().unwrap().bytes, bytes);
    }
}

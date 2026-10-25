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
#[allow(dead_code)]
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
#[path = "common/gc_table_retirement.rs"]
mod retirement;
#[path = "common/snapshot_files.rs"]
#[allow(dead_code)]
mod snapshot;
#[path = "common/table_staging.rs"]
#[allow(dead_code)]
mod staging;

use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogError, CatalogStore},
    commit::{CommitPublicationError, TableCreatePhase, TableRecoveryKind},
    gc::{GcPhase, GcRepository, GcTask, GcWorker},
    key::OperationId,
    operation::mutation_identity,
    record::StorageRecord,
    table::{name_key, storage_head_key, TableMappingState},
};
use retirement::TestRetiredTable;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn retired_catalog_never_reopens_an_initial_head_slot_without_a_creation_ledger() {
    for (native, head_won) in [(false, false), (false, true), (true, false), (true, true)] {
        let test = TestRetiredTable::new(native).await;
        let pending = test.pause_first_head().await;
        assert!(test.head().is_none());
        if head_won {
            test.creator().commit_staged(&test.request).await.unwrap();
            assert!(matches!(test.head(), Some(StorageRecord::TableHead(_))));
        }
        let records = test.records();
        assert!(!records
            .iter()
            .any(|record| matches!(record, StorageRecord::TableCreateOperation(_))));
        let metadata: Vec<_> = records
            .iter()
            .filter_map(|record| match record {
                StorageRecord::File(file) => Some(file.file),
                _ => None,
            })
            .collect();
        assert_eq!(metadata.len(), if head_won { 2 } else { 1 });
        if head_won {
            assert_ne!(metadata[0], metadata[1]);
        }
        let task = test.retire_catalog().await;
        let task = test.finish(task).await;
        assert_eq!(task.deleted, metadata.len() as u64);
        assert!(test.blocks.blocks.values.load().is_empty());
        assert!(!test.records().iter().any(|record| matches!(
            record,
            StorageRecord::TableHead(_) | StorageRecord::TableMapping(_)
        )));
        // Check the remote slot itself before allowing the delayed publisher to continue.
        let retired = test.head();
        test.staged.namespace.store.table_head_release.notify_one();
        let result = pending.await.unwrap();
        assert!(
            matches!(retired, Some(StorageRecord::RetiredTable(_))),
            "native={native}, head_won={head_won}, late result={result:?}, slot={retired:?}, late_slot={:?}",
            test.head()
        );
        assert!(matches!(
            result,
            Err(CommitPublicationError::Catalog(CatalogError::Conflict))
        ));
        assert_eq!(test.head(), retired);
        assert_eq!(
            test.blocks.deletes.load(Ordering::SeqCst),
            test.blocks.blocks.writes.load(Ordering::SeqCst)
        );
    }
}

#[tokio::test]
async fn unknown_head_or_mapping_retirement_stops_cleanup_and_survives_worker_restart() {
    for (native, with_mapping, head_won) in [
        (false, false, true),
        (false, true, true),
        (true, false, true),
        (true, true, true),
        (false, true, false),
        (true, true, false),
    ] {
        let test = TestRetiredTable::new(native).await;
        let pending = if head_won {
            test.creator().commit_staged(&test.request).await.unwrap();
            None
        } else {
            Some(test.pause_first_head().await)
        };
        if !with_mapping {
            test.remove_mapping().await;
        }
        let task = test.retire_catalog().await;
        let phase = if native && !with_mapping {
            GcPhase::CleanupNative
        } else {
            GcPhase::CleanupCatalog
        };
        let task = test.to_phase(task, phase).await;
        let store = &test.staged.namespace.store;
        let task_bytes = store
            .get(&task.key().encode().unwrap())
            .await
            .unwrap()
            .unwrap()
            .bytes;
        let mapping = name_key(
            test.request.context.catalog,
            test.staged.parent.namespace,
            "events",
        )
        .unwrap()
        .encode()
        .unwrap();
        let mapping_bytes = store.get(&mapping).await.unwrap().map(|value| value.bytes);
        store.retirement_reply_loss.store(true, Ordering::SeqCst);
        let reads = store.reads.load(Ordering::SeqCst);
        let result = GcWorker::new(test.repository(), test.blocks.clone(), retirement::limits())
            .unwrap()
            .step(&task, retirement::NOW)
            .await;
        assert!(result.is_err());
        // CleanupCatalog also checks the parent namespace's final slot before removing its name.
        // CleanupNative does not scan that name. These reads all precede the lost table-retirement reply.
        assert_eq!(
            store.reads.load(Ordering::SeqCst) - reads,
            if with_mapping {
                7
            } else if native {
                4
            } else {
                6
            }
        );
        assert_eq!(
            store
                .get(&task.key().encode().unwrap())
                .await
                .unwrap()
                .unwrap()
                .bytes,
            task_bytes
        );
        assert_eq!(
            store.get(&mapping).await.unwrap().map(|value| value.bytes),
            mapping_bytes
        );
        assert!(matches!(test.head(), Some(StorageRecord::RetiredTable(_))));
        let task = test.finish(task).await;
        assert_eq!(task.phase, GcPhase::Complete);
        assert!(test.blocks.blocks.values.load().is_empty());
        assert!(matches!(test.head(), Some(StorageRecord::RetiredTable(_))));
        if let Some(pending) = pending {
            test.staged.namespace.store.table_head_release.notify_one();
            assert!(matches!(
                pending.await.unwrap(),
                Err(CommitPublicationError::Catalog(CatalogError::Conflict))
            ));
        }
    }
}

#[tokio::test]
async fn retirement_keeps_the_final_slot_closed_after_legacy_creation_record_was_removed() {
    for native in [false, true] {
        let mut test = legacy::TestLegacyCreation::with_layout(native).await;
        let blocks = std::sync::Arc::new(gc_blocks::TestReclaimBlocks::default());
        test.publish_chunked_metadata(blocks.clone()).await;
        test.operation.phase = TableCreatePhase::Publishing;
        test.operation.stage = None;
        test.persist().await;
        let head = storage_head_key(test.operation.candidate.storage_location())
            .encode()
            .unwrap();
        let bytes = StorageRecord::TableHead(Box::new(test.operation.candidate.clone()))
            .encode()
            .unwrap();
        let store = &test.staged.namespace.store;
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
        let report = test
            .recovery()
            .recover_page(test.operation.context, TableRecoveryKind::Create, None, 2000)
            .await
            .unwrap();
        assert_eq!((report.progressed, report.failures.len()), (1, 0));
        assert!(store
            .get(&test.operation.key().encode().unwrap())
            .await
            .unwrap()
            .is_none());
        test.retire_catalog().await;
        let limits = retirement::limits();
        let repository = GcRepository::new(store.clone());
        let mut task =
            GcTask::plan(test.operation.context, OperationId::random(), None, 1000, limits).unwrap();
        repository.create(&task).await.unwrap();
        for _ in 0..500 {
            if task.phase == GcPhase::Complete {
                break;
            }
            task = GcWorker::new(repository.clone(), blocks.clone(), limits)
                .unwrap()
                .step(&task, retirement::NOW.max(task.retry_at_ms))
                .await
                .unwrap();
        }
        assert_eq!(task.phase, GcPhase::Complete);
        assert!(blocks.blocks.values.load().is_empty());
        assert!(matches!(test.head(), Some(StorageRecord::RetiredTable(_))));
        assert!(matches!(
            store
                .compare_exchange(&head, None, &bytes, mutation_identity(&head, None, &bytes))
                .await
                .unwrap(),
            CasOutcome::Conflict(Some(_))
        ));
    }
}

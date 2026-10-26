#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/gc_retirement_conflict.rs"]
mod conflict;
#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/gc_blocks.rs"]
mod gc_blocks;
#[path = "common/gc_store.rs"]
mod gc_store;
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
#[path = "common/gc_table_retirement.rs"]
#[allow(dead_code)]
mod retirement;
#[path = "common/snapshot_files.rs"]
#[allow(dead_code)]
mod snapshot;
#[path = "common/table_staging.rs"]
#[allow(dead_code)]
mod staging;

use crowdb_access_iceberg::{
    catalog::CatalogStore,
    gc::{GcPhase, GcRepository, GcTask, GcWorker},
    key::OperationId,
    operation::mutation_identity,
    record::StorageRecord,
    table::{storage_head_key, TableLifecycle, TablePurgeTask},
};
use retirement::TestRetiredTable;
use std::sync::Arc;

#[tokio::test]
async fn head_retirement_uses_the_returned_new_generation_as_its_exact_cas_base() {
    for native in [false, true] {
        let test = TestRetiredTable::new(native).await;
        test.creator().commit_staged(&test.request).await.unwrap();
        test.remove_mapping().await;
        let StorageRecord::TableHead(mut head) = test.head().unwrap() else {
            panic!()
        };
        let before = StorageRecord::TableHead(head.clone()).encode().unwrap();
        head.generation += 1;
        head.operation_fence += 1;
        let replacement = StorageRecord::TableHead(head).encode().unwrap();
        let task = test.retire_catalog().await;
        let task = test
            .to_phase(
                task,
                if native {
                    GcPhase::CleanupNative
                } else {
                    GcPhase::CleanupCatalog
                },
            )
            .await;
        let store = Arc::new(conflict::TestRetirementConflict::new(
            test.staged.namespace.store.clone(),
            storage_head_key(test.table()),
            replacement.clone(),
        ));
        let task = GcWorker::new(
            GcRepository::new(store.clone()),
            test.blocks.clone(),
            retirement::limits(),
        )
        .unwrap()
        .step(&task, retirement::NOW)
        .await
        .unwrap();
        assert_eq!(store.expected.load().as_ref(), &vec![before, replacement]);
        assert!(matches!(test.head(), Some(StorageRecord::RetiredTable(_))));
        test.finish(task).await;
    }
}

#[tokio::test]
async fn retired_cleanup_cannot_retire_a_head_while_its_catalog_is_still_active() {
    for native in [false, true] {
        let test = TestRetiredTable::new(native).await;
        test.creator().commit_staged(&test.request).await.unwrap();
        let before = test.head();
        let mut task = GcTask::plan(
            test.request.context,
            OperationId::random(),
            None,
            1000,
            retirement::limits(),
        )
        .unwrap();
        task.phase = if native {
            GcPhase::CleanupNative
        } else {
            GcPhase::CleanupCatalog
        };
        test.repository().create(&task).await.unwrap();
        assert!(
            GcWorker::new(test.repository(), test.blocks.clone(), retirement::limits())
                .unwrap()
                .step(&task, retirement::NOW)
                .await
                .is_err()
        );
        assert_eq!(test.head(), before);
    }
}

#[tokio::test]
async fn table_purge_retains_its_tombstone_head_in_the_active_catalog() {
    for native in [false, true] {
        let test = TestRetiredTable::new(native).await;
        test.creator().commit_staged(&test.request).await.unwrap();
        let StorageRecord::TableHead(mut head) = test.head().unwrap() else {
            panic!()
        };
        let before = StorageRecord::TableHead(head.clone()).encode().unwrap();
        head.lifecycle = TableLifecycle::Tombstone;
        head.pending_operation = Some(OperationId::random());
        let key = storage_head_key(test.table()).encode().unwrap();
        let after = StorageRecord::TableHead(head.clone()).encode().unwrap();
        let store = &test.staged.namespace.store;
        store
            .compare_exchange(
                &key,
                Some(&before),
                &after,
                mutation_identity(&key, Some(&before), &after),
            )
            .await
            .unwrap();
        let marker = TablePurgeTask {
            activation_epoch: test.request.context.activation_epoch,
            head: *head,
            dropped_ms: 1000,
        };
        let key = marker.key().encode().unwrap();
        let bytes = StorageRecord::TablePurgeTask(Box::new(marker.clone()))
            .encode()
            .unwrap();
        store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
        let task = test
            .repository()
            .admit_purge(test.request.context, &marker, 2000, retirement::limits())
            .await
            .unwrap();
        test.finish(task).await;
        let StorageRecord::TableHead(head) = test.head().unwrap() else {
            panic!("purge must preserve its table tombstone")
        };
        assert_eq!(head.lifecycle, TableLifecycle::Tombstone);
        assert_eq!(head.operation_fence, marker.head.operation_fence + 2);
        assert!(test.blocks.blocks.values.load().is_empty());
    }
}

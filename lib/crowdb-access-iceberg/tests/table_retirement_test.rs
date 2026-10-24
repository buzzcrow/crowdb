#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/table_creation.rs"]
#[allow(dead_code)]
mod creation;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/gc_blocks.rs"]
mod gc_blocks;
#[path = "common/gc_store.rs"]
mod gc_store;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogAuthority, CatalogLifecycle, CatalogStore, RootState},
    file::TableLocation,
    gc::{GcLimits, GcPhase, GcRepository, GcTask, GcWorker},
    key::{CatalogId, CatalogScope, IcebergKey, NamespaceId, OperationId, TableId},
    namespace::NamespaceDropper,
    operation::mutation_identity,
    record::StorageRecord,
    table::{name_key, storage_head_key, TableMapping, TableMappingState, TableRepository},
};

#[tokio::test]
async fn catalog_gc_keeps_permanent_retirement_fences_after_worker_restarts() {
    for native in [false, true] {
        let fixture = fixture::TestNamespace::new().await;
        let table = TableLocation {
            catalog: fixture.context.catalog,
            table: TableId::random(),
            storage_namespace: native.then(NamespaceId::random),
        };
        let key = storage_head_key(table).encode().unwrap();
        let bytes = StorageRecord::RetiredTable(table).encode().unwrap();
        fixture
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
        let mut authority = if native {
            CatalogAuthority::new_native(table.catalog, "retired".into()).unwrap()
        } else {
            CatalogAuthority::new(table.catalog, "retired".into()).unwrap()
        };
        authority.lifecycle = CatalogLifecycle::Retired;
        fixture
            .put(
                IcebergKey::Catalog {
                    catalog: table.catalog,
                    scope: CatalogScope::Authority,
                    suffix: Vec::new(),
                },
                StorageRecord::Authority(authority),
            )
            .await;
        fixture
            .root(
                fixture.context.replacement(CatalogId::random()).unwrap(),
                RootState::Ready,
            )
            .await;
        let limits = GcLimits {
            minimum_retention_ms: 10,
            ..GcLimits::default()
        };
        let mut task = GcTask::plan(fixture.context, OperationId::random(), None, 1000, limits).unwrap();
        let repository = GcRepository::new(fixture.store.clone());
        repository.create(&task).await.unwrap();
        let blocks = std::sync::Arc::new(gc_blocks::TestReclaimBlocks::default());
        for _ in 0..150 {
            task = GcWorker::new(repository.clone(), blocks.clone(), limits)
                .unwrap()
                .step(&task, 2000.max(task.retry_at_ms))
                .await
                .unwrap();
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
        assert_eq!(fixture.store.get(&key).await.unwrap().unwrap().bytes, bytes);
        assert!(matches!(
            fixture
                .store
                .compare_exchange(&key, None, b"late", mutation_identity(&key, None, b"late"))
                .await
                .unwrap(),
            CasOutcome::Conflict(Some(_))
        ));
    }
}

#[test]
fn retirement_is_bound_to_the_exact_final_table_key_in_both_layouts() {
    let table = TableLocation {
        catalog: CatalogId::random(),
        table: TableId::random(),
        storage_namespace: None,
    };
    for namespace in [None, Some(NamespaceId::random())] {
        let table = TableLocation {
            storage_namespace: namespace,
            ..table
        };
        let record = StorageRecord::RetiredTable(table);
        let bytes = record.encode().unwrap();
        assert!(
            bytes.len() <= 128,
            "retirement contains only final resource identity"
        );
        assert_eq!(
            StorageRecord::decode_storage(&storage_head_key(table), &bytes).unwrap(),
            record
        );
        for changed in 0..3 {
            let mut wrong = table;
            match changed {
                0 => wrong.catalog = CatalogId::random(),
                1 => wrong.table = TableId::random(),
                _ => wrong.storage_namespace = Some(NamespaceId::random()),
            }
            assert!(StorageRecord::decode_storage(&storage_head_key(wrong), &bytes).is_err());
        }
    }
}

#[tokio::test]
async fn strict_namespace_drop_fences_an_abandoned_reservation_before_removing_its_name() {
    let test = creation::TestCreation::new().await;
    let table = TableLocation {
        catalog: test.fixture.context.catalog,
        table: TableId::random(),
        storage_namespace: None,
    };
    let mapping = TableMapping {
        catalog: table.catalog,
        table: table.table,
        storage_namespace: None,
        namespace: test.parent.namespace,
        name: "abandoned".into(),
        name_epoch: 1,
        operation: OperationId::random(),
        state: TableMappingState::Reserved,
    };
    let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)
        .unwrap()
        .encode()
        .unwrap();
    let bytes = StorageRecord::TableMapping(mapping).encode().unwrap();
    test.fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    assert_eq!(
        NamespaceDropper::new(test.fixture.store.clone())
            .drop_namespace(&test.drop_request())
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
    assert!(test.fixture.store.get(&key).await.unwrap().is_none());
    let head = storage_head_key(table);
    let head_key = head.encode().unwrap();
    let value = test.fixture.store.get(&head_key).await.unwrap().unwrap();
    assert_eq!(
        StorageRecord::decode_storage(&head, &value.bytes).unwrap(),
        StorageRecord::RetiredTable(table)
    );
    let late = b"late initial head";
    assert!(matches!(
        test.fixture.store.compare_exchange(&head_key, None, late, mutation_identity(&head_key, None, late)).await.unwrap(),
        CasOutcome::Conflict(Some(observed)) if observed.bytes == value.bytes
    ));
    assert!(TableRepository::new(test.fixture.store.clone())
        .select(test.fixture.context, test.parent.namespace, "abandoned")
        .await
        .unwrap()
        .is_none());
}

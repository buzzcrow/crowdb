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
#[path = "common/namespace_store.rs"]
mod namespace_store;

#[path = "common/legacy_table_drop.rs"]
mod legacy;

use creation::TestCreation;
use crowdb_access_iceberg::{
    catalog::CatalogStore,
    key::{CatalogScope, IcebergKey, OperationId},
    namespace::{NamespaceDropper, NamespaceRepository},
    operation::RequestIdentity,
    record::StorageRecord,
    table::{
        head_key, TableHead, TableLifecycle, TableLifecycleAction, TableLifecyclePhase,
        TableLifecycleRequest, TableLifecycles, TableRepository,
    },
};
use std::sync::atomic::Ordering;

async fn setup(mode: u8) -> (TestCreation, TableLifecycleRequest) {
    let test = TestCreation::new().await;
    assert_eq!(test.creator().create(&test.request).await.unwrap().status, 200);
    let action = if mode < 2 {
        TableLifecycleAction::Drop {
            purge_requested: mode == 1,
        }
    } else {
        let namespace = if mode == 2 {
            test.parent.identifier.clone()
        } else {
            let target = test.fixture.authority(None, &["destination"]);
            test.fixture.publish(&target).await;
            target.identifier
        };
        TableLifecycleAction::Rename {
            namespace,
            name: "renamed".into(),
        }
    };
    let request = TableLifecycleRequest {
        context: test.fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        namespace: test.parent.identifier.clone(),
        name: "events".into(),
        action,
    };
    (test, request)
}

async fn assert_finished(test: &TestCreation, request: &TableLifecycleRequest, before: &TableHead) {
    assert!(TableLifecycles::new(test.fixture.store.clone())
        .load(request.context, request.identity.operation)
        .await
        .unwrap()
        .is_none());
    let tables = TableRepository::new(test.fixture.store.clone());
    assert!(tables
        .select(request.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .is_none());
    let key = head_key(request.context.catalog, before.table);
    let value = test
        .fixture
        .store
        .get(&key.encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
        panic!()
    };
    assert_eq!(head.metadata_location, before.metadata_location);
    assert_eq!(head.metadata_digest, before.metadata_digest);
    assert_eq!(head.table_uuid, before.table_uuid);
    assert_eq!(head.generation, before.generation);
    assert_eq!(head.operation_fence, before.operation_fence + 1);
    match &request.action {
        TableLifecycleAction::Drop { purge_requested } => {
            assert_eq!(head.lifecycle, TableLifecycle::Tombstone);
            let tasks: Vec<_> = test
                .fixture
                .store
                .values
                .load()
                .iter()
                .filter_map(|(key, value)| {
                    let key = IcebergKey::decode(key).unwrap();
                    match StorageRecord::decode(&key, &value.bytes).unwrap() {
                        StorageRecord::TablePurgeTask(task) => Some(task),
                        _ => None,
                    }
                })
                .collect();
            assert_eq!(tasks.len(), usize::from(*purge_requested));
            if *purge_requested {
                assert_eq!(&tasks[0].head, head.as_ref());
            }
        }
        TableLifecycleAction::Rename { namespace, name } => {
            assert_eq!(head.lifecycle, TableLifecycle::Ready);
            assert!(head.pending_operation.is_none());
            assert_eq!(head.name_epoch, before.name_epoch + 1);
            let parent = NamespaceRepository::new(test.fixture.store.clone())
                .load(request.context, namespace)
                .await
                .unwrap()
                .unwrap();
            assert!(parent.pending_operation.is_none());
            let selected = tables
                .select(request.context, parent.namespace, name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(selected.head.table, before.table);
        }
    }
}

#[tokio::test]
async fn lifecycle_recovery_survives_every_durable_lost_response_without_file_io() {
    for mode in 0..2 {
        let (baseline, request) = setup(mode).await;
        let before = baseline.fixture.store.writes.load(Ordering::SeqCst);
        TableLifecycles::new(baseline.fixture.store.clone())
            .execute(&request)
            .await
            .unwrap();
        let count = baseline.fixture.store.writes.load(Ordering::SeqCst) - before;
        assert_eq!(count, if mode == 1 { 3 } else { 2 });
        for offset in 1..=count {
            let (test, request) = setup(mode).await;
            let before = TableRepository::new(test.fixture.store.clone())
                .select(request.context, test.parent.namespace, "events")
                .await
                .unwrap()
                .unwrap()
                .head;
            let writes = test.fixture.store.writes.load(Ordering::SeqCst);
            let block_reads = test.blocks.reads.load(Ordering::SeqCst);
            let block_writes = test.blocks.writes.load(Ordering::SeqCst);
            let files: Vec<_> = test
                .fixture
                .store
                .values
                .load()
                .iter()
                .filter(|(key, _)| {
                    matches!(
                        IcebergKey::decode(key).unwrap(),
                        IcebergKey::Catalog {
                            scope: CatalogScope::File | CatalogScope::FileLocation,
                            ..
                        }
                    )
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            test.fixture
                .store
                .fail_after
                .store(writes + offset, Ordering::SeqCst);
            assert!(
                TableLifecycles::new(test.fixture.store.clone())
                    .execute(&request)
                    .await
                    .is_err(),
                "mode={mode} offset={offset}"
            );
            test.fixture.store.fail_after.store(0, Ordering::SeqCst);
            let recovered = TableLifecycles::new(test.fixture.store.clone())
                .execute(&request)
                .await
                .unwrap();
            assert_eq!(
                recovered.status,
                if mode == 1 && offset == 1 { 204 } else { 404 },
                "mode={mode} offset={offset}"
            );
            assert_finished(&test, &request, &before).await;
            assert_eq!(test.blocks.reads.load(Ordering::SeqCst), block_reads);
            assert_eq!(test.blocks.writes.load(Ordering::SeqCst), block_writes);
            for (key, value) in files {
                assert_eq!(test.fixture.store.get(&key).await.unwrap().unwrap(), value);
            }
        }
    }
}

#[tokio::test]
async fn old_named_delete_targets_the_current_recreated_source() {
    for mode in 0..2 {
        let (test, request) = setup(mode).await;
        let manager = TableLifecycles::new(test.fixture.store.clone());
        assert_eq!(manager.execute(&request).await.unwrap().status, 204);
        let mut recreated = test.request.clone();
        recreated.identity.operation = OperationId::random();
        assert_eq!(test.creator().create(&recreated).await.unwrap().status, 200);
        assert!(TableRepository::new(test.fixture.store.clone())
            .select(request.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .is_some());
        assert_eq!(manager.execute(&request).await.unwrap().status, 204);
        assert!(TableRepository::new(test.fixture.store.clone())
            .select(request.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn named_delete_has_no_purge_or_principal_response_ledger() {
    let (test, request) = setup(1).await;
    let manager = TableLifecycles::new(test.fixture.store.clone());
    manager.execute(&request).await.unwrap();
    let mut changed = request.clone();
    changed.action = TableLifecycleAction::Drop {
        purge_requested: false,
    };
    assert_eq!(manager.execute(&changed).await.unwrap().status, 404);
    changed.principal = "other".into();
    assert_eq!(manager.execute(&changed).await.unwrap().status, 404);
}

#[tokio::test]
async fn namespace_can_drop_after_table_drop_or_cross_namespace_move() {
    for mode in [0, 1] {
        let (test, request) = setup(mode).await;
        TableLifecycles::new(test.fixture.store.clone())
            .execute(&request)
            .await
            .unwrap();
        let result = NamespaceDropper::new(test.fixture.store.clone())
            .drop_namespace(&test.drop_request())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, 204);
    }
}

#[tokio::test]
async fn legacy_cleanup_cannot_remove_a_source_recreated_before_recovery() {
    let (test, request) = setup(1).await;
    let before = TableRepository::new(test.fixture.store.clone())
        .select(request.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap()
        .head;
    legacy::install(
        test.fixture.store.clone(),
        &request,
        before,
        TableLifecyclePhase::Published,
    )
    .await;
    let mut recreated = test.request.clone();
    recreated.identity.operation = OperationId::random();
    test.creator().create(&recreated).await.unwrap();
    let tables = TableRepository::new(test.fixture.store.clone());
    let replacement = tables
        .select(request.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        TableLifecycles::new(test.fixture.store.clone())
            .resume(request.context, request.identity.operation)
            .await
            .unwrap()
            .status,
        204
    );
    assert_eq!(
        tables
            .select(request.context, test.parent.namespace, "events")
            .await
            .unwrap(),
        Some(replacement)
    );
}

#[tokio::test]
async fn lifecycle_records_reject_metadata_mutation_foreign_keys_and_retired_contexts() {
    let (test, request) = setup(1).await;
    let manager = TableLifecycles::new(test.fixture.store.clone());
    let before = TableRepository::new(test.fixture.store.clone())
        .select(request.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap()
        .head;
    let operation = legacy::install(
        test.fixture.store.clone(),
        &request,
        before,
        TableLifecyclePhase::Prepared,
    )
    .await;
    let mut corrupted = operation.clone();
    corrupted.candidate.generation += 1;
    assert!(StorageRecord::TableLifecycleOperation(Box::new(corrupted))
        .encode()
        .is_err());
    let mut corrupted = operation.clone();
    corrupted.candidate.namespace = crowdb_access_iceberg::key::NamespaceId::random();
    assert!(StorageRecord::TableLifecycleOperation(Box::new(corrupted))
        .encode()
        .is_err());
    let mut corrupted = operation.clone();
    corrupted.source.name_epoch += 1;
    assert!(StorageRecord::TableLifecycleOperation(Box::new(corrupted))
        .encode()
        .is_err());
    let mut foreign = operation.clone();
    foreign.identity.operation = OperationId::random();
    let bytes = StorageRecord::TableLifecycleOperation(Box::new(operation))
        .encode()
        .unwrap();
    assert!(StorageRecord::decode(&foreign.key(), &bytes).is_err());
    test.fixture
        .root(
            request.context,
            crowdb_access_iceberg::catalog::RootState::Fencing,
        )
        .await;
    assert!(manager.execute(&request).await.is_err());
}

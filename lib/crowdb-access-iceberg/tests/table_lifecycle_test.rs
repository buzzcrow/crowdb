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

use creation::TestCreation;
use crowdb_access_iceberg::{
    catalog::{CatalogError, CatalogStore},
    key::{CatalogScope, IcebergKey, OperationId},
    namespace::{NamespaceDropper, NamespaceRepository},
    operation::RequestIdentity,
    record::StorageRecord,
    table::{
        head_key, TableLifecycle, TableLifecycleAction, TableLifecyclePhase, TableLifecycleRequest,
        TableLifecycles, TableRepository,
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

async fn assert_finished(test: &TestCreation, request: &TableLifecycleRequest) {
    let manager = TableLifecycles::new(test.fixture.store.clone());
    let operation = manager
        .load(request.context, request.identity.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.phase, TableLifecyclePhase::Complete);
    let tables = TableRepository::new(test.fixture.store.clone());
    assert!(tables
        .select(request.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .is_none());
    let key = head_key(request.context.catalog, operation.before.table);
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
    assert_eq!(head.metadata_location, operation.before.metadata_location);
    assert_eq!(head.metadata_digest, operation.before.metadata_digest);
    assert_eq!(head.table_uuid, operation.before.table_uuid);
    assert_eq!(head.generation, operation.before.generation);
    assert_eq!(head.operation_fence, operation.before.operation_fence + 1);
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
            assert_eq!(head.name_epoch, operation.before.name_epoch + 1);
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
            assert_eq!(selected.head.table, operation.before.table);
        }
    }
    let record = StorageRecord::TableLifecycleOperation(Box::new(operation.clone()));
    assert_eq!(
        StorageRecord::decode(&operation.key(), &record.encode().unwrap()).unwrap(),
        record
    );
}

#[tokio::test]
async fn lifecycle_recovery_survives_every_durable_lost_response_without_file_io() {
    for mode in 0..4 {
        let (baseline, request) = setup(mode).await;
        let before = baseline.fixture.store.writes.load(Ordering::SeqCst);
        TableLifecycles::new(baseline.fixture.store.clone())
            .execute(&request)
            .await
            .unwrap();
        let count = baseline.fixture.store.writes.load(Ordering::SeqCst) - before;
        assert!(count >= 5);
        for offset in 1..=count {
            let (test, request) = setup(mode).await;
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
            assert_eq!(recovered.status, 204, "mode={mode} offset={offset}");
            assert_finished(&test, &request).await;
            assert_eq!(test.blocks.reads.load(Ordering::SeqCst), block_reads);
            assert_eq!(test.blocks.writes.load(Ordering::SeqCst), block_writes);
            for (key, value) in files {
                assert_eq!(test.fixture.store.get(&key).await.unwrap().unwrap(), value);
            }
        }
    }
}

#[tokio::test]
async fn old_lifecycle_replay_cannot_remove_recreated_source_or_destination() {
    for mode in 0..4 {
        let (test, request) = setup(mode).await;
        let manager = TableLifecycles::new(test.fixture.store.clone());
        assert_eq!(manager.execute(&request).await.unwrap().status, 204);
        let mut recreated = test.request.clone();
        recreated.identity.operation = OperationId::random();
        assert_eq!(test.creator().create(&recreated).await.unwrap().status, 200);
        let tables = TableRepository::new(test.fixture.store.clone());
        let source = tables
            .select(request.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap();
        let mut later = None;
        if let TableLifecycleAction::Rename { namespace, name } = &request.action {
            let drop = TableLifecycleRequest {
                namespace: namespace.clone(),
                name: name.clone(),
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: 100,
                },
                action: TableLifecycleAction::Drop {
                    purge_requested: true,
                },
                ..request.clone()
            };
            assert_eq!(manager.execute(&drop).await.unwrap().status, 204);
            recreated.identity.operation = OperationId::random();
            recreated.namespace = namespace.clone();
            let mut body: serde_json::Value = serde_json::from_slice(&recreated.body).unwrap();
            body["name"] = name.clone().into();
            recreated.body = serde_json::to_vec(&body).unwrap();
            assert_eq!(test.creator().create(&recreated).await.unwrap().status, 200);
            let parent = NamespaceRepository::new(test.fixture.store.clone())
                .load(request.context, namespace)
                .await
                .unwrap()
                .unwrap();
            later = Some(
                tables
                    .select(request.context, parent.namespace, name)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(manager.execute(&request).await.unwrap().status, 204);
        assert_eq!(
            tables
                .select(request.context, test.parent.namespace, "events")
                .await
                .unwrap()
                .unwrap(),
            source
        );
        if let Some(later) = later {
            assert_eq!(
                tables
                    .select(request.context, later.head.namespace, &later.head.name)
                    .await
                    .unwrap()
                    .unwrap(),
                later
            );
        }
    }
}

#[tokio::test]
async fn lifecycle_identity_reuse_rejects_changed_purge_or_principal() {
    let (test, request) = setup(1).await;
    let manager = TableLifecycles::new(test.fixture.store.clone());
    manager.execute(&request).await.unwrap();
    let mut changed = request.clone();
    changed.action = TableLifecycleAction::Drop {
        purge_requested: false,
    };
    assert!(matches!(
        manager.execute(&changed).await,
        Err(CatalogError::Conflict)
    ));
    changed = request.clone();
    changed.principal = "other".into();
    assert!(matches!(
        manager.execute(&changed).await,
        Err(CatalogError::Conflict)
    ));
    changed = request.clone();
    changed.identity.operation = OperationId::random();
    assert_eq!(manager.execute(&changed).await.unwrap().status, 404);
}

#[tokio::test]
async fn namespace_can_drop_after_table_drop_or_cross_namespace_move() {
    for mode in [0, 1, 3] {
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
async fn interrupted_lifecycle_cannot_cleanup_a_source_recreated_before_recovery() {
    for mode in [1, 3] {
        let (baseline, request) = setup(mode).await;
        let start = baseline.fixture.store.writes.load(Ordering::SeqCst);
        TableLifecycles::new(baseline.fixture.store.clone())
            .execute(&request)
            .await
            .unwrap();
        let count = baseline.fixture.store.writes.load(Ordering::SeqCst) - start;
        let mut exercised = 0;
        for offset in 1..=count {
            let (test, request) = setup(mode).await;
            let store = test.fixture.store.clone();
            store
                .fail_after
                .store(store.writes.load(Ordering::SeqCst) + offset, Ordering::SeqCst);
            assert!(TableLifecycles::new(store.clone())
                .execute(&request)
                .await
                .is_err());
            store.fail_after.store(0, Ordering::SeqCst);
            let tables = TableRepository::new(store.clone());
            if tables
                .select(request.context, test.parent.namespace, "events")
                .await
                .unwrap()
                .is_some()
            {
                continue;
            }
            let mut recreated = test.request.clone();
            recreated.identity.operation = OperationId::random();
            assert_eq!(test.creator().create(&recreated).await.unwrap().status, 200);
            let replacement = tables
                .select(request.context, test.parent.namespace, "events")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                TableLifecycles::new(store)
                    .execute(&request)
                    .await
                    .unwrap()
                    .status,
                204
            );
            assert_eq!(
                tables
                    .select(request.context, test.parent.namespace, "events")
                    .await
                    .unwrap()
                    .unwrap(),
                replacement
            );
            exercised += 1;
        }
        assert!(exercised >= 3);
    }
}

#[tokio::test]
async fn lifecycle_records_reject_metadata_mutation_foreign_keys_and_retired_contexts() {
    let (test, request) = setup(3).await;
    let manager = TableLifecycles::new(test.fixture.store.clone());
    manager.execute(&request).await.unwrap();
    let operation = manager
        .load(request.context, request.identity.operation)
        .await
        .unwrap()
        .unwrap();
    let mut corrupted = operation.clone();
    corrupted.candidate.generation += 1;
    assert!(StorageRecord::TableLifecycleOperation(Box::new(corrupted))
        .encode()
        .is_err());
    let mut corrupted = operation.clone();
    corrupted.purge_requested = true;
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

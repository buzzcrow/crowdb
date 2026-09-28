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
    catalog::CatalogError,
    key::OperationId,
    namespace::{NamespaceDropRequest, NamespaceDropper, NamespaceIdentifier, NamespaceRepository},
    operation::RequestIdentity,
    table::{
        TableLifecycleAction, TableLifecyclePhase, TableLifecycleRequest, TableLifecycles, TableRepository,
    },
};
use std::sync::atomic::Ordering;

fn request(test: &TestCreation, namespace: NamespaceIdentifier) -> TableLifecycleRequest {
    TableLifecycleRequest {
        context: test.fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        namespace: test.parent.identifier.clone(),
        name: "events".into(),
        action: TableLifecycleAction::Rename {
            namespace,
            name: "renamed".into(),
        },
    }
}

#[tokio::test]
async fn namespace_drop_helps_rename_with_delayed_head_cas_response_and_cannot_remove_destination() {
    for after in [false, true] {
        let test = TestCreation::new().await;
        test.creator().create(&test.request).await.unwrap();
        let parent = test.fixture.authority(None, &["destination"]);
        test.fixture.publish(&parent).await;
        let request = request(&test, parent.identifier.clone());
        let store = test.fixture.store.clone();
        if after {
            store.table_head_pause_after.store(true, Ordering::SeqCst);
        } else {
            store.table_head_pause_before.store(true, Ordering::SeqCst);
        }
        let task_request = request.clone();
        let task_store = store.clone();
        let pending =
            tokio::spawn(async move { TableLifecycles::new(task_store).execute(&task_request).await });
        store.table_head_entered.notified().await;
        let result = NamespaceDropper::new(store.clone())
            .drop_namespace(&NamespaceDropRequest {
                context: request.context,
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: 100,
                },
                principal: "writer".into(),
                identifier: parent.identifier.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, 409);
        let tables = TableRepository::new(store.clone());
        assert!(tables
            .select(request.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .is_none());
        let selected = tables
            .select(request.context, parent.namespace, "renamed")
            .await
            .unwrap()
            .unwrap();
        store.table_head_release.notify_one();
        let delayed = pending.await.unwrap();
        assert!(delayed.is_ok() || matches!(delayed, Err(CatalogError::Busy)));
        assert_eq!(
            TableLifecycles::new(store.clone())
                .execute(&request)
                .await
                .unwrap()
                .status,
            204
        );
        assert_eq!(
            tables
                .select(request.context, parent.namespace, "renamed")
                .await
                .unwrap()
                .unwrap(),
            selected
        );
    }
}

#[tokio::test]
async fn losing_rename_cannot_publish_after_concurrent_drop_or_remove_recreated_source() {
    let test = TestCreation::new().await;
    test.creator().create(&test.request).await.unwrap();
    let parent = test.fixture.authority(None, &["destination"]);
    test.fixture.publish(&parent).await;
    let request = request(&test, parent.identifier.clone());
    let store = test.fixture.store.clone();
    store.table_head_pause_before.store(true, Ordering::SeqCst);
    let task_request = request.clone();
    let task_store = store.clone();
    let pending = tokio::spawn(async move { TableLifecycles::new(task_store).execute(&task_request).await });
    store.table_head_entered.notified().await;
    let drop = TableLifecycleRequest {
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        action: TableLifecycleAction::Drop {
            purge_requested: true,
        },
        ..request.clone()
    };
    assert_eq!(
        TableLifecycles::new(store.clone())
            .execute(&drop)
            .await
            .unwrap()
            .status,
        204
    );
    let mut recreated = test.request.clone();
    recreated.identity.operation = OperationId::random();
    assert_eq!(test.creator().create(&recreated).await.unwrap().status, 200);
    store.table_head_release.notify_one();
    assert_eq!(pending.await.unwrap().unwrap().status, 409);
    let tables = TableRepository::new(store.clone());
    assert!(tables
        .select(request.context, parent.namespace, "renamed")
        .await
        .unwrap()
        .is_none());
    assert!(tables
        .select(request.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        NamespaceDropper::new(store)
            .drop_namespace(&NamespaceDropRequest {
                context: request.context,
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: 100
                },
                principal: "writer".into(),
                identifier: parent.identifier,
            })
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
}

#[tokio::test]
async fn every_interrupted_rename_phase_composes_with_destination_namespace_drop() {
    let baseline = TestCreation::new().await;
    baseline.creator().create(&baseline.request).await.unwrap();
    let parent = baseline.fixture.authority(None, &["destination"]);
    baseline.fixture.publish(&parent).await;
    let start = baseline.fixture.store.writes.load(Ordering::SeqCst);
    TableLifecycles::new(baseline.fixture.store.clone())
        .execute(&request(&baseline, parent.identifier))
        .await
        .unwrap();
    let count = baseline.fixture.store.writes.load(Ordering::SeqCst) - start;
    for offset in 1..=count {
        let test = TestCreation::new().await;
        test.creator().create(&test.request).await.unwrap();
        let parent = test.fixture.authority(None, &["destination"]);
        test.fixture.publish(&parent).await;
        let request = request(&test, parent.identifier.clone());
        let store = test.fixture.store.clone();
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + offset, Ordering::SeqCst);
        assert!(TableLifecycles::new(store.clone())
            .execute(&request)
            .await
            .is_err());
        store.fail_after.store(0, Ordering::SeqCst);
        let result = NamespaceDropper::new(store.clone())
            .drop_namespace(&NamespaceDropRequest {
                context: request.context,
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: 100,
                },
                principal: "writer".into(),
                identifier: parent.identifier.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let outcome = TableLifecycles::new(store.clone())
            .execute(&request)
            .await
            .unwrap();
        let exists = NamespaceRepository::new(store.clone())
            .load(request.context, &parent.identifier)
            .await
            .unwrap()
            .is_some();
        let moved = TableRepository::new(store.clone())
            .select(request.context, parent.namespace, "renamed")
            .await
            .unwrap()
            .is_some();
        match result.status {
            204 => {
                assert!(!exists && !moved);
                assert_eq!(outcome.status, 404);
            }
            409 => {
                assert!(exists && moved);
                assert_eq!(outcome.status, 204);
            }
            status => panic!("unexpected status {status} offset={offset}"),
        }
        if let Some(operation) = TableLifecycles::new(store)
            .load(request.context, request.identity.operation)
            .await
            .unwrap()
        {
            assert!(matches!(
                operation.phase,
                TableLifecyclePhase::Complete | TableLifecyclePhase::Aborted
            ));
        }
    }
}

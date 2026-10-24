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
#[path = "common/namespace_recovery_store.rs"]
mod namespace_recovery_store;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/table_rename.rs"]
#[allow(dead_code)]
mod rename;

use crowdb_access_iceberg::{
    catalog::CatalogError,
    key::OperationId,
    namespace::{NamespaceDropRequest, NamespaceDropper, NamespaceRepository},
    operation::RequestIdentity,
    table::{TableLifecycleAction, TableLifecycleRequest, TableLifecycles, TableRepository},
};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn canceled_request_retry_uses_a_fresh_reservation_against_delayed_cleanup() {
    use crowdb_access_iceberg::{
        catalog::{CasOutcome, CatalogStore},
        namespace::NamespaceStore,
        operation::mutation_identity,
        table::name_key,
    };
    let (test, request, _) = rename::setup(false).await;
    let store = test.fixture.store.clone();
    let key = name_key(request.context.catalog, test.parent.namespace, "renamed")
        .unwrap()
        .encode()
        .unwrap();
    let mut canceled = None;
    for attempt in 0..2 {
        store.table_head_pause_before.store(true, Ordering::SeqCst);
        let task_store = store.clone();
        let task_request = request.clone();
        let pending =
            tokio::spawn(async move { TableLifecycles::new(task_store).execute(&task_request).await });
        store.table_head_entered.notified().await;
        let reserved = store.get(&key).await.unwrap().unwrap().bytes;
        if attempt == 0 {
            let drop = test.drop_request();
            assert!(matches!(
                NamespaceDropper::new(store.clone()).drop_namespace(&drop).await,
                Err(CatalogError::Busy)
            ));
            canceled = Some(reserved);
        } else {
            let old = canceled.as_ref().unwrap();
            assert_ne!(old, &reserved);
            let result = store
                .delete_mapping(&key, old, mutation_identity(&key, Some(old), &[]))
                .await
                .unwrap();
            assert!(matches!(result, CasOutcome::Conflict(Some(value)) if value.bytes == reserved));
        }
        store.table_head_release.notify_one();
        assert_eq!(
            pending.await.unwrap().unwrap().status,
            if attempt == 0 { 409 } else { 204 }
        );
    }
    assert_eq!(rename::visible(&test, &request).await, (false, true));
}

#[tokio::test]
async fn drop_fences_delayed_rename_or_preserves_the_committed_destination() {
    for after in [false, true] {
        let (test, request, before) = rename::setup(true).await;
        let TableLifecycleAction::Rename { namespace, .. } = &request.action else {
            panic!()
        };
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
        let drop = NamespaceDropRequest {
            context: request.context,
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: 100,
            },
            principal: "writer".into(),
            identifier: namespace.clone(),
        };
        let dropper = NamespaceDropper::new(store.clone());
        assert!(matches!(
            dropper.drop_namespace(&drop).await,
            Err(CatalogError::Busy)
        ));
        let outcome = dropper.drop_namespace(&drop).await.unwrap().unwrap();
        assert_eq!(outcome.status, if after { 409 } else { 204 });
        assert_eq!(rename::visible(&test, &request).await, (!after, after));
        store.table_head_release.notify_one();
        assert_eq!(
            pending.await.unwrap().unwrap().status,
            if after { 204 } else { 409 }
        );
        assert_eq!(rename::visible(&test, &request).await, (!after, after));
        assert!(rename::head(&test, &before).await.operation_fence > before.operation_fence);
    }
}

#[tokio::test]
async fn interrupted_occupancies_recover_from_resources_at_every_mutation() {
    for cross in [false, true] {
        let (baseline, request, _) = rename::setup(cross).await;
        let start = baseline.fixture.store.writes.load(Ordering::SeqCst);
        TableLifecycles::new(baseline.fixture.store.clone())
            .execute(&request)
            .await
            .unwrap();
        let count = baseline.fixture.store.writes.load(Ordering::SeqCst) - start;
        assert_eq!(count, if cross { 8 } else { 6 });
        for offset in 1..=count {
            let (test, request, before) = rename::setup(cross).await;
            let store = test.fixture.store.clone();
            let reads = test.blocks.reads.load(Ordering::SeqCst);
            let writes = test.blocks.writes.load(Ordering::SeqCst);
            store
                .fail_after
                .store(store.writes.load(Ordering::SeqCst) + offset, Ordering::SeqCst);
            assert!(
                TableLifecycles::new(store.clone())
                    .execute(&request)
                    .await
                    .is_err(),
                "cross={cross} offset={offset}"
            );
            store.fail_after.store(0, Ordering::SeqCst);
            rename::repair(&test).await;
            let state = rename::visible(&test, &request).await;
            assert!(
                matches!(state, (true, false) | (false, true)),
                "cross={cross} offset={offset}"
            );
            let head = rename::head(&test, &before).await;
            assert_eq!(head.metadata_location, before.metadata_location);
            assert_eq!(head.generation, before.generation);
            assert_eq!(head.metadata_digest, before.metadata_digest);
            let outcome = TableLifecycles::new(store.clone())
                .execute(&request)
                .await
                .unwrap();
            assert_eq!(outcome.status, if state.0 { 204 } else { 404 });
            assert_eq!(rename::visible(&test, &request).await, (false, true));
            assert_eq!(test.blocks.reads.load(Ordering::SeqCst), reads);
            assert_eq!(test.blocks.writes.load(Ordering::SeqCst), writes);
        }
    }
}

#[tokio::test]
async fn losing_rename_cannot_change_a_concurrent_drop_or_recreated_source() {
    let (test, request, _) = rename::setup(true).await;
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
    let tables = TableRepository::new(store.clone());
    let replacement = tables
        .select(request.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    store.table_head_release.notify_one();
    assert_eq!(pending.await.unwrap().unwrap().status, 409);
    assert_eq!(
        tables
            .select(request.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap(),
        replacement
    );
    let TableLifecycleAction::Rename { namespace, .. } = &request.action else {
        panic!()
    };
    let target = NamespaceRepository::new(store)
        .load(request.context, namespace)
        .await
        .unwrap()
        .unwrap();
    assert!(target.rename.is_none());
}

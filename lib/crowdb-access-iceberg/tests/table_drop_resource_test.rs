// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

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
#[path = "common/gc_store.rs"]
mod gc_store;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "table_drop_resource/purge.rs"]
mod purge;
#[path = "common/table_drop_resource.rs"]
mod support;

use crowdb_access_iceberg::{
    key::{CatalogScope, IcebergKey, OperationId},
    operation::RequestIdentity,
    table::{TableLifecycleAction, TableLifecycleRequest, TableLifecycles},
};
use std::sync::atomic::Ordering;

use support::TestDrop;

#[tokio::test]
async fn every_unknown_stops_without_readback_and_retries_use_current_resources() {
    use crowdb_access_iceberg::{
        catalog::CatalogStore,
        table::{storage_head_key, TableLifecycle},
    };
    for failed in 1..=3 {
        let test = TestDrop::new(true).await;
        let store = test.store();
        let writes = store.writes.load(Ordering::SeqCst);
        let reads = store.reads.load(Ordering::SeqCst);
        store.fail_after.store(writes + failed, Ordering::SeqCst);
        assert!(TableLifecycles::new(store.clone())
            .execute(&test.request)
            .await
            .is_err());
        assert_eq!(store.reads.load(Ordering::SeqCst) - reads, 7);
        assert_eq!(store.writes.load(Ordering::SeqCst) - writes, failed);
        store.fail_after.store(0, Ordering::SeqCst);
        let marker = test.marker();
        let head = test.head().await;
        assert_eq!(marker.dropped_ms, 0);
        assert_eq!(
            head.lifecycle,
            if failed == 1 {
                TableLifecycle::Ready
            } else {
                TableLifecycle::Tombstone
            }
        );
        let status = TableLifecycles::new(store.clone())
            .execute(&test.request)
            .await
            .unwrap()
            .status;
        assert_eq!(status, if failed == 1 { 204 } else { 404 });
        assert_eq!(test.head().await.lifecycle, TableLifecycle::Tombstone);
        assert!(store
            .get(&storage_head_key(test.before.storage_location()).encode().unwrap())
            .await
            .unwrap()
            .is_some());
        assert!(test.selected().await.is_none());
    }
}

#[tokio::test]
async fn fresh_named_delete_can_delete_a_replacement_but_old_purge_cannot_select_it() {
    use crowdb_access_iceberg::gc::{GcLimits, GcRepository, PurgeAdmission};
    for requested in [false, true] {
        let test = TestDrop::new(requested).await;
        let manager = TableLifecycles::new(test.store());
        assert_eq!(manager.execute(&test.request).await.unwrap().status, 204);
        let replacement = test.recreate().await;
        assert_ne!(replacement.table, test.before.table);
        if requested {
            let PurgeAdmission::Ready(task) = GcRepository::new(test.store())
                .discover_purge(test.request.context, &test.marker(), 5000, GcLimits::default())
                .await
                .unwrap()
            else {
                panic!("task");
            };
            assert_eq!(task.head.as_ref().unwrap().table, test.before.table);
            assert_eq!(test.selected().await, Some(replacement.clone()));
        }
        assert_eq!(manager.execute(&test.request).await.unwrap().status, 204);
        assert!(test.selected().await.is_none());
        assert_eq!(test.head().await.table, test.before.table);
    }
}

#[tokio::test]
async fn drop_writes_only_final_resources_and_no_request_payload_or_journal() {
    assert_cost(false).await;
}

#[tokio::test]
async fn purge_drop_writes_only_final_resources_and_no_request_payload_or_journal() {
    assert_cost(true).await;
}

async fn assert_cost(purge: bool) {
    let test = creation::TestCreation::new().await;
    test.creator().create(&test.request).await.unwrap();
    let request = TableLifecycleRequest {
        context: test.fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        namespace: test.parent.identifier.clone(),
        name: "events".into(),
        action: TableLifecycleAction::Drop {
            purge_requested: purge,
        },
    };
    let before = test.fixture.store.values.load_full();
    let reads = test.fixture.store.reads.load(Ordering::SeqCst);
    let writes = test.fixture.store.writes.load(Ordering::SeqCst);
    assert_eq!(
        TableLifecycles::new(test.fixture.store.clone())
            .execute(&request)
            .await
            .unwrap()
            .status,
        204
    );
    let cost = (
        test.fixture.store.reads.load(Ordering::SeqCst) - reads,
        test.fixture.store.writes.load(Ordering::SeqCst) - writes,
    );
    eprintln!("drop purge={purge}: {} GET, {} mutations", cost.0, cost.1);
    assert_eq!(cost.1, if purge { 3 } else { 2 });
    for key in test
        .fixture
        .store
        .values
        .load()
        .keys()
        .filter(|key| !before.contains_key(*key))
    {
        assert!(
            !matches!(
                IcebergKey::decode(key).unwrap(),
                IcebergKey::Catalog {
                    scope: CatalogScope::TableLifecycleOperation | CatalogScope::OperationPayload,
                    ..
                }
            ),
            "new process record: {key:?}"
        );
    }
}

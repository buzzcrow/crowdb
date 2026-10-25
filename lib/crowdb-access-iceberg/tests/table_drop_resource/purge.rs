// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    support::{TestDrop, TestPurgeStore},
    TableLifecycles,
};
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore},
    gc::{GcLimits, GcRepository, GcTask, PurgeAdmission},
    operation::mutation_identity,
    record::StorageRecord,
    table::{storage_head_key, TableLifecycle, TableLifecycleAction},
};
use std::sync::{atomic::Ordering, Arc};

async fn interrupted() -> TestDrop {
    let test = TestDrop::new(true).await;
    let store = test.store();
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(TableLifecycles::new(store.clone())
        .execute(&test.request)
        .await
        .is_err());
    store.fail_after.store(0, Ordering::SeqCst);
    test
}

#[tokio::test]
async fn orphan_marker_is_fenced_before_delete_and_late_drop_cannot_resurrect_it() {
    let test = interrupted().await;
    let marker = test.marker();
    let metered = Arc::new(TestPurgeStore::new(test.store()));
    assert!(matches!(
        GcRepository::new(metered.clone())
            .discover_purge(test.request.context, &marker, 5000, GcLimits::default())
            .await
            .unwrap(),
        PurgeAdmission::Cancelled
    ));
    assert_eq!(metered.counts(), (4, 2));
    let key = storage_head_key(test.before.storage_location()).encode().unwrap();
    let before = StorageRecord::TableHead(Box::new(test.before.clone()))
        .encode()
        .unwrap();
    let bytes = StorageRecord::TableHead(Box::new(marker.head)).encode().unwrap();
    assert!(matches!(
        test.store()
            .compare_exchange(
                &key,
                Some(&before),
                &bytes,
                mutation_identity(&key, Some(&before), &bytes)
            )
            .await
            .unwrap(),
        CasOutcome::Conflict(Some(_))
    ));
    assert_eq!(test.head().await.lifecycle, TableLifecycle::Ready);
}

#[tokio::test]
async fn unknown_cancel_fence_and_marker_delete_keep_a_restart_safe_resource_chain() {
    for delete_reply in [false, true] {
        let test = interrupted().await;
        let store = test.store();
        let marker = test.marker();
        if delete_reply {
            store.gc_delete_reply_loss.store(true, Ordering::SeqCst);
        } else {
            store
                .fail_after
                .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        }
        let metered = Arc::new(TestPurgeStore::new(store.clone()));
        assert!(GcRepository::new(metered.clone())
            .discover_purge(test.request.context, &marker, 5000, GcLimits::default())
            .await
            .is_err());
        assert_eq!(metered.counts(), (4, if delete_reply { 2 } else { 1 }));
        store.fail_after.store(0, Ordering::SeqCst);
        assert!(test.head().await.operation_fence > test.before.operation_fence);
        assert!(matches!(
            GcRepository::new(store.clone())
                .discover_purge(test.request.context, &marker, 6000, GcLimits::default())
                .await
                .unwrap(),
            PurgeAdmission::Cancelled
        ));
        assert!(store
            .get(&marker.key().encode().unwrap())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            TableLifecycles::new(store)
                .execute(&test.request)
                .await
                .unwrap()
                .status,
            204
        );
    }
}

#[tokio::test]
async fn missing_head_is_permanently_fenced_before_an_orphan_marker_disappears() {
    let test = interrupted().await;
    let store = test.store();
    let marker = test.marker();
    let key = storage_head_key(test.before.storage_location()).encode().unwrap();
    store.values.rcu(|map| {
        let mut next = (**map).clone();
        next.remove(&key);
        next
    });
    assert!(matches!(
        GcRepository::new(store.clone())
            .discover_purge(test.request.context, &marker, 5000, GcLimits::default())
            .await
            .unwrap(),
        PurgeAdmission::Cancelled
    ));
    let value = store.get(&key).await.unwrap().unwrap();
    assert!(matches!(
        StorageRecord::decode(&storage_head_key(test.before.storage_location()), &value.bytes).unwrap(),
        StorageRecord::RetiredTable(_)
    ));
    let bytes = StorageRecord::TableHead(Box::new(test.before)).encode().unwrap();
    assert!(matches!(
        store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap(),
        CasOutcome::Conflict(Some(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn fence_conflict_consumes_the_winning_tombstone_without_an_extra_get() {
    let test = interrupted().await;
    let marker = test.marker();
    let metered = Arc::new(TestPurgeStore::new(test.store()));
    metered.pause_fence.store(true, Ordering::SeqCst);
    let task_store = metered.clone();
    let task_marker = marker.clone();
    let context = test.request.context;
    let pending = tokio::spawn(async move {
        GcRepository::new(task_store)
            .discover_purge(context, &task_marker, 5000, GcLimits::default())
            .await
    });
    metered.entered.notified().await;
    tokio::time::advance(std::time::Duration::from_secs(15)).await;
    assert_eq!(
        TableLifecycles::new(test.store())
            .execute(&test.request)
            .await
            .unwrap()
            .status,
        204
    );
    metered.release.notify_one();
    let PurgeAdmission::Ready(task) = pending.await.unwrap().unwrap() else {
        panic!("winning tombstone");
    };
    // Creating the real GC task still checks the catalog's retirement fence.
    assert_eq!(metered.counts(), (5, 2));
    assert_eq!(task.head.as_ref(), Some(&marker.head));
    assert_eq!(task.created_ms, 20_000);
    assert_eq!(task.not_before_ms, 20_000 + GcTask::PURGE_DELAY_MS);
    assert_eq!(GcTask::PURGE_DELAY_MS, 20 * 60_000);
    let repeated = GcRepository::new(test.store())
        .admit_purge(context, &marker, 8000, GcLimits::default())
        .await
        .unwrap();
    assert_eq!(repeated, *task);
}

#[tokio::test]
async fn stale_marker_can_be_replaced_after_a_monotonic_fence_and_nonpurge_never_adopts_it() {
    for nonpurge in [false, true] {
        let mut test = interrupted().await;
        let marker = test.marker();
        let key = storage_head_key(test.before.storage_location()).encode().unwrap();
        let before = StorageRecord::TableHead(Box::new(test.before.clone()))
            .encode()
            .unwrap();
        let mut next = test.before.clone();
        next.operation_fence += 1;
        let bytes = StorageRecord::TableHead(Box::new(next)).encode().unwrap();
        test.store()
            .compare_exchange(
                &key,
                Some(&before),
                &bytes,
                mutation_identity(&key, Some(&before), &bytes),
            )
            .await
            .unwrap();
        if nonpurge {
            test.request.action = TableLifecycleAction::Drop {
                purge_requested: false,
            };
        }
        assert_eq!(
            TableLifecycles::new(test.store())
                .execute(&test.request)
                .await
                .unwrap()
                .status,
            204
        );
        if nonpurge {
            assert!(matches!(
                GcRepository::new(test.store())
                    .discover_purge(test.request.context, &marker, 5000, GcLimits::default())
                    .await
                    .unwrap(),
                PurgeAdmission::Cancelled
            ));
        } else {
            let current = test.marker();
            assert_ne!(current.head, marker.head);
            assert_eq!(current.head, test.head().await);
        }
    }
}

#[tokio::test]
async fn cancel_or_nonpurge_can_win_against_a_real_delayed_purge_cas() {
    for cancel in [false, true] {
        let test = interrupted().await;
        let store = test.store();
        let marker = test.marker();
        store.table_head_pause_before.store(true, Ordering::SeqCst);
        let task_store = store.clone();
        let task_request = test.request.clone();
        let pending =
            tokio::spawn(async move { TableLifecycles::new(task_store).execute(&task_request).await });
        store.table_head_entered.notified().await;
        if cancel {
            assert!(matches!(
                GcRepository::new(store.clone())
                    .discover_purge(test.request.context, &marker, 5000, GcLimits::default())
                    .await
                    .unwrap(),
                PurgeAdmission::Cancelled
            ));
        } else {
            let mut request = test.request.clone();
            request.action = TableLifecycleAction::Drop {
                purge_requested: false,
            };
            assert_eq!(
                TableLifecycles::new(store.clone())
                    .execute(&request)
                    .await
                    .unwrap()
                    .status,
                204
            );
        }
        store.table_head_release.notify_one();
        assert_eq!(pending.await.unwrap().unwrap().status, 409);
        assert_eq!(
            test.head().await.lifecycle,
            if cancel {
                TableLifecycle::Ready
            } else {
                TableLifecycle::Tombstone
            }
        );
        assert!(matches!(
            GcRepository::new(store.clone())
                .discover_purge(test.request.context, &marker, 6000, GcLimits::default())
                .await
                .unwrap(),
            PurgeAdmission::Cancelled
        ));
        assert!(GcRepository::new(store)
            .task(
                test.request.context.catalog,
                crowdb_access_iceberg::key::OperationId::from_bytes(test.before.table.as_bytes()).unwrap()
            )
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn changed_catalog_stops_discovery_without_mutating_the_old_marker() {
    let test = interrupted().await;
    let marker = test.marker();
    test.creation
        .fixture
        .root(
            test.request.context,
            crowdb_access_iceberg::catalog::RootState::Fencing,
        )
        .await;
    let metered = Arc::new(TestPurgeStore::new(test.store()));
    assert!(GcRepository::new(metered.clone())
        .discover_purge(test.request.context, &marker, 5000, GcLimits::default())
        .await
        .is_err());
    assert_eq!(metered.counts(), (1, 0));
    assert_eq!(test.marker(), marker);
}

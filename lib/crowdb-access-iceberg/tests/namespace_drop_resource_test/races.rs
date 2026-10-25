// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::Ordering;
use std::time::Duration;

use crowdb_access_iceberg::catalog::{CatalogError, CatalogStore, StoreError};
use crowdb_access_iceberg::namespace::{
    authority_key, name_key, NamespaceCreator, NamespaceDropper, NamespaceLifecycle, NamespaceRecovery,
    NamespaceRepository, NamespaceStore,
};
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_access_iceberg::record::StorageRecord;

use crate::resource::{creation, setup};

#[tokio::test]
async fn every_unknown_write_stops_without_confirmation_and_restart_uses_resource_facts() {
    for before in [false, true] {
        for offset in 1..=3 {
            let (fixture, store, authority, request) = setup(1).await;
            store.fail_before.store(before, Ordering::SeqCst);
            store.fail_at.store(offset, Ordering::SeqCst);
            let result = NamespaceDropper::new(store.clone())
                .drop_namespace(&request)
                .await;
            assert!(
                matches!(result, Err(CatalogError::Store(StoreError::Response))),
                "{before}/{offset}: {result:?}"
            );
            assert_eq!(store.fault_hits.load(Ordering::SeqCst), 1);
            assert_eq!(store.post_fault.load(Ordering::SeqCst), 0);
            store.faulted.store(false, Ordering::SeqCst);
            store.fail_at.store(0, Ordering::SeqCst);
            let result = NamespaceDropper::new(store.clone())
                .drop_namespace(&request)
                .await
                .unwrap();
            assert!(result.is_none() || result.is_some_and(|outcome| outcome.status == 204));
            let key = authority_key(fixture.context.catalog, authority.namespace);
            let value = fixture.store.get(&key.encode().unwrap()).await.unwrap().unwrap();
            assert!(matches!(
                StorageRecord::decode(&key, &value.bytes).unwrap(),
                StorageRecord::RetiredNamespace { .. }
            ));
            assert!(NamespaceRepository::new(fixture.store.clone())
                .load(fixture.context, &request.identifier)
                .await
                .unwrap()
                .is_none());
        }
    }
}

#[tokio::test]
async fn changed_dropping_generation_discards_the_complete_old_empty_proof() {
    let (fixture, store, _, request) = setup(1).await;
    store.terminal_pause.store(true, Ordering::SeqCst);
    let pending = {
        let store = store.clone();
        let request = request.clone();
        tokio::spawn(async move { NamespaceDropper::new(store).drop_namespace(&request).await })
    };
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .unwrap();
    let reader = NamespaceRepository::new(fixture.store.clone());
    let mut ready = reader
        .load(fixture.context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.lifecycle, NamespaceLifecycle::Dropping);
    ready.lifecycle = NamespaceLifecycle::Ready;
    ready.pending_operation = None;
    ready.admission_fence += 1;
    ready.mutation_revision += 1;
    // A rival's nonempty proof restored F2. A real creator now publishes a child.
    fixture
        .put(
            authority_key(ready.catalog, ready.namespace),
            StorageRecord::NamespaceAuthority(Box::new(ready)),
        )
        .await;
    NamespaceCreator::new(fixture.store.clone())
        .create(&creation(&fixture, &["n0", "child"]))
        .await
        .unwrap();
    let mut f3 = reader
        .load(fixture.context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    f3.lifecycle = NamespaceLifecycle::Dropping;
    f3.admission_fence += 1;
    f3.mutation_revision += 1;
    fixture
        .put(
            authority_key(f3.catalog, f3.namespace),
            StorageRecord::NamespaceAuthority(Box::new(f3.clone())),
        )
        .await;
    let before = store.counts();
    assert_eq!(before.3, 2, "D1 completed both F1 ranges before the pause");
    store.release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap()
            .status,
        409
    );
    let after = store.counts();
    assert_eq!(after.3 - before.3, 1, "F3 starts again at the namespace range");
    assert_eq!(
        after.0 - before.0,
        3,
        "child authority and two Root checks; no conflict confirmation GET"
    );
    let restored = reader
        .load(fixture.context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.lifecycle, NamespaceLifecycle::Ready);
    assert_eq!(restored.namespace, f3.namespace);
    assert_eq!(restored.admission_fence, f3.admission_fence + 1);
    assert!(reader
        .exists(fixture.context, &creation(&fixture, &["n0", "child"]).identifier)
        .await
        .unwrap());
}

#[tokio::test]
async fn a_selected_invocation_never_deletes_a_replacement_mapping() {
    let (fixture, store, original, request) = setup(1).await;
    store.delete_pause.store(true, Ordering::SeqCst);
    let pending = {
        let store = store.clone();
        let request = request.clone();
        tokio::spawn(async move { NamespaceDropper::new(store).drop_namespace(&request).await })
    };
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .unwrap();
    let key = name_key(original.catalog, original.parent, original.identifier.name())
        .unwrap()
        .encode()
        .unwrap();
    let old = fixture.store.get(&key).await.unwrap().unwrap();
    fixture
        .store
        .delete_mapping(&key, &old.bytes, mutation_identity(&key, Some(&old.bytes), &[]))
        .await
        .unwrap();
    NamespaceCreator::new(fixture.store.clone())
        .create(&creation(&fixture, &["n0"]))
        .await
        .unwrap();
    let reader = NamespaceRepository::new(fixture.store.clone());
    let replacement = reader
        .load(fixture.context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(replacement.namespace, original.namespace);
    store.release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap()
            .status,
        204
    );
    assert_eq!(
        reader.load(fixture.context, &request.identifier).await.unwrap(),
        Some(replacement)
    );
    assert_eq!(store.counts(), (6, 2, 1, 2));
}

#[tokio::test]
async fn mapping_repair_finishes_ownerless_dropping_without_a_legacy_operation() {
    for child in [false, true] {
        let (fixture, store, _, request) = setup(1).await;
        if child {
            NamespaceCreator::new(fixture.store.clone())
                .create(&creation(&fixture, &["n0", "child"]))
                .await
                .unwrap();
        }
        store.fail_at.store(1, Ordering::SeqCst);
        assert!(matches!(
            NamespaceDropper::new(store.clone())
                .drop_namespace(&request)
                .await,
            Err(CatalogError::Store(StoreError::Response))
        ));
        assert_eq!(store.post_fault.load(Ordering::SeqCst), 0);
        store.fail_at.store(0, Ordering::SeqCst);
        store.faulted.store(false, Ordering::SeqCst);
        let selected = NamespaceRepository::new(fixture.store.clone())
            .load(fixture.context, &request.identifier)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selected.lifecycle, NamespaceLifecycle::Dropping);
        assert_eq!(selected.pending_operation, None);
        let page = NamespaceRecovery::new(store.clone())
            .repair_page(fixture.context, None)
            .await
            .unwrap();
        assert!(page.failures.is_empty(), "{:?}", page.failures);
        assert_eq!(page.deferred, 0);
        let result = NamespaceRepository::new(fixture.store.clone())
            .load(fixture.context, &request.identifier)
            .await
            .unwrap();
        if child {
            let restored = result.unwrap();
            assert_eq!(restored.lifecycle, NamespaceLifecycle::Ready);
            assert_eq!(restored.pending_operation, None);
        } else {
            assert!(result.is_none());
        }
    }
}

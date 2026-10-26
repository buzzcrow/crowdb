// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/gc_blocks.rs"]
mod gc_blocks;
#[path = "common/gc_store.rs"]
mod gc_store;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace_recovery_store.rs"]
mod recovery_store;

use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogError, CatalogLifecycle, CatalogStore, RootState, StoreError},
    gc::{GcLimits, GcPhase, GcRepository, GcTask, GcWorker},
    key::{CatalogId, CatalogScope, IcebergKey, NamespaceId, OperationId},
    namespace::{
        authority_key, name_key, NamespaceCreateRequest, NamespaceCreator, NamespaceIdentifier,
        NamespaceMapping, NamespaceMappingState, NamespaceProperties, NamespacePropertyRequest,
        NamespaceRecovery, NamespaceRepository, PropertyChanges,
    },
    operation::{mutation_identity, RequestIdentity, RETRY_WINDOW_MS},
    record::StorageRecord,
};
use fixture::TestNamespace;
use std::sync::{atomic::Ordering, Arc};

fn request(fixture: &TestNamespace) -> NamespaceCreateRequest {
    NamespaceCreateRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(vec!["candidate".into()]).unwrap(),
        properties: NamespaceProperties::default(),
    }
}

fn mapping(fixture: &TestNamespace) -> NamespaceMapping {
    let key = name_key(fixture.context.catalog, None, "candidate").unwrap();
    let values = fixture.store.values.load();
    let StorageRecord::NamespaceMapping(mapping) =
        StorageRecord::decode(&key, &values[&key.encode().unwrap()].bytes).unwrap()
    else {
        panic!()
    };
    mapping
}

async fn interrupt_reservation(fixture: &TestNamespace) -> NamespaceMapping {
    fixture
        .store
        .fail_after
        .store(fixture.store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(matches!(
        NamespaceCreator::new(fixture.store.clone())
            .create(&request(fixture))
            .await,
        Err(CatalogError::Store(StoreError::Response))
    ));
    mapping(fixture)
}

#[tokio::test]
async fn absent_child_retirement_survives_lost_reply_and_rejects_late_initial_publication() {
    let fixture = TestNamespace::new().await;
    let reserved = interrupt_reservation(&fixture).await;
    let key = authority_key(fixture.context.catalog, reserved.namespace);
    let mut late = fixture.authority(None, &["candidate"]);
    late.namespace = reserved.namespace;
    let late = StorageRecord::NamespaceAuthority(Box::new(late))
        .encode()
        .unwrap();
    fixture
        .store
        .fail_after
        .store(fixture.store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    let page = NamespaceRecovery::new(fixture.store.clone())
        .repair_page(fixture.context, None)
        .await
        .unwrap();
    assert_eq!(page.failures.len(), 1);
    assert!(matches!(
        page.failures[0].1,
        CatalogError::Store(StoreError::Response)
    ));
    assert_eq!(mapping(&fixture), reserved);
    assert!(matches!(
        StorageRecord::decode(&key, &fixture.store.values.load()[&key.encode().unwrap()].bytes).unwrap(),
        StorageRecord::RetiredNamespace { .. }
    ));
    let page = NamespaceRecovery::new(fixture.store.clone())
        .repair_page(fixture.context, None)
        .await
        .unwrap();
    assert!(page.failures.is_empty());
    assert_eq!(page.completed, 1);
    let encoded = key.encode().unwrap();
    assert!(matches!(
        fixture
            .store
            .compare_exchange(&encoded, None, &late, mutation_identity(&encoded, None, &late))
            .await
            .unwrap(),
        CasOutcome::Conflict(Some(_))
    ));
    assert_eq!(
        NamespaceCreator::new(fixture.store.clone())
            .create(&request(&fixture))
            .await
            .unwrap()
            .status,
        200
    );
    assert_ne!(mapping(&fixture).namespace, reserved.namespace);
}

#[tokio::test]
async fn abandoned_parent_occupancy_retires_the_child_before_releasing_admission() {
    let fixture = TestNamespace::new().await;
    let mut parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    let child = fixture.authority(Some(parent.namespace), &["parent", "child"]);
    fixture
        .put(
            authority_key(child.catalog, child.namespace),
            StorageRecord::NamespaceAuthority(Box::new(child.clone())),
        )
        .await;
    parent.creating_child = Some(child.namespace);
    parent.mutation_revision += 1;
    fixture
        .put(
            authority_key(parent.catalog, parent.namespace),
            StorageRecord::NamespaceAuthority(Box::new(parent.clone())),
        )
        .await;
    let repository = NamespaceRepository::new(fixture.store.clone());
    repository
        .update_properties(&NamespacePropertyRequest {
            context: fixture.context,
            identifier: parent.identifier.clone(),
            changes: PropertyChanges {
                removals: Vec::new(),
                updates: [("owner".into(), "new".into())].into(),
            },
        })
        .await
        .unwrap()
        .unwrap();
    let parent = repository
        .load(fixture.context, &parent.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parent.creating_child, None);
    assert_eq!(parent.properties.entries()["owner"], "new");
    let key = authority_key(child.catalog, child.namespace);
    assert_eq!(
        StorageRecord::decode(&key, &fixture.store.values.load()[&key.encode().unwrap()].bytes).unwrap(),
        StorageRecord::RetiredNamespace {
            catalog: child.catalog,
            namespace: child.namespace
        }
    );
    let old_reserved = NamespaceMapping {
        catalog: child.catalog,
        parent: child.parent,
        namespace: child.namespace,
        name: "child".into(),
        name_epoch: 1,
        operation: OperationId::random(),
        state: NamespaceMappingState::ReservedResource,
    };
    let before = StorageRecord::NamespaceMapping(old_reserved.clone())
        .encode()
        .unwrap();
    let published = StorageRecord::NamespaceMapping(NamespaceMapping {
        state: NamespaceMappingState::Published,
        ..old_reserved
    })
    .encode()
    .unwrap();
    let key = name_key(child.catalog, child.parent, "child")
        .unwrap()
        .encode()
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .compare_exchange(
                &key,
                Some(&before),
                &published,
                mutation_identity(&key, Some(&before), &published)
            )
            .await
            .unwrap(),
        CasOutcome::Conflict(None)
    ));
    assert!(!repository
        .exists(fixture.context, &child.identifier)
        .await
        .unwrap());
}

async fn retirement(fixture: &TestNamespace) -> (GcRepository, GcTask, GcLimits) {
    let authority_key = IcebergKey::Catalog {
        catalog: fixture.context.catalog,
        scope: CatalogScope::Authority,
        suffix: Vec::new(),
    };
    let StorageRecord::Authority(mut authority) = StorageRecord::decode(
        &authority_key,
        &fixture.store.values.load()[&authority_key.encode().unwrap()].bytes,
    )
    .unwrap() else {
        panic!()
    };
    authority.lifecycle = CatalogLifecycle::Retired;
    fixture
        .put(authority_key, StorageRecord::Authority(authority))
        .await;
    fixture
        .root(
            fixture.context.replacement(CatalogId::random()).unwrap(),
            RootState::Ready,
        )
        .await;
    let limits = GcLimits {
        minimum_retention_ms: 1,
        ..GcLimits::default()
    };
    let repository = GcRepository::new(fixture.store.clone());
    let task = GcTask::plan(fixture.context, OperationId::random(), None, 1000, limits).unwrap();
    repository.create(&task).await.unwrap();
    (repository, task, limits)
}

async fn advance(repository: &GcRepository, task: GcTask, limits: GcLimits) -> GcTask {
    GcWorker::new(
        repository.clone(),
        Arc::new(gc_blocks::TestReclaimBlocks::default()),
        limits,
    )
    .unwrap()
    .step(&task, (10 * RETRY_WINDOW_MS).max(task.retry_at_ms))
    .await
    .unwrap()
}

#[tokio::test]
async fn catalog_gc_preserves_final_namespace_slots_without_create_ledgers_and_after_unknown() {
    for (has_authority, has_mapping, unknown) in [
        (false, true, false),
        (true, false, false),
        (true, true, false),
        (false, true, true),
        (true, true, true),
    ] {
        let fixture = TestNamespace::new().await;
        let child = fixture.authority(None, &["candidate"]);
        if has_mapping {
            fixture.publish(&child).await;
        } else {
            fixture
                .put(
                    authority_key(child.catalog, child.namespace),
                    StorageRecord::NamespaceAuthority(Box::new(child.clone())),
                )
                .await;
        }
        let key = authority_key(child.catalog, child.namespace);
        if !has_authority {
            let mut values = (**fixture.store.values.load()).clone();
            values.remove(&key.encode().unwrap());
            fixture.store.values.store(Arc::new(values));
        }
        let before = StorageRecord::NamespaceAuthority(Box::new(child.clone()))
            .encode()
            .unwrap();
        let (repository, mut task, limits) = retirement(&fixture).await;
        for _ in 0..100 {
            if task.phase == GcPhase::CleanupCatalog {
                break;
            }
            task = advance(&repository, task, limits).await;
        }
        assert_eq!(task.phase, GcPhase::CleanupCatalog);
        if unknown {
            fixture
                .store
                .fail_after
                .store(fixture.store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
            let result = GcWorker::new(
                repository.clone(),
                Arc::new(gc_blocks::TestReclaimBlocks::default()),
                limits,
            )
            .unwrap()
            .step(&task, 10 * RETRY_WINDOW_MS)
            .await;
            assert!(result.is_err());
        }
        for _ in 0..100 {
            if task.phase == GcPhase::Complete {
                break;
            }
            task = advance(&repository, task, limits).await;
        }
        assert_eq!(task.phase, GcPhase::Complete);
        let encoded = key.encode().unwrap();
        assert_eq!(
            StorageRecord::decode(&key, &fixture.store.values.load()[&encoded].bytes).unwrap(),
            StorageRecord::RetiredNamespace {
                catalog: child.catalog,
                namespace: child.namespace
            }
        );
        assert!(matches!(
            fixture
                .store
                .compare_exchange(
                    &encoded,
                    None,
                    &before,
                    mutation_identity(&encoded, None, &before)
                )
                .await
                .unwrap(),
            CasOutcome::Conflict(Some(_))
        ));
        let other = authority_key(child.catalog, NamespaceId::random())
            .encode()
            .unwrap();
        assert!(fixture.store.get(&other).await.unwrap().is_none());
    }
}

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/table_creation.rs"]
mod creation;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use std::sync::{atomic::Ordering, Arc};

use creation::TestCreation;
use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogError},
    commit::CommitPublicationError,
    key::{CatalogScope, IcebergKey, OperationId},
    namespace::{NamespaceDropper, NamespaceRepository},
    record::StorageRecord,
    table::TableRepository,
};

#[tokio::test]
async fn namespace_drop_resolves_each_interrupted_create_from_final_resource_facts() {
    for boundary in 1..=4 {
        let test = TestCreation::new().await;
        let store = test.fixture.store.clone();
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + boundary, Ordering::SeqCst);
        assert!(test.creator().create(&test.request).await.is_err());
        store.fail_after.store(0, Ordering::SeqCst);
        let outcome = NamespaceDropper::new(store.clone())
            .drop_namespace(&test.drop_request())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome.status,
            if boundary < 3 { 204 } else { 409 },
            "boundary {boundary}"
        );
        let parent = NamespaceRepository::new(store.clone())
            .load(test.fixture.context, &test.parent.identifier)
            .await
            .unwrap();
        let selected = TableRepository::new(store.clone())
            .select(test.fixture.context, test.parent.namespace, "events")
            .await
            .unwrap();
        if boundary < 3 {
            assert!(parent.is_none());
            assert!(selected.is_none());
            assert!(matches!(
                test.creator().create(&test.request).await,
                Err(CommitPublicationError::NamespaceMissing)
            ));
        } else {
            assert_eq!(parent.unwrap().pending_operation, None);
            assert_eq!(selected.unwrap().head.pending_operation, None);
            assert!(matches!(
                test.creator().create(&test.request).await,
                Err(CommitPublicationError::TableExists)
            ));
        }
    }
}

#[tokio::test]
async fn namespace_drop_and_initial_head_cas_compete_without_a_parent_create_lock() {
    for native in [false, true] {
        for head_wins in [false, true] {
            let test = TestCreation::new().await;
            if native {
                test.fixture
                    .put(
                        IcebergKey::Catalog {
                            catalog: test.fixture.context.catalog,
                            scope: CatalogScope::Authority,
                            suffix: Vec::new(),
                        },
                        StorageRecord::Authority(
                            CatalogAuthority::new_native(test.fixture.context.catalog, "native".into())
                                .unwrap(),
                        ),
                    )
                    .await;
            }
            let store = test.fixture.store.clone();
            if head_wins {
                store.table_head_pause_after.store(true, Ordering::SeqCst);
            } else {
                store.table_head_pause_before.store(true, Ordering::SeqCst);
            }
            let creator = test.creator();
            let request = test.request.clone();
            let create = tokio::spawn(async move { creator.create(&request).await });
            tokio::time::timeout(
                std::time::Duration::from_secs(3),
                store.table_head_entered.notified(),
            )
            .await
            .unwrap();
            let drop = NamespaceDropper::new(store.clone())
                .drop_namespace(&test.drop_request())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(drop.status, if head_wins { 409 } else { 204 });
            let reads = store.reads.load(Ordering::SeqCst);
            store.table_head_release.notify_one();
            let outcome = create.await.unwrap();
            assert_eq!(store.reads.load(Ordering::SeqCst) - reads, usize::from(head_wins));
            if head_wins {
                assert_eq!(outcome.unwrap().status, 200);
            } else {
                assert!(matches!(
                    outcome,
                    Err(CommitPublicationError::Catalog(CatalogError::Conflict))
                ));
            }
            let selected = TableRepository::new(store.clone())
                .select(test.fixture.context, test.parent.namespace, "events")
                .await
                .unwrap();
            assert_eq!(selected.is_some(), head_wins);
            let parent = NamespaceRepository::new(store)
                .load(test.fixture.context, &test.parent.identifier)
                .await
                .unwrap();
            assert_eq!(parent.is_some(), head_wins);
        }
    }
}

#[tokio::test]
async fn competing_same_name_creates_have_one_final_winner_without_process_records() {
    let mut fixture = fixture::TestNamespace::new().await;
    let store = common::TestStore {
        table_reservation_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..Default::default()
    };
    store.values.store(fixture.store.values.load_full());
    fixture.store = Arc::new(store);
    let test = TestCreation::with_fixture(fixture).await;
    let creator = test.creator();
    let mut second = test.request.clone();
    second.identity.operation = OperationId::random();
    let (first, second) = tokio::join!(creator.create(&test.request), creator.create(&second));
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let loser = if first.is_err() { first } else { second };
    assert!(matches!(
        loser,
        Err(CommitPublicationError::TableExists | CommitPublicationError::Catalog(CatalogError::Busy))
    ));
    let selected = TableRepository::new(test.fixture.store.clone())
        .select(test.fixture.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.head.pending_operation, None);
    assert_eq!(selected.head.generation, 1);
}

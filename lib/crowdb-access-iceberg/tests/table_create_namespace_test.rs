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
    catalog::{CatalogError, CatalogStore},
    commit::{CommitPublicationError, TableCreateJournal, TableCreatePhase},
    key::OperationId,
    namespace::{authority_key, NamespaceDropper, NamespaceLifecycle, NamespaceRepository},
    record::StorageRecord,
    table::{name_key, TableRepository},
};

#[tokio::test]
async fn namespace_drop_recovers_each_interrupted_create_boundary() {
    let baseline = TestCreation::new().await;
    let before = baseline.fixture.store.writes.load(Ordering::SeqCst);
    baseline.creator().create(&baseline.request).await.unwrap();
    let writes = baseline.fixture.store.writes.load(Ordering::SeqCst) - before;
    for offset in 1..=writes {
        let test = TestCreation::new().await;
        let store = test.fixture.store.clone();
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + offset, Ordering::SeqCst);
        assert!(test.creator().create(&test.request).await.is_err());
        store.fail_after.store(0, Ordering::SeqCst);
        let interrupted = TableCreateJournal::new(store.clone())
            .load(test.fixture.context, test.request.identity.operation)
            .await
            .unwrap();
        let drop = NamespaceDropper::new(store.clone())
            .drop_namespace(&test.drop_request())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "offset {offset}, phase {:?}: {error:?}",
                    interrupted.as_ref().map(|operation| operation.phase)
                )
            })
            .unwrap();
        let recovered = test.creator().create(&test.request).await;
        let selected = TableRepository::new(store.clone())
            .select(test.fixture.context, test.parent.namespace, "events")
            .await
            .unwrap();
        match drop.status {
            204 => {
                match recovered {
                    Ok(result) => assert_eq!(result.status, 404, "offset {offset}"),
                    Err(CommitPublicationError::NamespaceMissing) => {}
                    result => panic!("unexpected recovery at {offset}: {result:?}"),
                }
                assert!(selected.is_none(), "offset {offset}");
                let key = name_key(test.fixture.context.catalog, test.parent.namespace, "events").unwrap();
                assert!(store.get(&key.encode().unwrap()).await.unwrap().is_none());
            }
            409 => {
                assert_eq!(recovered.unwrap().status, 200, "offset {offset}");
                assert!(selected.is_some());
                let parent = NamespaceRepository::new(store.clone())
                    .load(test.fixture.context, &test.parent.identifier)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(parent.pending_operation, None);
            }
            status => panic!("unexpected drop status {status} at {offset}"),
        }
    }
}

#[tokio::test]
async fn actual_parent_admission_races_drop_without_exposing_a_child_under_tombstone() {
    let mut fixture = fixture::TestNamespace::new().await;
    let store = common::TestStore {
        namespace_update_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..Default::default()
    };
    store.values.store(fixture.store.values.load_full());
    fixture.store = Arc::new(store);
    let test = TestCreation::with_fixture(fixture).await;
    let creator = test.creator();
    let dropper = NamespaceDropper::new(test.fixture.store.clone());
    let request = test.drop_request();
    let (create, drop) = tokio::join!(creator.create(&test.request), dropper.drop_namespace(&request));
    let create = match create {
        Err(CommitPublicationError::Catalog(CatalogError::Busy)) => {
            creator.create(&test.request).await.unwrap()
        }
        result => result.unwrap(),
    };
    let drop = match drop {
        Err(CatalogError::Busy) => dropper.drop_namespace(&request).await.unwrap().unwrap(),
        result => result.unwrap().unwrap(),
    };
    assert!(matches!((create.status, drop.status), (200, 409) | (404, 204)));
    let key = authority_key(test.fixture.context.catalog, test.parent.namespace);
    let bytes = test
        .fixture
        .store
        .get(&key.encode().unwrap())
        .await
        .unwrap()
        .unwrap()
        .bytes;
    let StorageRecord::NamespaceAuthority(parent) = StorageRecord::decode(&key, &bytes).unwrap() else {
        panic!("namespace authority")
    };
    let table = TableRepository::new(test.fixture.store.clone())
        .select(test.fixture.context, test.parent.namespace, "events")
        .await
        .unwrap();
    assert_eq!(table.is_none(), parent.lifecycle == NamespaceLifecycle::Tombstone);
}

#[tokio::test]
async fn competing_same_name_creates_retain_exact_terminal_winner_and_loser() {
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
    let (first, second_result) = tokio::join!(creator.create(&test.request), creator.create(&second));
    let first = match first {
        Err(CommitPublicationError::Catalog(CatalogError::Busy)) => {
            creator.create(&test.request).await.unwrap()
        }
        result => result.unwrap(),
    };
    let second_result = match second_result {
        Err(CommitPublicationError::Catalog(CatalogError::Busy)) => creator.create(&second).await.unwrap(),
        result => result.unwrap(),
    };
    assert!(matches!(
        (first.status, second_result.status),
        (200, 409) | (409, 200)
    ));
    let journal = TableCreateJournal::new(test.fixture.store.clone());
    let first_operation = journal
        .load(test.fixture.context, test.request.identity.operation)
        .await
        .unwrap()
        .unwrap();
    let second_operation = journal
        .load(test.fixture.context, second.identity.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first_operation.phase,
        if first.status == 200 {
            TableCreatePhase::Complete
        } else {
            TableCreatePhase::Aborted
        }
    );
    assert_eq!(
        second_operation.phase,
        if second_result.status == 200 {
            TableCreatePhase::Complete
        } else {
            TableCreatePhase::Aborted
        }
    );
    assert_eq!(creator.create(&test.request).await.unwrap(), first);
    assert_eq!(creator.create(&second).await.unwrap(), second_result);
}

#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::catalog::{CatalogError, CatalogStore};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::namespace::{
    name_key, NamespaceAuthority, NamespaceCreateRequest, NamespaceCreator, NamespaceDropRequest,
    NamespaceDropper, NamespaceIdentifier, NamespaceProperties, NamespaceRepository,
};
use crowdb_access_iceberg::operation::RequestIdentity;
use fixture::TestNamespace;

fn request(fixture: &TestNamespace, name: &str) -> NamespaceCreateRequest {
    NamespaceCreateRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(vec!["parent".into(), name.into()]).unwrap(),
        properties: NamespaceProperties::default(),
    }
}

async fn setup() -> (TestNamespace, NamespaceAuthority) {
    let fixture = TestNamespace::new().await;
    let parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    (fixture, parent)
}

async fn settle(creator: &NamespaceCreator, request: &NamespaceCreateRequest) -> u16 {
    for _ in 0..4 {
        match creator.create(request).await {
            Ok(outcome) => return outcome.status,
            Err(CatalogError::Busy) => {}
            error => panic!("unexpected create result: {error:?}"),
        }
    }
    panic!("bounded create recovery made no progress");
}

#[tokio::test]
async fn different_children_contend_on_admission_without_losing_either_creation() {
    let (mut fixture, parent) = setup().await;
    fixture.store = Arc::new(common::TestStore {
        values: arc_swap::ArcSwap::from(fixture.store.values.load_full()),
        namespace_update_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..Default::default()
    });
    let first = request(&fixture, "first");
    let second = request(&fixture, "second");
    let creator = NamespaceCreator::new(fixture.store.clone());
    let results = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(creator.create(&first), creator.create(&second))
    })
    .await
    .unwrap();
    for result in [results.0, results.1] {
        match result {
            Ok(outcome) => assert_eq!(outcome.status, 200),
            Err(CatalogError::Busy) => {}
            other => panic!("unexpected concurrent result: {other:?}"),
        }
    }
    let reader = NamespaceRepository::new(fixture.store.clone());
    for request in [&first, &second] {
        assert_eq!(settle(&creator, request).await, 409);
        assert!(reader.exists(fixture.context, &request.identifier).await.unwrap());
    }
    let selected = reader
        .load(fixture.context, &parent.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.pending_operation, None);
    assert_eq!(selected.admission_fence, parent.admission_fence);
    assert_eq!(selected.property_revision, parent.property_revision);
}

#[tokio::test]
async fn competing_name_reservations_publish_one_namespace_and_one_conflict() {
    let (mut fixture, _) = setup().await;
    fixture.store = Arc::new(common::TestStore {
        values: arc_swap::ArcSwap::from(fixture.store.values.load_full()),
        namespace_reservation_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..Default::default()
    });
    let first = request(&fixture, "same");
    let second = request(&fixture, "same");
    let creator = NamespaceCreator::new(fixture.store.clone());
    let results = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(creator.create(&first), creator.create(&second))
    })
    .await
    .unwrap();
    for result in [results.0, results.1] {
        match result {
            Ok(outcome) => assert!(matches!(outcome.status, 200 | 409)),
            Err(CatalogError::Busy | CatalogError::Conflict) => {}
            other => panic!("unexpected reservation result: {other:?}"),
        }
    }
    let mut statuses = [settle(&creator, &first).await, settle(&creator, &second).await];
    statuses.sort_unstable();
    assert_eq!(statuses, [409, 409]);
    assert!(NamespaceRepository::new(fixture.store.clone())
        .exists(fixture.context, &first.identifier)
        .await
        .unwrap());
}

async fn interrupted_admission(applied: bool) -> (TestNamespace, NamespaceAuthority, NamespaceCreateRequest) {
    let (fixture, parent) = setup().await;
    let request = request(&fixture, "child");
    fixture.store.fail_after.store(
        fixture.store.writes.load(Ordering::SeqCst) + if applied { 3 } else { 2 },
        Ordering::SeqCst,
    );
    assert!(NamespaceCreator::new(fixture.store.clone())
        .create(&request)
        .await
        .is_err());
    let selected = NamespaceRepository::new(fixture.store.clone())
        .load(fixture.context, &parent.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.creating_child.is_some(), applied);
    (fixture, selected, request)
}

#[tokio::test]
async fn drop_fence_before_admission_aborts_and_cleanup_preserves_a_recreated_name() {
    let (fixture, mut parent, request) = interrupted_admission(false).await;
    let reservation_key = name_key(fixture.context.catalog, Some(parent.namespace), "child").unwrap();
    assert!(fixture
        .store
        .get(&reservation_key.encode().unwrap())
        .await
        .unwrap()
        .is_some());
    let drop = NamespaceDropRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: parent.identifier.clone(),
    };
    assert_eq!(
        NamespaceDropper::new(fixture.store.clone())
            .drop_namespace(&drop)
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
    assert!(fixture
        .store
        .get(&reservation_key.encode().unwrap())
        .await
        .unwrap()
        .is_none());
    let creator = NamespaceCreator::new(fixture.store.clone());
    assert!(matches!(
        creator.create(&request).await,
        Err(CatalogError::Invalid(_))
    ));
    parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    let replacement = fixture.authority(Some(parent.namespace), &["parent", "child"]);
    fixture.publish(&replacement).await;
    assert_eq!(creator.create(&request).await.unwrap().status, 409);
    assert_eq!(
        NamespaceRepository::new(fixture.store.clone())
            .load(fixture.context, &request.identifier)
            .await
            .unwrap(),
        Some(replacement)
    );
}

#[tokio::test]
async fn uncertain_admission_is_helped_before_the_next_parent_writer() {
    let (fixture, parent, original) = interrupted_admission(true).await;
    assert!(parent.creating_child.is_some());
    let next = request(&fixture, "other-child");
    let creator = NamespaceCreator::new(fixture.store.clone());
    assert_eq!(settle(&creator, &next).await, 200);
    assert_eq!(settle(&creator, &original).await, 409);
    let reader = NamespaceRepository::new(fixture.store.clone());
    for request in [&original, &next] {
        assert!(reader.exists(fixture.context, &request.identifier).await.unwrap());
    }
    assert_eq!(
        reader
            .load(fixture.context, &parent.identifier)
            .await
            .unwrap()
            .unwrap()
            .pending_operation,
        None
    );
}

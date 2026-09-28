#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace_recovery_store.rs"]
mod recovery_store;

use crowdb_access_iceberg::catalog::CatalogStore;
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::namespace::{name_key, NamespaceMappingState, NamespaceRecovery};
use crowdb_access_iceberg::record::StorageRecord;
use fixture::TestNamespace;

#[tokio::test]
async fn repair_removes_stale_mappings_but_preserves_live_and_unresolved_reservations() {
    let fixture = TestNamespace::new().await;
    let live = fixture.authority(None, &["live"]);
    fixture.publish(&live).await;
    let stale = fixture.authority(None, &["stale"]);
    let mut mapping = fixture.publish(&stale).await;
    mapping.name_epoch += 1;
    let stale_key = name_key(mapping.catalog, mapping.parent, &mapping.name).unwrap();
    fixture
        .put(stale_key.clone(), StorageRecord::NamespaceMapping(mapping))
        .await;
    let reserved = fixture.authority(None, &["reserved"]);
    let mut mapping = fixture.publish(&reserved).await;
    mapping.state = NamespaceMappingState::Reserved;
    mapping.operation = OperationId::random();
    let reserved_key = name_key(mapping.catalog, mapping.parent, &mapping.name).unwrap();
    fixture
        .put(reserved_key.clone(), StorageRecord::NamespaceMapping(mapping))
        .await;
    let page = NamespaceRecovery::new(fixture.store.clone())
        .repair_page(fixture.context, None)
        .await
        .unwrap();
    assert_eq!(page.completed, 2);
    assert_eq!(page.failures.len(), 1);
    assert!(page.continuation.is_none());
    assert!(fixture
        .store
        .get(&stale_key.encode().unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(fixture
        .store
        .get(&reserved_key.encode().unwrap())
        .await
        .unwrap()
        .is_some());
    assert!(fixture
        .store
        .get(
            &name_key(fixture.context.catalog, None, "live")
                .unwrap()
                .encode()
                .unwrap()
        )
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn mapping_repair_uses_bounded_pages_and_rejects_corrupt_records() {
    let fixture = TestNamespace::new().await;
    for index in 0..9 {
        let authority = fixture.authority(None, &[&format!("name-{index}")]);
        fixture.publish(&authority).await;
    }
    let recovery = NamespaceRecovery::new(fixture.store.clone());
    let mut cursor = None;
    let mut visited = 0;
    loop {
        let page = recovery.repair_page(fixture.context, cursor).await.unwrap();
        assert!(page.completed <= 4);
        assert!(page.failures.is_empty());
        visited += page.completed;
        cursor = page.continuation;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(visited, 9);
    let key = name_key(fixture.context.catalog, None, "bad").unwrap();
    fixture.bytes(key.clone(), b"corrupt").await;
    assert!(recovery.repair_page(fixture.context, None).await.is_err());
    assert_eq!(
        fixture
            .store
            .get(&key.encode().unwrap())
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"corrupt"
    );
}

#[tokio::test]
async fn mapping_repair_finishes_a_durable_reservation_instead_of_deleting_it() {
    use crowdb_access_iceberg::namespace::{
        NamespaceCreateRequest, NamespaceCreator, NamespaceIdentifier, NamespaceProperties,
        NamespaceRepository,
    };
    use crowdb_access_iceberg::operation::RequestIdentity;
    use std::sync::atomic::Ordering;
    let fixture = TestNamespace::new().await;
    let parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    let request = NamespaceCreateRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(vec!["parent".into(), "child".into()]).unwrap(),
        properties: NamespaceProperties::default(),
    };
    fixture
        .store
        .fail_after
        .store(fixture.store.writes.load(Ordering::SeqCst) + 4, Ordering::SeqCst);
    let creator = NamespaceCreator::new(fixture.store.clone());
    assert!(creator.create(&request).await.is_err());
    let page = NamespaceRecovery::new(fixture.store.clone())
        .repair_page(fixture.context, None)
        .await
        .unwrap();
    assert!(page.failures.is_empty());
    assert_eq!(page.deferred, 0);
    assert!(NamespaceRepository::new(fixture.store.clone())
        .exists(fixture.context, &request.identifier)
        .await
        .unwrap());
    assert_eq!(creator.create(&request).await.unwrap().status, 200);
}

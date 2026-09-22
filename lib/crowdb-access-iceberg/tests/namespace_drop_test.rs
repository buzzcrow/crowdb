#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::catalog::{CatalogError, CatalogStore};
use crowdb_access_iceberg::key::{CatalogScope, IcebergKey, NameSuffix, NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    authority_key, name_key, NamespaceAuthority, NamespaceCreateRequest, NamespaceCreator,
    NamespaceDropRequest, NamespaceDropper, NamespaceIdentifier, NamespaceLifecycle, NamespaceMapping,
    NamespaceMappingState, NamespaceProperties, NamespaceRepository,
};
use crowdb_access_iceberg::operation::RequestIdentity;
use crowdb_access_iceberg::record::StorageRecord;
use fixture::TestNamespace;

fn create_request(fixture: &TestNamespace, names: &[&str]) -> NamespaceCreateRequest {
    NamespaceCreateRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(names.iter().map(|name| (*name).into()).collect()).unwrap(),
        properties: NamespaceProperties::default(),
    }
}

fn drop_request(fixture: &TestNamespace, identifier: NamespaceIdentifier) -> NamespaceDropRequest {
    NamespaceDropRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier,
    }
}

async fn setup(child: bool) -> (TestNamespace, NamespaceAuthority, NamespaceDropRequest) {
    let fixture = TestNamespace::new().await;
    let creator = NamespaceCreator::new(fixture.store.clone());
    let request = create_request(&fixture, &["parent"]);
    creator.create(&request).await.unwrap();
    if child {
        creator
            .create(&create_request(&fixture, &["parent", "child"]))
            .await
            .unwrap();
    }
    let authority = NamespaceRepository::new(fixture.store.clone())
        .load(fixture.context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    let request = drop_request(&fixture, request.identifier);
    (fixture, authority, request)
}

#[tokio::test]
async fn empty_drop_tombstones_authority_and_replay_never_deletes_a_recreated_name() {
    let (fixture, authority, request) = setup(false).await;
    let dropper = NamespaceDropper::new(fixture.store.clone());
    let outcome = dropper.drop_namespace(&request).await.unwrap().unwrap();
    assert_eq!(outcome.status, 204);
    assert!(NamespaceRepository::new(fixture.store.clone())
        .load(fixture.context, &request.identifier)
        .await
        .unwrap()
        .is_none());
    let key = authority_key(fixture.context.catalog, authority.namespace);
    let value = fixture.store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::NamespaceAuthority(tombstone) = StorageRecord::decode(&key, &value.bytes).unwrap()
    else {
        panic!("namespace authority");
    };
    assert_eq!(tombstone.lifecycle, NamespaceLifecycle::Tombstone);
    NamespaceCreator::new(fixture.store.clone())
        .create(&create_request(&fixture, &["parent"]))
        .await
        .unwrap();
    let reader = NamespaceRepository::new(fixture.store.clone());
    let replacement = reader
        .load(fixture.context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(replacement.namespace, authority.namespace);
    assert_eq!(dropper.drop_namespace(&request).await.unwrap(), Some(outcome));
    assert_eq!(
        reader.load(fixture.context, &request.identifier).await.unwrap(),
        Some(replacement)
    );
}

#[tokio::test]
async fn nonempty_drop_restores_ready_without_changing_name_or_properties() {
    let (fixture, authority, request) = setup(true).await;
    let dropper = NamespaceDropper::new(fixture.store.clone());
    let outcome = dropper.drop_namespace(&request).await.unwrap().unwrap();
    assert_eq!(outcome.status, 409);
    let reader = NamespaceRepository::new(fixture.store.clone());
    let restored = reader
        .load(fixture.context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.lifecycle, NamespaceLifecycle::Ready);
    assert_eq!(restored.pending_operation, None);
    assert_eq!(restored.property_revision, authority.property_revision);
    assert_eq!(restored.name_epoch, authority.name_epoch);
    assert_eq!(restored.admission_fence, authority.admission_fence + 2);
    assert!(reader
        .exists(
            fixture.context,
            &NamespaceIdentifier::new(vec!["parent".into(), "child".into()]).unwrap()
        )
        .await
        .unwrap());
    assert_eq!(dropper.drop_namespace(&request).await.unwrap(), Some(outcome));
}

#[tokio::test]
async fn every_lost_drop_write_recovers_empty_and_nonempty_outcomes() {
    for child in [false, true] {
        let (baseline, _, request) = setup(child).await;
        let before = baseline.store.writes.load(Ordering::SeqCst);
        NamespaceDropper::new(baseline.store.clone())
            .drop_namespace(&request)
            .await
            .unwrap();
        let writes = baseline.store.writes.load(Ordering::SeqCst) - before;
        assert!(writes > 8);
        for offset in 1..=writes {
            let (fixture, _, request) = setup(child).await;
            fixture.store.fail_after.store(
                fixture.store.writes.load(Ordering::SeqCst) + offset,
                Ordering::SeqCst,
            );
            assert!(
                NamespaceDropper::new(fixture.store.clone())
                    .drop_namespace(&request)
                    .await
                    .is_err(),
                "child {child}, offset {offset}"
            );
            let restarted = NamespaceDropper::new(fixture.store.clone());
            let outcome = restarted.drop_namespace(&request).await.unwrap().unwrap();
            assert_eq!(outcome.status, if child { 409 } else { 204 });
            assert_eq!(restarted.drop_namespace(&request).await.unwrap(), Some(outcome));
        }
    }
}

#[tokio::test]
async fn corruption_in_either_child_range_is_not_an_empty_proof() {
    for scope in [CatalogScope::NamespaceName, CatalogScope::TableName] {
        let (fixture, authority, request) = setup(false).await;
        let key = IcebergKey::Catalog {
            catalog: fixture.context.catalog,
            scope,
            suffix: NameSuffix {
                parent: Some(authority.namespace),
                name: "corrupt",
            }
            .encode()
            .unwrap(),
        };
        fixture.bytes(key, b"invalid-record").await;
        assert!(matches!(
            NamespaceDropper::new(fixture.store.clone())
                .drop_namespace(&request)
                .await,
            Err(CatalogError::Invalid(_))
        ));
        let selected = NamespaceRepository::new(fixture.store.clone())
            .load(fixture.context, &request.identifier)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selected.lifecycle, NamespaceLifecycle::Dropping);
    }
}

#[tokio::test]
async fn stale_pages_and_exhausted_work_never_hide_a_later_live_child() {
    let (fixture, authority, request) = setup(false).await;
    for index in 0..260 {
        let mapping = NamespaceMapping {
            catalog: fixture.context.catalog,
            parent: Some(authority.namespace),
            name: format!("{index:06}"),
            namespace: NamespaceId::random(),
            name_epoch: 1,
            operation: OperationId::random(),
            state: NamespaceMappingState::Published,
        };
        fixture
            .put(
                name_key(mapping.catalog, mapping.parent, &mapping.name).unwrap(),
                StorageRecord::NamespaceMapping(mapping),
            )
            .await;
    }
    fixture
        .publish(&fixture.authority(Some(authority.namespace), &["parent", "zzzzzz"]))
        .await;
    let dropper = NamespaceDropper::new(fixture.store.clone());
    assert!(matches!(
        dropper.drop_namespace(&request).await,
        Err(CatalogError::Busy)
    ));
    assert_eq!(
        NamespaceRepository::new(fixture.store.clone())
            .load(fixture.context, &request.identifier)
            .await
            .unwrap()
            .unwrap()
            .lifecycle,
        NamespaceLifecycle::Dropping
    );
    assert_eq!(
        dropper.drop_namespace(&request).await.unwrap().unwrap().status,
        409
    );
}

#[tokio::test]
async fn creator_and_empty_drop_share_one_admission_boundary() {
    let (mut fixture, _, request) = setup(false).await;
    fixture.store = Arc::new(common::TestStore {
        values: arc_swap::ArcSwap::from(fixture.store.values.load_full()),
        namespace_update_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..Default::default()
    });
    let child = create_request(&fixture, &["parent", "child"]);
    let creator = NamespaceCreator::new(fixture.store.clone());
    let dropper = NamespaceDropper::new(fixture.store.clone());
    let results = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(creator.create(&child), dropper.drop_namespace(&request))
    })
    .await
    .unwrap();
    assert!(results.0.is_ok() || matches!(results.0, Err(CatalogError::Busy)));
    assert!(results.1.is_ok() || matches!(results.1, Err(CatalogError::Busy)));
    let created = creator.create(&child).await.unwrap().status;
    let drop_status = dropper.drop_namespace(&request).await.unwrap().unwrap().status;
    assert!(matches!((created, drop_status), (200, 409) | (400, 204)));
    let reader = NamespaceRepository::new(fixture.store.clone());
    assert_eq!(
        reader.exists(fixture.context, &child.identifier).await.unwrap(),
        created == 200
    );
    assert_eq!(
        reader.exists(fixture.context, &request.identifier).await.unwrap(),
        drop_status == 409
    );
}

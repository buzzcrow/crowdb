#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use crowdb_access_iceberg::catalog::CatalogError;
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::namespace::{
    NamespaceCreateRequest, NamespaceCreator, NamespaceIdentifier, NamespaceProperties, NamespaceRepository,
};
use crowdb_access_iceberg::operation::{PayloadStore, RequestIdentity};
use fixture::TestNamespace;

fn request(fixture: &TestNamespace, names: &[&str]) -> NamespaceCreateRequest {
    NamespaceCreateRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(names.iter().map(|name| (*name).into()).collect()).unwrap(),
        properties: NamespaceProperties::new(BTreeMap::from([("owner".into(), "数据库".into())])).unwrap(),
    }
}

#[tokio::test]
async fn native_create_publishes_top_level_and_nested_names_with_stable_replay() {
    let fixture = TestNamespace::new().await;
    let creator = NamespaceCreator::new(fixture.store.clone());
    let reader = NamespaceRepository::new(fixture.store.clone());
    for names in [&["parent"][..], &["parent", "child"][..]] {
        let request = request(&fixture, names);
        let outcome = creator.create(&request).await.unwrap();
        assert_eq!(outcome.status, 200);
        let body = PayloadStore::new(fixture.store.clone())
            .get(&outcome.body)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({
                "namespace": names, "properties": {"owner": "数据库"},
            })
        );
        let authority = reader
            .load(fixture.context, &request.identifier)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(authority.properties, request.properties);
        assert_eq!(authority.pending_operation, None);
        assert_eq!(authority.name_epoch, 1);
        assert_eq!(authority.property_revision, 1);
        assert_eq!(authority.admission_fence, 1);
        assert_eq!(creator.create(&request).await.unwrap(), outcome);
        assert_eq!(
            reader.load(fixture.context, &request.identifier).await.unwrap(),
            Some(authority)
        );
    }
    let parent = reader
        .load(
            fixture.context,
            &NamespaceIdentifier::new(vec!["parent".into()]).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parent.pending_operation, None);
    assert_eq!(parent.property_revision, 1);
    assert_eq!(parent.admission_fence, 1);
    assert_eq!(parent.mutation_revision, 4);
}

#[tokio::test]
async fn missing_parent_is_rejected_before_payload_or_reservation_writes() {
    let fixture = TestNamespace::new().await;
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert!(matches!(
        NamespaceCreator::new(fixture.store.clone())
            .create(&request(&fixture, &["missing", "child"]))
            .await,
        Err(CatalogError::Invalid(_))
    ));
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
}

#[tokio::test]
async fn same_name_has_one_identity_and_duplicate_request_has_a_durable_conflict() {
    let fixture = TestNamespace::new().await;
    let creator = NamespaceCreator::new(fixture.store.clone());
    let first = request(&fixture, &["namespace"]);
    let duplicate = request(&fixture, &["namespace"]);
    assert_eq!(creator.create(&first).await.unwrap().status, 200);
    let conflict = creator.create(&duplicate).await.unwrap();
    assert_eq!(conflict.status, 409);
    assert_eq!(creator.create(&duplicate).await.unwrap(), conflict);
    assert_eq!(creator.create(&first).await.unwrap().status, 200);
    let mut changed = first;
    changed.principal = "other-writer".into();
    assert!(matches!(
        creator.create(&changed).await,
        Err(CatalogError::Conflict)
    ));
}

#[tokio::test]
async fn every_lost_create_write_recovers_on_another_instance_without_new_identity() {
    for nested in [false, true] {
        let baseline = TestNamespace::new().await;
        if nested {
            baseline.publish(&baseline.authority(None, &["parent"])).await;
        }
        let names = if nested {
            vec!["parent", "child"]
        } else {
            vec!["child"]
        };
        let before = baseline.store.writes.load(Ordering::SeqCst);
        NamespaceCreator::new(baseline.store.clone())
            .create(&request(&baseline, &names))
            .await
            .unwrap();
        let writes = baseline.store.writes.load(Ordering::SeqCst) - before;
        assert!(writes > 12);
        for offset in 1..=writes {
            let fixture = TestNamespace::new().await;
            if nested {
                fixture.publish(&fixture.authority(None, &["parent"])).await;
            }
            let request = request(&fixture, &names);
            fixture.store.fail_after.store(
                fixture.store.writes.load(Ordering::SeqCst) + offset,
                Ordering::SeqCst,
            );
            assert!(
                NamespaceCreator::new(fixture.store.clone())
                    .create(&request)
                    .await
                    .is_err(),
                "nested {nested} offset {offset}"
            );
            let restarted = NamespaceCreator::new(fixture.store.clone());
            let outcome = restarted.create(&request).await.unwrap();
            assert_eq!(outcome.status, 200, "nested {nested} offset {offset}");
            let reader = NamespaceRepository::new(fixture.store.clone());
            let selected = reader
                .load(fixture.context, &request.identifier)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(selected.pending_operation, None);
            assert_eq!(selected.property_revision, 1);
            assert_eq!(restarted.create(&request).await.unwrap(), outcome);
            assert_eq!(
                reader.load(fixture.context, &request.identifier).await.unwrap(),
                Some(selected)
            );
        }
    }
}

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
use crowdb_access_iceberg::operation::RequestIdentity;
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
async fn simultaneous_creators_publish_one_complete_namespace() {
    let mut fixture = TestNamespace::new().await;
    fixture.store = std::sync::Arc::new(common::TestStore {
        values: arc_swap::ArcSwap::from(fixture.store.values.load_full()),
        yield_operations: true,
        ..Default::default()
    });
    let request = request(&fixture, &["reference"]);
    let creators = std::array::from_fn::<_, 3, _>(|_| NamespaceCreator::new(fixture.store.clone()));
    let (first, second, third) = tokio::join!(
        creators[0].create(&request),
        creators[1].create(&request),
        creators[2].create(&request)
    );
    let mut statuses = [first, second, third].map(|result| match result {
        Ok(outcome) => outcome.status,
        Err(CatalogError::Conflict) => 409,
        Err(error) => panic!("namespace creation failed: {error:?}"),
    });
    statuses.sort_unstable();
    assert_eq!(statuses, [200, 409, 409]);
    let selected = NamespaceRepository::new(fixture.store.clone())
        .load(fixture.context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.properties, request.properties);
    assert_eq!(selected.pending_operation, None);
}

#[tokio::test]
async fn native_create_publishes_complete_resources_and_retries_existing_names_as_conflicts() {
    let fixture = TestNamespace::new().await;
    let creator = NamespaceCreator::new(fixture.store.clone());
    let reader = NamespaceRepository::new(fixture.store.clone());
    for names in [&["parent"][..], &["parent", "child"][..]] {
        let request = request(&fixture, names);
        let outcome = creator.create(&request).await.unwrap();
        assert_eq!(outcome.status, 200);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&outcome.body).unwrap(),
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
        assert_eq!(authority.creating_child, None);
        assert_eq!(authority.name_epoch, 1);
        assert_eq!(authority.property_revision, 1);
        assert_eq!(authority.admission_fence, 1);
        assert_eq!(creator.create(&request).await.unwrap().status, 409);
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
    assert_eq!(parent.mutation_revision, 3);
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
async fn same_name_has_one_identity_and_complete_retries_do_not_replay_responses() {
    let fixture = TestNamespace::new().await;
    let creator = NamespaceCreator::new(fixture.store.clone());
    let first = request(&fixture, &["namespace"]);
    let duplicate = request(&fixture, &["namespace"]);
    assert_eq!(creator.create(&first).await.unwrap().status, 200);
    let conflict = creator.create(&duplicate).await.unwrap();
    assert_eq!(conflict.status, 409);
    assert_eq!(creator.create(&duplicate).await.unwrap(), conflict);
    assert_eq!(creator.create(&first).await.unwrap().status, 409);
    let mut changed = first;
    changed.principal = "other-writer".into();
    assert_eq!(creator.create(&changed).await.unwrap().status, 409);
}

#[tokio::test]
async fn every_lost_create_write_is_reconciled_from_resources_on_another_instance() {
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
        assert_eq!(writes, if nested { 5 } else { 3 });
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
            let reads = fixture.store.reads.load(Ordering::SeqCst);
            assert!(
                NamespaceCreator::new(fixture.store.clone())
                    .create(&request)
                    .await
                    .is_err(),
                "nested {nested} offset {offset}"
            );
            assert_eq!(
                fixture.store.reads.load(Ordering::SeqCst) - reads,
                if nested { 3 } else { 1 },
                "unknown write must not trigger a confirmation GET"
            );
            let restarted = NamespaceCreator::new(fixture.store.clone());
            let outcome = restarted.create(&request).await.unwrap();
            assert_eq!(
                outcome.status,
                if offset == 1 { 200 } else { 409 },
                "nested {nested} offset {offset}"
            );
            let reader = NamespaceRepository::new(fixture.store.clone());
            let selected = reader
                .load(fixture.context, &request.identifier)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(selected.pending_operation, None);
            assert_eq!(selected.creating_child, None);
            assert_eq!(selected.property_revision, 1);
            assert_eq!(restarted.create(&request).await.unwrap().status, 409);
            assert_eq!(
                reader.load(fixture.context, &request.identifier).await.unwrap(),
                Some(selected)
            );
        }
    }
}

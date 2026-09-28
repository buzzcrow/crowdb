#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use std::collections::BTreeMap;
use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::catalog::{CatalogError, RootState};
use crowdb_access_iceberg::error::ValidationError;
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::namespace::{
    NamespaceAuthority, NamespaceLifecycle, NamespaceProperties, NamespacePropertyRequest,
    NamespaceRepository, PropertyChanges,
};
use crowdb_access_iceberg::operation::{PayloadStore, RequestIdentity};
use fixture::TestNamespace;

fn request(fixture: &TestNamespace, authority: &NamespaceAuthority) -> NamespacePropertyRequest {
    NamespacePropertyRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: authority.identifier.clone(),
        changes: PropertyChanges {
            removals: vec!["old".into(), "absent".into()],
            updates: BTreeMap::from([("owner".into(), "数据库".into())]),
        },
    }
}

async fn setup() -> (TestNamespace, NamespaceAuthority) {
    let fixture = TestNamespace::new().await;
    let mut authority = fixture.authority(None, &["namespace"]);
    authority.properties = NamespaceProperties::new(BTreeMap::from([
        ("old".into(), "remove".into()),
        ("keep".into(), "value".into()),
    ]))
    .unwrap();
    fixture.publish(&authority).await;
    (fixture, authority)
}

#[tokio::test]
async fn property_publication_is_atomic_and_does_not_change_identity_or_admission_fence() {
    let (fixture, original) = setup().await;
    let request = request(&fixture, &original);
    let repository = NamespaceRepository::new(fixture.store.clone());
    let result = repository.update_properties(&request).await.unwrap().unwrap();
    assert_eq!(result.status, 200);
    let body = PayloadStore::new(fixture.store.clone())
        .get(&result.body)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({
            "removed": ["old"], "updated": ["owner"], "missing": ["absent"],
        })
    );
    let selected = repository
        .load(fixture.context, &original.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.namespace, original.namespace);
    assert_eq!(selected.name_epoch, original.name_epoch);
    assert_eq!(selected.admission_fence, original.admission_fence);
    assert_eq!(selected.property_revision, original.property_revision + 1);
    assert_eq!(selected.mutation_revision, original.mutation_revision + 2);
    assert_eq!(selected.pending_operation, None);
    assert_eq!(
        selected.properties.entries(),
        &BTreeMap::from([("keep".into(), "value".into()), ("owner".into(), "数据库".into()),])
    );
    assert_eq!(
        repository.update_properties(&request).await.unwrap(),
        Some(result)
    );
    assert_eq!(
        repository
            .load(fixture.context, &original.identifier)
            .await
            .unwrap(),
        Some(selected)
    );
}

#[tokio::test]
async fn every_lost_write_reply_resumes_one_property_revision_and_original_response() {
    let (baseline, authority) = setup().await;
    let before = baseline.store.writes.load(Ordering::SeqCst);
    NamespaceRepository::new(baseline.store.clone())
        .update_properties(&request(&baseline, &authority))
        .await
        .unwrap();
    let writes = baseline.store.writes.load(Ordering::SeqCst) - before;
    assert!(writes >= 10);
    for offset in 1..=writes {
        let (fixture, authority) = setup().await;
        let request = request(&fixture, &authority);
        fixture.store.fail_after.store(
            fixture.store.writes.load(Ordering::SeqCst) + offset,
            Ordering::SeqCst,
        );
        assert!(
            NamespaceRepository::new(fixture.store.clone())
                .update_properties(&request)
                .await
                .is_err(),
            "offset {offset}"
        );
        let restarted = NamespaceRepository::new(fixture.store.clone());
        let outcome = restarted.update_properties(&request).await.unwrap().unwrap();
        assert_eq!(outcome.status, 200, "offset {offset}");
        assert_eq!(
            restarted.update_properties(&request).await.unwrap(),
            Some(outcome)
        );
        let selected = restarted
            .load(fixture.context, &authority.identifier)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            selected.property_revision,
            authority.property_revision + 1,
            "offset {offset}"
        );
        assert_eq!(selected.pending_operation, None, "offset {offset}");
    }
}

#[tokio::test]
async fn changed_request_input_or_principal_never_reuses_a_completed_result() {
    let (fixture, authority) = setup().await;
    let original = request(&fixture, &authority);
    let repository = NamespaceRepository::new(fixture.store.clone());
    repository.update_properties(&original).await.unwrap();
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    let mut changed = original.clone();
    changed.changes.updates.insert("different".into(), "value".into());
    assert!(matches!(
        repository.update_properties(&changed).await,
        Err(CatalogError::Conflict)
    ));
    changed = original;
    changed.principal = "another-writer".into();
    assert!(matches!(
        repository.update_properties(&changed).await,
        Err(CatalogError::Conflict)
    ));
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
}

#[tokio::test]
async fn invalid_changes_and_exhausted_revisions_fail_before_any_durable_write() {
    let (fixture, mut authority) = setup().await;
    let repository = NamespaceRepository::new(fixture.store.clone());
    let mut invalid = request(&fixture, &authority);
    invalid.changes.removals.push("owner".into());
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert!(matches!(
        repository.update_properties(&invalid).await,
        Err(CatalogError::Invalid(ValidationError::PropertyOverlap))
    ));
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    invalid = request(&fixture, &authority);
    invalid.changes.updates = (0..8)
        .map(|index| (index.to_string(), "v".repeat(8191)))
        .collect();
    assert!(matches!(
        repository.update_properties(&invalid).await,
        Err(CatalogError::Invalid(ValidationError::RecordTooLarge))
    ));
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    authority.mutation_revision = u64::MAX - 1;
    fixture.publish(&authority).await;
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert!(matches!(
        repository.update_properties(&request(&fixture, &authority)).await,
        Err(CatalogError::Invalid(ValidationError::GenerationExhausted))
    ));
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
}

#[tokio::test]
async fn concurrent_property_writers_rebase_only_after_a_definitive_cas_loss() {
    let (mut fixture, authority) = setup().await;
    fixture.store = Arc::new(common::TestStore {
        values: arc_swap::ArcSwap::from(fixture.store.values.load_full()),
        namespace_update_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..Default::default()
    });
    let first = request(&fixture, &authority);
    let mut second = request(&fixture, &authority);
    second.changes = PropertyChanges {
        removals: Vec::new(),
        updates: BTreeMap::from([("second".into(), "retained".into())]),
    };
    let repository = NamespaceRepository::new(fixture.store.clone());
    let results = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            repository.update_properties(&first),
            repository.update_properties(&second)
        )
    })
    .await
    .unwrap();
    for result in [results.0, results.1] {
        match result {
            Ok(Some(outcome)) => assert_eq!(outcome.status, 200),
            Err(CatalogError::Busy) => {}
            other => panic!("unexpected concurrent result: {other:?}"),
        }
    }
    for request in [&first, &second] {
        assert_eq!(
            repository
                .update_properties(request)
                .await
                .unwrap()
                .unwrap()
                .status,
            200
        );
    }
    let selected = repository
        .load(fixture.context, &authority.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.property_revision, authority.property_revision + 2);
    assert_eq!(selected.properties.entries().get("owner").unwrap(), "数据库");
    assert_eq!(selected.properties.entries().get("second").unwrap(), "retained");
    assert_eq!(selected.properties.entries().get("keep").unwrap(), "value");
    assert_eq!(selected.pending_operation, None);
}

#[tokio::test]
async fn absent_dropping_and_retired_namespaces_do_not_accept_property_writes() {
    let fixture = TestNamespace::new().await;
    let mut authority = fixture.authority(None, &["namespace"]);
    let repository = NamespaceRepository::new(fixture.store.clone());
    let request = request(&fixture, &authority);
    assert!(repository.update_properties(&request).await.unwrap().is_none());
    authority.lifecycle = NamespaceLifecycle::Dropping;
    authority.pending_operation = Some(OperationId::random());
    fixture.publish(&authority).await;
    assert!(repository.update_properties(&request).await.is_err());
    fixture.root(fixture.context, RootState::Fencing).await;
    assert!(matches!(
        repository.update_properties(&request).await,
        Err(CatalogError::Conflict)
    ));
}

#[tokio::test]
async fn foreign_property_marker_is_rejected_without_helping_its_owner() {
    let (fixture, original) = setup().await;
    let repository = NamespaceRepository::new(fixture.store.clone());
    let owner = request(&fixture, &original);
    repository.update_properties(&owner).await.unwrap();
    let mut foreign = fixture.authority(None, &["foreign"]);
    foreign.pending_operation = Some(owner.identity.operation);
    fixture.publish(&foreign).await;
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert!(matches!(
        repository.update_properties(&request(&fixture, &foreign)).await,
        Err(CatalogError::Invalid(ValidationError::IdentityMismatch))
    ));
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    assert_eq!(
        repository
            .load(fixture.context, &foreign.identifier)
            .await
            .unwrap(),
        Some(foreign)
    );
}

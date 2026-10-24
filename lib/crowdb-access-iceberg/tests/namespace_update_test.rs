#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace_property_race.rs"]
mod race;
#[path = "common/namespace_recovery_store.rs"]
mod recovery_store;

use std::collections::BTreeMap;
use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::catalog::{CatalogError, RootState};
use crowdb_access_iceberg::error::ValidationError;
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::namespace::{
    NamespaceAuthority, NamespaceLifecycle, NamespaceProperties, NamespacePropertyRequest,
    NamespaceRepository, PropertyChanges,
};
use fixture::TestNamespace;

fn request(fixture: &TestNamespace, authority: &NamespaceAuthority) -> NamespacePropertyRequest {
    NamespacePropertyRequest {
        context: fixture.context,
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
    assert_eq!(result.removed, ["old"]);
    assert_eq!(result.updated, ["owner"]);
    assert_eq!(result.missing, ["absent"]);
    let selected = repository
        .load(fixture.context, &original.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.namespace, original.namespace);
    assert_eq!(selected.name_epoch, original.name_epoch);
    assert_eq!(selected.admission_fence, original.admission_fence);
    assert_eq!(selected.property_revision, original.property_revision + 1);
    assert_eq!(selected.mutation_revision, original.mutation_revision + 1);
    assert_eq!(selected.pending_operation, None);
    assert_eq!(
        selected.properties.entries(),
        &BTreeMap::from([("keep".into(), "value".into()), ("owner".into(), "数据库".into()),])
    );
    let before = fixture.store.writes.load(Ordering::Relaxed);
    let replay = repository.update_properties(&request).await.unwrap().unwrap();
    assert!(replay.removed.is_empty());
    assert_eq!(replay.missing, ["absent", "old"]);
    assert_eq!(fixture.store.writes.load(Ordering::Relaxed), before);
    assert_eq!(
        repository
            .load(fixture.context, &original.identifier)
            .await
            .unwrap(),
        Some(selected)
    );
}

#[tokio::test]
async fn lost_write_reply_is_retried_from_resource_facts_without_a_response_record() {
    let (fixture, authority) = setup().await;
    let request = request(&fixture, &authority);
    let repository = NamespaceRepository::new(fixture.store.clone());
    let writes = fixture.store.writes.load(Ordering::Relaxed);
    let reads = fixture.store.reads.load(Ordering::Relaxed);
    let records = fixture.store.values.load().len();
    fixture.store.fail_after.store(writes + 1, Ordering::Relaxed);
    assert!(repository.update_properties(&request).await.is_err());
    assert_eq!(fixture.store.reads.load(Ordering::Relaxed) - reads, 3);
    let restarted = NamespaceRepository::new(fixture.store.clone());
    let result = restarted.update_properties(&request).await.unwrap().unwrap();
    assert!(result.removed.is_empty());
    assert_eq!(result.missing, ["absent", "old"]);
    assert_eq!(fixture.store.writes.load(Ordering::Relaxed), writes + 1);
    assert_eq!(fixture.store.values.load().len(), records);
}

#[tokio::test]
async fn independent_requests_apply_changes_to_current_properties_without_a_ledger() {
    let (fixture, authority) = setup().await;
    let mut request = request(&fixture, &authority);
    let repository = NamespaceRepository::new(fixture.store.clone());
    repository.update_properties(&request).await.unwrap();
    request.changes.updates.insert("owner".into(), "second".into());
    repository.update_properties(&request).await.unwrap();
    request.changes.updates.insert("owner".into(), "数据库".into());
    repository.update_properties(&request).await.unwrap();
    let selected = repository
        .load(fixture.context, &authority.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.properties.entries().get("owner").unwrap(), "数据库");
    assert_eq!(selected.property_revision, authority.property_revision + 3);
    assert!(selected.pending_operation.is_none());
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
    authority.mutation_revision = u64::MAX;
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
        result.unwrap().unwrap();
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
        Err(CatalogError::Busy)
    ));
    let mut replacement = fixture.context;
    replacement.activation_epoch += 1;
    fixture.root(replacement, RootState::Ready).await;
    assert!(matches!(
        repository.update_properties(&request).await,
        Err(CatalogError::Conflict)
    ));
}

#[tokio::test]
async fn normal_property_write_is_one_resource_cas_and_no_new_records() {
    let (fixture, authority) = setup().await;
    let repository = NamespaceRepository::new(fixture.store.clone());
    let records = fixture.store.values.load().len();
    let reads = fixture.store.reads.load(Ordering::Relaxed);
    let attempts = fixture.store.cas_attempts.load(Ordering::Relaxed);
    let writes = fixture.store.writes.load(Ordering::Relaxed);
    let request = request(&fixture, &authority);
    repository.update_properties(&request).await.unwrap().unwrap();
    assert_eq!(fixture.store.reads.load(Ordering::Relaxed) - reads, 4);
    assert_eq!(fixture.store.cas_attempts.load(Ordering::Relaxed) - attempts, 1);
    assert_eq!(fixture.store.writes.load(Ordering::Relaxed) - writes, 1);
    assert_eq!(fixture.store.values.load().len(), records);
    repository.update_properties(&request).await.unwrap().unwrap();
    assert_eq!(fixture.store.reads.load(Ordering::Relaxed) - reads, 8);
    assert_eq!(fixture.store.cas_attempts.load(Ordering::Relaxed) - attempts, 1);
}

#[tokio::test]
async fn legacy_property_journal_can_finish_before_a_new_stateless_update() {
    use crowdb_access_iceberg::{
        namespace::{
            NamespaceAction, NamespaceJournal, NamespaceOperation, NamespacePhase, NamespaceRecovery,
        },
        operation::{PayloadStore, RequestIdentity},
    };
    let (fixture, original) = setup().await;
    let request = request(&fixture, &original);
    let operation = OperationId::random();
    let input = PayloadStore::new(fixture.store.clone())
        .put(
            fixture.context.catalog,
            operation,
            &serde_json::to_vec(&request.changes).unwrap(),
        )
        .await
        .unwrap();
    NamespaceJournal::new(fixture.store.clone())
        .begin(NamespaceOperation {
            context: fixture.context,
            identity: RequestIdentity {
                operation,
                issued_ms: 100,
            },
            principal: "legacy-writer".into(),
            action: NamespaceAction::Update,
            identifier: original.identifier.clone(),
            namespace: original.namespace,
            parent: original.parent,
            phase: NamespacePhase::Prepared,
            revision: 1,
            input,
            mutation: None,
            scan_after: Vec::new(),
            scan_generation: 0,
            outcome: None,
        })
        .await
        .unwrap();
    let recovery = NamespaceRecovery::new(fixture.store.clone());
    for _ in 0..4 {
        let page = recovery.recover_page(fixture.context, None).await.unwrap();
        assert!(page.failures.is_empty(), "{:?}", page.failures);
        if NamespaceJournal::new(fixture.store.clone())
            .load(fixture.context, operation)
            .await
            .unwrap()
            .unwrap()
            .phase
            == NamespacePhase::Complete
        {
            break;
        }
    }
    let records = fixture.store.values.load().len();
    let writes = fixture.store.writes.load(Ordering::Relaxed);
    let result = NamespaceRepository::new(fixture.store.clone())
        .update_properties(&request)
        .await
        .unwrap()
        .unwrap();
    assert!(result.removed.is_empty());
    assert_eq!(fixture.store.values.load().len(), records);
    assert_eq!(fixture.store.writes.load(Ordering::Relaxed), writes);
}

#[tokio::test]
async fn four_real_property_conflicts_merge_the_observed_authority_without_get() {
    let (fixture, original) = setup().await;
    let mut current = original.clone();
    let mut observations = Vec::new();
    for index in 0..4 {
        let mut properties = current.properties.entries().clone();
        properties.insert(format!("parallel-{index}"), "retained".into());
        current.properties = NamespaceProperties::new(properties).unwrap();
        current.property_revision += 1;
        current.mutation_revision += 1;
        observations.push(current.clone());
    }
    let key = crowdb_access_iceberg::namespace::authority_key(original.catalog, original.namespace)
        .encode()
        .unwrap();
    let store = race::TestPropertyRaceStore::new(fixture.store.clone(), key, observations);
    let repository = NamespaceRepository::new(store.clone());
    repository
        .update_properties(&request(&fixture, &original))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(store.attempts.load(Ordering::Relaxed), 5);
    assert_eq!(store.reads.load(Ordering::Relaxed), 4);
    let selected = repository
        .load(fixture.context, &original.identifier)
        .await
        .unwrap()
        .unwrap();
    for index in 0..4 {
        assert_eq!(
            selected
                .properties
                .entries()
                .get(&format!("parallel-{index}"))
                .unwrap(),
            "retained"
        );
    }
    assert_eq!(selected.properties.entries().get("owner").unwrap(), "数据库");
    assert_eq!(selected.properties.entries().get("keep").unwrap(), "value");
}

#[tokio::test]
async fn observed_deletion_stops_property_writes_without_helping_or_reloading() {
    let (fixture, original) = setup().await;
    let mut dropping = original.clone();
    dropping.lifecycle = NamespaceLifecycle::Dropping;
    dropping.pending_operation = Some(OperationId::random());
    dropping.mutation_revision += 1;
    let key = crowdb_access_iceberg::namespace::authority_key(original.catalog, original.namespace)
        .encode()
        .unwrap();
    let store = race::TestPropertyRaceStore::new(fixture.store.clone(), key, vec![dropping]);
    assert!(matches!(
        NamespaceRepository::new(store.clone())
            .update_properties(&request(&fixture, &original))
            .await,
        Err(CatalogError::Conflict)
    ));
    assert_eq!(store.reads.load(Ordering::Relaxed), 3);
    assert_eq!(store.attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn foreign_property_marker_is_rejected_without_helping_its_owner() {
    let (fixture, _) = setup().await;
    let repository = NamespaceRepository::new(fixture.store.clone());
    let mut foreign = fixture.authority(None, &["foreign"]);
    foreign.pending_operation = Some(OperationId::random());
    fixture.publish(&foreign).await;
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert!(matches!(
        repository.update_properties(&request(&fixture, &foreign)).await,
        Err(CatalogError::Invalid(_))
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

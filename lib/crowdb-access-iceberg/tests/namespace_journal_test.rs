#[path = "common/store.rs"]
mod common;

use std::sync::{atomic::Ordering, Arc};

use common::TestStore;
use crowdb_access_iceberg::catalog::{CatalogError, CatalogRepository, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::key::{CatalogScope, IcebergKey, NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    NamespaceAction, NamespaceIdentifier, NamespaceJournal, NamespaceOperation, NamespaceOutcome,
    NamespacePhase,
};
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, PayloadStore, RequestIdentity};
use crowdb_access_iceberg::record::StorageRecord;

async fn setup(action: NamespaceAction) -> (Arc<TestStore>, NamespaceOperation) {
    let store = Arc::new(TestStore::default());
    let catalog = CatalogRepository::new(store.clone(), ClearBounds::default()).unwrap();
    catalog
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: 100,
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "catalog".into(),
                confirmation: None,
            },
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let context = catalog.status().await.unwrap().0.context;
    let identity = RequestIdentity {
        operation: OperationId::random(),
        issued_ms: 100,
    };
    let input = PayloadStore::new(store.clone())
        .put(context.catalog, identity.operation, b"request")
        .await
        .unwrap();
    let operation = NamespaceOperation {
        context,
        identity,
        principal: "writer".into(),
        action,
        identifier: NamespaceIdentifier::new(vec!["namespace".into()]).unwrap(),
        namespace: NamespaceId::random(),
        parent: None,
        phase: NamespacePhase::Prepared,
        revision: 1,
        input,
        mutation: None,
        scan_after: Vec::new(),
        scan_generation: 0,
        outcome: None,
    };
    (store, operation)
}

fn next(operation: &NamespaceOperation, phase: NamespacePhase) -> NamespaceOperation {
    NamespaceOperation {
        phase,
        revision: operation.revision + 1,
        ..operation.clone()
    }
}

#[tokio::test]
async fn same_request_replays_original_target_and_rejects_principal_or_input_changes() {
    let (store, operation) = setup(NamespaceAction::Create).await;
    let journal = NamespaceJournal::new(store.clone());
    assert_eq!(journal.begin(operation.clone()).await.unwrap(), operation);
    let mut retry = operation.clone();
    retry.namespace = NamespaceId::random();
    assert_eq!(
        NamespaceJournal::new(store.clone()).begin(retry).await.unwrap(),
        operation
    );
    let mut changed = operation.clone();
    changed.principal = "reader".into();
    assert!(matches!(
        journal.begin(changed).await,
        Err(CatalogError::Conflict)
    ));
    let mut changed = operation.clone();
    changed.input = PayloadStore::new(store)
        .put(
            operation.context.catalog,
            operation.identity.operation,
            b"changed",
        )
        .await
        .unwrap();
    assert!(matches!(
        journal.begin(changed).await,
        Err(CatalogError::Conflict)
    ));
    let bytes = StorageRecord::NamespaceOperation(Box::new(operation.clone()))
        .encode()
        .unwrap();
    let result_key = IcebergKey::Catalog {
        catalog: operation.context.catalog,
        scope: CatalogScope::Operation,
        suffix: operation.identity.operation.as_bytes().to_vec(),
    };
    assert!(StorageRecord::decode(&result_key, &bytes).is_err());
}

#[tokio::test]
async fn lost_phase_replies_reload_durable_progress_on_another_instance() {
    let (store, mut operation) = setup(NamespaceAction::Create).await;
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(NamespaceJournal::new(store.clone())
        .begin(operation.clone())
        .await
        .is_err());
    assert_eq!(
        NamespaceJournal::new(store.clone())
            .begin(operation.clone())
            .await
            .unwrap(),
        operation
    );
    for phase in [
        NamespacePhase::Reserved,
        NamespacePhase::Admitting,
        NamespacePhase::Admitted,
        NamespacePhase::Publishing,
        NamespacePhase::Published,
    ] {
        let proposed = next(&operation, phase);
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        assert!(NamespaceJournal::new(store.clone())
            .advance(&operation, &proposed)
            .await
            .is_err());
        operation = NamespaceJournal::new(store.clone())
            .load(operation.context, operation.identity.operation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation, proposed);
    }
    let body = PayloadStore::new(store.clone())
        .put(operation.context.catalog, operation.identity.operation, b"result")
        .await
        .unwrap();
    let mut completed = next(&operation, NamespacePhase::Complete);
    completed.outcome = Some(NamespaceOutcome { status: 200, body });
    assert!(NamespaceJournal::new(store.clone())
        .advance(&operation, &completed)
        .await
        .unwrap());
    assert!(NamespaceJournal::new(store)
        .advance(&completed, &next(&completed, NamespacePhase::Aborting))
        .await
        .is_err());
}

#[tokio::test]
async fn publication_and_abort_are_arbitrated_by_one_durable_phase_cas() {
    for _ in 0..32 {
        let (store, mut operation) = setup(NamespaceAction::Create).await;
        let first = NamespaceJournal::new(store.clone());
        let second = NamespaceJournal::new(store.clone());
        first.begin(operation.clone()).await.unwrap();
        for phase in [
            NamespacePhase::Reserved,
            NamespacePhase::Admitting,
            NamespacePhase::Admitted,
        ] {
            let proposed = next(&operation, phase);
            assert!(first.advance(&operation, &proposed).await.unwrap());
            operation = proposed;
        }
        let publishing = next(&operation, NamespacePhase::Publishing);
        let aborting = next(&operation, NamespacePhase::Aborting);
        let (published, aborted) = tokio::join!(
            first.advance(&operation, &publishing),
            second.advance(&operation, &aborting)
        );
        assert_ne!(published.unwrap(), aborted.unwrap());
        let current = first
            .load(operation.context, operation.identity.operation)
            .await
            .unwrap()
            .unwrap();
        if current.phase == NamespacePhase::Publishing {
            assert!(second
                .advance(&current, &next(&current, NamespacePhase::Aborting))
                .await
                .is_err());
        } else {
            assert_eq!(current.phase, NamespacePhase::Aborting);
            assert!(first
                .advance(&current, &next(&current, NamespacePhase::Publishing))
                .await
                .is_err());
        }
    }
}

#[tokio::test]
async fn drop_phase_cannot_skip_either_child_range_or_cross_parent_bounds() {
    let (store, operation) = setup(NamespaceAction::Drop).await;
    let journal = NamespaceJournal::new(store);
    journal.begin(operation.clone()).await.unwrap();
    let fencing = next(&operation, NamespacePhase::Fencing);
    assert!(journal.advance(&operation, &fencing).await.unwrap());
    let probing = next(&fencing, NamespacePhase::ProbingNamespaces);
    assert!(journal.advance(&fencing, &probing).await.unwrap());
    assert!(journal
        .advance(&probing, &next(&probing, NamespacePhase::Tombstoning))
        .await
        .is_err());
    let mut progress = next(&probing, NamespacePhase::ProbingNamespaces);
    progress.scan_after = crowdb_access_iceberg::namespace::name_key(
        operation.context.catalog,
        Some(NamespaceId::random()),
        "child",
    )
    .unwrap()
    .encode()
    .unwrap();
    progress.scan_generation = 1;
    assert!(journal.advance(&probing, &progress).await.is_err());
    progress.scan_after = crowdb_access_iceberg::namespace::name_key(
        operation.context.catalog,
        Some(operation.namespace),
        "child",
    )
    .unwrap()
    .encode()
    .unwrap();
    assert!(journal.advance(&probing, &progress).await.unwrap());
    assert!(journal
        .advance(&progress, &next(&progress, NamespacePhase::ProbingNamespaces))
        .await
        .is_err());
    let mut tables = next(&progress, NamespacePhase::ProbingTables);
    tables.scan_after.clear();
    tables.scan_generation = 0;
    assert!(journal.advance(&progress, &tables).await.unwrap());
    assert!(journal
        .advance(&tables, &next(&tables, NamespacePhase::Tombstoning))
        .await
        .unwrap());
}

#[tokio::test]
async fn mutation_snapshots_are_key_bound_and_frozen_after_publication_starts() {
    use crowdb_access_iceberg::namespace::{
        authority_key, NamespaceAuthority, NamespaceLifecycle, NamespaceMutation, NamespaceProperties,
    };
    let (store, operation) = setup(NamespaceAction::Update).await;
    let journal = NamespaceJournal::new(store.clone());
    journal.begin(operation.clone()).await.unwrap();
    let authority = NamespaceAuthority {
        catalog: operation.context.catalog,
        namespace: operation.namespace,
        parent: None,
        identifier: operation.identifier.clone(),
        name_epoch: 1,
        property_revision: 1,
        admission_fence: 1,
        mutation_revision: 1,
        lifecycle: NamespaceLifecycle::Ready,
        pending_operation: None,
        properties: NamespaceProperties::default(),
    };
    let before_bytes = StorageRecord::NamespaceAuthority(Box::new(authority.clone()))
        .encode()
        .unwrap();
    let mut after = authority;
    after.property_revision += 1;
    after.mutation_revision += 1;
    after.pending_operation = Some(operation.identity.operation);
    let after_bytes = StorageRecord::NamespaceAuthority(Box::new(after))
        .encode()
        .unwrap();
    let payloads = PayloadStore::new(store.clone());
    let before = payloads
        .put(
            operation.context.catalog,
            operation.identity.operation,
            &before_bytes,
        )
        .await
        .unwrap();
    let after = payloads
        .put(
            operation.context.catalog,
            operation.identity.operation,
            &after_bytes,
        )
        .await
        .unwrap();
    let mut publishing = next(&operation, NamespacePhase::Publishing);
    publishing.mutation = Some(NamespaceMutation {
        key: authority_key(operation.context.catalog, operation.namespace)
            .encode()
            .unwrap(),
        before: Some(before.clone()),
        after,
    });
    let mut foreign = publishing.clone();
    foreign.mutation.as_mut().unwrap().key = authority_key(operation.context.catalog, NamespaceId::random())
        .encode()
        .unwrap();
    assert!(journal.advance(&operation, &foreign).await.is_err());
    assert!(journal.advance(&operation, &publishing).await.unwrap());
    let mut changed = next(&publishing, NamespacePhase::Published);
    changed.mutation.as_mut().unwrap().after = before;
    assert!(journal.advance(&publishing, &changed).await.is_err());
    assert!(journal
        .advance(&publishing, &next(&publishing, NamespacePhase::Published))
        .await
        .unwrap());
}

#[tokio::test]
async fn retired_catalog_cannot_resume_namespace_phases() {
    let (store, operation) = setup(NamespaceAction::Create).await;
    let journal = NamespaceJournal::new(store.clone());
    journal.begin(operation.clone()).await.unwrap();
    let catalog = CatalogRepository::new(store, ClearBounds::default()).unwrap();
    let clear = ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "clearer".into(),
        action: ManagementAction::Clear,
        expected_epoch: operation.context.activation_epoch,
        display_name: "replacement".into(),
        confirmation: Some(operation.context.catalog),
    };
    let _ = catalog
        .execute(clear.clone(), ManagementPrivilege::Clear, 101)
        .await;
    catalog
        .execute(clear, ManagementPrivilege::Clear, 20_000)
        .await
        .unwrap();
    assert!(matches!(
        journal
            .load(operation.context, operation.identity.operation)
            .await,
        Err(CatalogError::Conflict)
    ));
    assert!(matches!(
        journal
            .advance(&operation, &next(&operation, NamespacePhase::Reserved))
            .await,
        Err(CatalogError::Conflict)
    ));
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::Ordering;

use crowdb_access_iceberg::{
    catalog::StoreError,
    key::OperationId,
    namespace::{
        NamespaceAction, NamespaceJournal, NamespaceMutation, NamespaceOperation, NamespaceOutcome,
        NamespacePhase,
    },
    operation::{PayloadStore, RequestIdentity},
};

use super::{
    authority_key, setup, BTreeMap, CatalogError, Duration, NamespaceAuthority, NamespaceDropRequest,
    NamespaceDropper, NamespaceRepository, PropertyChanges, StorageRecord, TestNamespace,
    TestPropertyRetirementStore, ValidationError,
};

async fn publishing(
    fixture: &TestNamespace,
    authority: &NamespaceAuthority,
    forged: bool,
) -> NamespaceOperation {
    let identity = RequestIdentity {
        operation: OperationId::random(),
        issued_ms: 100,
    };
    let changes = PropertyChanges {
        removals: Vec::new(),
        updates: BTreeMap::from([("owner".into(), "legacy".into())]),
    };
    let payloads = PayloadStore::new(fixture.store.clone());
    let operation = NamespaceOperation {
        context: fixture.context,
        identity,
        principal: "legacy-writer".into(),
        action: NamespaceAction::Update,
        identifier: authority.identifier.clone(),
        namespace: authority.namespace,
        parent: authority.parent,
        phase: NamespacePhase::Prepared,
        revision: 1,
        input: payloads
            .put(
                fixture.context.catalog,
                identity.operation,
                &serde_json::to_vec(&changes).unwrap(),
            )
            .await
            .unwrap(),
        mutation: None,
        scan_after: Vec::new(),
        scan_generation: 0,
        outcome: None,
    };
    let journal = NamespaceJournal::new(fixture.store.clone());
    journal.begin(operation.clone()).await.unwrap();
    let before = StorageRecord::NamespaceAuthority(Box::new(authority.clone()))
        .encode()
        .unwrap();
    let mut after = authority.clone();
    after.properties = authority.properties.apply(&changes).unwrap().properties;
    if forged {
        after.properties = crowdb_access_iceberg::namespace::NamespaceProperties::new(BTreeMap::from([(
            "owner".into(),
            "forged".into(),
        )]))
        .unwrap();
    }
    after.property_revision += 1;
    after.mutation_revision += 1;
    after.pending_operation = Some(identity.operation);
    let after = StorageRecord::NamespaceAuthority(Box::new(after))
        .encode()
        .unwrap();
    let mut next = operation.clone();
    next.phase = NamespacePhase::Publishing;
    next.revision += 1;
    next.mutation = Some(NamespaceMutation {
        key: authority_key(authority.catalog, authority.namespace)
            .encode()
            .unwrap(),
        before: Some(
            payloads
                .put(fixture.context.catalog, identity.operation, &before)
                .await
                .unwrap(),
        ),
        after: payloads
            .put(fixture.context.catalog, identity.operation, &after)
            .await
            .unwrap(),
    });
    assert!(journal.advance(&operation, &next).await.unwrap());
    next
}

async fn retire(fixture: &TestNamespace, authority: &NamespaceAuthority) {
    let request = NamespaceDropRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: authority.identifier.clone(),
    };
    assert_eq!(
        NamespaceDropper::new(fixture.store.clone())
            .drop_namespace(&request)
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
}

#[tokio::test]
async fn captured_legacy_publishing_retires_to_404_after_real_delete_without_reloading_the_authority() {
    let (fixture, authority) = setup(1).await;
    let operation = publishing(&fixture, &authority, false).await;
    let encoded = authority_key(authority.catalog, authority.namespace)
        .encode()
        .unwrap();
    let store = TestPropertyRetirementStore::new(fixture.store.clone(), encoded.clone());
    let pending = {
        let repository = NamespaceRepository::new(store.clone());
        let operation = operation.clone();
        tokio::spawn(async move {
            repository
                .resume_property_update(operation.context, operation.identity.operation)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        store.guard_hits(),
        1,
        "valid captured input must reach the actual authority CAS"
    );
    assert_eq!(store.counts(), (6, 1, 0));
    retire(&fixture, &authority).await;
    let retired = fixture.store.values.load()[&encoded].clone();
    store.release.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(outcome.status, 404);
    assert_eq!(store.retired_observations(), 1);
    assert_eq!(store.authority_readbacks(), 0);
    assert_eq!(
        store.counts(),
        (12, 3, 8),
        "legacy receipt completion reads only its own payload/journal and context"
    );
    let journal = NamespaceJournal::new(fixture.store.clone());
    let completed = journal
        .load(fixture.context, operation.identity.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.phase, NamespacePhase::Complete);
    assert_eq!(completed.mutation, operation.mutation);
    assert_eq!(completed.outcome.as_ref(), Some(&outcome));
    let body = PayloadStore::new(fixture.store.clone())
        .get(&outcome.body)
        .await
        .unwrap();
    let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(response["error"]["code"], 404);
    assert_eq!(response["error"]["type"], "NoSuchNamespaceException");
    let before = store.counts();
    assert_eq!(
        NamespaceRepository::new(store.clone())
            .resume_property_update(fixture.context, operation.identity.operation)
            .await
            .unwrap(),
        outcome
    );
    let after = store.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (3, 0));
    assert_eq!(store.authority_readbacks(), 0);
    assert_eq!(fixture.store.values.load()[&encoded], retired);
}

#[tokio::test]
async fn a_forged_legacy_capture_is_not_misreported_as_a_missing_namespace() {
    let (fixture, authority) = setup(1).await;
    let operation = publishing(&fixture, &authority, true).await;
    retire(&fixture, &authority).await;
    let store = TestPropertyRetirementStore::new(
        fixture.store.clone(),
        authority_key(authority.catalog, authority.namespace)
            .encode()
            .unwrap(),
    );
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        NamespaceRepository::new(store.clone())
            .resume_property_update(fixture.context, operation.identity.operation),
    )
    .await
    .unwrap();
    assert!(
        matches!(result, Err(CatalogError::Invalid(ValidationError::Record))),
        "{result:?}"
    );
    assert_eq!(store.guard_hits(), 0);
    assert_eq!(store.retired_observations(), 0);
    assert_eq!(store.counts(), (5, 0, 0));
    assert_eq!(
        NamespaceJournal::new(fixture.store.clone())
            .load(fixture.context, operation.identity.operation)
            .await
            .unwrap(),
        Some(operation)
    );
}

#[tokio::test]
async fn unknown_legacy_property_cas_stops_before_receipt_writes_and_fresh_recovery_finishes_404() {
    for before_apply in [false, true] {
        let (fixture, authority) = setup(1).await;
        let operation = publishing(&fixture, &authority, false).await;
        let key = authority_key(authority.catalog, authority.namespace)
            .encode()
            .unwrap();
        let store = TestPropertyRetirementStore::new(fixture.store.clone(), key.clone());
        let pending = {
            let repository = NamespaceRepository::new(store.clone());
            let operation = operation.clone();
            tokio::spawn(async move {
                repository
                    .resume_property_update(operation.context, operation.identity.operation)
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
            .await
            .unwrap();
        retire(&fixture, &authority).await;
        let retired = fixture.store.values.load()[&key].clone();
        if before_apply {
            store.fail_before.store(true, Ordering::SeqCst);
        } else {
            store.lose_reply.store(true, Ordering::SeqCst);
        }
        store.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(result, Err(CatalogError::Store(StoreError::Response))),
            "before_apply={before_apply}: {result:?}"
        );
        assert_eq!(store.guard_hits(), 1);
        assert_eq!(store.fault_hits(), 1);
        assert_eq!(store.counts(), (6, 1, 0));
        assert_eq!(store.authority_readbacks(), 0);
        assert_eq!(store.retired_observations(), usize::from(!before_apply));
        assert_eq!(
            NamespaceJournal::new(fixture.store.clone())
                .load(fixture.context, operation.identity.operation)
                .await
                .unwrap(),
            Some(operation.clone())
        );
        let fresh = TestPropertyRetirementStore::new(fixture.store.clone(), key.clone());
        fresh.release.notify_one();
        let outcome = NamespaceRepository::new(fresh.clone())
            .resume_property_update(fixture.context, operation.identity.operation)
            .await
            .unwrap();
        assert_eq!(outcome.status, 404);
        assert_eq!(fresh.guard_hits(), 1);
        assert_eq!(fresh.retired_observations(), 1);
        assert_eq!(fresh.authority_readbacks(), 0);
        assert_eq!(fresh.counts(), (12, 3, 8));
        assert_eq!(fixture.store.values.load()[&key], retired);
    }
}

#[tokio::test]
async fn publishing_can_finish_with_an_error_receipt_but_cannot_skip_to_success() {
    let (fixture, authority) = setup(1).await;
    let operation = publishing(&fixture, &authority, false).await;
    let payloads = PayloadStore::new(fixture.store.clone());
    let body = payloads
        .put(fixture.context.catalog, operation.identity.operation, b"{}")
        .await
        .unwrap();
    let journal = NamespaceJournal::new(fixture.store.clone());
    let mut next = operation.clone();
    next.phase = NamespacePhase::Complete;
    next.revision += 1;
    next.outcome = Some(NamespaceOutcome {
        status: 200,
        body: body.clone(),
    });
    let before = fixture.store.cas_attempts.load(Ordering::SeqCst);
    assert!(matches!(
        journal.advance(&operation, &next).await,
        Err(CatalogError::Invalid(ValidationError::Record))
    ));
    assert_eq!(fixture.store.cas_attempts.load(Ordering::SeqCst), before);
    assert_eq!(
        journal
            .load(fixture.context, operation.identity.operation)
            .await
            .unwrap(),
        Some(operation.clone())
    );
    next.outcome = Some(NamespaceOutcome { status: 404, body });
    assert!(journal.advance(&operation, &next).await.unwrap());
    assert_eq!(
        journal
            .load(fixture.context, operation.identity.operation)
            .await
            .unwrap(),
        Some(next)
    );
}

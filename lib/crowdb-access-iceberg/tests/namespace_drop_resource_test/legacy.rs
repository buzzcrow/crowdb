// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{
    key::OperationId,
    namespace::{
        authority_key, NamespaceAction, NamespaceCreator, NamespaceDropper, NamespaceJournal,
        NamespaceLifecycle, NamespaceMutation, NamespaceOperation, NamespacePhase, NamespaceRepository,
    },
    operation::{PayloadStore, RequestIdentity},
    record::StorageRecord,
};

use crate::resource::{creation, setup};

#[tokio::test]
async fn existing_prepared_and_unapplied_fencing_journals_cannot_follow_a_recreated_name() {
    for fencing in [false, true] {
        let (fixture, store, original, request) = setup(1).await;
        let operation = prepare_legacy(&fixture, &original, fencing).await;
        let identity = operation.identity;
        let journal = NamespaceJournal::new(fixture.store.clone());
        let dropper = NamespaceDropper::new(store);
        assert_eq!(
            dropper.drop_namespace(&request).await.unwrap().unwrap().status,
            204
        );
        assert_eq!(
            journal
                .load(fixture.context, identity.operation)
                .await
                .unwrap()
                .unwrap(),
            operation
        );
        NamespaceCreator::new(fixture.store.clone())
            .create(&creation(&fixture, &["n0"]))
            .await
            .unwrap();
        let reader = NamespaceRepository::new(fixture.store.clone());
        let replacement = reader
            .load(fixture.context, &request.identifier)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(replacement.namespace, original.namespace);
        assert_eq!(
            dropper
                .resume(fixture.context, identity.operation)
                .await
                .unwrap()
                .status,
            404
        );
        assert_eq!(
            reader.load(fixture.context, &request.identifier).await.unwrap(),
            Some(replacement)
        );
        // A genuinely new DELETE uses the current name, even if its key was used before.
        assert_eq!(
            dropper.drop_namespace(&request).await.unwrap().unwrap().status,
            204
        );
    }
}

async fn prepare_legacy(
    fixture: &crate::fixture::TestNamespace,
    original: &crowdb_access_iceberg::namespace::NamespaceAuthority,
    fencing: bool,
) -> NamespaceOperation {
    let identity = RequestIdentity {
        operation: OperationId::random(),
        issued_ms: 100,
    };
    let payloads = PayloadStore::new(fixture.store.clone());
    let input = payloads
        .put(fixture.context.catalog, identity.operation, b"{}")
        .await
        .unwrap();
    let mut operation = NamespaceOperation {
        context: fixture.context,
        identity,
        principal: "legacy-writer".into(),
        action: NamespaceAction::Drop,
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
    };
    let journal = NamespaceJournal::new(fixture.store.clone());
    journal.begin(operation.clone()).await.unwrap();
    if fencing {
        let before = StorageRecord::NamespaceAuthority(Box::new(original.clone()))
            .encode()
            .unwrap();
        let mut dropping = original.clone();
        dropping.lifecycle = NamespaceLifecycle::Dropping;
        dropping.pending_operation = Some(identity.operation);
        dropping.admission_fence += 1;
        dropping.mutation_revision += 1;
        let after = StorageRecord::NamespaceAuthority(Box::new(dropping))
            .encode()
            .unwrap();
        let mut next = operation.clone();
        next.phase = NamespacePhase::Fencing;
        next.revision += 1;
        next.mutation = Some(NamespaceMutation {
            key: authority_key(original.catalog, original.namespace)
                .encode()
                .unwrap(),
            before: Some(
                payloads
                    .put(original.catalog, identity.operation, &before)
                    .await
                    .unwrap(),
            ),
            after: payloads
                .put(original.catalog, identity.operation, &after)
                .await
                .unwrap(),
        });
        assert!(journal.advance(&operation, &next).await.unwrap());
        operation = next;
    }
    operation
}

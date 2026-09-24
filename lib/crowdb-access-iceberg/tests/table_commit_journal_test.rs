#[path = "common/store.rs"]
mod common;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod namespaces;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogContext, CatalogError, RootState},
    commit::{TableCommitJournal, TableCommitOperation, TableCommitOutcome, TableCommitPhase as Phase},
    key::{FileId, IcebergKey, OperationId},
    operation::{PayloadStore, RequestIdentity},
    record::StorageRecord,
    table::head_key,
};

async fn setup() -> (namespaces::TestNamespace, TableCommitOperation) {
    let fixture = namespaces::TestNamespace {
        store: Arc::new(common::TestStore::default()),
        context: CatalogContext {
            catalog: metadata::table().catalog,
            activation_epoch: 1,
        },
    };
    fixture.root(fixture.context, RootState::Ready).await;
    let identity = RequestIdentity {
        operation: OperationId::random(),
        issued_ms: 100,
    };
    let input = PayloadStore::new(fixture.store.clone())
        .put(
            fixture.context.catalog,
            identity.operation,
            br#"{"requirements":[],"updates":[]}"#,
        )
        .await
        .unwrap();
    let before = metadata::head(b"metadata", 2, Some(uuid::Uuid::new_v4()));
    fixture
        .put(
            head_key(before.catalog, before.table),
            StorageRecord::TableHead(Box::new(before.clone())),
        )
        .await;
    let operation = TableCommitOperation {
        context: fixture.context,
        identity,
        principal: "writer".into(),
        revision: 1,
        timestamp_ms: 1000,
        phase: Phase::Prepared,
        input,
        before,
        candidate: None,
        outcome: None,
    };
    (fixture, operation)
}

fn next(operation: &TableCommitOperation, phase: Phase) -> TableCommitOperation {
    let mut next = operation.clone();
    next.revision += 1;
    next.phase = phase;
    if phase == Phase::Validated {
        let mut candidate = operation.before.clone();
        candidate.generation += 1;
        candidate.operation_fence += 1;
        candidate.pending_operation = Some(operation.identity.operation);
        candidate.metadata_file = FileId::random();
        candidate.metadata_location = metadata::table().file("metadata/next.json").unwrap();
        next.candidate = Some(candidate);
    }
    next
}

async fn rejected(
    fixture: &namespaces::TestNamespace,
    operation: &TableCommitOperation,
) -> TableCommitOperation {
    let mut rejected = next(operation, Phase::Rejected);
    rejected.outcome = Some(TableCommitOutcome {
        status: 409,
        body: PayloadStore::new(fixture.store.clone())
            .put(fixture.context.catalog, operation.identity.operation, b"conflict")
            .await
            .unwrap(),
    });
    rejected
}

#[tokio::test]
async fn journal_records_bind_key_phase_candidate_and_payload_domains() {
    let (_, operation) = setup().await;
    for operation in [operation.clone(), next(&operation, Phase::Validated)] {
        let record = StorageRecord::TableCommitOperation(Box::new(operation.clone()));
        let bytes = record.encode().unwrap();
        assert_eq!(StorageRecord::decode(&operation.key(), &bytes).unwrap(), record);
        let mut key = operation.key();
        if let IcebergKey::Catalog { suffix, .. } = &mut key {
            *suffix = OperationId::random().as_bytes().to_vec();
        }
        assert!(StorageRecord::decode(&key, &bytes).is_err());
    }
    let valid = next(&operation, Phase::Validated);
    for variant in 0..6 {
        let mut invalid = valid.clone();
        let candidate = invalid.candidate.as_mut().unwrap();
        match variant {
            0 => candidate.generation += 1,
            1 => candidate.operation_fence = operation.before.operation_fence,
            2 => candidate.pending_operation = Some(OperationId::random()),
            3 => candidate.metadata_file = operation.before.metadata_file,
            4 => candidate.name_epoch += 1,
            _ => invalid.input.operation = OperationId::random(),
        }
        assert!(invalid.validate().is_err());
    }
}

#[tokio::test]
async fn retries_keep_the_original_generation_and_candidate_but_reject_changed_requests() {
    let (fixture, operation) = setup().await;
    let journal = TableCommitJournal::new(fixture.store.clone());
    journal.begin(operation.clone()).await.unwrap();
    let validated = next(&operation, Phase::Validated);
    assert!(journal.advance(&operation, &validated).await.unwrap());
    let mut retry = operation.clone();
    retry.before.generation += 10;
    assert_eq!(journal.begin(retry).await.unwrap(), validated);
    let mut changed = operation.clone();
    changed.principal = "another-writer".into();
    assert!(matches!(
        journal.begin(changed).await,
        Err(CatalogError::Conflict)
    ));
    let mut changed = operation.clone();
    changed.input = PayloadStore::new(fixture.store.clone())
        .put(
            fixture.context.catalog,
            operation.identity.operation,
            b"different",
        )
        .await
        .unwrap();
    assert!(matches!(
        journal.begin(changed).await,
        Err(CatalogError::Conflict)
    ));
    let mut rebased = next(&validated, Phase::Writing);
    rebased.before.generation += 1;
    rebased.candidate.as_mut().unwrap().generation += 1;
    assert!(journal.advance(&validated, &rebased).await.is_err());
    let mut retimed = next(&validated, Phase::Writing);
    retimed.timestamp_ms += 1;
    assert!(journal.advance(&validated, &retimed).await.is_err());
}

#[tokio::test]
async fn abort_and_publication_are_arbitrated_by_the_same_phase_revision() {
    let (fixture, operation) = setup().await;
    let journal = TableCommitJournal::new(fixture.store.clone());
    journal.begin(operation.clone()).await.unwrap();
    let validated = next(&operation, Phase::Validated);
    journal.advance(&operation, &validated).await.unwrap();
    let writing = next(&validated, Phase::Writing);
    journal.advance(&validated, &writing).await.unwrap();
    let publishing = next(&writing, Phase::Publishing);
    let aborted = rejected(&fixture, &writing).await;
    assert!(journal.advance(&writing, &publishing).await.unwrap());
    assert!(!journal.advance(&writing, &aborted).await.unwrap());
    let rejected = rejected(&fixture, &publishing).await;
    assert!(journal.advance(&publishing, &rejected).await.is_err());
    assert!(journal
        .advance(&publishing, &next(&publishing, Phase::Published))
        .await
        .is_err());
    let mut winner = publishing.candidate.clone().unwrap();
    winner.pending_operation = Some(OperationId::random());
    winner.metadata_file = FileId::random();
    fixture
        .put(
            head_key(winner.catalog, winner.table),
            StorageRecord::TableHead(Box::new(winner)),
        )
        .await;
    assert!(journal.advance(&publishing, &rejected).await.unwrap());
}

#[tokio::test]
async fn lost_phase_replies_resume_exact_intent_and_success_needs_the_selected_candidate() {
    let (fixture, operation) = setup().await;
    let journal = TableCommitJournal::new(fixture.store.clone());
    fixture
        .store
        .fail_after
        .store(fixture.store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(journal.begin(operation.clone()).await.is_err());
    assert_eq!(journal.begin(operation.clone()).await.unwrap(), operation);
    let mut current = operation;
    for phase in [
        Phase::Validated,
        Phase::Writing,
        Phase::Publishing,
        Phase::Published,
        Phase::Complete,
    ] {
        let mut target = next(&current, phase);
        if phase == Phase::Published {
            let candidate = current.candidate.clone().unwrap();
            fixture
                .put(
                    head_key(candidate.catalog, candidate.table),
                    StorageRecord::TableHead(Box::new(candidate)),
                )
                .await;
        }
        if phase == Phase::Complete {
            target.outcome = Some(TableCommitOutcome {
                status: 200,
                body: PayloadStore::new(fixture.store.clone())
                    .put(fixture.context.catalog, current.identity.operation, b"success")
                    .await
                    .unwrap(),
            });
        }
        fixture
            .store
            .fail_after
            .store(fixture.store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        assert!(journal.advance(&current, &target).await.is_err());
        current = TableCommitJournal::new(fixture.store.clone())
            .load(fixture.context, current.identity.operation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current, target);
    }
    assert_eq!(current.outcome.unwrap().status, 200);
}

#[tokio::test]
async fn retired_contexts_missing_payloads_and_terminal_mutations_fail_closed() {
    let (fixture, operation) = setup().await;
    let journal = TableCommitJournal::new(fixture.store.clone());
    let mut missing = operation.clone();
    missing.input.digest = [0; 32];
    assert!(journal.begin(missing).await.is_err());
    journal.begin(operation.clone()).await.unwrap();
    let rejected = rejected(&fixture, &operation).await;
    assert!(journal.advance(&operation, &rejected).await.unwrap());
    assert!(journal
        .advance(&rejected, &next(&rejected, Phase::Validated))
        .await
        .is_err());
    let mut context = fixture.context;
    context.activation_epoch += 1;
    fixture.root(context, RootState::Ready).await;
    assert!(journal
        .load(fixture.context, operation.identity.operation)
        .await
        .is_err());
    assert!(journal.load(context, operation.identity.operation).await.is_err());
}

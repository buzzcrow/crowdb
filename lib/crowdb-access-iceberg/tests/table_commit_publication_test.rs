#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/manifest_list.rs"]
#[allow(dead_code)]
mod list_fixture;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod namespaces;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod parquet;
#[path = "common/commit_provenance.rs"]
#[allow(dead_code)]
mod provenance;
#[path = "common/table_recovery.rs"]
mod recovery_store;
#[path = "common/snapshot_files.rs"]
#[allow(dead_code)]
mod snapshot;
#[path = "common/manifest_stream.rs"]
#[allow(dead_code)]
mod stream;

use std::sync::atomic::Ordering;

use crowdb_access_iceberg::{
    catalog::CatalogStore,
    commit::{
        prepare_table_commit, recover_table_commit, CandidateAuxiliaryLimits, CandidateSnapshotLimits,
        CommitPreparationLimits, CommitProofLimits, CommitRequestLimits, EvaluationLimits,
        PreparedTableCommit, RequirementLimits, TableCommitJournal, TableCommitOperation, TableCommitPhase,
    },
    key::{FileId, OperationId},
    operation::{PayloadStore, RequestIdentity},
    record::StorageRecord,
    table::{head_key, TableHead},
};
use provenance::TestPrior;
use serde_json::json;

fn limits() -> CommitProofLimits {
    CommitProofLimits {
        preparation: CommitPreparationLimits {
            request: CommitRequestLimits {
                json: metadata::limits(),
                requirements: 100,
                updates: 100,
            },
            evaluation: EvaluationLimits {
                metadata: metadata::limits(),
                requirements: RequirementLimits {
                    count: 100,
                    text_bytes: 4096,
                },
                updates: 100,
                work_bytes: 8 * 1024 * 1024,
            },
        },
        prior: provenance::limits(),
        snapshots: CandidateSnapshotLimits {
            snapshots: 10,
            entries: 100,
            manifest_bytes: 1_000_000,
            ranges: 100,
            files: snapshot::limits(),
        },
        auxiliary: CandidateAuxiliaryLimits {
            files: 10,
            bytes: 1_000_000,
            work: 1000,
            puffin_encoded_bytes: 100_000,
            puffin_decoded_bytes: 100_000,
            parquet: snapshot::limits().position_deletes.metadata,
            partition_rows: crowdb_access_iceberg::manifest::PartitionStatisticsRowLimits {
                page: snapshot::limits().position_deletes.page,
                rows: 1000,
                buffered_bytes: 64 * 1024 * 1024,
            },
        },
    }
}

async fn prepare(fixture: &TestPrior, owner: &str) -> (TableCommitOperation, PreparedTableCommit) {
    let identity = RequestIdentity {
        operation: OperationId::random(),
        issued_ms: 1000,
    };
    let payload = serde_json::to_vec(&json!({"requirements":[],"updates":[
        {"action":"set-properties","updates":{"owner":owner}}]}))
    .unwrap();
    let input = PayloadStore::new(fixture.namespace.store.clone())
        .put(fixture.namespace.context.catalog, identity.operation, &payload)
        .await
        .unwrap();
    let operation = TableCommitOperation {
        context: fixture.namespace.context,
        identity,
        principal: "writer".into(),
        revision: 1,
        timestamp_ms: 1100,
        phase: TableCommitPhase::Prepared,
        input,
        before: fixture.selected.head.clone(),
        candidate: None,
        outcome: None,
    };
    TableCommitJournal::new(fixture.namespace.store.clone())
        .begin(operation.clone())
        .await
        .unwrap();
    let mut target = operation.before.clone();
    target.generation += 1;
    target.operation_fence += 1;
    target.pending_operation = Some(identity.operation);
    target.metadata_file = FileId::random();
    target.metadata_location = fixture::table()
        .file(&format!("metadata/{}.json", identity.operation))
        .unwrap();
    let proof = prepare_table_commit(
        fixture.namespace.store.clone(),
        fixture.blocks.clone(),
        &operation,
        target,
        limits(),
    )
    .await
    .unwrap();
    (operation, proof)
}

async fn head(fixture: &TestPrior) -> TableHead {
    let key = head_key(fixture.selected.head.catalog, fixture.selected.head.table);
    let value = fixture
        .namespace
        .store
        .get(&key.encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
        panic!("not a head")
    };
    *head
}

async fn lifecycle_request(
    fixture: &TestPrior,
    rename: bool,
) -> crowdb_access_iceberg::table::TableLifecycleRequest {
    use crowdb_access_iceberg::table::{name_key, TableLifecycleAction, TableMapping, TableMappingState};
    let mut parent = fixture.namespace.authority(None, &["parent"]);
    parent.namespace = fixture.selected.head.namespace;
    fixture.namespace.publish(&parent).await;
    let head = &fixture.selected.head;
    fixture
        .namespace
        .put(
            name_key(head.catalog, head.namespace, &head.name).unwrap(),
            StorageRecord::TableMapping(TableMapping {
                catalog: head.catalog,
                namespace: head.namespace,
                name: head.name.clone(),
                table: head.table,
                name_epoch: head.name_epoch,
                operation: OperationId::random(),
                state: TableMappingState::Published,
            }),
        )
        .await;
    let destination = fixture.namespace.authority(None, &["destination"]);
    fixture.namespace.publish(&destination).await;
    crowdb_access_iceberg::table::TableLifecycleRequest {
        context: fixture.namespace.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        namespace: parent.identifier,
        name: head.name.clone(),
        action: if rename {
            TableLifecycleAction::Rename {
                namespace: destination.identifier,
                name: "renamed".into(),
            }
        } else {
            TableLifecycleAction::Drop {
                purge_requested: true,
            }
        },
    }
}

#[tokio::test]
async fn prepared_commit_cannot_publish_across_rename_or_drop_fences() {
    use crowdb_access_iceberg::table::TableLifecycles;
    for rename in [false, true] {
        for publishing in [false, true] {
            let fixture = TestPrior::new().await;
            let request = lifecycle_request(&fixture, rename).await;
            let (operation, proof) = prepare(&fixture, "stale").await;
            assert_eq!(
                TableLifecycles::new(fixture.namespace.store.clone())
                    .execute(&request)
                    .await
                    .unwrap()
                    .status,
                204
            );
            let lifecycle_head = head(&fixture).await;
            let result = if publishing {
                proof.publish().await.unwrap()
            } else {
                recover_table_commit(
                    fixture.namespace.store.clone(),
                    fixture.blocks.clone(),
                    fixture.namespace.context,
                    operation.identity.operation,
                    limits(),
                )
                .await
                .unwrap()
            };
            assert_eq!(result.status, 409);
            assert_eq!(head(&fixture).await, lifecycle_head);
            assert_eq!(
                recover_table_commit(
                    fixture.namespace.store.clone(),
                    fixture.blocks.clone(),
                    fixture.namespace.context,
                    operation.identity.operation,
                    limits()
                )
                .await
                .unwrap(),
                result
            );
        }
    }
}

#[tokio::test]
async fn commit_winning_head_cas_forces_pending_lifecycle_to_abort_without_rebasing() {
    use crowdb_access_iceberg::table::TableLifecycles;
    for rename in [false, true] {
        let fixture = TestPrior::new().await;
        let request = lifecycle_request(&fixture, rename).await;
        let (_, proof) = prepare(&fixture, "winner").await;
        let store = fixture.namespace.store.clone();
        store.table_head_pause_before.store(true, Ordering::SeqCst);
        let task_store = store.clone();
        let task_request = request.clone();
        let pending =
            tokio::spawn(async move { TableLifecycles::new(task_store).execute(&task_request).await });
        store.table_head_entered.notified().await;
        assert_eq!(proof.publish().await.unwrap().status, 200);
        let committed = head(&fixture).await;
        store.table_head_release.notify_one();
        assert_eq!(pending.await.unwrap().unwrap().status, 409);
        assert_eq!(head(&fixture).await, committed);
        assert_eq!(
            TableLifecycles::new(store)
                .execute(&request)
                .await
                .unwrap()
                .status,
            409
        );
    }
}

#[tokio::test]
async fn background_lifecycle_sweep_recovers_tombstone_without_client_retry() {
    use crowdb_access_iceberg::{
        commit::{TableRecovery, TableRecoveryKind},
        table::{TableLifecyclePhase, TableLifecycles},
    };
    let fixture = TestPrior::new().await;
    let request = lifecycle_request(&fixture, false).await;
    let store = fixture.namespace.store.clone();
    store.table_head_pause_after.store(true, Ordering::SeqCst);
    let task_store = store.clone();
    let task_request = request.clone();
    let pending = tokio::spawn(async move { TableLifecycles::new(task_store).execute(&task_request).await });
    store.table_head_entered.notified().await;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let recovery = TableRecovery::new(store.clone(), fixture.blocks.clone(), limits());
    let page = recovery
        .recover_page(request.context, TableRecoveryKind::Lifecycle, None, 2000)
        .await
        .unwrap();
    assert_eq!(page.progressed, 1);
    assert!(page.failures.is_empty());
    assert!(page.continuation.is_none());
    assert_eq!(
        TableLifecycles::new(store)
            .load(request.context, request.identity.operation)
            .await
            .unwrap()
            .unwrap()
            .phase,
        TableLifecyclePhase::Complete
    );
}

#[tokio::test]
async fn recovery_sweeps_prepared_commits_with_bounded_catalog_and_kind_cursors() {
    use crowdb_access_iceberg::commit::{TableRecovery, TableRecoveryKind};
    let fixture = TestPrior::new().await;
    let mut operations = Vec::new();
    for _ in 0..5 {
        operations.push(prepare(&fixture, "swept").await.0);
    }
    let recovery = TableRecovery::new(fixture.namespace.store.clone(), fixture.blocks.clone(), limits());
    let first = recovery
        .recover_page(fixture.namespace.context, TableRecoveryKind::Update, None, 2000)
        .await
        .unwrap();
    assert_eq!(first.progressed, 4);
    assert!(first.failures.is_empty());
    let cursor = first.continuation.unwrap();
    assert!(recovery
        .recover_page(
            fixture.namespace.context,
            TableRecoveryKind::Create,
            Some(cursor.clone()),
            2000
        )
        .await
        .is_err());
    let second = recovery
        .recover_page(
            fixture.namespace.context,
            TableRecoveryKind::Update,
            Some(cursor),
            2000,
        )
        .await
        .unwrap();
    assert_eq!(second.progressed, 1);
    assert!(second.failures.is_empty());
    assert!(second.continuation.is_none());
    assert_eq!(
        head(&fixture).await.generation,
        fixture.selected.head.generation + 1
    );
    let journal = TableCommitJournal::new(fixture.namespace.store.clone());
    let mut winners = 0;
    for operation in operations {
        let current = journal
            .load(operation.context, operation.identity.operation)
            .await
            .unwrap()
            .unwrap();
        let status = current.outcome.unwrap().status;
        assert!(matches!(status, 200 | 409));
        winners += usize::from(status == 200);
    }
    assert_eq!(winners, 1);
}

#[tokio::test]
async fn publication_selects_one_complete_generation_and_replays_after_settlement() {
    let fixture = TestPrior::new().await;
    let (operation, proof) = prepare(&fixture, "one").await;
    let candidate = proof.head().clone();
    let result = proof.publish().await.unwrap();
    assert_eq!(result.status, 200);
    let selected = head(&fixture).await;
    assert_eq!(selected.generation, operation.before.generation + 1);
    assert_eq!(selected.metadata_file, candidate.metadata_file);
    assert!(selected.pending_operation.is_none());
    let bytes = PayloadStore::new(fixture.namespace.store.clone())
        .get(&result.body)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["metadata"]["properties"]["owner"], "one");
    assert_eq!(body["metadata-location"], candidate.metadata_location.to_string());
    let replay = recover_table_commit(
        fixture.namespace.store.clone(),
        fixture.blocks.clone(),
        operation.context,
        operation.identity.operation,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(replay, result);
    assert_eq!(head(&fixture).await, selected);
}

#[tokio::test]
async fn concurrent_validated_writers_have_one_winner_and_a_durable_non_rebasing_loser() {
    let fixture = TestPrior::new().await;
    let (first, first_proof) = prepare(&fixture, "first").await;
    let (second, second_proof) = prepare(&fixture, "second").await;
    let (first_result, second_result) = tokio::join!(first_proof.publish(), second_proof.publish());
    let results = [first_result.unwrap(), second_result.unwrap()];
    assert_eq!(results.iter().filter(|result| result.status == 200).count(), 1);
    assert_eq!(results.iter().filter(|result| result.status == 409).count(), 1);
    assert_eq!(head(&fixture).await.generation, first.before.generation + 1);
    for (operation, expected) in [first, second].iter().zip(results) {
        assert_eq!(
            recover_table_commit(
                fixture.namespace.store.clone(),
                fixture.blocks.clone(),
                operation.context,
                operation.identity.operation,
                limits()
            )
            .await
            .unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn every_lost_metadata_journal_head_and_settlement_reply_recovers_original_result() {
    let fixture = TestPrior::new().await;
    let (_, proof) = prepare(&fixture, "baseline").await;
    let before = fixture.namespace.store.writes.load(Ordering::SeqCst);
    proof.publish().await.unwrap();
    let writes = fixture.namespace.store.writes.load(Ordering::SeqCst) - before;
    assert!(writes >= 8);
    for offset in 1..=writes {
        let fixture = TestPrior::new().await;
        let (operation, proof) = prepare(&fixture, "recovered").await;
        let candidate = proof.head().clone();
        fixture.namespace.store.fail_after.store(
            fixture.namespace.store.writes.load(Ordering::SeqCst) + offset,
            Ordering::SeqCst,
        );
        assert!(proof.publish().await.is_err(), "offset {offset}");
        let result = recover_table_commit(
            fixture.namespace.store.clone(),
            fixture.blocks.clone(),
            operation.context,
            operation.identity.operation,
            limits(),
        )
        .await
        .unwrap_or_else(|error| panic!("offset {offset}: {error}"));
        assert_eq!(result.status, 200, "offset {offset}");
        let selected = head(&fixture).await;
        assert_eq!(selected.metadata_file, candidate.metadata_file, "offset {offset}");
        assert_eq!(
            selected.generation,
            operation.before.generation + 1,
            "offset {offset}"
        );
        assert!(selected.pending_operation.is_none());
    }
}

#[tokio::test]
async fn interrupted_prepublication_loser_records_conflict_instead_of_revalidating_a_new_head() {
    let fixture = TestPrior::new().await;
    let (loser, proof) = prepare(&fixture, "loser").await;
    let (_, winner) = prepare(&fixture, "winner").await;
    fixture.namespace.store.fail_after.store(
        fixture.namespace.store.writes.load(Ordering::SeqCst) + 1,
        Ordering::SeqCst,
    );
    assert!(proof.publish().await.is_err());
    assert_eq!(winner.publish().await.unwrap().status, 200);
    let selected = head(&fixture).await;
    let result = recover_table_commit(
        fixture.namespace.store.clone(),
        fixture.blocks.clone(),
        loser.context,
        loser.identity.operation,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(result.status, 409);
    assert_eq!(head(&fixture).await, selected);
}

#[tokio::test]
async fn chunked_candidate_authority_reply_loss_reuses_the_original_tree() {
    let fixture = TestPrior::new().await;
    let (operation, proof) = prepare(&fixture, &"large".repeat(18_000)).await;
    let candidate = proof.head().clone();
    fixture.namespace.store.fail_after.store(
        fixture.namespace.store.writes.load(Ordering::SeqCst) + 3,
        Ordering::SeqCst,
    );
    assert!(proof.publish().await.is_err());
    let blocks = fixture.blocks.writes.load(Ordering::SeqCst);
    assert_eq!(
        recover_table_commit(
            fixture.namespace.store.clone(),
            fixture.blocks.clone(),
            operation.context,
            operation.identity.operation,
            limits()
        )
        .await
        .unwrap()
        .status,
        200
    );
    assert_eq!(fixture.blocks.writes.load(Ordering::SeqCst), blocks);
    assert_eq!(head(&fixture).await.metadata_file, candidate.metadata_file);
}

#[tokio::test]
async fn interrupted_chunk_write_cannot_publish_partial_metadata() {
    let fixture = TestPrior::new().await;
    let (operation, proof) = prepare(&fixture, &"large".repeat(18_000)).await;
    fixture
        .blocks
        .fail_after
        .store(fixture.blocks.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(proof.publish().await.is_err());
    assert_eq!(head(&fixture).await, operation.before);
    assert_eq!(
        recover_table_commit(
            fixture.namespace.store.clone(),
            fixture.blocks.clone(),
            operation.context,
            operation.identity.operation,
            limits()
        )
        .await
        .unwrap()
        .status,
        200
    );
    assert_eq!(head(&fixture).await.generation, operation.before.generation + 1);
}

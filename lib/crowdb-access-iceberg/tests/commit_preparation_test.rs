#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod namespaces;

use std::sync::Arc;

use crowdb_access_iceberg::{
    catalog::{CatalogContext, RootState},
    commit::{
        evaluate_durable_commit, CommitPreparationLimits, CommitRequestLimits, EvaluationLimits,
        RequirementLimits, TableCommitJournal, TableCommitOperation, TableCommitPhase as Phase,
    },
    file::{ContentFormat, FileContent, FileKind, FileRecord, FileRepository},
    key::{FileId, OperationId},
    operation::{PayloadStore, RequestIdentity},
    record::StorageRecord,
    table::{head_key, TableHead},
};

fn limits() -> CommitPreparationLimits {
    CommitPreparationLimits {
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
    }
}

async fn setup() -> (namespaces::TestNamespace, TableCommitOperation, TableHead) {
    let fixture = namespaces::TestNamespace {
        store: Arc::new(common::TestStore::default()),
        context: CatalogContext {
            catalog: metadata::table().catalog,
            activation_epoch: 1,
        },
    };
    fixture.root(fixture.context, RootState::Ready).await;
    let bytes = serde_json::to_vec(&metadata::metadata(2)).unwrap();
    let before = metadata::head(
        &bytes,
        2,
        Some(uuid::Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap()),
    );
    let record = FileRecord {
        file: before.metadata_file,
        location: before.metadata_location.clone(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: bytes.len() as u64,
        digest: before.metadata_digest,
        content: FileContent::select_inline(FileKind::Metadata, &bytes).unwrap(),
        hint: None,
    };
    FileRepository::new(fixture.store.clone())
        .publish(fixture.context, &record)
        .await
        .unwrap();
    fixture
        .put(
            head_key(before.catalog, before.table),
            StorageRecord::TableHead(Box::new(before.clone())),
        )
        .await;
    let identity = RequestIdentity {
        operation: OperationId::random(),
        issued_ms: 100,
    };
    let input = PayloadStore::new(fixture.store.clone())
        .put(
            fixture.context.catalog,
            identity.operation,
            br#"{"requirements":[],"updates":[{"action":"set-properties","updates":{"owner":"persisted"}}]}"#,
        )
        .await
        .unwrap();
    let operation = TableCommitOperation {
        context: fixture.context,
        identity,
        principal: "writer".into(),
        revision: 1,
        timestamp_ms: 1100,
        phase: Phase::Prepared,
        input,
        before: before.clone(),
        candidate: None,
        outcome: None,
    };
    let mut target = before;
    target.generation += 1;
    target.operation_fence += 1;
    target.pending_operation = Some(identity.operation);
    target.metadata_file = FileId::random();
    target.metadata_location = metadata::table().file("metadata/candidate.json").unwrap();
    TableCommitJournal::new(fixture.store.clone())
        .begin(operation.clone())
        .await
        .unwrap();
    (fixture, operation, target)
}

#[tokio::test]
async fn persisted_input_and_timestamp_rebuild_byte_identical_candidates_without_writing() {
    let (fixture, mut operation, target) = setup().await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let evaluated = evaluate_durable_commit(
        fixture.store.clone(),
        blocks.clone(),
        &operation,
        target,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(evaluated.document.fields()["properties"]["owner"], "persisted");
    assert_eq!(evaluated.document.fields()["last-updated-ms"], 1100);
    let journal = TableCommitJournal::new(fixture.store.clone());
    let previous = operation.clone();
    operation.revision += 1;
    operation.phase = Phase::Validated;
    operation.candidate = Some(evaluated.head.clone());
    assert!(journal.advance_for_tests(&previous, &operation).await.unwrap());
    let writes = fixture.store.writes.load(std::sync::atomic::Ordering::SeqCst);
    let recovered = evaluate_durable_commit(
        fixture.store.clone(),
        blocks,
        &operation,
        evaluated.head.clone(),
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.head, evaluated.head);
    assert_eq!(recovered.document.canonical(), evaluated.document.canonical());
    assert_eq!(
        fixture.store.writes.load(std::sync::atomic::Ordering::SeqCst),
        writes
    );
    assert!(FileRepository::new(fixture.store.clone())
        .load(fixture.context, &evaluated.head.metadata_location)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn unjournaled_changed_intents_and_stale_heads_never_rebase_or_write_candidates() {
    let (fixture, operation, target) = setup().await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let mut changed = operation.clone();
    changed.timestamp_ms += 1;
    assert!(evaluate_durable_commit(
        fixture.store.clone(),
        blocks.clone(),
        &changed,
        target.clone(),
        limits()
    )
    .await
    .is_err());
    let mut changed_target = target.clone();
    changed_target.pending_operation = None;
    assert!(evaluate_durable_commit(
        fixture.store.clone(),
        blocks.clone(),
        &operation,
        changed_target,
        limits()
    )
    .await
    .is_err());
    let mut head = operation.before.clone();
    head.operation_fence += 1;
    fixture
        .put(
            head_key(head.catalog, head.table),
            StorageRecord::TableHead(Box::new(head)),
        )
        .await;
    let writes = fixture.store.writes.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        evaluate_durable_commit(fixture.store.clone(), blocks, &operation, target, limits())
            .await
            .is_err()
    );
    assert_eq!(
        fixture.store.writes.load(std::sync::atomic::Ordering::SeqCst),
        writes
    );
}

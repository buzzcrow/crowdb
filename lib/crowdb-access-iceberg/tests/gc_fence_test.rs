use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore},
    gc::{GcPhase, GcPin, GcRepository, GcStalledReason, GcTask, GcTaskKind, ReaderPins},
    key::{NamespaceId, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
    table::{head_key, TableHead, TableLifecycle},
};

mod common {
    pub mod store;
    pub use store::TestStore;
    pub mod file;
    pub mod gc_store;
}

async fn fixture() -> (common::file::TestFile, GcTask) {
    let fixture = common::file::TestFile::new(common::TestStore::default()).await;
    let file = fixture.record("metadata/first.json", b"{}");
    let head = TableHead {
        catalog: fixture.context.catalog,
        table: fixture.table.table,
        namespace: NamespaceId::random(),
        name: "events".into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Ready,
        generation: 1,
        metadata_file: file.file,
        metadata_location: file.location,
        metadata_digest: file.digest,
        format_version: 1,
        table_uuid: None,
        operation_fence: 1,
        pending_operation: None,
    };
    let key = head_key(head.catalog, head.table).encode().unwrap();
    let bytes = StorageRecord::TableHead(Box::new(head.clone())).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    let task = GcTask {
        discovery_scope: 0,
        proof: crowdb_access_iceberg::gc::GcProofState::default(),
        sweep_round: 0,
        deferred_ranges: false,
        context: fixture.context,
        identity: OperationId::random(),
        kind: GcTaskKind::LiveTable,
        phase: GcPhase::Fence,
        revision: 1,
        created_ms: 1,
        not_before_ms: 1000,
        retry_at_ms: 0,
        attempts: 0,
        paused: false,
        fenced: false,
        stalled: GcStalledReason::None,
        head: Some(head),
        scan_after: Vec::new(),
        queue_read: 0,
        queue_write: 0,
        marked: 0,
        deleted: 0,
        reclaimed_bytes: 0,
    };
    (fixture, task)
}

#[tokio::test]
async fn publication_using_pre_sweep_head_cannot_succeed_after_release() {
    let (fixture, task) = fixture().await;
    let repository = GcRepository::new(fixture.store.clone());
    repository.create(&task).await.unwrap();
    repository.fence_table(&task).await.unwrap();
    repository.verify_table_fence(&task).await.unwrap();
    repository.release_table_fence(&task).await.unwrap();
    assert!(repository.verify_table_fence(&task).await.is_err());
    let before = task.head.unwrap();
    let mut candidate = before.clone();
    candidate.generation += 1;
    candidate.operation_fence += 1;
    let key = head_key(before.catalog, before.table).encode().unwrap();
    let before = StorageRecord::TableHead(Box::new(before)).encode().unwrap();
    let after = StorageRecord::TableHead(Box::new(candidate)).encode().unwrap();
    let outcome = fixture
        .store
        .compare_exchange(
            &key,
            Some(&before),
            &after,
            mutation_identity(&key, Some(&before), &after),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, CasOutcome::Conflict(_)));
}

#[tokio::test]
async fn new_reader_pin_cannot_be_acknowledged_during_sweep() {
    let (fixture, task) = fixture().await;
    let pins = ReaderPins::new(fixture.store.clone());
    let pin = GcPin {
        context: fixture.context,
        identity: OperationId::random(),
        head: task.head.clone().unwrap(),
        principal: "reader".into(),
        expires_ms: 2000,
        released: false,
        operator: false,
        protects_uploads: false,
    };
    pins.acquire(&pin).await.unwrap();
    let repository = GcRepository::new(fixture.store.clone());
    repository.create(&task).await.unwrap();
    repository.fence_table(&task).await.unwrap();
    let mut newcomer = pin.clone();
    newcomer.identity = OperationId::random();
    assert!(pins.acquire(&newcomer).await.is_err());
    assert!(fixture
        .store
        .get(&pin.key().encode().unwrap())
        .await
        .unwrap()
        .is_some());
    assert!(pin.protects(1999));
    assert!(!pin.protects(2000));
    pins.release(&pin).await.unwrap();
    pins.release(&pin).await.unwrap();
    repository.release_table_fence(&task).await.unwrap();
}

#[tokio::test]
async fn changed_table_generation_rejects_gc_before_deletion() {
    let (fixture, task) = fixture().await;
    let before = task.head.as_ref().unwrap();
    let mut after = before.clone();
    after.generation += 1;
    after.operation_fence += 1;
    let key = head_key(before.catalog, before.table).encode().unwrap();
    let before = StorageRecord::TableHead(Box::new(before.clone()))
        .encode()
        .unwrap();
    let after = StorageRecord::TableHead(Box::new(after)).encode().unwrap();
    fixture
        .store
        .compare_exchange(
            &key,
            Some(&before),
            &after,
            mutation_identity(&key, Some(&before), &after),
        )
        .await
        .unwrap();
    assert!(GcRepository::new(fixture.store).fence_table(&task).await.is_err());
}

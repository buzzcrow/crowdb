use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogLifecycle, CatalogStore, RootState},
    file::{file_key, ContentFormat, FileContent, FileIdentity, FileKind, FileRepository, FileTreeWriter},
    gc::{GcLimits, GcPhase, GcRepository, GcStalledReason, GcTask, GcWorker},
    key::{CatalogId, CatalogScope, FileId, IcebergKey, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
};

#[path = "common/gc_adoption.rs"]
mod adoption;
#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/gc_blocks.rs"]
mod gc_blocks;
#[path = "common/multipart.rs"]
#[allow(dead_code)]
mod multipart_fixtures;
mod common {
    pub mod store;
    pub use store::TestStore;
    #[allow(dead_code)]
    pub mod file;
    pub mod gc_store;
}

async fn fixture(
    retire: bool,
) -> (
    common::file::TestFile,
    Arc<gc_blocks::TestReclaimBlocks>,
    GcTask,
    GcLimits,
    FileId,
) {
    let fixture = common::file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(gc_blocks::TestReclaimBlocks::default());
    let owner = FileIdentity {
        table: fixture.table,
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 128).unwrap();
    writer.push(&vec![17; 4096]).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let file = crowdb_access_iceberg::file::FileRecord {
        file: owner.file,
        location: fixture.table.file("data/object.parquet").unwrap(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    FileRepository::new(fixture.store.clone())
        .publish(fixture.context, &file)
        .await
        .unwrap();
    let mut retired = CatalogAuthority::new(fixture.context.catalog, "retired".into()).unwrap();
    if retire {
        retired.lifecycle = CatalogLifecycle::Retired;
    }
    let key = IcebergKey::Catalog {
        catalog: fixture.context.catalog,
        scope: CatalogScope::Authority,
        suffix: Vec::new(),
    }
    .encode()
    .unwrap();
    let bytes = StorageRecord::Authority(retired).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    if retire {
        fixture
            .root(
                fixture.context.replacement(CatalogId::random()).unwrap(),
                RootState::Ready,
            )
            .await;
    }
    let limits = GcLimits {
        minimum_retention_ms: 10,
        ..GcLimits::default()
    };
    let task = GcTask::plan(fixture.context, OperationId::random(), None, 1000, limits).unwrap();
    GcRepository::new(fixture.store.clone())
        .create(&task)
        .await
        .unwrap();
    (fixture, blocks, task, limits, file.file)
}

#[tokio::test]
async fn retired_file_reclamation_survives_worker_restart_at_every_step() {
    let (fixture, blocks, mut task, limits, file) = fixture(true).await;
    for _ in 0..300 {
        let repository = GcRepository::new(fixture.store.clone());
        let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
        task = worker.step(&task, 2000_u64.max(task.retry_at_ms)).await.unwrap();
        task = repository
            .task(task.context.catalog, task.identity)
            .await
            .unwrap()
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert_eq!(task.deleted, 1);
    assert_eq!(task.reclaimed_bytes, 4096);
    assert!(blocks.blocks.values.load().is_empty());
    assert!(fixture
        .store
        .get(&file_key(fixture.context.catalog, file).encode().unwrap())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn inactive_discovery_preserves_file_when_gc_workspace_is_unavailable() {
    let (fixture, blocks, task, limits, file) = fixture(true).await;
    let repository = GcRepository::new(fixture.store.clone());
    let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
    fixture.store.gc_workspace_denied.store(true, Ordering::SeqCst);
    let mut task = worker.run(&task, 1_000_000).await.unwrap();
    assert_eq!(task.phase, GcPhase::Discover);
    assert_eq!(task.stalled, GcStalledReason::Resource);
    assert_eq!(task.deleted, 0);
    assert!(fixture
        .store
        .get(&file_key(fixture.context.catalog, file).encode().unwrap())
        .await
        .unwrap()
        .is_some());
    fixture.store.gc_workspace_denied.store(false, Ordering::SeqCst);
    for _ in 0..300 {
        task = worker
            .run(&task, 1_000_000_u64.max(task.retry_at_ms))
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 1);
    assert!(blocks.blocks.values.load().is_empty());
}

#[tokio::test]
async fn expired_aborted_multipart_part_reclaims_tree_before_session_record() {
    use crowdb_access_iceberg::file::{MultipartPart, MultipartPhase};
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let mut session = multipart_fixtures::session();
    session.context = fixture.context;
    session.owner.table = fixture.table;
    session.location = fixture.table.file("multipart/aborted.parquet").unwrap();
    session.phase = MultipartPhase::Aborted;
    session.limits.max_part_bytes = 8192;
    session.limits.max_file_bytes = 8192;
    session.limits.max_staged_bytes = 8192;
    session.part_count = 1;
    session.staged_bytes = 4096;
    let owner = FileIdentity {
        table: fixture.table,
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 128).unwrap();
    writer.push(&vec![9; 4096]).await.unwrap();
    let part = MultipartPart {
        upload: session.upload,
        number: 1,
        revision: 1,
        modified_ms: 100,
        owner,
        tree: writer.finish().await.unwrap(),
    };
    for (key, record) in [
        (
            session.key(),
            StorageRecord::MultipartSession(Box::new(session.clone())),
        ),
        (part.key(), StorageRecord::MultipartPart(Box::new(part.clone()))),
    ] {
        let key = key.encode().unwrap();
        let bytes = record.encode().unwrap();
        fixture
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
    }
    let repository = GcRepository::new(fixture.store.clone());
    let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
    for _ in 0..500 {
        task = worker
            .run(&task, 1_000_000_u64.max(task.retry_at_ms))
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert_eq!(task.deleted, 2);
    assert_eq!(task.reclaimed_bytes, 8192);
    assert!(fixture
        .store
        .get(&part.key().encode().unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(fixture
        .store
        .get(&session.key().encode().unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(blocks.blocks.values.load().is_empty());
}

#[tokio::test]
async fn retired_cleanup_removes_expired_primary_binding_and_projection_without_touching_collision() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let (slot, overflow, new, result_key, projection) = prepare_retry_collision(&fixture).await;
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks, limits).unwrap();
    for _ in 0..500 {
        task = worker
            .run(&task, 1_000_000_u64.max(task.retry_at_ms))
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert!(fixture
        .store
        .get(&slot.encode().unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(fixture.store.get(&result_key).await.unwrap().is_none());
    assert!(fixture.store.get(&projection).await.unwrap().is_none());
    let value = fixture
        .store
        .get(&overflow.encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        StorageRecord::decode(&overflow, &value.bytes).unwrap(),
        StorageRecord::Retry(Box::new(new.clone()))
    );
    assert!(matches!(
        crowdb_access_iceberg::operation::RetryLedger::new(fixture.store.clone())
            .begin(new, 1_000_000)
            .await
            .unwrap(),
        crowdb_access_iceberg::operation::RetryAdmission::Resume(_)
    ));
}

#[tokio::test]
async fn retired_cleanup_retains_aborted_assembly_checkpoint_until_its_chunks_are_reclaimed() {
    use crowdb_access_iceberg::file::MultipartPhase;
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let mut session = multipart_fixtures::session();
    session.context = fixture.context;
    session.owner.table = fixture.table;
    session.location = fixture.table.file("multipart/checkpoint.parquet").unwrap();
    session.phase = MultipartPhase::Aborted;
    session.part_count = 1;
    session.staged_bytes = 1;
    session.completion = Some(multipart_fixtures::completion(&session));
    let mut writer = FileTreeWriter::new(blocks.clone(), session.owner, 1).unwrap();
    writer.push(b"x").await.unwrap();
    let progress = &mut session.completion.as_mut().unwrap().progress;
    progress.writer = Some(writer.checkpoint().await.unwrap());
    progress.completed_bytes = 1;
    let key = session.key().encode().unwrap();
    let bytes = StorageRecord::MultipartSession(Box::new(session))
        .encode()
        .unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    for _ in 0..300 {
        task = worker
            .run(&task, 1_000_000_u64.max(task.retry_at_ms))
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert!(fixture.store.get(&key).await.unwrap().is_none());
    assert!(blocks.blocks.values.load().is_empty());
}

#[tokio::test]
async fn retired_cleanup_preserves_active_root_management_replay_and_removes_expired_audit() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let (retained_key, expired_key, audit_key) = prepare_management_records(&fixture).await;
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks, limits).unwrap();
    for _ in 0..500 {
        task = worker
            .run(&task, 1_000_000_u64.max(task.retry_at_ms))
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert!(fixture
        .store
        .get(&retained_key.encode().unwrap())
        .await
        .unwrap()
        .is_some());
    assert!(fixture
        .store
        .get(&expired_key.encode().unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(fixture
        .store
        .get(&audit_key.encode().unwrap())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn pending_retired_retry_binding_blocks_physical_deletion_until_result_expires() {
    use crowdb_access_iceberg::{
        key::SystemScope,
        operation::{ledger_key, RequestIdentity, RetryRecord},
    };
    let (fixture, blocks, mut task, limits, file) = fixture(true).await;
    let identity = OperationId::random();
    let mut binding = RetryRecord {
        identity: RequestIdentity {
            operation: identity,
            issued_ms: 100,
        },
        principal: "writer".into(),
        route: "POST /tables".into(),
        digest: [7; 32],
        context: fixture.context,
        retained_until_ms: 2_000_000,
        status: 0,
        body: Vec::new(),
    };
    let key = ledger_key(SystemScope::RetryBinding, identity).unwrap();
    let encoded = key.encode().unwrap();
    let before = StorageRecord::Retry(Box::new(binding.clone())).encode().unwrap();
    fixture
        .store
        .compare_exchange(
            &encoded,
            None,
            &before,
            mutation_identity(&encoded, None, &before),
        )
        .await
        .unwrap();
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    for _ in 0..100 {
        task = worker
            .run(&task, 1_000_000_u64.max(task.retry_at_ms))
            .await
            .unwrap();
        if task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Waiting, "{task:?}");
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert!(fixture
        .store
        .get(&file_key(fixture.context.catalog, file).encode().unwrap())
        .await
        .unwrap()
        .is_some());
    binding.status = 409;
    let mut result = binding.clone();
    result.body = b"conflict".to_vec();
    insert_record(
        &fixture,
        result.result_key(),
        StorageRecord::Retry(Box::new(result)),
    )
    .await;
    let after = StorageRecord::Retry(Box::new(binding)).encode().unwrap();
    fixture
        .store
        .compare_exchange(
            &encoded,
            Some(&before),
            &after,
            mutation_identity(&encoded, Some(&before), &after),
        )
        .await
        .unwrap();
    for _ in 0..500 {
        task = worker
            .run(&task, 3_000_000_u64.max(task.retry_at_ms))
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert!(fixture.store.get(&encoded).await.unwrap().is_none());
    assert!(blocks.blocks.values.load().is_empty());
}

#[tokio::test]
async fn final_system_scan_catches_binding_arriving_after_initial_protection() {
    use crowdb_access_iceberg::{
        key::SystemScope,
        operation::{ledger_key, RequestIdentity, RetryRecord},
    };
    let (fixture, blocks, mut task, limits, file) = fixture(true).await;
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    for _ in 0..100 {
        task = worker.step(&task, 1_000_000).await.unwrap();
        if task.phase == GcPhase::Rescan {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Rescan);
    let operation = OperationId::random();
    let key = ledger_key(SystemScope::RetryBinding, operation).unwrap();
    insert_record(
        &fixture,
        key,
        StorageRecord::Retry(Box::new(RetryRecord {
            identity: RequestIdentity {
                operation,
                issued_ms: 100,
            },
            principal: "writer".into(),
            route: "POST /tables".into(),
            digest: [8; 32],
            context: fixture.context,
            retained_until_ms: 2_000_000,
            status: 0,
            body: Vec::new(),
        })),
    )
    .await;
    for _ in 0..100 {
        task = worker.step(&task, 1_000_000).await.unwrap();
        if task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Waiting, "{task:?}");
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert!(fixture
        .store
        .get(&file_key(fixture.context.catalog, file).encode().unwrap())
        .await
        .unwrap()
        .is_some());
}

fn old_id_result_key(catalog: CatalogId, operation: OperationId) -> Vec<u8> {
    IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::Operation,
        suffix: operation.as_bytes().to_vec(),
    }
    .encode()
    .unwrap()
}

async fn prepare_retry_collision(
    fixture: &common::file::TestFile,
) -> (
    IcebergKey,
    IcebergKey,
    crowdb_access_iceberg::operation::RetryRecord,
    Vec<u8>,
    Vec<u8>,
) {
    use crowdb_access_iceberg::{
        key::SystemScope,
        operation::{ledger_key, RequestIdentity, RetryRecord},
    };
    let old_id = OperationId::random();
    let slot = ledger_key(SystemScope::RetryBinding, old_id).unwrap();
    let active_id = (0..100_000)
        .map(|_| OperationId::random())
        .find(|identity| {
            *identity != old_id && ledger_key(SystemScope::RetryBinding, *identity).unwrap() == slot
        })
        .unwrap();
    let binding = |identity, context, status| RetryRecord {
        identity: RequestIdentity {
            operation: identity,
            issued_ms: 100,
        },
        principal: "reader".into(),
        route: "POST /namespaces".into(),
        digest: [1; 32],
        context,
        retained_until_ms: 2000,
        status,
        body: Vec::new(),
    };
    let old = binding(old_id, fixture.context, 409);
    let root_key = IcebergKey::System {
        scope: SystemScope::ActiveRoot,
        suffix: Vec::new(),
    };
    let root_value = fixture
        .store
        .get(&root_key.encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    let StorageRecord::Active(root) = StorageRecord::decode(&root_key, &root_value.bytes).unwrap() else {
        panic!("active root")
    };
    let mut new = binding(active_id, root.context, 0);
    new.retained_until_ms = 2_000_000;
    let overflow = IcebergKey::System {
        scope: SystemScope::RetryOverflow,
        suffix: active_id.as_bytes().to_vec(),
    };
    for (key, record) in [
        (slot.clone(), StorageRecord::Retry(Box::new(old.clone()))),
        (old.result_key(), StorageRecord::Retry(Box::new(old))),
        (overflow.clone(), StorageRecord::Retry(Box::new(new.clone()))),
    ] {
        insert_record(fixture, key, record).await;
    }
    let mut suffix = fixture.table.table.as_bytes().to_vec();
    suffix.extend_from_slice(&1_u64.to_be_bytes());
    suffix.extend_from_slice(&[1; 32]);
    suffix.extend_from_slice(&2_u16.to_be_bytes());
    suffix.extend_from_slice(&0_u16.to_be_bytes());
    suffix.extend_from_slice(&0_u16.to_be_bytes());
    let projection = IcebergKey::Catalog {
        catalog: fixture.context.catalog,
        scope: CatalogScope::MetadataProjection,
        suffix,
    }
    .encode()
    .unwrap();
    fixture
        .store
        .compare_exchange(
            &projection,
            None,
            b"orphan",
            mutation_identity(&projection, None, b"orphan"),
        )
        .await
        .unwrap();
    (
        slot,
        overflow,
        new,
        old_id_result_key(fixture.context.catalog, old_id),
        projection,
    )
}

async fn prepare_management_records(
    fixture: &common::file::TestFile,
) -> (IcebergKey, IcebergKey, IcebergKey) {
    use crowdb_access_iceberg::{
        catalog::ClearBounds,
        key::SystemScope,
        operation::{
            ledger_key, ManagementAction, ManagementOperation, ManagementPhase, ManagementRequest,
            RequestIdentity,
        },
    };
    let root_key = IcebergKey::System {
        scope: SystemScope::ActiveRoot,
        suffix: Vec::new(),
    };
    let value = fixture
        .store
        .get(&root_key.encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    let StorageRecord::Active(root) = StorageRecord::decode(&root_key, &value.bytes).unwrap() else {
        panic!("active root")
    };
    let make_operation = |identity: OperationId| {
        let candidate = CatalogId::random();
        let authority = CatalogAuthority::new(candidate, "replacement".into()).unwrap();
        ManagementOperation {
            request: ManagementRequest {
                identity: RequestIdentity {
                    operation: identity,
                    issued_ms: 100,
                },
                principal: "manager".into(),
                action: ManagementAction::Clear,
                expected_epoch: fixture.context.activation_epoch,
                display_name: "replacement".into(),
                confirmation: Some(fixture.context.catalog),
                capabilities: None,
            },
            phase: ManagementPhase::Conflict,
            candidate,
            original_root: Vec::new(),
            original_authority: Vec::new(),
            result_authority: StorageRecord::Authority(authority).encode().unwrap(),
            bounds: ClearBounds::default(),
            retained_until_ms: 2000,
            publication_proof: Vec::new(),
            grace_completed_ms: 0,
        }
    };
    let expired_id = (0..100_000)
        .map(|_| OperationId::random())
        .find(|identity| {
            *identity != root.operation
                && ledger_key(SystemScope::ManagementOperation, *identity).unwrap()
                    != ledger_key(SystemScope::ManagementOperation, root.operation).unwrap()
                && ledger_key(SystemScope::Audit, *identity).unwrap()
                    != ledger_key(SystemScope::Audit, root.operation).unwrap()
        })
        .unwrap();
    let retained_key = ledger_key(SystemScope::ManagementOperation, root.operation).unwrap();
    let expired_key = ledger_key(SystemScope::ManagementOperation, expired_id).unwrap();
    let audit_key = ledger_key(SystemScope::Audit, expired_id).unwrap();
    for (key, operation) in [
        (retained_key.clone(), make_operation(root.operation)),
        (expired_key.clone(), make_operation(expired_id)),
        (audit_key.clone(), make_operation(expired_id)),
    ] {
        insert_record(fixture, key, StorageRecord::Management(Box::new(operation))).await;
    }
    (retained_key, expired_key, audit_key)
}

async fn insert_record(fixture: &common::file::TestFile, key: IcebergKey, record: StorageRecord) {
    let key = key.encode().unwrap();
    let bytes = record.encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
}

#[tokio::test]
async fn files_arriving_after_initial_discovery_are_rescanned_before_sweep() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let repository = GcRepository::new(fixture.store.clone());
    let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
    for _ in 0..10 {
        task = worker.step(&task, 2000).await.unwrap();
        if task.phase == GcPhase::Roots {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Roots);
    let file = fixture.record("metadata/late.json", b"{}");
    let key = file_key(fixture.context.catalog, file.file).encode().unwrap();
    let bytes = StorageRecord::File(Box::new(file)).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    let mut now = 3000;
    for _ in 0..300 {
        now = now.max(task.retry_at_ms);
        task = GcWorker::new(repository.clone(), blocks.clone(), limits)
            .unwrap()
            .run(&task, now)
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 2);
    assert_eq!(task.reclaimed_bytes, 4098);
    assert!(fixture.store.get(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn missing_file_claim_stops_sweep_before_any_physical_deletion() {
    use crowdb_access_iceberg::gc::{GcScan, GcStore};
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    for _ in 0..30 {
        task = worker.step(&task, 2000).await.unwrap();
        if task.phase == GcPhase::Sweep {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Sweep);
    let page = fixture
        .store
        .scan_gc(GcScan {
            catalog: task.context.catalog,
            scope: Some(CatalogScope::GcClaim),
            prefix: Vec::new(),
            after: Vec::new(),
            items: 1,
            bytes: 128 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    let claim = &page.items[0];
    fixture
        .store
        .delete_gc_record(
            &claim.key,
            &claim.value,
            mutation_identity(&claim.key, Some(&claim.value), &[]),
        )
        .await
        .unwrap();
    assert!(worker.step(&task, 3000).await.is_err());
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn lost_delete_reply_retries_the_durable_intent_without_rereading_deleted_bytes() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    blocks.reply_loss.store(true, Ordering::Relaxed);
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    let mut now = 2000;
    let mut failed = false;
    for _ in 0..300 {
        task = worker.run(&task, now.max(task.retry_at_ms)).await.unwrap();
        if task.stalled == GcStalledReason::Storage {
            failed = true;
            assert_eq!(task.deleted, 0);
            now = task.retry_at_ms;
        }
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert!(failed);
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 1);
    assert!(blocks.blocks.values.load().is_empty());
    assert_eq!(worker.status().active, 0);
}

#[tokio::test]
async fn repeated_corruption_quarantines_without_deleting_any_block() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    blocks.blocks.corrupt_reads.store(true, Ordering::Relaxed);
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    let mut now = 2000;
    for _ in 0..50 {
        task = worker.run(&task, now.max(task.retry_at_ms)).await.unwrap();
        now = now.max(task.retry_at_ms);
        if task.phase == GcPhase::Quarantined {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Quarantined);
    assert_eq!(task.quarantined_from, Some(GcPhase::Sweep));
    assert_eq!(task.stalled, GcStalledReason::Corruption);
    assert_eq!(task.attempts, u32::from(limits.corruption_attempts));
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert_eq!(worker.run(&task, now).await.unwrap(), task);
    let repository = GcRepository::new(fixture.store.clone());
    let retried = repository.retry(&task).await.unwrap();
    assert_eq!(retried.phase, GcPhase::Sweep);
    assert_eq!(retried.quarantined_from, None);
    assert_eq!(retried.attempts, 0);
    assert_eq!(retried.retry_at_ms, 0);
    blocks.blocks.corrupt_reads.store(false, Ordering::Relaxed);
    let next = GcWorker::new(repository, blocks, limits)
        .unwrap()
        .run(&retried, now)
        .await
        .unwrap();
    assert_ne!(next.phase, GcPhase::Quarantined);
}

#[tokio::test]
async fn background_timeout_keeps_the_deletion_intent_and_releases_admission() {
    let (fixture, blocks, mut task, mut limits, _) = fixture(true).await;
    limits.step_ms = 10;
    blocks.delay_ms.store(1000, Ordering::Relaxed);
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    for _ in 0..100 {
        task = worker.run(&task, 2000_u64.max(task.retry_at_ms)).await.unwrap();
        if task.stalled == GcStalledReason::Storage {
            break;
        }
    }
    assert_eq!(task.stalled, GcStalledReason::Storage);
    assert_eq!(task.attempts, 1);
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert_eq!(worker.status().active, 0);
    blocks.delay_ms.store(0, Ordering::Relaxed);
    let now = task.retry_at_ms;
    for _ in 0..300 {
        task = worker.run(&task, now.max(task.retry_at_ms)).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 1);
}

#[tokio::test]
async fn unsupported_shared_ranges_keep_durable_work_and_never_claim_reclaimed_bytes() {
    let (fixture, blocks, mut task, limits, file) = fixture(true).await;
    blocks.deferred.store(true, Ordering::Relaxed);
    let repository = GcRepository::new(fixture.store.clone());
    let worker = GcWorker::new(repository, blocks.clone(), limits).unwrap();
    for _ in 0..100 {
        task = worker.step(&task, 2000_u64.max(task.retry_at_ms)).await.unwrap();
        if task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Waiting);
    assert_eq!(task.stalled, GcStalledReason::UnsupportedRange);
    assert_eq!(task.reclaimed_bytes, 0);
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert!(fixture
        .store
        .get(&file_key(fixture.context.catalog, file).encode().unwrap())
        .await
        .unwrap()
        .is_some());
    blocks.deferred.store(false, Ordering::Relaxed);
    for _ in 0..300 {
        task = worker.step(&task, 100_000).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert_eq!(task.reclaimed_bytes, 4096);
}

#[tokio::test]
async fn deferred_rounds_do_not_count_completed_files_more_than_once() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let inline = fixture.record("metadata/orphan.json", b"{}");
    let key = file_key(fixture.context.catalog, inline.file).encode().unwrap();
    let bytes = StorageRecord::File(Box::new(inline)).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    blocks.deferred.store(true, Ordering::Relaxed);
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    for _ in 0..100 {
        task = worker.run(&task, 2000_u64.max(task.retry_at_ms)).await.unwrap();
        if task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Waiting);
    assert_eq!(task.deleted, 1);
    assert_eq!(task.reclaimed_bytes, 2);
    blocks.deferred.store(false, Ordering::Relaxed);
    for _ in 0..300 {
        task = worker.run(&task, 100_000).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert_eq!(task.deleted, 2);
    assert_eq!(task.reclaimed_bytes, 4098);
}

#[tokio::test]
async fn late_candidate_discovery_starts_a_fresh_retention_window() {
    let (fixture, blocks, mut task, limits, _) = fixture(true).await;
    let worker = GcWorker::new(GcRepository::new(fixture.store.clone()), blocks.clone(), limits).unwrap();
    let discovered_ms = 1_000_000;
    for _ in 0..20 {
        task = worker.run(&task, discovered_ms).await.unwrap();
        if task.stalled == GcStalledReason::Retention {
            break;
        }
    }
    assert_eq!(task.retry_at_ms, discovered_ms + limits.minimum_retention_ms);
    assert_eq!(task.stalled, GcStalledReason::Retention);
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert_eq!(worker.run(&task, task.retry_at_ms - 1).await.unwrap(), task);
    let eligible_ms = task.retry_at_ms;
    for _ in 0..300 {
        task = worker.run(&task, eligible_ms).await.unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert_eq!(task.deleted, 1);
}

#[tokio::test]
async fn purge_fences_new_readers_and_waits_for_reader_and_delegated_pins() {
    let (fixture, blocks, _, limits, _) = fixture(false).await;
    let (head, pin, delegated, pins) = create_purge_with_pins(&fixture).await;
    let repository = GcRepository::new(fixture.store.clone());
    let mut task = GcTask::plan(fixture.context, OperationId::random(), Some(head), 1000, limits).unwrap();
    repository.create(&task).await.unwrap();
    let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
    for _ in 0..50 {
        task = worker.step(&task, 2000_u64.max(task.retry_at_ms)).await.unwrap();
        if task.phase == GcPhase::Waiting {
            break;
        }
    }
    assert!(task.fenced);
    assert_eq!(task.stalled, GcStalledReason::Protected);
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    let mut newcomer = pin.clone();
    newcomer.identity = OperationId::random();
    assert!(pins.acquire(&newcomer).await.is_err());
    pins.release(&pin).await.unwrap();
    let restarted = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
    for _ in 0..30 {
        task = restarted
            .step(&task, 3000_u64.max(task.retry_at_ms))
            .await
            .unwrap();
        assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
        if task.stalled == GcStalledReason::Protected {
            break;
        }
    }
    assert_eq!(task.stalled, GcStalledReason::Protected);
    pins.release(&delegated).await.unwrap();
    for _ in 0..300 {
        task = restarted
            .step(&task, 10_000_u64.max(task.retry_at_ms))
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(task.deleted, 2);
    assert!(!task.fenced);
    assert!(blocks.blocks.values.load().is_empty());
}

async fn create_purge_with_pins(
    fixture: &common::file::TestFile,
) -> (
    crowdb_access_iceberg::table::TableHead,
    crowdb_access_iceberg::gc::GcPin,
    crowdb_access_iceberg::gc::GcPin,
    crowdb_access_iceberg::gc::ReaderPins,
) {
    use crowdb_access_iceberg::{
        gc::{GcPin, ReaderPins},
        key::NamespaceId,
        table::{head_key, TableHead, TableLifecycle, TablePurgeTask},
    };
    let metadata = fixture.record("metadata/table.json", b"{}");
    FileRepository::new(fixture.store.clone())
        .publish(fixture.context, &metadata)
        .await
        .unwrap();
    let mut head = TableHead {
        catalog: fixture.context.catalog,
        table: fixture.table.table,
        namespace: NamespaceId::random(),
        name: "purged".into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Ready,
        generation: 1,
        metadata_file: metadata.file,
        metadata_location: metadata.location,
        metadata_digest: metadata.digest,
        format_version: 1,
        table_uuid: None,
        operation_fence: 1,
        pending_operation: None,
    };
    let key = head_key(head.catalog, head.table).encode().unwrap();
    let before = StorageRecord::TableHead(Box::new(head.clone())).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &before, mutation_identity(&key, None, &before))
        .await
        .unwrap();
    let pin = GcPin {
        context: fixture.context,
        identity: OperationId::random(),
        head: head.clone(),
        principal: "reader".into(),
        expires_ms: 5000,
        released: false,
        operator: false,
        protects_uploads: true,
    };
    let pins = ReaderPins::new(fixture.store.clone());
    pins.acquire(&pin).await.unwrap();
    let mut delegated = pin.clone();
    delegated.identity = OperationId::random();
    delegated.principal = "delegated-credential".into();
    pins.acquire(&delegated).await.unwrap();
    head.lifecycle = TableLifecycle::Tombstone;
    head.operation_fence += 1;
    head.pending_operation = Some(OperationId::random());
    let after = StorageRecord::TableHead(Box::new(head.clone())).encode().unwrap();
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
    let purge = TablePurgeTask {
        activation_epoch: fixture.context.activation_epoch,
        head: head.clone(),
    };
    let key = purge.key().encode().unwrap();
    let bytes = StorageRecord::TablePurgeTask(Box::new(purge)).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    (head, pin, delegated, pins)
}

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogAuthority, CatalogLifecycle, CatalogStore, RootState},
    file::{
        canonical_file_key, ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileRepository,
        FileTreeWriter,
    },
    gc::{GcCandidate, GcLimits, GcPhase, GcRepository, GcTask, GcWorker},
    key::{CatalogId, CatalogScope, FileId, IcebergKey, NamespaceId, OperationId, StorageKey},
    operation::mutation_identity,
    record::StorageRecord,
    table::{storage_head_key, TableHead, TableLifecycle},
};

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/gc_blocks.rs"]
mod gc_blocks;
#[path = "common/gc_store.rs"]
mod gc_store;
mod common {
    pub mod store;
    pub use store::TestStore;
    #[allow(dead_code)]
    pub mod file;
}

struct TestInitial {
    file: common::file::TestFile,
    metadata: FileRecord,
    blocks: Arc<gc_blocks::TestReclaimBlocks>,
    limits: GcLimits,
}

impl TestInitial {
    async fn new(native: bool) -> Self {
        let mut file = common::file::TestFile::new(common::TestStore::default()).await;
        let authority = if native {
            file.table.storage_namespace = Some(NamespaceId::random());
            CatalogAuthority::new_native(file.context.catalog, "active".into()).unwrap()
        } else {
            CatalogAuthority::new(file.context.catalog, "active".into()).unwrap()
        };
        let key = IcebergKey::Catalog {
            catalog: file.context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        }
        .encode()
        .unwrap();
        let bytes = StorageRecord::Authority(authority).encode().unwrap();
        file.store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
        let blocks = Arc::new(gc_blocks::TestReclaimBlocks::default());
        let owner = FileIdentity {
            table: file.table,
            file: FileId::random(),
        };
        let mut writer = FileTreeWriter::new(blocks.clone(), owner, 128).unwrap();
        writer.push(b"initial metadata").await.unwrap();
        let tree = writer.finish().await.unwrap();
        let metadata = FileRecord {
            file: owner.file,
            location: file
                .table
                .file(&format!("metadata/1-{}.metadata.json", owner.file))
                .unwrap(),
            kind: FileKind::Metadata,
            format: ContentFormat::Json,
            length: tree.length,
            digest: tree.digest,
            content: FileContent::Chunks { root: tree.root },
            hint: None,
        };
        FileRepository::new(file.store.clone())
            .publish(file.context, &metadata)
            .await
            .unwrap();
        Self {
            file,
            metadata,
            blocks,
            limits: GcLimits {
                minimum_retention_ms: 10,
                ..GcLimits::default()
            },
        }
    }

    fn repository(&self) -> GcRepository {
        GcRepository::new(self.file.store.clone())
    }

    async fn candidate(&self, key: &IcebergKey) -> GcCandidate {
        let value = self
            .file
            .store
            .get(&key.encode().unwrap())
            .await
            .unwrap()
            .unwrap();
        let StorageRecord::GcCandidate(candidate) = StorageRecord::decode(key, &value.bytes).unwrap() else {
            panic!("candidate")
        };
        *candidate
    }

    async fn step(&self, task: &GcTask, now: u64) -> Result<GcTask, crowdb_access_iceberg::gc::GcWorkError> {
        GcWorker::new(self.repository(), self.blocks.clone(), self.limits)
            .unwrap()
            .step(task, now)
            .await
    }

    async fn discover(&self) -> (GcTask, GcCandidate) {
        let mut task = self
            .repository()
            .admit_multipart(self.file.context, 1000, self.limits)
            .await
            .unwrap();
        for _ in 0..50 {
            if task.phase == GcPhase::Sweep {
                break;
            }
            task = self.step(&task, 1000).await.unwrap();
        }
        assert_eq!(task.phase, GcPhase::Sweep);
        let values = self.file.store.values.load();
        let candidates: Vec<_> = values
            .iter()
            .filter_map(|(key, value)| {
                let key = StorageKey::decode(key).unwrap();
                match StorageRecord::decode(&key, &value.bytes).unwrap() {
                    StorageRecord::GcCandidate(candidate)
                        if candidate.key().encode().unwrap() == *key.encode().unwrap() =>
                    {
                        Some(*candidate)
                    }
                    _ => None,
                }
            })
            .collect();
        assert_eq!(candidates.len(), 1);
        (task, candidates.into_iter().next().unwrap())
    }

    async fn finish(&self, mut task: GcTask, now: u64) -> GcTask {
        for _ in 0..250 {
            if task.phase == GcPhase::Complete {
                return task;
            }
            task = self.step(&task, now.max(task.retry_at_ms)).await.unwrap();
            task = self
                .repository()
                .task(task.context.catalog, task.identity)
                .await
                .unwrap()
                .unwrap();
        }
        panic!("incomplete GC: {task:?}");
    }

    fn head(&self) -> TableHead {
        TableHead {
            catalog: self.file.context.catalog,
            table: self.file.table.table,
            namespace: NamespaceId::random(),
            name: "table".into(),
            name_epoch: 1,
            lifecycle: TableLifecycle::Ready,
            generation: 1,
            metadata_file: self.metadata.file,
            metadata_location: self.metadata.location.clone(),
            metadata_digest: self.metadata.digest,
            commit_binding: None,
            format_version: 2,
            table_uuid: Some(uuid::Uuid::new_v4()),
            operation_fence: 1,
            pending_operation: None,
        }
    }
}

#[tokio::test]
async fn initial_metadata_gc_waits_across_restart_and_fences_a_late_first_head() {
    for native in [false, true] {
        let test = TestInitial::new(native).await;
        let expected_deletes = test.blocks.blocks.values.load().len();
        let mut other = test.file.record("metadata/other.json", b"other metadata");
        other.location = test
            .file
            .table
            .file(&format!("metadata/2-{}.metadata.json", other.file))
            .unwrap();
        FileRepository::new(test.file.store.clone())
            .publish(test.file.context, &other)
            .await
            .unwrap();
        let (task, candidate) = test.discover().await;
        assert_eq!(candidate.first_seen_ms, 1000);
        assert_eq!(candidate.not_before_ms, 1000 + GcTask::PURGE_DELAY_MS);
        let task = test.step(&task, candidate.not_before_ms - 1).await.unwrap();
        assert_eq!(test.blocks.deletes.load(Ordering::Relaxed), 0);
        let head = storage_head_key(test.file.table);
        assert!(test
            .file
            .store
            .get(&head.encode().unwrap())
            .await
            .unwrap()
            .is_none());
        let completed = test.finish(task, candidate.not_before_ms - 1).await;
        assert_eq!(completed.phase, GcPhase::Complete);
        assert_eq!(completed.deleted, 0);
        assert_eq!(test.blocks.deletes.load(Ordering::Relaxed), 0);
        assert!(test
            .file
            .store
            .get(&head.encode().unwrap())
            .await
            .unwrap()
            .is_none());
        let candidate_key = candidate.key();
        assert_eq!(test.candidate(&candidate_key).await, candidate);
        let next_period_ms = candidate.first_seen_ms + 30 * 60_000;
        let task = test
            .repository()
            .admit_multipart(test.file.context, next_period_ms, test.limits)
            .await
            .unwrap();
        assert_eq!(task.phase, GcPhase::Discover);
        assert_eq!(task.identity, completed.identity);
        assert_eq!(task.sweep_round, completed.sweep_round);
        assert_eq!(task.created_ms, completed.created_ms);
        assert_eq!(task.not_before_ms, completed.not_before_ms);
        let task = test.finish(task, next_period_ms).await;
        assert_eq!(task.deleted, 1);
        assert_eq!(task.sweep_round, completed.sweep_round + 1);
        let final_candidate = test.candidate(&candidate_key).await;
        assert_eq!(final_candidate.first_seen_ms, candidate.first_seen_ms);
        assert_eq!(final_candidate.not_before_ms, candidate.not_before_ms);
        assert_eq!(test.blocks.deletes.load(Ordering::Relaxed), expected_deletes);
        assert!(test
            .file
            .store
            .get(&canonical_file_key(&other.location).encode().unwrap())
            .await
            .unwrap()
            .is_some());
        let key = head.encode().unwrap();
        let value = test.file.store.get(&key).await.unwrap().unwrap();
        assert_eq!(
            StorageRecord::decode(&head, &value.bytes).unwrap(),
            StorageRecord::RetiredTable(test.file.table)
        );
        let late = StorageRecord::TableHead(Box::new(test.head())).encode().unwrap();
        assert!(matches!(
            test.file
                .store
                .compare_exchange(&key, None, &late, mutation_identity(&key, None, &late))
                .await
                .unwrap(),
            CasOutcome::Conflict(Some(_))
        ));
    }
}

#[tokio::test]
async fn a_published_head_wins_over_orphan_gc_and_its_candidate_can_later_be_adopted() {
    for native in [false, true] {
        let test = TestInitial::new(native).await;
        let expected_deletes = test.blocks.blocks.values.load().len();
        let (task, candidate) = test.discover().await;
        let key = storage_head_key(test.file.table).encode().unwrap();
        let bytes = StorageRecord::TableHead(Box::new(test.head())).encode().unwrap();
        test.file
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
        let task = test.finish(task, candidate.not_before_ms).await;
        assert_eq!(task.deleted, 0);
        assert_eq!(test.blocks.deletes.load(Ordering::Relaxed), 0);
        assert_eq!(test.file.store.get(&key).await.unwrap().unwrap().bytes, bytes);
        let authority_key = IcebergKey::Catalog {
            catalog: test.file.context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        }
        .encode()
        .unwrap();
        let previous = test.file.store.get(&authority_key).await.unwrap().unwrap();
        let StorageRecord::Authority(mut authority) =
            StorageRecord::decode(&IcebergKey::decode(&authority_key).unwrap(), &previous.bytes).unwrap()
        else {
            panic!()
        };
        authority.lifecycle = CatalogLifecycle::Retired;
        let bytes = StorageRecord::Authority(authority).encode().unwrap();
        test.file
            .store
            .compare_exchange(
                &authority_key,
                Some(&previous.bytes),
                &bytes,
                mutation_identity(&authority_key, Some(&previous.bytes), &bytes),
            )
            .await
            .unwrap();
        test.file
            .root(
                test.file.context.replacement(CatalogId::random()).unwrap(),
                RootState::Ready,
            )
            .await;
        let retired = GcTask::plan(
            test.file.context,
            OperationId::random(),
            None,
            candidate.not_before_ms,
            test.limits,
        )
        .unwrap();
        test.repository().create(&retired).await.unwrap();
        let retired = test.finish(retired, candidate.not_before_ms + 1000).await;
        assert_eq!(retired.phase, GcPhase::Complete);
        assert_eq!(test.blocks.deletes.load(Ordering::Relaxed), expected_deletes);
    }
}

#[tokio::test]
async fn unknown_retirement_reply_stops_physical_reclamation_until_a_new_gc_step() {
    let test = TestInitial::new(true).await;
    let expected_deletes = test.blocks.blocks.values.load().len();
    let (task, candidate) = test.discover().await;
    test.file.store.fail_after.store(
        test.file.store.writes.load(Ordering::SeqCst) + 1,
        Ordering::SeqCst,
    );
    assert!(test.step(&task, candidate.not_before_ms).await.is_err());
    assert_eq!(test.blocks.deletes.load(Ordering::Relaxed), 0);
    let key = storage_head_key(test.file.table);
    let value = test
        .file
        .store
        .get(&key.encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        StorageRecord::decode(&key, &value.bytes).unwrap(),
        StorageRecord::RetiredTable(test.file.table)
    );
    let task = test.finish(task, candidate.not_before_ms).await;
    assert_eq!(task.deleted, 1);
    assert_eq!(test.blocks.deletes.load(Ordering::Relaxed), expected_deletes);
}

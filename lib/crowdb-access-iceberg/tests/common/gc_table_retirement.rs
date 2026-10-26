use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogLifecycle, CatalogStore, RootState},
    commit::{StagedCommitRequest, TableCreator},
    file::TableLocation,
    gc::{GcLimits, GcPhase, GcRepository, GcTask, GcWorker},
    key::{CatalogId, CatalogScope, IcebergKey, OperationId, StorageKey},
    record::StorageRecord,
    table::{name_key, storage_head_key},
};

use crate::{gc_blocks::TestReclaimBlocks, staging::TestStaged};

pub const NOW: u64 = 10 * crowdb_access_iceberg::operation::RETRY_WINDOW_MS;

pub struct TestRetiredTable {
    pub staged: TestStaged,
    pub request: StagedCommitRequest,
    pub blocks: Arc<TestReclaimBlocks>,
}

impl TestRetiredTable {
    pub async fn new(native: bool) -> Self {
        let staged = TestStaged::new().await;
        if native {
            staged
                .namespace
                .put(
                    IcebergKey::Catalog {
                        catalog: staged.namespace.context.catalog,
                        scope: CatalogScope::Authority,
                        suffix: Vec::new(),
                    },
                    StorageRecord::Authority(
                        CatalogAuthority::new_native(staged.namespace.context.catalog, "native".into())
                            .unwrap(),
                    ),
                )
                .await;
        }
        let mut request = staged.commit_request().await;
        let mut body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        body["updates"][10]["updates"]["large"] = serde_json::json!("metadata".repeat(12_000));
        request.body = serde_json::to_vec(&body).unwrap();
        Self {
            staged,
            request,
            blocks: Arc::new(TestReclaimBlocks::default()),
        }
    }

    pub fn creator(&self) -> TableCreator {
        TableCreator::new(self.staged.namespace.store.clone(), self.blocks.clone())
            .with_staged_limits(crate::staging::limits())
    }

    pub fn table(&self) -> TableLocation {
        let body: serde_json::Value = serde_json::from_slice(&self.request.body).unwrap();
        format!("{}/", body["updates"][8]["location"].as_str().unwrap())
            .parse()
            .unwrap()
    }

    pub fn head(&self) -> Option<StorageRecord> {
        let key = storage_head_key(self.table());
        self.staged
            .namespace
            .store
            .values
            .load()
            .get(&key.encode().unwrap())
            .map(|value| StorageRecord::decode(&key, &value.bytes).unwrap())
    }

    pub fn records(&self) -> Vec<StorageRecord> {
        self.staged
            .namespace
            .store
            .values
            .load()
            .iter()
            .map(|(key, value)| {
                StorageRecord::decode(&StorageKey::decode(key).unwrap(), &value.bytes).unwrap()
            })
            .collect()
    }

    pub async fn retire_catalog(&self) -> GcTask {
        let key = IcebergKey::Catalog {
            catalog: self.request.context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        };
        let value = self
            .staged
            .namespace
            .store
            .get(&key.encode().unwrap())
            .await
            .unwrap()
            .unwrap();
        let StorageRecord::Authority(mut authority) = StorageRecord::decode(&key, &value.bytes).unwrap()
        else {
            panic!()
        };
        authority.lifecycle = CatalogLifecycle::Retired;
        self.staged
            .namespace
            .put(key, StorageRecord::Authority(authority))
            .await;
        self.staged
            .namespace
            .root(
                self.request.context.replacement(CatalogId::random()).unwrap(),
                RootState::Ready,
            )
            .await;
        let task = GcTask::plan(self.request.context, OperationId::random(), None, 1000, limits()).unwrap();
        self.repository().create(&task).await.unwrap();
        task
    }

    pub fn repository(&self) -> GcRepository {
        GcRepository::new(self.staged.namespace.store.clone())
    }

    pub async fn to_phase(&self, mut task: GcTask, phase: GcPhase) -> GcTask {
        for _ in 0..500 {
            if task.phase == phase {
                return task;
            }
            assert_ne!(task.phase, GcPhase::Complete, "did not reach {phase:?}");
            task = GcWorker::new(self.repository(), self.blocks.clone(), limits())
                .unwrap()
                .step(&task, NOW.max(task.retry_at_ms))
                .await
                .unwrap();
        }
        panic!("did not reach {phase:?}: {task:?}");
    }

    pub async fn finish(&self, mut task: GcTask) -> GcTask {
        for _ in 0..500 {
            if task.phase == GcPhase::Complete {
                return task;
            }
            let repository = self.repository();
            task = GcWorker::new(repository.clone(), self.blocks.clone(), limits())
                .unwrap()
                .step(&task, NOW.max(task.retry_at_ms))
                .await
                .unwrap();
            task = repository
                .task(task.context.catalog, task.identity)
                .await
                .unwrap()
                .unwrap();
        }
        panic!("incomplete retirement: {task:?}");
    }

    pub async fn pause_first_head(
        &self,
    ) -> tokio::task::JoinHandle<
        Result<
            crowdb_access_iceberg::commit::TableCreationOutcome,
            crowdb_access_iceberg::commit::CommitPublicationError,
        >,
    > {
        let store = &self.staged.namespace.store;
        store.table_head_pause_before.store(true, Ordering::SeqCst);
        let creator = self.creator();
        let request = self.request.clone();
        let pending = tokio::spawn(async move { creator.commit_staged(&request).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            store.table_head_entered.notified(),
        )
        .await
        .unwrap();
        pending
    }

    pub async fn remove_mapping(&self) {
        use crowdb_access_iceberg::{namespace::NamespaceStore, operation::mutation_identity};
        let key = name_key(
            self.request.context.catalog,
            self.staged.parent.namespace,
            "events",
        )
        .unwrap()
        .encode()
        .unwrap();
        let expected = self.staged.namespace.store.get(&key).await.unwrap().unwrap();
        self.staged
            .namespace
            .store
            .delete_mapping(
                &key,
                &expected.bytes,
                mutation_identity(&key, Some(&expected.bytes), &[]),
            )
            .await
            .unwrap();
    }
}

pub fn limits() -> GcLimits {
    GcLimits {
        minimum_retention_ms: 1,
        ..GcLimits::default()
    }
}

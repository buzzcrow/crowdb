// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogStore},
    file::{FileRecord, FileRepository},
    gc::{BudgetedGcStore, GcIoBudget, GcLimits, GcRepository, GcTask},
    key::{CatalogScope, IcebergKey, NamespaceId, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
};

use crate::common::{file::TestFile, TestStore};

pub struct TestDiscovery {
    pub file: TestFile,
    pub files: Vec<FileRecord>,
    pub task: GcTask,
    pub budget: Arc<GcIoBudget>,
    pub store: Arc<BudgetedGcStore>,
}

impl TestDiscovery {
    pub async fn new(native: bool) -> Self {
        let mut file = TestFile::new(TestStore::default()).await;
        let authority = if native {
            file.table.storage_namespace = Some(NamespaceId::random());
            CatalogAuthority::new_native(file.context.catalog, "budget".into()).unwrap()
        } else {
            CatalogAuthority::new(file.context.catalog, "budget".into()).unwrap()
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
        let repository = FileRepository::new(file.store.clone());
        let mut files = Vec::new();
        for index in 0..128 {
            let record = file.record(&format!("metadata/{index}.json"), b"{}");
            repository.publish(file.context, &record).await.unwrap();
            files.push(record);
        }
        let mut task = GcTask::plan(
            file.context,
            OperationId::random(),
            None,
            1000,
            GcLimits::default(),
        )
        .unwrap();
        task.discovery_scope = if native { 4 } else { 1 };
        GcRepository::new(file.store.clone()).create(&task).await.unwrap();
        let budget = Arc::new(GcIoBudget::new(64 * 1024 * 1024, 128, 8 * 1024 * 1024, 128));
        let store = Arc::new(BudgetedGcStore::new(file.store.clone(), budget.clone()));
        Self {
            file,
            files,
            task,
            budget,
            store,
        }
    }

    pub fn durable(&self) -> GcRepository {
        GcRepository::new(self.file.store.clone())
    }

    pub fn repository(&self) -> GcRepository {
        GcRepository::new(self.store.clone())
    }

    pub async fn exhaust_requests(&self) {
        let key = crowdb_access_iceberg::file::canonical_file_key(&self.files[0].location)
            .encode()
            .unwrap();
        for _ in 0..128 {
            self.store.get(&key).await.unwrap();
        }
    }
}

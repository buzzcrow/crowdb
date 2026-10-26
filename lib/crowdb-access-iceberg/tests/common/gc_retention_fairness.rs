// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::fixture::TestNamespace;
use crowdb_access_iceberg::{
    catalog::CatalogStore,
    file::{FileRecord, FileRepository, TableLocation},
    gc::{GcCandidate, GcLimits, GcPhase, GcRepository, GcTask},
    key::{FileId, TableId},
    record::StorageRecord,
};

pub const NOW: u64 = 2_000_000;

pub struct TestRetention {
    pub fixture: TestNamespace,
    pub future: FileRecord,
    pub expired: FileRecord,
    pub task: GcTask,
}

impl TestRetention {
    pub async fn new() -> Self {
        let fixture = TestNamespace::new().await;
        let table = TableLocation {
            catalog: fixture.context.catalog,
            storage_namespace: None,
            table: TableId::from_bytes(&[11; 16]).unwrap(),
        };
        let files = FileRepository::new(fixture.store.clone());
        let future = record(table, "new.json", 1);
        let expired = record(table, "old.json", 2);
        assert!(
            GcCandidate::deleted_key(&future).encode().unwrap()
                < GcCandidate::deleted_key(&expired).encode().unwrap()
        );
        for (file, deleted_ms) in [(&future, NOW), (&expired, 1)] {
            assert_eq!(files.publish(fixture.context, file).await.unwrap(), *file);
            let deleted = files
                .mark_deleted(fixture.context, file, deleted_ms)
                .await
                .unwrap();
            assert_eq!(deleted.deleted_ms, deleted_ms);
        }
        let task = GcRepository::new(fixture.store.clone())
            .admit_multipart(fixture.context, NOW, GcLimits::default())
            .await
            .unwrap();
        Self {
            fixture,
            future,
            expired,
            task,
        }
    }

    pub async fn candidate(&self, file: &FileRecord) -> GcCandidate {
        let key = GcCandidate::deleted_key(file);
        let value = self
            .fixture
            .store
            .get(&key.encode().unwrap())
            .await
            .unwrap()
            .unwrap();
        let StorageRecord::GcCandidate(candidate) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
            panic!("candidate missing")
        };
        *candidate
    }

    pub async fn until_sweep(&self, worker: &crowdb_access_iceberg::gc::GcWorker) -> GcTask {
        let mut task = self.task.clone();
        for _ in 0..100 {
            if task.phase == GcPhase::Sweep {
                return task;
            }
            task = worker.step(&task, NOW).await.unwrap();
        }
        panic!("discovery did not reach Sweep: {task:?}");
    }
}

fn record(table: TableLocation, name: &str, identity: u8) -> FileRecord {
    use crowdb_access_iceberg::file::{ContentFormat, FileContent, FileKind};
    use sha2::{Digest, Sha256};
    FileRecord {
        file: FileId::from_bytes(&[identity; 16]).unwrap(),
        location: table.file(&format!("data/{name}")).unwrap(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: 2,
        digest: Sha256::digest(b"{}").into(),
        content: FileContent::select_inline(FileKind::Metadata, b"{}").unwrap(),
        hint: None,
    }
}

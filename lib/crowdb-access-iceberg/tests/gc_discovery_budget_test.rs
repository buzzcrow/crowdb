// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{
    catalog::{CatalogError, CatalogStore, StoreError},
    file::{canonical_file_key, FileRepository},
    gc::{GcLimits, GcPhase},
    key::{CatalogScope, IcebergKey, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
};

mod common {
    pub mod store;
    pub use store::TestStore;
    pub mod file;
    pub mod gc_discovery;
    pub mod gc_store;
}
use common::gc_discovery::TestDiscovery;

#[tokio::test]
async fn discovery_checkpoints_a_page_that_exceeds_the_window_request_budget() {
    for native in [false, true] {
        let test = TestDiscovery::new(native).await;
        let mut task = test.task.clone();
        for _ in 0..12 {
            test.budget.reset();
            let next = test
                .repository()
                .discover_files(&task, GcLimits::default(), 2000)
                .await
                .unwrap();
            assert_eq!(
                test.durable()
                    .task(task.context.catalog, task.identity)
                    .await
                    .unwrap()
                    .unwrap(),
                next
            );
            assert!(next.scan_after > task.scan_after || next.discovery_scope != task.discovery_scope);
            task = next;
            if task.discovery_scope != test.task.discovery_scope {
                break;
            }
        }
        assert_ne!(
            task.discovery_scope, test.task.discovery_scope,
            "discovery did not converge: {task:?}"
        );
        let values = test.file.store.values.load();
        let candidates: Vec<_> = values
            .iter()
            .filter_map(|(key, value)| {
                let key = IcebergKey::decode(key).ok()?;
                if !matches!(
                    key,
                    IcebergKey::Catalog {
                        scope: CatalogScope::GcCandidate,
                        ..
                    }
                ) {
                    return None;
                }
                let StorageRecord::GcCandidate(candidate) =
                    StorageRecord::decode(&key, &value.bytes).unwrap()
                else {
                    panic!("candidate record")
                };
                Some(candidate)
            })
            .collect();
        assert_eq!(candidates.len(), test.files.len());
        for file in &test.files {
            let candidate = candidates
                .iter()
                .find(|candidate| candidate.file == *file)
                .unwrap();
            assert_eq!(candidate.task, task.identity);
            assert_eq!(candidate.first_seen_ms, 2000);
            assert_eq!(
                candidate.not_before_ms,
                2000 + GcLimits::default().minimum_retention_ms
            );
            assert_eq!(
                FileRepository::new(test.file.store.clone())
                    .load(test.file.context, &file.location)
                    .await
                    .unwrap(),
                Some(file.clone())
            );
            assert!(values.contains_key(&canonical_file_key(&file.location).encode().unwrap()));
        }
    }
}

#[tokio::test]
async fn exhausted_windows_keep_retirement_checks_and_bounded_progress_writes() {
    let test = TestDiscovery::new(false).await;
    test.exhaust_requests().await;
    let mut task = test.task.clone();
    for _ in 0..8 {
        let mut next = task.clone();
        next.revision += 1;
        test.repository().update(&task, &next).await.unwrap();
        task = next;
    }
    let mut next = task.clone();
    next.revision += 1;
    assert!(matches!(
        test.repository().update(&task, &next).await,
        Err(CatalogError::Store(StoreError::Budget))
    ));
    assert_eq!(
        test.durable()
            .task(task.context.catalog, task.identity)
            .await
            .unwrap()
            .unwrap(),
        task
    );

    let key = IcebergKey::Catalog {
        catalog: task.context.catalog,
        scope: CatalogScope::GcRetirement,
        suffix: Vec::new(),
    }
    .encode()
    .unwrap();
    let mut owner = task.clone();
    owner.identity = OperationId::random();
    owner.phase = GcPhase::CleanupGc;
    owner.discovery_scope = 0;
    let bytes = StorageRecord::GcTask(Box::new(owner)).encode().unwrap();
    test.file
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    test.budget.reset();
    test.exhaust_requests().await;
    assert!(matches!(
        test.repository().update(&task, &next).await,
        Err(CatalogError::Busy)
    ));
    assert_eq!(
        test.durable()
            .task(task.context.catalog, task.identity)
            .await
            .unwrap()
            .unwrap(),
        task
    );
}

#[tokio::test]
async fn malformed_files_remain_errors_without_checkpointing_past_them() {
    let test = TestDiscovery::new(false).await;
    let mut keys: Vec<_> = test
        .files
        .iter()
        .map(|file| canonical_file_key(&file.location).encode().unwrap())
        .collect();
    keys.sort();
    let before = test.file.store.get(&keys[1]).await.unwrap().unwrap();
    test.file
        .store
        .compare_exchange(
            &keys[1],
            Some(&before.bytes),
            b"invalid file record",
            mutation_identity(&keys[1], Some(&before.bytes), b"invalid file record"),
        )
        .await
        .unwrap();
    assert!(matches!(
        test.repository()
            .discover_files(&test.task, GcLimits::default(), 2000)
            .await,
        Err(CatalogError::Invalid(_))
    ));
    assert_eq!(
        test.durable()
            .task(test.task.context.catalog, test.task.identity)
            .await
            .unwrap()
            .unwrap(),
        test.task
    );
}

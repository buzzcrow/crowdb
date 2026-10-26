// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{CatalogContext, CatalogStore, GcTask, TestStore};
use crowdb_access_iceberg::{
    catalog::{CatalogError, CatalogRepository, ClearBounds, ManagementPrivilege, RootState},
    gc::{GcLimits, GcRepository},
    key::OperationId,
    operation::{mutation_identity, ManagementAction, ManagementRequest, RequestIdentity},
    record::StorageRecord,
};
use std::sync::Arc;

pub const IDLE_MS: u64 = 30 * 60_000;

pub fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

pub async fn replace_task(store: &TestStore, task: &GcTask) {
    let key = task.key().encode().unwrap();
    let before = store.get(&key).await.unwrap().unwrap();
    let bytes = StorageRecord::GcTask(Box::new(task.clone())).encode().unwrap();
    assert!(matches!(
        store
            .compare_exchange(
                &key,
                Some(&before.bytes),
                &bytes,
                mutation_identity(&key, Some(&before.bytes), &bytes)
            )
            .await
            .unwrap(),
        crowdb_access_iceberg::catalog::CasOutcome::Applied(_)
    ));
}

pub async fn cleared_catalog() -> (Arc<TestStore>, CatalogContext, GcTask) {
    let store = Arc::new(TestStore::default());
    let catalog = CatalogRepository::new(
        store.clone(),
        ClearBounds {
            root_lease_ms: 0,
            request_ms: 10,
            delegated_access_ms: 0,
            clock_skew_ms: 1,
        },
    )
    .unwrap();
    let request = |action, epoch| ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "operator".into(),
        action,
        expected_epoch: epoch,
        display_name: "catalog".into(),
        confirmation: None,
        capabilities: None,
    };
    let initialized = catalog
        .execute(
            request(ManagementAction::Initialize, 0),
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let mut clear = request(ManagementAction::Clear, 1);
    clear.confirmation = Some(initialized.catalog);
    assert!(matches!(
        catalog
            .execute(clear.clone(), ManagementPrivilege::Clear, 101)
            .await,
        Err(CatalogError::Busy)
    ));
    let RootState::Published(transition) = catalog.status().await.unwrap().0.state else {
        panic!("expected published clear");
    };
    catalog
        .execute(
            clear.clone(),
            ManagementPrivilege::Clear,
            transition.complete_after_ms,
        )
        .await
        .unwrap();
    let active = catalog.status().await.unwrap().0.context;
    let operation = catalog
        .operation(clear.identity.operation)
        .await
        .unwrap()
        .unwrap();
    let task = GcRepository::new(store.clone())
        .admit_retired(&operation, now_ms(), GcLimits::default())
        .await
        .unwrap();
    (store, active, task)
}

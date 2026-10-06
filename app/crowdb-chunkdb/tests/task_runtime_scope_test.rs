// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_chunkdb::routing::{BindingCache, BindingTable};
use crowdb_chunkdb::task::{TaskExecutor, TaskManager, TaskStore};
use crowdb_protocol::chunk_domain::ChunkDomain;
use crowdb_protocol::chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup};
use crowdb_protocol::chunk_task::ChunkTaskState;
use crowdb_protocol::common::ChunkId;

#[path = "common/slot_groups.rs"]
mod slot_groups;
#[path = "common/task_execution.rs"]
mod task_execution;
use slot_groups::{finalize, TestGroups};
use task_execution::TestTaskHandler;

#[tokio::test]
async fn blocked_user_work_cannot_consume_system_capacity_or_execute_system_claims() {
    let test = TestGroups::start().await;
    let layout = ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: vec![ChunkStorageGroup {
            store_id: 0,
            group_id: 1,
        }],
    };
    let routes = BindingCache::new();
    routes
        .replace(BindingTable::new(layout.storage_map().unwrap()))
        .unwrap();
    let guard = Arc::new(RangeGuard::new());
    guard.install(&layout.service_map().unwrap(), 1).unwrap();
    let mut stores = Vec::new();
    let mut managers = Vec::new();
    let mut claims = Vec::new();
    for (purpose, domain) in [(1_u64, ChunkDomain::System), (5, ChunkDomain::UserData)] {
        let store = Arc::new(
            TaskStore::new(Arc::clone(&test.kv), routes.clone()).with_scope(Arc::clone(&guard), domain),
        );
        let manager = Arc::new(TaskManager::new(Arc::clone(&store), 1, 60_000));
        let mut task = finalize(ChunkId {
            high: purpose << 56,
            low: 1,
        });
        task.kind = 42;
        manager.admit(task).await.unwrap();
        let ready = store.scan_ready(100, 1).await.unwrap();
        claims.push(manager.claim(&ready[0], 100).await.unwrap().unwrap());
        stores.push(store);
        managers.push(manager);
    }
    let release = Arc::new(tokio::sync::Notify::new());
    let system_handler = TestTaskHandler::new(None);
    let user_handler = TestTaskHandler::new(Some(Arc::clone(&release)));
    let system =
        Arc::new(TaskExecutor::new(Arc::clone(&managers[0]), 1, vec![system_handler.clone()]).unwrap());
    let user = Arc::new(TaskExecutor::new(Arc::clone(&managers[1]), 1, vec![user_handler.clone()]).unwrap());
    assert!(user.execute(claims[0].clone()).await.is_err());
    assert_eq!(user_handler.calls.load(Ordering::SeqCst), 0);
    let user_claim = claims[1].clone();
    let worker = tokio::spawn({
        let user = Arc::clone(&user);
        async move { user.execute(user_claim).await }
    });
    tokio::time::timeout(Duration::from_secs(5), user_handler.started.notified())
        .await
        .unwrap();
    assert_eq!(user.available_capacity(), 0);
    assert_eq!(system.available_capacity(), 1);
    system.execute(claims[0].clone()).await.unwrap();
    assert_eq!(system_handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        stores[0]
            .get(&claims[0].task.partition_id, 42, &claims[0].task.task_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        ChunkTaskState::Completed
    );
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn restarted_owner_replaces_expired_claim_and_fences_old_execution_and_completion() {
    let test = TestGroups::start().await;
    let layout = ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: vec![ChunkStorageGroup {
            store_id: 0,
            group_id: 1,
        }],
    };
    let routes = BindingCache::new();
    routes
        .replace(BindingTable::new(layout.storage_map().unwrap()))
        .unwrap();
    let guard = Arc::new(RangeGuard::new());
    guard.install(&layout.service_map().unwrap(), 1).unwrap();
    let store = Arc::new(
        TaskStore::new(Arc::clone(&test.kv), routes.clone())
            .with_scope(Arc::clone(&guard), ChunkDomain::System),
    );
    let before = Arc::new(TaskManager::new(Arc::clone(&store), 1, 100));
    let mut task = finalize(ChunkId {
        high: 1 << 56,
        low: 1,
    });
    task.kind = 42;
    before.admit(task).await.unwrap();
    let index = store.scan_ready(100, 1).await.unwrap()[0];
    let stale = before.claim(&index, 100).await.unwrap().unwrap();
    let cold = Arc::new(TaskStore::new(Arc::clone(&test.kv), routes).with_scope(guard, ChunkDomain::System));
    let after = Arc::new(TaskManager::new(Arc::clone(&cold), 1, 100));
    after
        .recover_expired(&cold.scan_expired_leases(200, 1).await.unwrap()[0], 200)
        .await
        .unwrap();
    let current = after
        .claim(&cold.scan_ready(200, 1).await.unwrap()[0], 200)
        .await
        .unwrap()
        .unwrap();
    assert!(current.task.claim_generation > stale.task.claim_generation);
    let handler = TestTaskHandler::new(None);
    let executor = TaskExecutor::new(Arc::clone(&before), 1, vec![handler.clone()]).unwrap();
    assert!(executor.execute(stale.clone()).await.is_err());
    assert!(before.complete(&stale, 201).await.is_err());
    assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
    after.complete(&current, 201).await.unwrap();
}

#[path = "common/submission_epoch.rs"]
mod submission_epoch;

#[tokio::test]
async fn dynamic_epoch_regrant_revokes_task_renewal_and_completion_in_both_domains() {
    use crowdb_chunkdb::task::{TaskManagerError, TaskStoreError};
    use crowdb_protocol::chunk_slot::ChunkSlot;
    let test = TestGroups::start().await;
    let layout = ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: vec![ChunkStorageGroup {
            store_id: 0,
            group_id: 1,
        }],
    };
    let routes = BindingCache::new();
    routes
        .replace(BindingTable::new(layout.storage_map().unwrap()))
        .unwrap();
    for (purpose, domain) in [(1_u64, ChunkDomain::System), (5, ChunkDomain::UserData)] {
        let id = ChunkId {
            high: purpose << 56,
            low: 55,
        };
        let slot = ChunkSlot::for_chunk(&id);
        let guard = Arc::new(RangeGuard::new());
        let map = submission_epoch::TestEpochLayout::map(1, slot, 1, 1, 1);
        guard.install_dynamic(&map, 1).unwrap();
        let store = Arc::new(
            TaskStore::new(Arc::clone(&test.kv), routes.clone()).with_scope(Arc::clone(&guard), domain),
        );
        let manager = Arc::new(TaskManager::new(Arc::clone(&store), 1, 100));
        let mut task = finalize(id);
        task.kind = 42;
        manager.admit(task).await.unwrap();
        let stale = manager
            .claim(&store.scan_ready(100, 1).await.unwrap()[0], 100)
            .await
            .unwrap()
            .unwrap();
        guard
            .install_dynamic(&map.reassign(&[(slot, 1)]).unwrap(), 1)
            .unwrap();
        assert!(matches!(
            manager.renew(&stale, 150).await,
            Err(TaskManagerError::Store(TaskStoreError::Authority))
        ));
        assert!(matches!(
            manager.complete(&stale, 150).await,
            Err(TaskManagerError::Store(TaskStoreError::Authority))
        ));
        let handler = TestTaskHandler::new(None);
        let executor = TaskExecutor::new(Arc::clone(&manager), 1, vec![handler.clone()]).unwrap();
        assert!(executor.execute(stale).await.is_err());
        assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
        manager
            .recover_expired(&store.scan_expired_leases(200, 1).await.unwrap()[0], 200)
            .await
            .unwrap();
        let fresh = manager
            .claim(&store.scan_ready(200, 1).await.unwrap()[0], 200)
            .await
            .unwrap()
            .unwrap();
        executor.execute(fresh).await.unwrap();
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.get(&id, 42, &id).await.unwrap().unwrap().state,
            ChunkTaskState::Completed
        );
    }
}

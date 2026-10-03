// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunkdb::range_guard::{OwnedRange, RangeGuard};
use crowdb_chunkdb::routing::{BindingCache, BindingTable};
use crowdb_chunkdb::task::{TaskManager, TaskStore, TaskStoreError};
use crowdb_protocol::chunk_domain::ChunkDomain;
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotBootstrap, ChunkStorageGroup};
use crowdb_protocol::chunk_task::{ChunkTaskState, TASK_KIND_FINALIZE_CHUNK};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::ReadyChunkTaskKey;

#[path = "common/slot_groups.rs"]
mod slot_groups;
use slot_groups::{finalize, TestGroups};

fn routes() -> BindingCache {
    let layout = ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: (1..=3)
            .map(|group_id| ChunkStorageGroup {
                store_id: 0,
                group_id,
            })
            .collect(),
    };
    let routes = BindingCache::new();
    routes
        .replace(BindingTable::new(layout.storage_map().unwrap()))
        .unwrap();
    routes
}

fn guard() -> Arc<RangeGuard> {
    let guard = Arc::new(RangeGuard::new());
    guard.replace_for_tests(&[OwnedRange {
        start: 0,
        end: 511,
        sub_range_index: 0,
    }]);
    guard
}

fn chunk(purpose: u8, owned: bool) -> ChunkId {
    (1..10_000)
        .map(|low| ChunkId {
            high: u64::from(purpose) << 56,
            low,
        })
        .find(|id| (ChunkSlot::for_chunk(id).value() < 512) == owned)
        .unwrap()
}

#[tokio::test]
async fn scope_precedes_limit_and_delayed_work_does_not_hide_due_tasks() {
    let test = TestGroups::start().await;
    let bindings = routes();
    let writer = TaskStore::new(Arc::clone(&test.kv), bindings.clone());
    let authority = guard();
    let system = TaskStore::new(Arc::clone(&test.kv), bindings.clone())
        .with_scope(Arc::clone(&authority), ChunkDomain::System);
    let user = TaskStore::new(Arc::clone(&test.kv), bindings).with_scope(authority, ChunkDomain::UserData);
    let owned = chunk(5, true);
    // Fill more than one scan page with future high-priority work in the
    // same slot, followed by one eligible lower-priority task.
    for low in 1..=300 {
        let mut task = finalize(owned);
        task.task_id = ChunkId { high: 9, low };
        task.kind = TASK_KIND_FINALIZE_CHUNK + 1;
        task.eligible_at_ms = 10_000;
        writer.write_transition(None, &task).await.unwrap();
    }
    for id in [owned, chunk(1, true), chunk(5, false)] {
        let mut task = finalize(id);
        task.kind = TASK_KIND_FINALIZE_CHUNK + 1;
        task.priority = 0;
        writer.write_transition(None, &task).await.unwrap();
    }
    let ready = user.scan_ready(100, 1).await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].partition_id, owned);
    assert_eq!(
        system.scan_ready(100, 1).await.unwrap()[0].partition_id,
        chunk(1, true)
    );
}

#[tokio::test]
async fn domain_and_slot_authority_fence_claims_and_publication() {
    let test = TestGroups::start().await;
    let store =
        Arc::new(TaskStore::new(Arc::clone(&test.kv), routes()).with_scope(guard(), ChunkDomain::System));
    let manager = TaskManager::new(Arc::clone(&store), 1, 100);
    for id in [chunk(5, true), chunk(1, false)] {
        let task = finalize(id);
        assert!(matches!(
            store.write_transition(None, &task).await,
            Err(TaskStoreError::Authority)
        ));
        assert!(matches!(
            store.get(&id, task.kind, &id).await,
            Err(TaskStoreError::Authority)
        ));
        assert!(matches!(
            store.list_partition(&id).await,
            Err(TaskStoreError::Authority)
        ));
        let index = ReadyChunkTaskKey {
            priority_inverse: 0,
            eligible_at_ms: 100,
            partition_id: id,
            kind: task.kind,
            task_id: id,
        };
        assert!(manager.claim(&index, 100).await.is_err());
    }
}

#[tokio::test]
async fn finalize_and_expired_claims_are_isolated_and_empty_owner_has_no_work() {
    let test = TestGroups::start().await;
    let bindings = routes();
    let writer = TaskStore::new(Arc::clone(&test.kv), bindings.clone());
    for id in [chunk(1, true), chunk(5, true), chunk(1, false)] {
        let task = finalize(id);
        writer.write_transition(None, &task).await.unwrap();
        let mut running = task.clone();
        running.kind += 1;
        running.state = ChunkTaskState::Running;
        running.claim_owner = 1;
        running.claim_generation = 1;
        running.claim_deadline_ms = 100;
        writer.write_transition(None, &running).await.unwrap();
    }
    let scoped =
        TaskStore::new(Arc::clone(&test.kv), bindings.clone()).with_scope(guard(), ChunkDomain::System);
    let due = scoped.scan_finalize_due(100, 1).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].partition_id, chunk(1, true));
    let expired = scoped.scan_expired_leases(100, 1).await.unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].partition_id, chunk(1, true));
    let empty = TaskStore::new(Arc::clone(&test.kv), bindings)
        .with_scope(Arc::new(RangeGuard::new()), ChunkDomain::System);
    assert!(empty.scan_ready(u64::MAX, 1).await.unwrap().is_empty());
    assert!(empty.scan_finalize_due(u64::MAX, 1).await.unwrap().is_empty());
    assert!(empty.scan_expired_leases(u64::MAX, 1).await.unwrap().is_empty());
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::routing::{BindingCache, BindingTable};
use crowdb_chunkdb::task::TaskStore;
use crowdb_paxos_kv_client::ChunkSlotMapClient;
use crowdb_protocol::chunk_task::{ChunkTaskState, ChunkTaskValue};
use crowdb_protocol::common::ChunkId;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) async fn store() -> TaskStore {
    let kv = Arc::new(crate::common::hardware::kv_client());
    let maps = ChunkSlotMapClient::new(Arc::clone(&kv));
    let bindings = BindingCache::new();
    bindings
        .replace(BindingTable::new(maps.read_storage().await.unwrap()))
        .unwrap();
    TaskStore::new(kv, bindings)
}

pub(super) async fn completed(store: &TaskStore, id: ChunkId, kind: u16, task_id: ChunkId) -> ChunkTaskValue {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let task = store.get(&id, kind, &task_id).await.unwrap().unwrap();
        if task.state == ChunkTaskState::Completed {
            return task;
        }
        assert_ne!(task.state, ChunkTaskState::Failed, "{}", task.last_error);
        assert!(Instant::now() < deadline, "task did not complete: {task:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

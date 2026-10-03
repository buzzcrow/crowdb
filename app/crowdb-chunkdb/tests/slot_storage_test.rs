// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunkdb::routing::{BindingCache, BindingTable};
use crowdb_chunkdb::storage::ChunkStore;
use crowdb_chunkdb::task::TaskStore;
use crowdb_kv_client::{ChunkSlotMapClient, GetOutcome, ReadMode};
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotBootstrap, ChunkStorageGroup};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkType};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::key::{BinaryKey, ChunkTaskKey, FinalizeChunkTaskKey};

#[path = "common/slot_groups.rs"]
mod slot_groups;
use slot_groups::{finalize, TestGroups};

#[tokio::test]
async fn chunk_task_and_index_commit_together_only_in_selected_group() {
    let test = TestGroups::start().await;
    let layout = ChunkSlotBootstrap {
        service_instances: vec![11, 12, 13],
        storage_groups: (1..=3)
            .map(|group_id| ChunkStorageGroup {
                store_id: 0,
                group_id,
            })
            .collect(),
    };
    let maps = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    maps.initialize_layout(&layout).await.unwrap();
    let map = maps.read_storage().await.unwrap();
    let routes = BindingCache::new();
    routes.replace(BindingTable::new(map.clone())).unwrap();
    let chunks = ChunkStore::new(Arc::clone(&test.kv), routes.clone());
    let tasks = TaskStore::new(Arc::clone(&test.kv), routes);
    let mut covered = std::collections::HashSet::new();
    for low in 0..100 {
        let id = ChunkId { high: 1 << 56, low };
        let group = map.owner(ChunkSlot::for_chunk(&id)).group_id;
        if !covered.insert(group) {
            continue;
        }
        let chunk = Chunk {
            id: Some(id),
            modify_ts: 1,
            chunk_type: ChunkType::Wal as i32,
            state: ChunkState::Active as i32,
            ..Default::default()
        };
        let task = finalize(id);
        chunks
            .create_chunk_with_finalize_task(&chunk, &task)
            .await
            .unwrap();
        assert!(chunks
            .create_chunk_with_finalize_task(&chunk, &task)
            .await
            .is_err());
        assert_eq!(chunks.get_chunk(&id).await.unwrap(), chunk);
        assert_eq!(tasks.get(&id, task.kind, &id).await.unwrap(), Some(task.clone()));
        let mut chunk_key = b"/chunk/".to_vec();
        chunk_key.extend_from_slice(&id.high.to_be_bytes());
        chunk_key.extend_from_slice(&id.low.to_be_bytes());
        let keys = [
            chunk_key,
            ChunkTaskKey {
                partition_id: id,
                kind: task.kind,
                task_id: id,
            }
            .to_bytes(),
            FinalizeChunkTaskKey {
                expires_at_ms: task.eligible_at_ms,
                partition_id: id,
                task_id: id,
            }
            .to_bytes(),
        ];
        let mut revisions = Vec::new();
        for destination in 0..=3 {
            for key in &keys {
                match test
                    .kv
                    .get(0, destination, key, ReadMode::Linearizable, None)
                    .await
                    .unwrap()
                {
                    GetOutcome::Found { revision, .. } => {
                        assert_eq!(destination, group);
                        revisions.push(revision);
                    }
                    GetOutcome::NotFound => assert_ne!(destination, group),
                }
            }
        }
        assert_eq!(revisions.len(), 3);
        assert!(revisions.iter().all(|revision| *revision == revisions[0]));
    }
    assert_eq!(covered.len(), 3);
    // A cold service process needs only the remote map and records to recover.
    let cold = BindingCache::new();
    cold.replace(BindingTable::new(maps.read_storage().await.unwrap()))
        .unwrap();
    assert_eq!(
        ChunkStore::new(Arc::clone(&test.kv), cold)
            .list_chunks(None, 100)
            .await
            .unwrap()
            .len(),
        3
    );
}

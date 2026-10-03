// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use common::cluster::{seed_hardware, wait_for_leader, ChunkdbHarness, DiskdbServer, KvCluster};
use crowdb_chunkdb::lifecycle::LifecycleHandler;
use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_chunkdb::routing::{BindingCache, BindingTable};
use crowdb_chunkdb::storage::ChunkStore;
use crowdb_chunkdb::task::TaskStore;
use crowdb_kv_client::{ChunkSlotMapClient, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotBootstrap, ChunkStorageGroup};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::chunkdb::rpc::{ChunkType, StripType};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::key::{BinaryKey, ChunkTaskKey, FinalizeChunkTaskKey};

async fn assert_records(
    kv: &CrowdbKvClient,
    chunks: &ChunkStore,
    tasks: &TaskStore,
    id: ChunkId,
    group: u64,
) {
    let chunk = chunks.get_chunk(&id).await.unwrap();
    assert_eq!(u64::try_from(chunk.chunk_type).unwrap(), id.high >> 56);
    let task = tasks
        .list_partition(&id)
        .await
        .unwrap()
        .pop()
        .expect("finalize task");
    let mut chunk_key = b"/chunk/".to_vec();
    chunk_key.extend_from_slice(&id.high.to_be_bytes());
    chunk_key.extend_from_slice(&id.low.to_be_bytes());
    let keys = [
        chunk_key,
        ChunkTaskKey {
            partition_id: id,
            kind: task.kind,
            task_id: task.task_id,
        }
        .to_bytes(),
        FinalizeChunkTaskKey {
            expires_at_ms: task.eligible_at_ms,
            partition_id: id,
            task_id: task.task_id,
        }
        .to_bytes(),
    ];
    let mut revisions = Vec::new();
    for destination in 0..=3 {
        for key in &keys {
            match kv
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_preserve_all_purposes_across_independent_maps_and_paxos_failover() {
    let mut cluster = KvCluster::start_with_groups(&[0, 1, 2, 3]).await;
    let kv = cluster.make_crowdb_client();
    let maps = ChunkSlotMapClient::new(Arc::clone(&kv));
    maps.initialize_layout(&ChunkSlotBootstrap {
        service_instances: vec![11, 12, 13],
        storage_groups: (1..=3)
            .map(|group_id| ChunkStorageGroup {
                store_id: 0,
                group_id,
            })
            .collect(),
    })
    .await
    .unwrap();
    let service = maps.read_service().await.unwrap();
    let storage = maps.read_storage().await.unwrap();
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let bindings = BindingCache::new();
    bindings.replace(BindingTable::new(storage.clone())).unwrap();
    let chunks = Arc::new(ChunkStore::new(Arc::clone(&kv), bindings.clone()));
    let tasks = TaskStore::new(Arc::clone(&kv), bindings);
    let mut allocated = Vec::new();
    for instance in [11, 12, 13] {
        let guard = Arc::new(RangeGuard::new());
        guard.install(&service, instance).unwrap();
        let handler = LifecycleHandler::new(
            Arc::clone(&chunks),
            Arc::clone(&harness.allocator),
            harness.topology.clone(),
        )
        .with_range_guard(guard);
        let mut covered = HashSet::new();
        for purpose in [
            ChunkType::Wal,
            ChunkType::BtreePage,
            ChunkType::PageIndex,
            ChunkType::Stream,
            ChunkType::S3,
            ChunkType::IcebergTable,
        ] {
            let owner = if matches!(purpose, ChunkType::Wal | ChunkType::Stream) {
                StreamName {
                    high: 0,
                    low: instance,
                }
                .chunk_owner_key()
            } else {
                Vec::new()
            };
            let chunk = handler
                .allocate_chunk_owned(None, 1, 0, StripType::Mirror, 0, 0, 1, purpose, 17, 60_000, owner)
                .await
                .unwrap();
            let id = chunk.id.unwrap();
            let slot = ChunkSlot::for_chunk(&id);
            assert_eq!(service.owner(slot), instance);
            let group = storage.owner(slot).group_id;
            covered.insert(group);
            assert_records(&kv, &chunks, &tasks, id, group).await;
            allocated.push((id, group));
        }
        // Each service band spans all three persistent groups, irrespective of
        // the random allocation sample's distribution.
        let destinations: HashSet<_> = service
            .bindings()
            .iter()
            .find(|binding| binding.owner == instance)
            .unwrap()
            .slots
            .slots()
            .map(|slot| storage.owner(slot).group_id)
            .collect();
        assert_eq!(destinations.len(), 3);
        assert!(!covered.is_empty());
    }
    let leader = wait_for_leader(&cluster.nodes, 1, Duration::from_secs(30)).await;
    let mut stopped = cluster.nodes.remove(leader);
    stopped.stop();
    for group in 0..=3 {
        wait_for_leader(&cluster.nodes, group, Duration::from_secs(30)).await;
    }
    let cold = BindingCache::new();
    cold.replace(BindingTable::new(maps.read_storage().await.unwrap()))
        .unwrap();
    let cold_chunks = ChunkStore::new(Arc::clone(&kv), cold.clone());
    let cold_tasks = TaskStore::new(Arc::clone(&kv), cold);
    for (id, group) in allocated {
        assert_records(&kv, &cold_chunks, &cold_tasks, id, group).await;
    }
    assert_eq!(maps.read_service().await.unwrap().bindings(), service.bindings());
}

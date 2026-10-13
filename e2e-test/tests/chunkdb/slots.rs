// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::{control, management};
use crowdb_chunkdb::routing::{BindingCache, BindingTable};
use crowdb_chunkdb::storage::ChunkStore;
use crowdb_chunkdb::task::TaskStore;
use crowdb_chunkdb_client::ChunkdbRpcTransport;
use crowdb_kv_client::{ChunkSlotMapClient, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::chunk_slot::ChunkSlot;
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::chunkdb::rpc::{AllocateChunkRequest, ChunkType, StripType};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::key::{BinaryKey, ChunkTaskKey, FinalizeChunkTaskKey};
use std::collections::HashSet;
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires three packaged Chunkdb owners and three KV voters"]
async fn three_service_owners_preserve_all_purposes_across_paxos_failover() {
    let ips = management::addresses(7)[..3].to_vec();
    let kv = Arc::new(CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
        ips.iter().map(|ip| format!("http://{ip}:7000")).collect(),
    )));
    let maps = ChunkSlotMapClient::new(Arc::clone(&kv));
    let service = maps.read_service().await.unwrap();
    let storage = maps.read_storage().await.unwrap();
    let bindings = BindingCache::new();
    bindings.replace(BindingTable::new(storage.clone())).unwrap();
    let chunks = ChunkStore::new(Arc::clone(&kv), bindings.clone());
    let tasks = TaskStore::new(Arc::clone(&kv), bindings);
    let path = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap())
        .join("chunkdb-endpoints.json");
    let endpoints: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let transport = ChunkdbRpcTransport::new();
    let mut allocated = Vec::new();
    for endpoint in endpoints["nodes"].as_array().unwrap() {
        let instance = endpoint["instance_id"].as_u64().unwrap();
        let address = format!("{}:7302", endpoint["ip"].as_str().unwrap());
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
        allocated.extend(
            allocate_owner(
                &transport, &address, instance, &service, &storage, &kv, &chunks, &tasks,
            )
            .await,
        );
    }
    assert_eq!(allocated.len(), 18);
    let http = management::client();
    let leader = management::leader(&http, &ips, 1).await;
    assert_eq!(control::node("kill", leader).await["Running"], false);
    let survivors: Vec<_> = ips
        .into_iter()
        .enumerate()
        .filter(|(index, _)| *index != leader)
        .map(|(_, ip)| ip)
        .collect();
    for group in 0..=3 {
        management::leader(&http, &survivors, group).await;
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

#[allow(clippy::too_many_arguments)]
async fn allocate_owner(
    transport: &ChunkdbRpcTransport,
    address: &str,
    instance: u64,
    service: &crowdb_protocol::chunk_slot::ChunkSlotMap<u64>,
    storage: &crowdb_protocol::chunk_slot::ChunkSlotMap<crowdb_protocol::chunk_slot::ChunkStorageGroup>,
    kv: &CrowdbKvClient,
    chunks: &ChunkStore,
    tasks: &TaskStore,
) -> Vec<(ChunkId, u64)> {
    let mut allocated = Vec::new();
    for purpose in [
        ChunkType::Wal,
        ChunkType::BtreePage,
        ChunkType::PageIndex,
        ChunkType::Stream,
        ChunkType::S3,
        ChunkType::IcebergTable,
    ] {
        let owner_key = if matches!(purpose, ChunkType::Wal | ChunkType::Stream) {
            StreamName {
                high: 0,
                low: instance,
            }
            .chunk_owner_key()
        } else {
            Vec::new()
        };
        let chunk = transport
            .send_allocate_chunk(
                address,
                &AllocateChunkRequest {
                    write_granularity: 1,
                    strip_count: 1,
                    strip_type: StripType::Mirror as i32,
                    copy_count: 2,
                    chunk_type: purpose as i32,
                    writer_epoch: 17,
                    writer_lease_ms: 60_000,
                    owner_key,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .chunk
            .unwrap();
        let id = chunk.id.unwrap();
        let slot = ChunkSlot::for_chunk(&id);
        assert_eq!(service.owner(slot), instance);
        assert_eq!(id.high >> 56, u64::try_from(purpose as i32).unwrap());
        let safety = chunk.strips[0].placement_assessment.as_ref().unwrap();
        assert!(safety.rack_protected && safety.node_protected && safety.disk_protected);
        let group = storage.owner(slot).group_id;
        assert_records(kv, chunks, tasks, id, group).await;
        allocated.push((id, group));
    }
    allocated
}

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

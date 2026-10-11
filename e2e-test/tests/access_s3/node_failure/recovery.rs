// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_kv_client::{HardwareClient, RangeBindingClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{ChunkType, ListChunksRequest, QueryChunkRequest, Strip, StripType};
use crowdb_protocol::common::{ChunkId, HwStatus};
use std::sync::Arc;
use std::time::Duration;

pub(super) async fn degraded_chunks(addresses: &[String]) -> Vec<ChunkId> {
    let transport = ChunkdbRpcTransport::new();
    let mut chunks = Vec::new();
    for index in [6, 8] {
        let mut start_token = None;
        loop {
            let listed = transport
                .send_list_chunks(
                    &format!("{}:7302", addresses[index]),
                    &ListChunksRequest {
                        start_token,
                        max_keys: 1024,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let next = listed.next_token;
            chunks.extend(listed.chunks);
            if next.is_none() {
                break;
            }
            assert_ne!(next, start_token, "chunk listing must advance");
            start_token = next;
        }
    }
    let degraded: Vec<_> = chunks
        .into_iter()
        .filter(|chunk| chunk.chunk_type == ChunkType::S3 as i32)
        .filter(|chunk| {
            chunk.strips.iter().any(|strip| {
                strip.strip_type == StripType::Ec as i32
                    && matches!(strip.strip.as_ref(), Some(Strip::EcStrip(_)))
                    && strip.placement_repair_required
            })
        })
        .filter_map(|chunk| chunk.id)
        .collect();
    assert!(
        !degraded.is_empty(),
        "outage write did not persist degraded S3 EC placement"
    );
    degraded
}

pub(super) async fn restart() {
    let hardware = HardwareClient::new(super::kv_client());
    hardware.set_node_status(2, 2, HwStatus::Up).await.unwrap();
    for index in (0..14).rev() {
        crate::common::control::node("stop", index).await;
    }
    for index in 0..14 {
        crate::common::control::node("start", index).await;
    }
    let addresses = crate::common::management::addresses(14);
    let http = reqwest::Client::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if http
                .get(format!("http://{}:7501/_crowdb/health/ready", addresses[13]))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("restarted S3 readiness");
}

pub(super) async fn repaired(chunks: &[ChunkId]) {
    let kv = Arc::new(super::kv_client());
    let client = ChunkdbClient::new(
        ServiceRegistryClient::from_shared(Arc::clone(&kv)),
        Arc::new(ChunkdbRpcTransport::new()),
    )
    .with_range_binding(RangeBindingClient::from_shared(kv));
    client.refresh_routes().await.unwrap();
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let mut repaired = true;
            for id in chunks {
                let chunk = client
                    .query_chunk(QueryChunkRequest { chunk_id: Some(*id) })
                    .await
                    .unwrap()
                    .chunk
                    .unwrap();
                repaired &= chunk
                    .strips
                    .iter()
                    .filter(|strip| strip.strip_type == StripType::Ec as i32)
                    .all(|strip| {
                        !strip.placement_repair_required
                            && strip.placement_assessment.as_ref().is_some_and(|assessment| {
                                assessment.rack_protected
                                    && assessment.node_protected
                                    && assessment.disk_protected
                            })
                    });
            }
            if repaired {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("original 25-second placement repair bound after restart");
}

pub(super) async fn ownership(
    kv: Arc<crowdb_kv_client::CrowdbKvClient>,
    hardware: HardwareClient,
    failed_node: u64,
) {
    let bindings = RangeBindingClient::from_shared(kv.clone());
    let registry = ServiceRegistryClient::from_shared(kv);
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            if bindings.refresh().await.is_err() {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            let snapshot = bindings.snapshot();
            let instances = registry.read_all_diskdb_instances().await.unwrap();
            let owners = hardware.list_owners().await.unwrap();
            let groups = hardware.list_disk_groups().await.unwrap();
            let slots_recovered = crowdb_protocol::chunk_slot::ChunkSlot::all().all(|slot| {
                let owners: Vec<_> = snapshot
                    .iter()
                    .filter(|binding| binding.slots.contains(slot))
                    .collect();
                owners.len() == 1 && owners[0].instance_id != 20000 + failed_node - 1
            });
            let disks_recovered = !instances.iter().any(|(id, _)| *id == 10000 + failed_node)
                && groups
                    .iter()
                    .filter(|group| group.node_id != failed_node)
                    .all(|group| {
                        owners.iter().any(|owner| {
                            owner.dg_id == group.dg_id
                                && instances.iter().any(|(id, value)| {
                                    *id == owner.instance_id
                                        && value
                                            .extra
                                            .as_ref()
                                            .and_then(|extra| extra.diskdb.as_ref())
                                            .is_some_and(|diskdb| diskdb.owned_dg_ids.contains(&group.dg_id))
                                })
                        })
                    });
            if slots_recovered && disks_recovered {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("original 25-second range and Diskdb ownership recovery bound");
}

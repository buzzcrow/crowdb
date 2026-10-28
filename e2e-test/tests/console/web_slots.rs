// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv_client::{ChunkSlotMapClient, ClientConfig, PaxosKvClient};
use crowdb_protocol::chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup};
use reqwest::StatusCode;
use serde_json::Value;
use std::sync::Arc;

async fn get(origin: &str, query: &str) -> (StatusCode, Value) {
    let response = reqwest::Client::new()
        .get(format!("{origin}/api/chunk-slots?{query}"))
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.json().await.unwrap())
}

#[tokio::test]
#[ignore = "requires owned release Web and KV containers"]
async fn owner_windows_keep_disjoint_slots_generations_and_exact_ids() {
    let cluster = super::fixture::web_kv().await;
    let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    )));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    let addresses = crate::common::management::addresses(5);
    let state = format!("http://{}:9090", addresses[3]);
    let bootstrap = ChunkSlotBootstrap {
        service_instances: vec![1, u64::MAX],
        storage_groups: vec![
            ChunkStorageGroup {
                store_id: u64::MAX,
                group_id: 1,
            },
            ChunkStorageGroup {
                store_id: 0,
                group_id: 2,
            },
        ],
    };
    let maps = ChunkSlotMapClient::new(kv);
    maps.initialize_service(&bootstrap.service_map().unwrap())
        .await
        .unwrap();
    maps.initialize_storage(&bootstrap.storage_map().unwrap())
        .await
        .unwrap();
    let query = format!("layer=service&instance_id={}", u64::MAX);
    let (status, first) = get(&state, &query).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["owned_count"], 512);
    assert_eq!(first["slots"].as_array().unwrap().len(), 32);
    assert_eq!(first["generation"], "1");
    let last = first["slots"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .as_u64()
        .unwrap();
    assert_eq!(first["next"], last);
    let (status, next) = get(&state, &format!("{query}&after={last}&generation=1")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(next["slots"]
        .as_array()
        .unwrap()
        .iter()
        .all(|slot| slot.as_u64().unwrap() > last));
    assert_eq!(
        get(&state, &format!("{query}&generation=2")).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        get(&state, &format!("{query}&limit=101")).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get(&state, &format!("{query}&after=1024")).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get(&state, "layer=storage&store_id=0&group_id=0").await.0,
        StatusCode::BAD_REQUEST
    );
    let (status, storage) = get(&state, &format!("layer=storage&store_id={}&group_id=1", u64::MAX)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(storage["owned_count"], 512);
    assert_bitmaps(&state, &bootstrap).await;
    let (_, missing) = get(&state, "layer=service&instance_id=7").await;
    assert_eq!(missing["assigned"], false);
    assert_eq!(missing["owned_count"], 0);
    assert!(missing["next"].is_null());
}

async fn assert_bitmaps(state: &str, bootstrap: &ChunkSlotBootstrap) {
    for (layer, expected) in [
        (
            "service",
            bootstrap
                .service_map()
                .unwrap()
                .bindings()
                .iter()
                .map(|binding| (binding.owner.to_string(), binding.slots.clone()))
                .collect::<Vec<_>>(),
        ),
        (
            "storage",
            bootstrap
                .storage_map()
                .unwrap()
                .bindings()
                .iter()
                .map(|binding| {
                    (
                        format!("{}/{}", binding.owner.store_id, binding.owner.group_id),
                        binding.slots.clone(),
                    )
                })
                .collect(),
        ),
    ] {
        let (status, bitmap) = get(state, &format!("layer={layer}&view=bitmap")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(bitmap["generation"], "1");
        assert_eq!(bitmap["owners"].as_array().unwrap().len(), 1024);
        for (owner, slots) in expected {
            for slot in slots.slots() {
                assert_eq!(bitmap["owners"][slot.value() as usize], owner);
            }
        }
        assert_eq!(
            get(state, &format!("layer={layer}&view=bitmap&generation=2"))
                .await
                .0,
            StatusCode::CONFLICT
        );
    }
}

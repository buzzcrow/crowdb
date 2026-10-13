// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor, PartitionArtifact,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::key::{ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, TextKey};
use reqwest::StatusCode;
use serde_json::Value;
use std::sync::Arc;

async fn get(origin: &str, query: &str) -> (StatusCode, Value) {
    let response = reqwest::Client::new()
        .get(format!("{origin}/api/chunk-kv/catalog{query}"))
        .send()
        .await
        .unwrap();
    (response.status(), response.json().await.unwrap())
}

async fn runtime_get(origin: &str, query: &str) -> StatusCode {
    reqwest::Client::new().get(format!("{origin}/api/chunk-kv/runtime?page=0&offset=0&generation=9&id=ffffffffffffffff0000000000000001&{query}"))
        .send().await.unwrap().status()
}
#[tokio::test]
#[ignore = "requires owned release Web and KV containers"]
async fn published_owner_change_invalidates_saved_catalog_and_runtime_cursors() {
    let cluster = super::fixture::web_kv().await;
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    )));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    let addresses = crate::common::management::addresses(5);
    let app = format!("http://{}:9090", addresses[3]);
    let (mut head, mut page) = catalog_fixture();
    for generation in [9, 10] {
        page.generation = generation;
        if generation == 10 {
            page.entries[0].owner.instance_id = 42;
            page.entries[0].owner.rpc_endpoint = "127.0.0.1:42000".into();
        }
        page.seal().unwrap();
        head.generation = generation;
        head.previous_generation = Some(generation - 1);
        head.pages[0].page_generation = generation;
        head.pages[0].page_checksum = page.checksum;
        head.seal().unwrap();
        kv.put(
            0,
            0,
            ChunkKvRangeCatalogPageKey {
                generation,
                page_index: 0,
            }
            .to_path()
            .as_bytes(),
            &serde_json::to_vec(&page).unwrap(),
            None,
        )
        .await
        .unwrap();
        kv.put(
            0,
            0,
            ChunkKvRangeCatalogHeadKey.to_path().as_bytes(),
            &serde_json::to_vec(&head).unwrap(),
            None,
        )
        .await
        .unwrap();
        let (status, observed) = get(&app, "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(observed["generation"], generation.to_string());
        assert_eq!(
            observed["entries"][0]["owner_id"],
            page.entries[0].owner.instance_id.to_string()
        );
    }
    assert_eq!(
        get(&app, "?generation=9&offset=100").await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        runtime_get(&app, "epoch=18446744073709551615").await,
        StatusCode::CONFLICT
    );
}

fn catalog_fixture() -> (ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage) {
    let entries = (0..105u8)
        .map(|index| ChunkKvRangeCatalogEntry {
            partition_id: Id128 {
                high: u64::MAX,
                low: u64::from(index) + 1,
            },
            range: KeyRange {
                start: if index == 0 { vec![] } else { vec![index] },
                end: (index < 104).then(|| vec![index + 1]),
            },
            owner: OwnerDescriptor {
                instance_id: u64::MAX,
                rpc_endpoint: "127.0.0.1:41000".into(),
            },
            owner_epoch: u64::MAX,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: u64::from(index) + 1,
                stream_name: StreamName {
                    high: u64::MAX,
                    low: u64::from(index) + 1,
                },
                tail_overlay: None,
            },
            transition_id: None,
        })
        .collect();
    let mut page = ChunkKvRangeCatalogPage {
        generation: 9,
        page_index: 0,
        entries,
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 9,
        previous_generation: Some(8),
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 9,
            page_index: 0,
            first_key: vec![],
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    (head, page)
}

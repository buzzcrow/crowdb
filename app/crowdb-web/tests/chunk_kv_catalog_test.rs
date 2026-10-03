// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor, PartitionArtifact,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::key::{ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, TextKey};
use crowdb_web::{router, AppState};
use serde_json::Value;
use tower::ServiceExt;

async fn get(app: axum::Router, query: &str) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/chunk-kv/catalog{query}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn catalog_windows_pin_generation_validate_checksums_and_keep_exact_ids() {
    let cluster = crowdb_test_harness::cluster::KvCluster::start().await;
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    )));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    let state = AppState::new(cluster.mgmt_endpoints.clone());
    *state.kv_client.write().await = Some(Arc::clone(&kv));
    let app = router(state);
    assert_eq!(get(app.clone(), "?page=1").await.0, StatusCode::BAD_REQUEST);
    assert_eq!(get(app.clone(), "").await.0, StatusCode::NOT_FOUND);
    let (head, mut page) = catalog_fixture();
    let page_key = ChunkKvRangeCatalogPageKey {
        generation: 9,
        page_index: 0,
    }
    .to_path();
    let head_key = ChunkKvRangeCatalogHeadKey.to_path();
    kv.put(
        0,
        0,
        page_key.as_bytes(),
        &serde_json::to_vec(&page).unwrap(),
        None,
    )
    .await
    .unwrap();
    kv.put(
        0,
        0,
        head_key.as_bytes(),
        &serde_json::to_vec(&head).unwrap(),
        None,
    )
    .await
    .unwrap();
    let (status, result) = get(app.clone(), "").await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["entries"].as_array().unwrap().len(), 100);
    assert_eq!(result["next"]["offset"], 100);
    assert_eq!(result["entries"][0]["epoch"], u64::MAX.to_string());
    assert_eq!(
        result["entries"][0]["artifact"]["stream_name"]["high"],
        u64::MAX.to_string()
    );
    let (status, result) = get(app.clone(), "?generation=9&offset=100").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["entries"].as_array().unwrap().len(), 5);
    assert!(result["next"].is_null());
    assert_eq!(
        get(app.clone(), "?generation=8&offset=100").await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        get(app.clone(), "?generation=9&offset=105").await.0,
        StatusCode::BAD_REQUEST
    );
    page.entries[0].owner_epoch = 1;
    kv.put(
        0,
        0,
        page_key.as_bytes(),
        &serde_json::to_vec(&page).unwrap(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(get(app, "").await.0, StatusCode::BAD_GATEWAY);
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

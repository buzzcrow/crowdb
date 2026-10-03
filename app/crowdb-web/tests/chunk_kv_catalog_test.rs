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
    let app = router(state.clone());
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
    verify_runtime(&state, &app).await;
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

async fn runtime_get(app: axum::Router, query: &str) -> StatusCode {
    app.oneshot(
        Request::get(format!(
            "/api/chunk-kv/runtime?page=0&offset=0&generation=9&id=ffffffffffffffff0000000000000001&{query}"
        ))
        .body(Body::empty())
        .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

async fn verify_runtime(state: &AppState, app: &axum::Router) {
    use axum::{
        extract::{Query, State},
        routing::get,
        Json,
    };
    use crowdb_console_shared::config::{ServerEntry, ServiceType};
    use std::sync::atomic::{AtomicU8, Ordering};
    let query = format!("epoch={}", u64::MAX);
    assert_eq!(runtime_get(app.clone(), &query).await, StatusCode::NOT_FOUND);
    assert_eq!(runtime_get(app.clone(), "epoch=1").await, StatusCode::CONFLICT);
    let mode = Arc::new(AtomicU8::new(0));
    let owner = axum::Router::new()
        .route(
            "/partitions/:id/observation",
            get(|State(mode): State<Arc<AtomicU8>>, Query(query): Query<std::collections::HashMap<String, String>>| async move {
                let mut value = serde_json::json!({ "partition_id":"ffffffffffffffff0000000000000001",
            "catalog_generation":"9", "owner_epoch":u64::MAX.to_string(), "instance_id":u64::MAX.to_string(),
            "tree_id":"1", "stream_id":"ffffffffffffffff0000000000000001",
            "journal":{"generation":"17", "offset":query.get("stream_offset").unwrap().parse::<usize>().unwrap(), "extent_pages":[]}});
                match mode.load(Ordering::Acquire) {
                    1 => value["instance_id"] = "wrong-owner".into(),
                    2 => value["stream_id"] = "wrong-stream".into(),
                    3 => value["oversized"] = "x".repeat(65_537).into(),
                    4 => value["journal"]["offset"] = 0.into(),
                    _ => (),
                }
                Json(value)
            }),
        )
        .with_state(mode.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, owner).await.unwrap();
    });
    let mut entry = ServerEntry::new("test-chunk-kv", &origin);
    entry.service_type = ServiceType::ChunkKv;
    entry.rpc_url = Some("127.0.0.1:41000".into());
    state.config.write().unwrap().add_server(entry).unwrap();
    assert_eq!(runtime_get(app.clone(), &query).await, StatusCode::OK);
    let continuation = format!("{query}&stream_generation=17&stream_offset=100");
    assert_eq!(runtime_get(app.clone(), &continuation).await, StatusCode::OK);
    assert_eq!(
        runtime_get(app.clone(), &format!("{query}&stream_offset=100")).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        runtime_get(
            app.clone(),
            &format!("{query}&stream_generation=16&stream_offset=100")
        )
        .await,
        StatusCode::CONFLICT
    );
    mode.store(4, Ordering::Release);
    assert_eq!(
        runtime_get(app.clone(), &continuation).await,
        StatusCode::CONFLICT
    );
    mode.store(1, Ordering::Release);
    assert_eq!(runtime_get(app.clone(), &query).await, StatusCode::CONFLICT);
    mode.store(2, Ordering::Release);
    assert_eq!(runtime_get(app.clone(), &query).await, StatusCode::CONFLICT);
    mode.store(3, Ordering::Release);
    assert_eq!(runtime_get(app.clone(), &query).await, StatusCode::BAD_GATEWAY);
    mode.store(0, Ordering::Release);
    state.config.write().unwrap().servers.clear();
    verify_discovered_runtime(state, app, &query, &origin).await;
    server.abort();
}

async fn verify_discovered_runtime(state: &AppState, app: &axum::Router, query: &str, origin: &str) {
    let client = state.kv_client().await;
    let key = crowdb_protocol::key::InstanceKey {
        service: "chunk-kv".into(),
        instance_id: u64::MAX,
    }
    .to_path();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let base = serde_json::json!({ "instance_id":u64::MAX, "rpc_endpoint":"127.0.0.1:41000", "last_heartbeat_ms":u64::try_from(now).unwrap(),
        "extra":{"chunk_kv":{"node_id":7,"http_endpoint":origin,"capacity_bytes":0,"durable_bytes":0,"request_rate":0,"hosted":[]}} });
    for (field, changed, expected) in [
        ("", Value::Null, StatusCode::OK),
        ("rpc_endpoint", "127.0.0.1:41001".into(), StatusCode::CONFLICT),
        ("last_heartbeat_ms", 0.into(), StatusCode::CONFLICT),
        ("instance_id", 7.into(), StatusCode::CONFLICT),
        ("extra", Value::Null, StatusCode::NOT_FOUND),
        ("padding", "x".repeat(256 * 1024).into(), StatusCode::BAD_GATEWAY),
    ] {
        let mut value = base.clone();
        if !field.is_empty() {
            value[field] = changed;
        }
        client
            .put(0, 0, key.as_bytes(), &serde_json::to_vec(&value).unwrap(), None)
            .await
            .unwrap();
        assert_eq!(runtime_get(app.clone(), query).await, expected, "{field}");
    }
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

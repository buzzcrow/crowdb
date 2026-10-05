// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Actual large extent directory through the owned Chunk-KV process chain.

use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogMap, ChunkKvRangeCatalogSource, ChunkKvRpcTransport, ClientConfig,
    Group0ChunkKvRangeCatalogSource,
};
use crowdb_web::AppState;
use serde_json::Value;
use std::sync::Arc;

pub(super) struct TestNativeJournal;

impl TestNativeJournal {
    pub(super) async fn seed(app: &axum::Router, state: &AppState) {
        configure_geometry(app, state).await;
        let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(
            state.kv_client().await,
        ));
        let client = ChunkKvClient::new(
            ClientConfig::default(),
            source.clone(),
            Arc::new(ChunkKvRpcTransport::new(64, 1, 2)),
        )
        .unwrap();
        wait_initial_split(&source).await;
        let key = b"native-journal-window".to_vec();
        let value: Vec<_> = (0..12).flat_map(super::native_load::value).collect();
        // Each 768-KiB mutation produces thirteen bounded stream frames.
        for _ in 0..8 {
            assert!(client
                .put(key.clone(), value.clone())
                .await
                .unwrap()
                .result
                .is_ok());
        }
        let result = client.get(key, None).await.unwrap();
        let crowdb_protocol::chunk_kv::OperationResult::Value(Some(record)) = result.result.unwrap() else {
            panic!("large journal lost its actual value");
        };
        assert_eq!(record.value, value);
        verify_directory(app).await;
    }
}

async fn configure_geometry(app: &axum::Router, state: &AppState) {
    for node in 1..=3 {
        let service = format!("chunk-kv-{node}");
        super::call(
            app,
            "POST",
            &format!("/api/services/{service}/stop"),
            serde_json::json!({}),
        )
        .await;
        let root = state.node_workspace_dir(node).join("services").join(&service);
        let workspaces: Vec<_> = std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(workspaces.len(), 1, "fixture must own one launch per service");
        let path = workspaces[0].join("service.toml");
        let config = std::fs::read_to_string(&path).unwrap();
        assert_eq!(config.matches("[storage]\n").count(), 1);
        assert!(!config.contains("stream_extent_page_entries"));
        // Change only physical stream/directory geometry. Catalog identities,
        // payloads and published extent pages are produced by native owners.
        std::fs::write(
            path,
            config.replace(
                "[storage]\n",
                "[storage]\nstream_chunk_capacity_bytes = 1048576\nstream_extent_page_entries = 1\n",
            ),
        )
        .unwrap();
        super::call(
            app,
            "POST",
            &format!("/api/services/{service}/restart"),
            serde_json::json!({}),
        )
        .await;
    }
}

async fn verify_directory(app: &axum::Router) {
    let catalog = super::call(app, "GET", "/api/chunk-kv/catalog?page=0&offset=0", Value::Null).await;
    let mut found = false;
    for partition in catalog["entries"].as_array().unwrap() {
        let path = format!(
            "/api/chunk-kv/runtime?id={}&epoch={}&generation={}&page=0&offset=0",
            partition["id"].as_str().unwrap(),
            partition["epoch"].as_str().unwrap(),
            catalog["generation"].as_str().unwrap()
        );
        let observed = super::call(app, "GET", &path, Value::Null).await;
        if observed["journal"]["next_offset"] == 100 {
            assert_eq!(observed["journal"]["extent_pages"].as_array().unwrap().len(), 100);
            found = true;
            break;
        }
    }
    assert!(
        found,
        "actual writes must publish more than 100 extent page fences"
    );
}

async fn wait_initial_split(source: &Group0ChunkKvRangeCatalogSource) {
    let started = std::time::Instant::now();
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        interval.tick().await;
        let (head, pages) = source.load().await.unwrap();
        let catalog = ChunkKvRangeCatalogMap::decode(&head, &pages).unwrap();
        let owners: std::collections::BTreeSet<_> = catalog
            .entries()
            .iter()
            .map(|entry| entry.owner.instance_id)
            .collect();
        // First placement changes the owning stream. Produce the large current
        // directory after that real handoff rather than pinning its source.
        if catalog.entries().len() >= 2
            && owners.len() >= 2
            && catalog.entries().iter().all(|entry| {
                entry.transition_id.is_none()
                    && entry.artifact.tail_overlay.is_none()
                    && entry.state == crowdb_protocol::chunk_kv::ChunkKvRangeCatalogPartitionState::Serving
            })
        {
            eprintln!(
                "[PHASE] initial native Journal split settled: {}ms",
                started.elapsed().as_millis()
            );
            return;
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(60),
            "native initial split did not settle before directory workload"
        );
    }
}

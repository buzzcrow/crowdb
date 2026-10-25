// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Actual ordered Stream chunk records through the owned Chunk-KV process chain.

use super::native_context::TestNativeContext;
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogMap, ChunkKvRangeCatalogSource, ChunkKvRpcTransport, ClientConfig,
    Group0ChunkKvRangeCatalogSource,
};
use reqwest::Method;
use serde_json::Value;
use std::sync::Arc;

pub(super) struct TestNativeJournal;

impl TestNativeJournal {
    pub(super) async fn seed(context: &TestNativeContext) {
        // Restart all owners to prove geometry survives committed remote intents.
        for node in 1..=3 {
            context
                .api(
                    Method::POST,
                    &format!("/api/services/chunk-kv-{node}/restart"),
                    serde_json::json!({}),
                    200,
                )
                .await;
        }
        let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(context.kv.clone()));
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
        verify_chunk_records(context).await;
    }
}

async fn verify_chunk_records(context: &TestNativeContext) {
    let catalog = context
        .api(
            Method::GET,
            "/api/chunk-kv/catalog?page=0&offset=0",
            Value::Null,
            200,
        )
        .await;
    let mut found = false;
    for partition in catalog["entries"].as_array().unwrap() {
        let path = format!(
            "/api/chunk-kv/runtime?id={}&epoch={}&generation={}&page=0&offset=0",
            partition["id"].as_str().unwrap(),
            partition["epoch"].as_str().unwrap(),
            catalog["generation"].as_str().unwrap()
        );
        let observed = context.api(Method::GET, &path, Value::Null, 200).await;
        if observed["journal"]["chunk_record"]["logical_start"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
        {
            assert!(observed["journal"]["next_offset"].is_null());
            assert_eq!(observed["journal"]["chunk_record"]["state"], "Active");
            found = true;
            break;
        }
    }
    assert!(found, "actual writes must rotate to a later Stream chunk record");
}

pub(super) async fn wait_initial_split(source: &Group0ChunkKvRangeCatalogSource) {
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
        // Stream chunk records after that real handoff rather than pinning its source.
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
            "native initial split did not settle before chunk record workload: {:?}",
            catalog.entries()
        );
    }
}

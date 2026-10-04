// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_client::{
    BatchItem, ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::chunk_kv::{ChunkKvInstanceObservation, OperationResult, PointOperation};
use crowdb_web::AppState;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub(super) struct TestNativeLoad;

impl TestNativeLoad {
    pub(super) async fn verify(app: &axum::Router, state: &AppState) {
        let kv = state.kv_client().await;
        let registry = ServiceRegistryClient::from_shared(kv.clone());
        let client = ChunkKvClient::new(
            ClientConfig::default(),
            Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(kv)),
            Arc::new(ChunkKvRpcTransport::new(64, 1, 2)),
        )
        .unwrap();
        let before = observations(&registry).await;
        let mut previous = before.clone();
        let started = Instant::now();
        for batch in 0..32 {
            let items = (batch * 16..(batch + 1) * 16)
                .map(|index| BatchItem {
                    request_id: None,
                    operation: PointOperation::Put {
                        key: key(index),
                        value: value(index),
                    },
                })
                .collect();
            let outcomes = client.batch_mutate(items).await.unwrap();
            assert_eq!(outcomes.len(), 16);
            for outcome in outcomes {
                assert!(matches!(
                    outcome.unwrap().result.unwrap(),
                    OperationResult::Mutation { applied: true, .. }
                ));
            }
            assert_progress(&mut previous, observations(&registry).await);
        }
        eprintln!(
            "[PHASE] native 32-MiB Chunk-KV load: {}ms",
            started.elapsed().as_millis()
        );
        // Observe across the normal 12-second serving lease, without changing cadence.
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        let after = loop {
            interval.tick().await;
            let current = observations(&registry).await;
            assert_progress(&mut previous, current.clone());
            check_value(&client, 0).await;
            if started.elapsed() >= Duration::from_secs(13) {
                for previous in &before {
                    let latest = current
                        .iter()
                        .find(|row| row.instance_id == previous.instance_id)
                        .unwrap();
                    assert!(latest.last_heartbeat_ms > previous.last_heartbeat_ms + 6_000);
                }
                break current;
            }
        };
        let retained: u64 = after.iter().map(|row| row.durable_bytes).sum();
        eprintln!("[PHASE] native retained bytes before restart: {retained}");
        assert!(
            retained >= 4 * 1024 * 1024,
            "32 MiB incompressible data must not report negligible retained bytes: {after:?}"
        );
        assert!(
            retained <= 256 * 1024 * 1024,
            "advisory retained estimate must remain within pack padding scale: {after:?}"
        );
        super::call(
            app,
            "POST",
            "/api/services/chunk-kv-1/restart",
            serde_json::json!({}),
        )
        .await;
        for index in [0, 255, 511] {
            check_value(&client, index).await;
        }
        let recovered = observations(&registry).await;
        assert_fresh(&recovered);
        let recovered_bytes: u64 = recovered.iter().map(|row| row.durable_bytes).sum();
        eprintln!("[PHASE] native retained bytes after restart: {recovered_bytes}");
        assert!(
            recovered_bytes >= retained / 2 && recovered_bytes <= retained * 2,
            "owner recovery must preserve retained-data scale: before={after:?}, after={recovered:?}"
        );
    }
}

fn assert_progress(previous: &mut Vec<ChunkKvInstanceObservation>, current: Vec<ChunkKvInstanceObservation>) {
    assert_fresh(&current);
    for row in &current {
        let prior = previous
            .iter()
            .find(|prior| prior.instance_id == row.instance_id)
            .unwrap();
        assert!(
            row.last_heartbeat_ms.saturating_sub(prior.last_heartbeat_ms) < 6_000,
            "native sampling must not skip a normal heartbeat deadline: prior={prior:?}, latest={row:?}"
        );
    }
    *previous = current;
}

async fn observations(registry: &ServiceRegistryClient) -> Vec<ChunkKvInstanceObservation> {
    let rows = registry.read_all_chunk_kv_observations().await.unwrap();
    assert_eq!(rows.len(), 3);
    rows
}

fn assert_fresh(rows: &[ChunkKvInstanceObservation]) {
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap();
    for row in rows {
        assert!(
            now.saturating_sub(row.last_heartbeat_ms) < 6_000,
            "native load sampling must not cross the normal suspect deadline: {row:?}"
        );
    }
}

async fn check_value(client: &ChunkKvClient, index: u64) {
    let response = client.get(key(index), None).await.unwrap();
    let OperationResult::Value(Some(record)) = response.result.unwrap() else {
        panic!("native load record disappeared");
    };
    assert_eq!(record.value, value(index));
}

pub(super) fn key(index: u64) -> Vec<u8> {
    format!("native-load-{index:04}").into_bytes()
}

pub(super) fn value(index: u64) -> Vec<u8> {
    let mut state = index + 1;
    let mut bytes = Vec::with_capacity(64 * 1024);
    for _ in 0..8 * 1024 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes
}

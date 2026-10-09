// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Slow acceptance of the persisted production balance policy and real data.

use crowdb_chunk_kv_client::{
    BatchItem, ChunkKvClient, ChunkKvRangeCatalogMap, ChunkKvRangeCatalogSource, ChunkKvRpcTransport,
    ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::{
    chunk_kv::{ChunkKvRangeCatalogPartitionState, OperationResult, PointOperation},
    common::ChunkKvExtra,
};
use crowdb_web::AppState;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) struct TestNativeBalance;

impl TestNativeBalance {
    pub(super) async fn settle_for_inspection(state: &AppState) {
        let kv = state.kv_client().await;
        let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(kv.clone()));
        let registry = ServiceRegistryClient::from_shared(kv);
        let client = ChunkKvClient::new(
            ClientConfig::default(),
            source.clone(),
            Arc::new(ChunkKvRpcTransport::new(64, 1, 2)),
        )
        .unwrap();
        seed_values(&client).await;
        wait_balanced(&source, &registry, Instant::now()).await;
        verify_values(&client, &[]).await;
    }

    pub(super) async fn verify(state: &AppState) {
        let kv = state.kv_client().await;
        let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(kv.clone()));
        let registry = ServiceRegistryClient::from_shared(kv);
        let client = ChunkKvClient::new(
            ClientConfig::default(),
            source.clone(),
            Arc::new(ChunkKvRpcTransport::new(64, 1, 2)),
        )
        .unwrap();
        seed_values(&client).await;
        // The normal policy has a one-minute cooldown. This slow acceptance
        // observes that real horizon; it does not replace any request/lease budget.
        let started = Instant::now();
        let catalog = wait_balanced(&source, &registry, started).await;
        verify_values(&client, &[]).await;
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        let mut report = Instant::now();
        let hot_owner = catalog.entries()[0].owner.instance_id;
        let hot_keys: Vec<_> = (0..512)
            .filter(|index| {
                catalog
                    .route(&super::native_load::key(*index))
                    .unwrap()
                    .owner
                    .instance_id
                    == hot_owner
            })
            .collect();
        assert!(!hot_keys.is_empty());
        let mut previous = observe(&registry).await;
        let mut transferred = false;
        for index in &hot_keys {
            assert!(client
                .put(super::native_load::key(*index), hot_value(*index))
                .await
                .unwrap()
                .result
                .is_ok());
            let current = load(&source).await;
            if !transferred {
                transferred = verify_transfer(&catalog, &current, &previous);
            }
            previous = observe(&registry).await;
        }
        while !transferred {
            interval.tick().await;
            let current = load(&source).await;
            transferred = verify_transfer(&catalog, &current, &previous);
            previous = observe(&registry).await;
            if report.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[PHASE] native weighted balance {}s: counts={:?}, bytes={:?}",
                    started.elapsed().as_secs(),
                    counts(&current),
                    previous
                        .iter()
                        .map(|(owner, extra)| (*owner, extra.durable_bytes))
                        .collect::<Vec<_>>()
                );
                report = Instant::now();
            }
            assert!(
                started.elapsed() < Duration::from_secs(15 * 60),
                "unequal retained data did not redistribute"
            );
        }
        verify_values(&client, &hot_keys).await;
    }
}

async fn seed_values(client: &ChunkKvClient) {
    for batch in 0..32 {
        let items = (batch * 16..(batch + 1) * 16)
            .map(|index| BatchItem {
                request_id: None,
                operation: PointOperation::Put {
                    key: super::native_load::key(index),
                    value: super::native_load::value(index),
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
    }
}

async fn verify_values(client: &ChunkKvClient, hot_keys: &[u64]) {
    for index in 0..512 {
        let result = client.get(super::native_load::key(index), None).await.unwrap();
        let operation = result.result.unwrap();
        let crowdb_protocol::chunk_kv::OperationResult::Value(Some(record)) = operation else {
            panic!("native balance lost record {index}: {operation:?}");
        };
        let expected = if hot_keys.contains(&index) {
            hot_value(index)
        } else {
            super::native_load::value(index)
        };
        assert_eq!(record.value, expected, "native balance record {index}");
    }
}

async fn wait_balanced(
    source: &Group0ChunkKvRangeCatalogSource,
    registry: &ServiceRegistryClient,
    started: Instant,
) -> ChunkKvRangeCatalogMap {
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    let mut report = Instant::now();
    let mut stalled = None;
    let mut previous = None::<ChunkKvRangeCatalogMap>;
    loop {
        interval.tick().await;
        let catalog = load(source).await;
        let counts = counts(&catalog);
        let observations = observe(registry).await;
        let moved = previous.as_ref().is_some_and(|old| {
            old.entries().iter().any(|entry| {
                catalog.entries().iter().any(|current| {
                    current.partition_id == entry.partition_id
                        && current.owner.instance_id != entry.owner.instance_id
                })
            })
        });
        let idle = observations.keys().any(|owner| !counts.contains_key(owner));
        let movable = catalog.entries().iter().any(|entry| {
            counts[&entry.owner.instance_id] > 1
                && entry.state == ChunkKvRangeCatalogPartitionState::Serving
                && entry.transition_id.is_none()
                && entry.artifact.tail_overlay.is_none()
                && observations[&entry.owner.instance_id]
                    .partition_loads
                    .iter()
                    .any(|load| load.partition_id == entry.partition_id && load.independently_recoverable)
        });
        if moved || !idle || !movable {
            stalled = None;
        } else {
            let since = stalled.get_or_insert_with(Instant::now);
            assert!(
                since.elapsed() < Duration::from_secs(40),
                "healthy idle owner made no actual placement progress for forty seconds: {counts:?}"
            );
        }
        if report.elapsed() >= Duration::from_secs(30) {
            eprintln!(
                "[PHASE] native production balance {}s: counts={counts:?}, bytes={:?}",
                started.elapsed().as_secs(),
                observations
                    .iter()
                    .map(|(owner, extra)| (*owner, extra.durable_bytes))
                    .collect::<Vec<_>>()
            );
            report = Instant::now();
        }
        if counts.len() == 3
            && counts.values().all(|count| *count == 4)
            && catalog.entries().iter().all(|entry| {
                entry.state == ChunkKvRangeCatalogPartitionState::Serving
                    && entry.transition_id.is_none()
                    && entry.artifact.tail_overlay.is_none()
            })
        {
            return catalog;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10 * 60),
            "normal count balance did not converge: {counts:?}"
        );
        previous = Some(catalog);
    }
}

fn verify_transfer(
    before: &ChunkKvRangeCatalogMap,
    after: &ChunkKvRangeCatalogMap,
    observed: &BTreeMap<u64, ChunkKvExtra>,
) -> bool {
    let Some((old, moved)) = before.entries().iter().find_map(|old| {
        after
            .entries()
            .iter()
            .find(|entry| {
                entry.partition_id == old.partition_id && entry.owner.instance_id != old.owner.instance_id
            })
            .map(|entry| (old, entry))
    }) else {
        return false;
    };
    // Every owner had four partitions; a first transfer cannot fix count spread.
    assert!(counts(before).values().all(|count| *count == 4));
    let source_bytes = observed[&old.owner.instance_id].durable_bytes;
    let target_bytes = observed[&moved.owner.instance_id].durable_bytes;
    assert!(source_bytes > target_bytes, "actual transfer must leave the owner with more retained data: source={source_bytes}, target={target_bytes}");
    eprintln!(
        "[PHASE] native weighted transfer {:?}: {} ({source_bytes} bytes) -> {} ({target_bytes} bytes)",
        old.partition_id, old.owner.instance_id, moved.owner.instance_id
    );
    true
}

async fn load(source: &Group0ChunkKvRangeCatalogSource) -> ChunkKvRangeCatalogMap {
    let (head, pages) = source.load().await.unwrap();
    ChunkKvRangeCatalogMap::decode(&head, &pages).unwrap()
}
fn counts(catalog: &ChunkKvRangeCatalogMap) -> BTreeMap<u64, usize> {
    let mut counts = BTreeMap::new();
    for entry in catalog.entries() {
        *counts.entry(entry.owner.instance_id).or_default() += 1;
    }
    counts
}
async fn observe(registry: &ServiceRegistryClient) -> BTreeMap<u64, ChunkKvExtra> {
    let rows = registry.read_all_instance_observations("chunk-kv").await.unwrap();
    assert_eq!(rows.len(), 3);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    rows.into_iter()
        .map(|(owner, value)| {
            assert!(
                now.saturating_sub(u128::from(value.last_heartbeat_ms)) < 6_000,
                "native balancing crossed the normal suspect budget: owner={owner}"
            );
            (owner, value.extra.unwrap().chunk_kv.unwrap())
        })
        .collect()
}
fn hot_value(index: u64) -> Vec<u8> {
    let mut value = super::native_load::value(index);
    value.extend(super::native_load::value(index + 512));
    value.extend(super::native_load::value(index + 1024));
    value.extend(super::native_load::value(index + 1536));
    value
}

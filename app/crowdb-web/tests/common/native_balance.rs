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
        let registry = ServiceRegistryClient::from_shared(kv.clone());
        let client = ChunkKvClient::new(
            ClientConfig::default(),
            source.clone(),
            Arc::new(ChunkKvRpcTransport::new(64, 1, 2)),
        )
        .unwrap();
        seed_values(&client).await;
        wait_balanced(&source, &registry, &kv, Instant::now()).await;
        verify_values(&client).await;
    }

    pub(super) async fn verify(state: &AppState) {
        let kv = state.kv_client().await;
        let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(kv.clone()));
        let registry = ServiceRegistryClient::from_shared(kv.clone());
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
        wait_balanced(&source, &registry, &kv, started).await;
        verify_values(&client).await;
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

async fn verify_values(client: &ChunkKvClient) {
    for index in 0..512 {
        let result = client.get(super::native_load::key(index), None).await.unwrap();
        let operation = result.result.unwrap();
        let crowdb_protocol::chunk_kv::OperationResult::Value(Some(record)) = operation else {
            panic!("native balance lost record {index}: {operation:?}");
        };
        let expected = super::native_load::value(index);
        assert_eq!(record.value, expected, "native balance record {index}");
    }
}

async fn wait_balanced(
    source: &Group0ChunkKvRangeCatalogSource,
    registry: &ServiceRegistryClient,
    kv: &crowdb_kv_client::CrowdbKvClient,
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
            && catalog.entries().len() >= 12
            && placement_ready(kv, catalog.generation()).await
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
            "normal count-based placement did not converge: {counts:?}"
        );
        previous = Some(catalog);
    }
}

async fn placement_ready(kv: &crowdb_kv_client::CrowdbKvClient, generation: u64) -> bool {
    use crowdb_protocol::chunk_kv::balance::{BalanceObservation, OBSERVATION_KEY};
    let value = kv
        .get(
            0,
            0,
            OBSERVATION_KEY.as_bytes(),
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .unwrap();
    let crowdb_kv_client::GetOutcome::Found { value, .. } = value else {
        return false;
    };
    let observation: BalanceObservation = serde_json::from_slice(&value).unwrap();
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    observation.catalog_generation == generation
        && now.saturating_sub(observation.observed_at_ms) <= observation.valid_for_ms
        && (observation.reason == "within tolerance"
            || observation.reason == "no useful safe move"
                && observation.partitions.iter().all(|partition| {
                    partition.reason == "no improving target" || partition.reason == "insufficient benefit"
                }))
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

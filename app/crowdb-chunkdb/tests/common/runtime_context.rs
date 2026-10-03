// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::allocator::{ChunkAllocator, DiskdbClientPool};
use crowdb_chunkdb::chunkdb_config::ChunkdbConfig;
use crowdb_chunkdb::lifecycle::ChunkLockMap;
use crowdb_chunkdb::metrics::{ChunkdbMetrics, LifecycleMetrics};
use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_chunkdb::routing::BindingCache;
use crowdb_chunkdb::runtime::RuntimeContext;
use crowdb_chunkdb::topology::TopologyCache;
use crowdb_common::metrics::MetricsRegistry;
use crowdb_kv_client::{CrowdbKvClient, ServiceRegistryClient};
use std::sync::Arc;
use std::time::Duration;

pub fn runtime_context(
    kv: Arc<CrowdbKvClient>,
    bindings: BindingCache,
    guard: Arc<RangeGuard>,
) -> (RuntimeContext, tokio::sync::watch::Sender<bool>) {
    let pool = Arc::new(DiskdbClientPool::new(ServiceRegistryClient::from_shared(
        Arc::clone(&kv),
    )));
    let mut config = ChunkdbConfig::default();
    config.server.instance_id = Some("1".into());
    config.conversion.enabled = false;
    config.repair.enabled = false;
    config.placement_repair.enabled = false;
    config.placement_rebalance.enabled = false;
    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let context = RuntimeContext {
        config,
        kv,
        bindings,
        range_guard: guard,
        allocator: Arc::new(ChunkAllocator::new(Arc::clone(&pool))),
        cache: TopologyCache::new(),
        pool,
        lock_map: Arc::new(ChunkLockMap::new(
            100,
            Arc::new(LifecycleMetrics::new()),
            Duration::from_secs(10),
        )),
        workflow_metrics: Arc::new(ChunkdbMetrics::register(&mut MetricsRegistry::new())),
        stop_rx,
        allow_unsafe_ec: false,
        allow_degraded_failure_domains: false,
    };
    (context, stop)
}

pub async fn seed_runtime_chunks(
    store: &crowdb_chunkdb::storage::ChunkStore,
    guard: &RangeGuard,
) -> (
    Vec<crowdb_protocol::common::ChunkId>,
    Vec<crowdb_protocol::common::ChunkId>,
) {
    use super::slot_groups::finalize;
    use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState};
    use crowdb_protocol::common::ChunkId;
    let mut expected = Vec::new();
    let mut foreign = Vec::new();
    for purpose in 1_u64..=6 {
        for owned in [true, false] {
            let id = (1..10_000)
                .map(|low| ChunkId {
                    high: purpose << 56,
                    low,
                })
                .find(|id| guard.check(id).is_ok() == owned)
                .unwrap();
            let chunk = Chunk {
                id: Some(id),
                modify_ts: 1,
                chunk_type: i32::try_from(purpose).unwrap(),
                state: ChunkState::Sealed as i32,
                ..Default::default()
            };
            let mut task = finalize(id);
            task.eligible_at_ms = u64::MAX;
            store
                .create_chunk_with_finalize_task(&chunk, &task)
                .await
                .unwrap();
            if owned {
                expected.push(id);
            } else {
                foreign.push(id);
            }
        }
    }
    (expected, foreign)
}

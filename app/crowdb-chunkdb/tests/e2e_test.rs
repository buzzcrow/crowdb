// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E integration tests — verify the full component wiring:
//! topology → selector → allocator → lifecycle handler → service.
//!
//! These tests do not start a real KV cluster; they verify component
//! construction and integration without a live KV backend. Full-stack
//! E2E tests with real KV + diskdb are a follow-up (GAP-10).

#![allow(clippy::cast_possible_truncation, clippy::doc_markdown)]

use std::sync::Arc;

use crowdb_chunkdb::allocator::{ChunkAllocator, DiskdbClientPool};
use crowdb_chunkdb::lifecycle::LifecycleHandler;
use crowdb_chunkdb::metrics::ChunkdbMetrics;
use crowdb_chunkdb::routing::{default_binding_table, BindingCache};
use crowdb_chunkdb::service::ChunkdbRpcService;
use crowdb_chunkdb::storage::ChunkStore;
use crowdb_chunkdb::topology::TopologyCache;
use crowdb_common::metrics::MetricsRegistry;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use crowdb_protocol::common::{ChunkId, HwStatus};
use crowdb_protocol::diskdb::rpc::DiskGroupValue;
use crowdb_protocol::sysdata::DiskGroupEntry;

/// Build a test topology: 3 racks, 1 node per rack, 1 DG per node.
fn build_test_topology() -> TopologyCache {
    let cache = TopologyCache::new();
    for (dg_id, rack) in (100u64..).zip(1..=3u64) {
        let node = rack * 10;
        cache.update_rack(rack, HwStatus::Up as i32, vec![node]);
        cache.update_node_status(rack, node, HwStatus::Up as i32, vec![dg_id]);
        cache.update_disk_group(DiskGroupEntry {
            rack_id: rack,
            node_id: node,
            dg_id,
            value: DiskGroupValue {
                status: HwStatus::Up as i32,
                disk_ids: vec![],
                name: String::new(),
            },
        });
    }
    cache
}

/// Build a fully wired LifecycleHandler with a dummy KV client
/// (calls will fail, but construction verifies all wiring).
fn build_handler() -> LifecycleHandler {
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(vec![
        "http://127.0.0.1:1".into()
    ])));
    let svc = ServiceRegistryClient::from_shared(Arc::clone(&kv));
    let pool = Arc::new(DiskdbClientPool::new(svc));
    let allocator = Arc::new(ChunkAllocator::new(pool));
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(0, 1)).unwrap();
    let store = Arc::new(ChunkStore::new(kv, bindings));
    let topology = build_test_topology();
    LifecycleHandler::new(store, allocator, topology)
}

#[tokio::test]
async fn lifecycle_state_machine_transitions() {
    use crowdb_chunkdb::lifecycle::state::{ChunkState, StateTransitionError};

    // Active → can append, seal, delete
    assert!(ChunkState::Active.check_can_append().is_ok());
    assert!(ChunkState::Active.check_can_seal().is_ok());
    assert!(ChunkState::Active.check_can_delete().is_ok());

    // Sealed → can only delete
    assert!(ChunkState::Sealed.check_can_append().is_err());
    assert!(ChunkState::Sealed.check_can_seal().is_err());
    assert!(ChunkState::Sealed.check_can_delete().is_ok());

    // Deleted → nothing
    assert!(ChunkState::Deleted.check_can_append().is_err());
    assert!(ChunkState::Deleted.check_can_seal().is_err());
    assert!(ChunkState::Deleted.check_can_delete().is_err());

    // Error message contains state info
    let err = StateTransitionError::new(ChunkState::Deleted, "Active");
    assert!(err.to_string().contains("Deleted"));
}

#[tokio::test]
async fn lifecycle_handler_full_wiring() {
    // Verify the handler can be constructed with all components wired
    // (KV client, chunk store, allocator, topology cache).
    let _handler = build_handler();
}

#[tokio::test]
async fn service_construction() {
    // Verify the crowdb-rpc service can be constructed with a real handler.
    let handler = Arc::new(build_handler());
    let rt_handle = tokio::runtime::Handle::current();
    let mut registry = MetricsRegistry::new();
    let metrics = Arc::new(ChunkdbMetrics::register(&mut registry));
    let _service = ChunkdbRpcService::new(handler, metrics, rt_handle);
}

#[tokio::test]
async fn routing_and_storage_integration() {
    use crowdb_chunkdb::routing::{hash_to_bucket, route};

    let cache = BindingCache::new();
    cache.replace(default_binding_table(0, 1)).unwrap();

    let id = ChunkId { high: 1, low: 3 };
    let bucket = hash_to_bucket(&id);
    assert!(bucket < 65535);

    let r = route(&cache, &id).unwrap();
    assert_eq!(r.kv_store_id, 0);
    assert_eq!(r.kv_group_id, 1);
}

#[tokio::test]
async fn placement_selector_integration() {
    use crowdb_chunkdb::selector::{EcPlacement, MirrorPlacement, PlacementConstraints};

    let snap = build_test_topology().snapshot();

    // Mirror: 3 copies across 3 distinct racks.
    let plan = MirrorPlacement::select(&snap, 3, &PlacementConstraints::new()).unwrap();
    assert_eq!(plan.entries.len(), 3);
    let racks: std::collections::HashSet<_> = plan.entries.iter().map(|e| e.rack_id).collect();
    assert_eq!(racks.len(), 3);

    // EC: 4+2 = 6 blocks across 3 racks, safe mode.
    let ec_plan = EcPlacement::select(&snap, 4, 2, &PlacementConstraints::new()).unwrap();
    assert_eq!(ec_plan.entries.len(), 6);
    assert!(ec_plan.safe_mode);
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::{
    lifecycle::LifecycleError,
    range_guard::RangeGuard,
    runtime::{DomainRuntime, RuntimeContext},
};
use crowdb_protocol::{
    chunk_domain::ChunkDomain,
    chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup},
    chunkdb::rpc::{Chunk, ChunkState},
    common::ChunkId,
};
use std::{sync::Arc, time::Duration};

#[path = "common/cluster.rs"]
mod cluster;
#[path = "common/runtime_context.rs"]
#[allow(dead_code)]
mod runtime_context;
#[path = "common/slot_groups.rs"]
#[allow(dead_code)]
mod slot_groups;
use cluster::{seed_hardware, seeded_dg_ids, ChunkdbHarness, DiskdbServer, KvCluster, INSTANCE_ID};

struct TestRecovery {
    cluster: KvCluster,
    diskdb: DiskdbServer,
    harness: ChunkdbHarness,
    context: RuntimeContext,
    stop: tokio::sync::watch::Sender<bool>,
    id: ChunkId,
    persisted: Chunk,
}

impl TestRecovery {
    async fn start() -> Self {
        let cluster = KvCluster::start_pinned().await;
        seed_hardware(&cluster.make_hardware_client()).await;
        let diskdb = DiskdbServer::start(&cluster).await;
        let harness = ChunkdbHarness::start(&cluster).await;
        let mut persisted = harness
            .handler
            .allocate_chunk(
                None,
                1,
                1,
                crowdb_protocol::chunkdb::rpc::StripType::Mirror,
                0,
                0,
                3,
                crowdb_protocol::chunkdb::rpc::ChunkType::S3,
                0,
                0,
            )
            .await
            .unwrap();
        let id = persisted.id.unwrap();
        persisted.state = ChunkState::Deleted as i32;
        persisted.modify_ts += 1;
        harness.store.put_chunk(&persisted).await.unwrap();
        let guard = Arc::new(RangeGuard::new());
        guard
            .install(
                &ChunkSlotBootstrap {
                    service_instances: vec![1],
                    storage_groups: vec![ChunkStorageGroup {
                        store_id: 0,
                        group_id: 1,
                    }],
                }
                .service_map()
                .unwrap(),
                1,
            )
            .unwrap();
        let bindings = crowdb_chunkdb::routing::BindingCache::new();
        bindings
            .replace(crowdb_chunkdb::routing::default_binding_table(0, 1))
            .unwrap();
        let (context, stop) = runtime_context::runtime_context(cluster.make_crowdb_client(), bindings, guard);
        context
            .pool
            .update_disk_id_lookup(&harness.topology.snapshot().disk_groups());
        cluster
            .make_service_registry_client()
            .unregister("diskdb", INSTANCE_ID)
            .await
            .unwrap();
        Self {
            cluster,
            diskdb,
            harness,
            context,
            stop,
            id,
            persisted,
        }
    }

    async fn assert_waiting(
        &self,
        start: &mut tokio::task::JoinHandle<Result<DomainRuntime, LifecycleError>>,
    ) {
        assert!(
            tokio::time::timeout(Duration::from_millis(1_100), start)
                .await
                .is_err(),
            "missing owner must keep the same startup pending, without admitting requests or exiting"
        );
        assert_eq!(
            self.harness.store.get_chunk(&self.id).await.unwrap(),
            self.persisted,
            "recovery must retain the durable free intent until its owner confirms completion"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_waits_for_owner_then_completes_the_persisted_free() {
    let test = TestRecovery::start().await;
    let context = test.context.clone();
    let mut start = tokio::spawn(context.start(ChunkDomain::UserData));
    test.assert_waiting(&mut start).await;
    test.cluster
        .make_service_registry_client()
        .register_diskdb(INSTANCE_ID, &test.diskdb.rpc_endpoint, &seeded_dg_ids(), &[])
        .await
        .unwrap();
    let runtime = tokio::time::timeout(Duration::from_secs(5), start)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let recovered = test.harness.store.get_chunk(&test.id).await.unwrap();
    assert!(recovered.strips.is_empty());
    assert_eq!(recovered.capacity, 0);
    let kv = test.cluster.make_ddb_kv_client();
    for strip in &test.persisted.strips {
        let crowdb_protocol::chunkdb::rpc::Strip::MirrorStrip(mirror) = strip.strip.as_ref().unwrap() else {
            panic!("expected mirror")
        };
        for segment in &mirror.segments {
            let records = kv
                .read_zone_records((0, 1), &segment.disk_id.unwrap(), segment.zone_index)
                .await
                .unwrap();
            assert!(
                records
                    .free
                    .iter()
                    .any(|record| record.key.unit_offset == segment.unit_offset
                        && record.value.pre_allocation_ts == segment.allocation_ts
                        && record.value.previous_owner == segment.owner_chunk
                        && record.value.unit_count == segment.unit_count),
                "recovery must persist the exact incarnation-qualified free fact"
            );
        }
    }
    test.stop.send(true).unwrap();
    for worker in runtime.handles {
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_pending_startup_preserves_intents_without_admitting_requests() {
    let test = TestRecovery::start().await;
    let context = test.context.clone();
    let mut start = tokio::spawn(context.start(ChunkDomain::UserData));
    test.assert_waiting(&mut start).await;
    test.stop.send(true).unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), start)
            .await
            .unwrap()
            .unwrap(),
        Err(LifecycleError::InvalidRequest(_))
    ));
    assert_eq!(
        test.harness.store.get_chunk(&test.id).await.unwrap(),
        test.persisted
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_durable_segment_is_terminal_instead_of_waiting_for_an_owner() {
    let mut test = TestRecovery::start().await;
    let crowdb_protocol::chunkdb::rpc::Strip::MirrorStrip(mirror) =
        test.persisted.strips[0].strip.as_mut().unwrap()
    else {
        panic!("expected mirror")
    };
    mirror.segments[0].disk_id = None;
    test.persisted.modify_ts += 1;
    test.harness.store.put_chunk(&test.persisted).await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), test.context.start(ChunkDomain::UserData))
            .await
            .unwrap(),
        Err(LifecycleError::Cleanup(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_retains_retired_segment_intents_until_the_owner_returns() {
    let mut test = TestRecovery::start().await;
    let crowdb_protocol::chunkdb::rpc::Strip::MirrorStrip(mirror) =
        test.persisted.strips[0].strip.as_ref().unwrap()
    else {
        panic!("expected mirror")
    };
    test.persisted.cleanup_intents = vec![crowdb_protocol::chunkdb::rpc::StripCleanupIntent {
        operation_id: Some(ChunkId { high: 31, low: 41 }),
        retired_segments: mirror.segments.clone(),
        not_before_ms: 0,
    }];
    test.persisted.strips.clear();
    test.persisted.state = ChunkState::Sealed as i32;
    test.persisted.modify_ts += 1;
    test.harness.store.put_chunk(&test.persisted).await.unwrap();
    let context = test.context.clone();
    let mut start = tokio::spawn(context.start(ChunkDomain::UserData));
    test.assert_waiting(&mut start).await;
    test.cluster
        .make_service_registry_client()
        .register_diskdb(INSTANCE_ID, &test.diskdb.rpc_endpoint, &seeded_dg_ids(), &[])
        .await
        .unwrap();
    let runtime = tokio::time::timeout(Duration::from_secs(5), start)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(test
        .harness
        .store
        .get_chunk(&test.id)
        .await
        .unwrap()
        .cleanup_intents
        .is_empty());
    test.stop.send(true).unwrap();
    for worker in runtime.handles {
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
    }
}

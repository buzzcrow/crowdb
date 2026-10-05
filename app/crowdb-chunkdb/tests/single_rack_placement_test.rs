// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production placement preserves node protection when only one rack exists.
#[allow(dead_code)]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{seed_hardware_layout_with_zones, ChunkdbHarness, DiskdbServer, KvCluster};
use crowdb_chunkdb::chunkdb_config::DeploymentMode;
use crowdb_chunkdb::lifecycle::{LifecycleHandler, ReservationFence, ReservationUpdate, ReserveGroupSpec};
use crowdb_protocol::chunkdb::rpc::{ChunkType, StripReservationAction, StripType};
use crowdb_protocol::common::ChunkId;

#[tokio::test]
async fn production_single_rack_allocates_mirror_ec_and_publishes_conversion() {
    let cluster = KvCluster::start().await;
    let groups =
        seed_hardware_layout_with_zones(&cluster.make_hardware_client(), &[(100, vec![10, 11, 12])], 32)
            .await;
    let _diskdb = DiskdbServer::start_with_disk_groups_and_zones(&cluster, &groups, 32).await;
    let harness = ChunkdbHarness::start_with_disk_group_count(&cluster, Duration::from_secs(30), 3).await;
    let handler = LifecycleHandler::new(
        Arc::clone(&harness.store),
        Arc::clone(&harness.allocator),
        harness.topology.clone(),
    )
    .with_deployment_mode(DeploymentMode::Production)
    .with_locks(Arc::new(crowdb_chunkdb::lifecycle::ChunkLockMap::new(
        100,
        Arc::new(crowdb_chunkdb::metrics::LifecycleMetrics::new()),
        Duration::from_secs(60),
    )));
    for kind in [StripType::Mirror, StripType::Ec] {
        let chunk = handler
            .allocate_chunk(None, 1024, 1, kind, 2, 1, 3, ChunkType::S3, 0, 0)
            .await
            .expect("single-rack production allocation");
        let assessment = chunk.strips[0].placement_assessment.as_ref().unwrap();
        assert!(!assessment.rack_protected);
        assert!(!chunk.strips[0].placement_repair_required);
        assert!(assessment.node_protected);
        assert!(assessment.disk_protected);
        assert_eq!(assessment.max_fragments_per_node, 1);
    }
    let ec = handler
        .allocate_chunk(None, 1024, 1, StripType::Ec, 8, 4, 0, ChunkType::S3, 0, 0)
        .await
        .expect("three nodes support EC 8+4 within one rack");
    let assessment = ec.strips[0].placement_assessment.as_ref().unwrap();
    assert_eq!(assessment.max_fragments_per_node, 4);
    assert!(!assessment.rack_protected);
    assert!(assessment.node_protected && assessment.disk_protected);
    assert!(!ec.strips[0].placement_repair_required);
    publish_conversion(&handler).await;
}

async fn publish_conversion(handler: &LifecycleHandler) {
    let chunk = handler
        .allocate_chunk(
            None,
            1024,
            0,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::S3,
            1,
            30_000,
        )
        .await
        .unwrap();
    let id = chunk.id.unwrap();
    let group_id = ChunkId { high: 100, low: 1 };
    let mut fence = ReservationFence {
        expected_modify_ts: chunk.modify_ts,
        writer_epoch: 1,
        lease_generation: 1,
        lease_ms: 30_000,
    };
    let reserved = handler
        .reserve_strip_group(
            &id,
            &group_id,
            fence,
            ReserveGroupSpec {
                reservation_offset_kb: None,
                strip_size: 1,
                strip_count: 2,
                copy_count: 3,
                conversion_data_num: 2,
                conversion_code_num: 1,
            },
        )
        .await
        .unwrap();
    fence.expected_modify_ts = reserved.chunk.modify_ts;
    let group = reserved.group.unwrap();
    for strip in &group.strips {
        for action in [StripReservationAction::Consume, StripReservationAction::Confirm] {
            let result = handler
                .mutate_strip_reservation(
                    &id,
                    &group_id,
                    fence,
                    ReservationUpdate {
                        strip_sequence: strip.strip_sequence,
                        action,
                        acknowledged_cursor: u64::from(strip.chunk_offset + strip.capacity) * 1024,
                        closed_strip_sequence: Some(strip.strip_sequence),
                    },
                )
                .await
                .unwrap();
            fence.expected_modify_ts = result.chunk.modify_ts;
        }
    }
    let published = handler
        .mutate_strip_reservation(
            &id,
            &group_id,
            fence,
            ReservationUpdate {
                strip_sequence: group.strips[0].strip_sequence,
                action: StripReservationAction::Publish,
                acknowledged_cursor: 0,
                closed_strip_sequence: None,
            },
        )
        .await
        .expect("rack preference must not prevent EC publication");
    assert_eq!(published.chunk.strips.len(), 1);
    let assessment = published.chunk.strips[0].placement_assessment.as_ref().unwrap();
    assert!(!assessment.rack_protected);
    assert!(assessment.node_protected && assessment.disk_protected);
    assert!(!published.chunk.strips[0].placement_repair_required);
}

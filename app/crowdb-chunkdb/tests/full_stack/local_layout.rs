// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{seed_hardware, ChunkdbHarness, DeploymentMode, DiskdbServer, KvCluster, LifecycleHandler};
use crowdb_protocol::chunkdb::rpc::{ChunkType, Strip, StripType};
use std::{collections::HashSet, sync::Arc};

#[tokio::test]
async fn local_layout_allocates_one_or_two_distinct_physical_mirror_copies() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let handler = LifecycleHandler::new(
        Arc::clone(&harness.store),
        Arc::clone(&harness.allocator),
        harness.topology.clone(),
    )
    .with_deployment_mode(DeploymentMode::Local)
    .with_placement_policy(crowdb_chunkdb::selector::FailureDomainPriority::RackFirst, true);
    for copies in [1, 2] {
        let chunk = handler
            .allocate_chunk(
                None,
                1024,
                1,
                StripType::Mirror,
                0,
                0,
                copies,
                ChunkType::S3,
                0,
                0,
            )
            .await
            .unwrap();
        let Some(Strip::MirrorStrip(mirror)) = &chunk.strips[0].strip else {
            panic!("local layout must use mirror strips");
        };
        assert_eq!(mirror.segments.len(), copies as usize);
        let positions: HashSet<_> = mirror
            .segments
            .iter()
            .map(|segment| {
                (
                    segment.disk_id,
                    segment.zone_index,
                    segment.unit_offset,
                    segment.unit_count,
                )
            })
            .collect();
        assert_eq!(positions.len(), mirror.segments.len());
    }
}

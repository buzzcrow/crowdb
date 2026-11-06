// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster, UNIT_SIZE_BYTES};
use crowdb_protocol::chunkdb::rpc::{ChunkType, Strip, StripType};

#[tokio::test]
async fn requested_strip_capacity_rounds_up_to_complete_disk_units() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let unit_kb = UNIT_SIZE_BYTES / 1024;
    for requested in [unit_kb - 1, unit_kb, unit_kb + 1] {
        let chunk = harness
            .handler
            .allocate_chunk(
                None,
                requested,
                2,
                StripType::Mirror,
                0,
                0,
                3,
                ChunkType::BtreePage,
                17,
                30_000,
            )
            .await
            .unwrap();
        let expected_units = requested.div_ceil(unit_kb);
        assert_eq!(chunk.capacity, 2 * expected_units * unit_kb);
        assert_eq!(chunk.strips.len(), 2);
        for (index, strip) in chunk.strips.iter().enumerate() {
            assert!(
                strip.capacity >= requested,
                "requested {requested} KiB, allocated {} KiB",
                strip.capacity
            );
            assert_eq!(strip.capacity, expected_units * unit_kb);
            assert_eq!(strip.chunk_offset, u32::try_from(index).unwrap() * strip.capacity);
            let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                panic!("expected mirror layout")
            };
            assert_eq!(mirror.segments.len(), 3);
            assert!(mirror
                .segments
                .iter()
                .all(|segment| segment.unit_count == expected_units));
        }
        assert_eq!(harness.store.get_chunk(&chunk.id.unwrap()).await.unwrap(), chunk);
    }
}

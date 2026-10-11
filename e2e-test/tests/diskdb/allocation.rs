// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E full-flow test: smoke test (allocate / free / query drill-down)
//! + concurrent benchmark + compact-and-reclaim.
//!
//! Persist-only free model: free keeps bitmap set, compaction reclaims.

use std::sync::Arc;

use super::hardware::{make_disk_id, CAPACITY_UNITS, DG_ID, UNIT_SIZE_BYTES, ZONE_COUNT};
use super::{concurrency::run_concurrent_benchmark, make_chunk_id};
use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport, RetryConfig};
use crowdb_protocol::diskdb::rpc::{
    AllocateBlocksRequest, CommitBlocksRequest, CompactZoneRequest, FreeBlocksRequest,
};

#[tokio::test]
#[ignore = "requires the owned container fixture"]
#[allow(clippy::too_many_lines)]
async fn allocate_commit_and_free_through_client() {
    // 4. Build the DiskdbClient and refresh endpoints.
    let svc = super::registry();
    let transport = Arc::new(DiskdbRpcTransport::new());
    let client = Arc::new(DiskdbClient::new(svc, transport).with_retry_config(RetryConfig {
        max_retries: 5,
        initial_backoff: std::time::Duration::from_millis(100),
    }));
    client.refresh_endpoints().await.expect("refresh endpoints");

    // Concurrent first frees must route from their allocation responses
    // without a discovery stampede on an initially empty disk-to-group cache.
    let routing_client = Arc::clone(&client);
    tokio::time::timeout(std::time::Duration::from_secs(10), async move {
        let barrier = Arc::new(tokio::sync::Barrier::new(32));
        let mut tasks = Vec::with_capacity(32);
        for task_id in 0..32_u64 {
            let client = Arc::clone(&routing_client);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                let response = client
                    .allocate_blocks(AllocateBlocksRequest {
                        disk_group_id: DG_ID,
                        unit_count: 1,
                        count: 1,
                        exclude_disk_ids: vec![],
                        owner_chunk: Some(make_chunk_id(1, task_id)),
                        allow_disk_reuse: false,
                    })
                    .await
                    .expect("concurrent route-cache allocation");
                barrier.wait().await;
                client
                    .free_blocks(FreeBlocksRequest {
                        segments: response.segments,
                    })
                    .await
                    .expect("concurrent route-cache free")
            }));
        }
        for task in tasks {
            assert_eq!(task.await.expect("route-cache task").freed_count, 1);
        }
    })
    .await
    .expect("concurrent first frees must not stall routing");

    // 5. Smoke test: allocate 3 blocks, verify, free them.
    eprintln!("=== smoke test: allocate / free ===");
    let owner = make_chunk_id(0, 42);
    let alloc_req = AllocateBlocksRequest {
        disk_group_id: DG_ID,
        unit_count: 1,
        count: 3,
        exclude_disk_ids: vec![],
        owner_chunk: Some(owner),
        allow_disk_reuse: false,
    };
    let alloc_resp = client
        .allocate_blocks(alloc_req)
        .await
        .expect("allocate 3 blocks");
    assert_eq!(alloc_resp.segments.len(), 3, "expected 3 segments from allocate");

    for (i, seg) in alloc_resp.segments.iter().enumerate() {
        assert_eq!(seg.unit_count, 1, "segment {i} should have unit_count=1");
    }

    let commit_resp = client
        .commit_blocks(CommitBlocksRequest {
            segments: alloc_resp.segments.clone(),
        })
        .await
        .expect("commit 3 blocks");
    assert_eq!(commit_resp.committed_count, 3, "expected 3 committed");

    // Query disk-group info.
    let dg_info = client
        .get_disk_group_info(DG_ID)
        .await
        .expect("get_disk_group_info");
    let group = dg_info.group.expect("disk-group info should have group");
    assert_eq!(group.disk_group_id, DG_ID, "disk-group id mismatch");
    assert!(!group.disk_ids.is_empty(), "disk-group should have disks");

    // Query capacity stats.
    let cap = client.query_disk_group(DG_ID).await.expect("query_disk_group");
    let dg_cap = &cap.disk_groups[0];

    assert!(
        dg_cap.capacity_bytes > 0,
        "capacity should be non-zero after sync"
    );

    // Free the 3 committed blocks.
    let free_req = FreeBlocksRequest {
        segments: alloc_resp.segments.clone(),
    };
    let free_resp = client.free_blocks(free_req).await.expect("free 3 blocks");
    assert_eq!(free_resp.freed_count, 3, "expected 3 freed");

    // 6. Query drill-down: get_disk_info, query_disk, query_zone.
    eprintln!("=== query drill-down ===");
    let test_disk = make_disk_id(0, 1);

    let disk_info = client
        .get_disk_info(DG_ID, test_disk)
        .await
        .expect("get_disk_info");
    let di = disk_info.disk.expect("disk info should have disk");
    assert_eq!(di.disk_group_id, DG_ID, "disk_group_id mismatch");
    assert_eq!(di.disk_id, Some(test_disk), "disk_id mismatch");
    assert_eq!(di.zone_count, ZONE_COUNT, "zone_count mismatch");
    assert_eq!(
        di.capacity_bytes,
        CAPACITY_UNITS * u64::from(UNIT_SIZE_BYTES),
        "capacity_bytes mismatch"
    );

    let disk_cap = client.query_disk(DG_ID, test_disk).await.expect("query_disk");
    let dg_from_disk = &disk_cap.disk_groups[0];
    let disk_from_query = dg_from_disk
        .disks
        .iter()
        .find(|d| d.disk_id == Some(test_disk))
        .expect("query_disk should return the queried disk");
    assert!(
        !disk_from_query.zone_usages.is_empty(),
        "query_disk should return per-zone entries"
    );

    let zone_cap = client.query_zone(DG_ID, test_disk, 0).await.expect("query_zone");
    let zone_dg = &zone_cap.disk_groups[0];
    let zone_disk = zone_dg
        .disks
        .iter()
        .find(|d| d.disk_id == Some(test_disk))
        .expect("query_zone should return the queried disk");
    let zone_usage = &zone_disk.zone_usages[0];
    assert_eq!(zone_usage.zone_index, 0, "zone_index mismatch");
    let bitmap_len = zone_usage.usage_bitmap.as_ref().map_or(0, Vec::len);
    assert!(bitmap_len > 0, "query_zone should populate usage_bitmap");

    // 7. Concurrent benchmark.
    eprintln!("=== concurrent benchmark ===");
    run_concurrent_benchmark(&client).await;

    // 8. Compact + reclaim: verify the persist-only free model.
    eprintln!("=== compact + reclaim ===");
    let total_cap_bytes = 3 * CAPACITY_UNITS * u64::from(UNIT_SIZE_BYTES);

    let cap_before = client
        .query_disk_group(DG_ID)
        .await
        .expect("query before compact");
    let busy_before = cap_before.disk_groups[0].busy_bytes;
    let free_before = cap_before.disk_groups[0].free_bytes;

    assert!(
        busy_before > 0,
        "persist-only free should keep bitmap bits set (busy > 0)"
    );
    assert_eq!(
        busy_before + free_before,
        total_cap_bytes,
        "busy + free should equal capacity"
    );

    // Compact all zones on all 3 disks.
    let disk_ids = [make_disk_id(0, 1), make_disk_id(0, 2), make_disk_id(0, 3)];
    let mut total_compacted_zones = 0u32;
    let mut total_free_records_deleted = 0u32;
    for did in &disk_ids {
        let resp = client
            .compact_zone(CompactZoneRequest {
                disk_id: Some(*did),
                zone_indices: vec![],
            })
            .await
            .expect("compact_zone");
        total_compacted_zones += resp.compacted_zone_count;
        total_free_records_deleted += resp.total_free_records_deleted;
        assert!(
            resp.zones.iter().all(|z| z.success),
            "all zone compaction results should be success for disk {did:?}"
        );
    }

    assert_eq!(total_compacted_zones, 3 * ZONE_COUNT);
    assert!(total_free_records_deleted > 0);
    let cap_after = client.query_disk_group(DG_ID).await.expect("query after compact");
    let busy_after = cap_after.disk_groups[0].busy_bytes;
    let free_after = cap_after.disk_groups[0].free_bytes;

    assert_eq!(
        busy_after, 0,
        "after compaction all freed bits should be cleared (busy = 0)"
    );
    assert_eq!(
        free_after, total_cap_bytes,
        "after compaction all capacity should be free"
    );

    // Verify space is reclaimable: allocate 3 blocks (should succeed).
    let reclaim_owner = make_chunk_id(0, 77);
    let reclaim_req = AllocateBlocksRequest {
        disk_group_id: DG_ID,
        unit_count: 1,
        count: 3,
        exclude_disk_ids: vec![],
        owner_chunk: Some(reclaim_owner),
        allow_disk_reuse: false,
    };
    let reclaim_resp = client
        .allocate_blocks(reclaim_req)
        .await
        .expect("allocate after compaction should succeed");
    assert_eq!(
        reclaim_resp.segments.len(),
        3,
        "should allocate 3 blocks after compaction"
    );

    // Clean up: free the 3 blocks.
    let cleanup_req = FreeBlocksRequest {
        segments: reclaim_resp.segments,
    };
    let cleanup_resp = client.free_blocks(cleanup_req).await.expect("cleanup free");
    assert_eq!(cleanup_resp.freed_count, 3, "cleanup free should free 3");
}

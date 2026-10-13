// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Client E2E coverage for routing and compaction-time free validation.

use std::sync::Arc;
use std::time::Duration;

use super::hardware::{DG_ID, UNIT_SIZE_BYTES};
use super::make_chunk_id;
use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport, RetryConfig};
use crowdb_protocol::diskdb::rpc::{AllocateBlocksRequest, CompactZoneRequest, FreeBlocksRequest, Segment};

#[tokio::test]
#[ignore = "requires the owned container fixture"]
#[allow(clippy::too_many_lines)]
async fn wrong_owner_free_is_ignored_by_compaction() {
    // 3. Build client + refresh endpoints.
    let svc = super::registry();
    let transport = Arc::new(DiskdbRpcTransport::new());
    let client = Arc::new(
        DiskdbClient::new(svc.clone(), transport).with_retry_config(RetryConfig {
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
        }),
    );
    client.refresh_endpoints().await.expect("refresh endpoints");

    svc.register_diskdb(998, "127.0.0.1:1", &[102], &[])
        .await
        .expect("register temporary route");
    client.refresh_endpoints().await.expect("add temporary route");
    assert!(client.disk_group_ids().contains(&102));
    svc.unregister("diskdb", 998)
        .await
        .expect("remove temporary route");
    client.refresh_endpoints().await.expect("remove stale route");
    assert!(!client.disk_group_ids().contains(&102));

    // 4. Allocate 1 block with owner = chunk A.
    let owner_a = make_chunk_id(0, 42);
    let alloc_resp = client
        .allocate_blocks(AllocateBlocksRequest {
            disk_group_id: DG_ID,
            unit_count: 1,
            count: 1,
            exclude_disk_ids: vec![],
            owner_chunk: Some(owner_a),
            allow_disk_reuse: false,
        })
        .await
        .expect("allocate 1 block");
    assert_eq!(alloc_resp.segments.len(), 1, "expected 1 segment");
    let seg = &alloc_resp.segments[0];

    // 5. A wrong-owner free is accepted as an immutable fact. Compaction
    // rejects it because it does not match the current busy incarnation.
    let owner_b = make_chunk_id(0, 999);
    let wrong_seg = Segment {
        disk_id: seg.disk_id,
        zone_index: seg.zone_index,
        unit_offset: seg.unit_offset,
        unit_count: seg.unit_count,
        owner_chunk: Some(owner_b),
        allocation_ts: seg.allocation_ts,
    };
    let wrong_free = client
        .free_blocks(FreeBlocksRequest {
            segments: vec![wrong_seg],
        })
        .await
        .expect("wrong-owner free should persist");
    assert_eq!(wrong_free.freed_count, 1);
    assert!(wrong_free.failures.is_empty());
    let disk_id = seg.disk_id.expect("allocated segment has disk id");
    let wrong_compaction = client
        .compact_zone(CompactZoneRequest {
            disk_id: Some(disk_id),
            zone_indices: vec![seg.zone_index],
        })
        .await
        .expect("compact after mismatched wrong-owner free");
    assert!(wrong_compaction.zones.iter().all(|zone| zone.success));
    let after_wrong = client
        .query_disk_group(DG_ID)
        .await
        .expect("query after mismatch");
    assert_eq!(after_wrong.disk_groups[0].busy_bytes, u64::from(UNIT_SIZE_BYTES));
}

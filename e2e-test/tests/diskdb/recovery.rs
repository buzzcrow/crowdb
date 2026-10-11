// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::hardware::{DG_ID, UNIT_SIZE_BYTES};
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::diskdb::rpc::{
    AllocateBlocksRequest, CommitBlocksRequest, CompactZoneRequest, FreeBlocksRequest, Segment,
};
use crowdb_protocol::key::{BinaryKey, BusyBlockKey};
use serde_json::{json, Value};
use std::path::PathBuf;

#[tokio::test]
#[ignore = "requires the owned release-container Diskdb fixture"]
async fn diskdb_restart_preserves_acknowledged_records() {
    recovery().await;
}

#[tokio::test]
#[ignore = "requires the owned release-container Diskdb and KV fixture"]
async fn metadata_restart_preserves_acknowledged_records() {
    recovery().await;
}

async fn recovery() {
    let path = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap()).join("diskdb-witness.json");
    let client = super::make_client(super::registry());
    client
        .refresh_endpoints()
        .await
        .expect("discover recovered Diskdb");
    if std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write") {
        write_witness(&client, &path).await;
    } else {
        verify_recovery(&client, &path).await;
    }
}

async fn write_witness(client: &crowdb_diskdb_client::DiskdbClient, path: &std::path::Path) {
    let allocated = client
        .allocate_blocks(request(42))
        .await
        .expect("allocate before fault");
    assert_eq!(allocated.segments.len(), 3);
    assert_eq!(
        client
            .commit_blocks(CommitBlocksRequest {
                segments: allocated.segments.clone()
            })
            .await
            .unwrap()
            .committed_count,
        3
    );
    assert_eq!(
        client
            .free_blocks(FreeBlocksRequest {
                segments: vec![allocated.segments[0]]
            })
            .await
            .unwrap()
            .freed_count,
        1
    );
    assert_usage(client, 3).await;
    let records = records(&allocated.segments).await;
    std::fs::write(
        path,
        json!({"segments": allocated.segments, "records": records}).to_string(),
    )
    .unwrap();
}

async fn verify_recovery(client: &crowdb_diskdb_client::DiskdbClient, path: &std::path::Path) {
    let witness: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let segments: Vec<Segment> = serde_json::from_value(witness["segments"].clone()).unwrap();
    assert_eq!(
        records(&segments).await,
        witness["records"],
        "acknowledged busy records and revisions survive fault"
    );
    // Persist-only recovery conservatively retains every busy bit until compact.
    assert_usage(client, 3).await;
    let compacted = client
        .compact_zone(CompactZoneRequest {
            disk_id: segments[0].disk_id,
            zone_indices: vec![segments[0].zone_index],
        })
        .await
        .expect("reclaim pre-crash acknowledged free");
    assert!(compacted.zones.iter().all(|zone| zone.success));
    assert_usage(client, 2).await;
    assert_eq!(
        records(&segments[1..]).await,
        json!(&witness["records"].as_array().unwrap()[1..]),
        "compaction preserves neighboring records"
    );
    assert_eq!(
        client
            .commit_blocks(CommitBlocksRequest {
                segments: segments[1..].to_vec()
            })
            .await
            .unwrap()
            .committed_count,
        2
    );
    assert_eq!(
        client
            .free_blocks(FreeBlocksRequest {
                segments: segments[1..].to_vec()
            })
            .await
            .unwrap()
            .freed_count,
        2
    );
    for segment in &segments {
        let compacted = client
            .compact_zone(CompactZoneRequest {
                disk_id: segment.disk_id,
                zone_indices: vec![segment.zone_index],
            })
            .await
            .expect("compact recovered free facts");
        assert!(compacted.zones.iter().all(|zone| zone.success));
    }
    assert_usage(client, 0).await;
    let fresh = client
        .allocate_blocks(request(43))
        .await
        .expect("allocate after recovery and reclaim");
    assert_eq!(fresh.segments.len(), 3);
    assert_eq!(
        client
            .commit_blocks(CommitBlocksRequest {
                segments: fresh.segments
            })
            .await
            .unwrap()
            .committed_count,
        3
    );
    assert_usage(client, 3).await;
}

fn request(owner: u64) -> AllocateBlocksRequest {
    AllocateBlocksRequest {
        disk_group_id: DG_ID,
        unit_count: 1,
        count: 3,
        exclude_disk_ids: vec![],
        owner_chunk: Some(super::make_chunk_id(0, owner)),
        allow_disk_reuse: false,
    }
}

async fn records(segments: &[Segment]) -> Value {
    let kv = super::kv_client();
    let mut records = Vec::new();
    for segment in segments {
        let key = BusyBlockKey {
            disk_id: segment.disk_id.unwrap(),
            zone_index: segment.zone_index,
            unit_offset: segment.unit_offset,
        }
        .to_bytes();
        let record = kv.get(0, 1, &key, ReadMode::Linearizable, None).await.unwrap();
        let GetOutcome::Found { value, revision } = record else {
            panic!("acknowledged busy record missing")
        };
        records.push(json!({"bytes": value.as_ref(), "revision": revision}));
    }
    json!(records)
}

async fn assert_usage(client: &crowdb_diskdb_client::DiskdbClient, units: u64) {
    let response = client.query_disk_group(DG_ID).await.unwrap();
    assert_eq!(response.disk_groups.len(), 1);
    let group = &response.disk_groups[0];
    assert_eq!(group.busy_bytes, units * u64::from(UNIT_SIZE_BYTES));
    assert_eq!(group.busy_bytes + group.free_bytes, group.capacity_bytes);
}

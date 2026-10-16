// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/memory_kv.rs"]
mod memory_kv;
#[path = "common/storage_lifecycle.rs"]
mod storage_lifecycle;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    DiskIdExt,
};
use serde_json::{json, Value};
use storage_lifecycle::TestStorageApi;
use tower::ServiceExt;

fn disk(low: u64) -> Value {
    json!({"disk_id":DiskId { high:591, low }.to_display_string(),"disk_type":"Hdd",
        "capacity_bytes":4_096_000,"unit_size_bytes":4096,"zone_size_bytes":1_048_576})
}

async fn batch(fixture: &TestStorageApi, disks: Vec<Value>, status: StatusCode) -> Value {
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes/2/disk-groups/3/disks/batch")
                .header("content-type", "application/json")
                .body(Body::from(json!({"disks":disks}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let actual = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 65536).await.unwrap();
    if actual != status {
        eprintln!(
            "batch failure membership: {:?}",
            fixture.hardware.get_disk_group(1, 2, 3).await
        );
        eprintln!(
            "batch failure disks: {:?}",
            fixture.hardware.list_disks_in_group(1, 2, 3).await
        );
    }
    assert_eq!(actual, status, "{}", String::from_utf8_lossy(&bytes));
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn batch_success_commits_every_disk_and_membership_despite_status_contention() {
    let fixture = TestStorageApi::start().await;
    let original = fixture.hardware.disk_storage(&fixture.disk).await.unwrap();
    let create = batch(&fixture, vec![disk(1), disk(2)], StatusCode::CREATED);
    let changes = async {
        for index in 0..20 {
            fixture
                .hardware
                .set_node_status(
                    1,
                    2,
                    if index % 2 == 0 {
                        HwStatus::Suspect
                    } else {
                        HwStatus::Up
                    },
                )
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
    };
    let (result, ()) = tokio::join!(create, changes);
    assert_eq!(result["added"].as_array().unwrap().len(), 2);
    let group = fixture.hardware.get_disk_group(1, 2, 3).await.unwrap().unwrap();
    for low in [1, 2] {
        let id = DiskId { high: 591, low };
        assert!(group.value.disk_ids.contains(&id));
        assert!(fixture.hardware.get_disk(1, 2, 3, &id).await.unwrap().is_some());
    }
    let capacity = fixture.hardware.capacity_summary().await.unwrap();
    let group_capacity = capacity
        .disk_groups
        .iter()
        .find(|group| group.disk_group_id == 3)
        .unwrap();
    assert_eq!(group_capacity.disk_count, 3);
    assert_eq!(
        fixture.hardware.disk_storage(&fixture.disk).await.unwrap(),
        original
    );
    batch(&fixture, vec![disk(1), disk(2)], StatusCode::CREATED).await;
    assert_eq!(
        fixture.hardware.list_disks_in_group(1, 2, 3).await.unwrap().len(),
        3
    );
}

#[tokio::test]
async fn invalid_or_conflicting_batch_has_no_partial_membership() {
    let fixture = TestStorageApi::start().await;
    batch(&fixture, vec![disk(1), disk(1)], StatusCode::CONFLICT).await;
    assert_eq!(
        fixture.hardware.list_disks_in_group(1, 2, 3).await.unwrap().len(),
        1
    );
    batch(&fixture, vec![disk(1), disk(2)], StatusCode::CREATED).await;
    let mut conflict = disk(2);
    conflict["capacity_bytes"] = json!(8_192_000);
    batch(&fixture, vec![disk(3), conflict], StatusCode::CONFLICT).await;
    assert!(fixture
        .hardware
        .get_disk(1, 2, 3, &DiskId { high: 591, low: 3 })
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        fixture.hardware.list_disks_in_group(1, 2, 3).await.unwrap().len(),
        3
    );
}

#[tokio::test]
async fn physical_paths_cannot_bypass_verified_candidate_admission() {
    let fixture = TestStorageApi::start().await;
    let mut physical = disk(1);
    physical["device_path"] = json!("/opt/crowdb/data/disks/unverified.img");
    let error = batch(&fixture, vec![disk(2), physical.clone()], StatusCode::CONFLICT).await;
    assert!(error["error"].as_str().unwrap().contains("Join Candidates"));
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes/2/disk-groups/3/disks")
                .header("content-type", "application/json")
                .body(Body::from(physical.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        fixture.hardware.list_disks_in_group(1, 2, 3).await.unwrap().len(),
        1
    );
}

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
    disk_storage::{DiskExclusionRecord, DiskExclusionReporter},
    DiskIdExt,
};
use serde_json::{json, Value};
use storage_lifecycle::TestStorageApi;
use tower::ServiceExt;

async fn request(
    app: &axum::Router,
    method: &str,
    path: &str,
    body: Option<Value>,
    expected: StatusCode,
) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 65536).await.unwrap();
    assert_eq!(status, expected, "{path}: {}", String::from_utf8_lossy(&bytes));
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn scoped_http_offline_and_remove_require_confirmation_and_reconcile_one_receipt() {
    let fixture = TestStorageApi::start().await;
    verify_partial_zone_rejected(&fixture).await;
    let path = format!(
        "/api/nodes/2/disk-groups/3/disks/{}",
        fixture.disk.disk_id.to_display_string()
    );
    let current = request(
        &fixture.app,
        "GET",
        &format!("{path}/lifecycle"),
        None,
        StatusCode::OK,
    )
    .await;
    let online = current["disk_revision"].as_str().unwrap();
    assert_eq!(current["offline"], false);
    request(
        &fixture.app,
        "POST",
        &format!("{path}/removal"),
        Some(json!({"expected_revision": online})),
        StatusCode::CONFLICT,
    )
    .await;
    request(
        &fixture.app,
        "POST",
        &format!("{path}/offline"),
        Some(json!({"expected_revision": "01"})),
        StatusCode::BAD_REQUEST,
    )
    .await;
    request(
        &fixture.app,
        "POST",
        &format!("{path}/offline"),
        Some(json!({"expected_revision": (online.parse::<u64>().unwrap() + 1).to_string()})),
        StatusCode::CONFLICT,
    )
    .await;
    let offline = request(
        &fixture.app,
        "POST",
        &format!("{path}/offline"),
        Some(json!({"expected_revision": online})),
        StatusCode::OK,
    )
    .await;
    assert_eq!(offline["offline"], true);
    assert!(offline["operation_id"].as_str().unwrap().starts_with("offline/"));
    let revision = offline["disk_revision"].as_str().unwrap();
    verify_online_requires_original_medium(&fixture, &path, revision).await;
    assert!(offline["remove_conditions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|reason| reason.as_str().unwrap().contains("DiskIO 7")));
    request(
        &fixture.app,
        "POST",
        &format!("{path}/removal"),
        Some(json!({"expected_revision": revision})),
        StatusCode::CONFLICT,
    )
    .await;
    let outcome = confirm_and_remove(&fixture, &path, revision).await;
    let observed = request(
        &fixture.app,
        "GET",
        &format!("{path}/lifecycle"),
        None,
        StatusCode::OK,
    )
    .await;
    assert_eq!(observed["removed"], true);
    assert_eq!(observed["operation_id"], outcome["operation_id"]);
    assert_eq!(observed["removed_from_revision"], revision);
    verify_removed_candidate(&fixture).await;
    verify_rejoin_requires_original_medium(&fixture, &path, &outcome).await;
    assert!(
        fixture
            .hardware
            .disk_storage(&fixture.disk)
            .await
            .unwrap()
            .unwrap()
            .removed
    );
    assert!(fixture
        .hardware
        .get_disk_with_revision(&fixture.disk)
        .await
        .unwrap()
        .is_none());
}

async fn confirm_and_remove(fixture: &TestStorageApi, path: &str, revision: &str) -> Value {
    fixture
        .hardware
        .acknowledge_disk_exclusion(&DiskExclusionRecord {
            version: 1,
            rack_id: 1,
            node_id: 2,
            disk_group_id: 3,
            disk_id: fixture.disk.disk_id,
            disk_revision: revision.parse().unwrap(),
            reporter: DiskExclusionReporter::Diskio,
            instance_id: 7,
            owner_revision: None,
        })
        .await
        .unwrap();
    let ready = request(
        &fixture.app,
        "GET",
        &format!("{path}/lifecycle"),
        None,
        StatusCode::OK,
    )
    .await;
    assert_eq!(ready["remove_conditions"], json!([]));
    let outcome = request(
        &fixture.app,
        "POST",
        &format!("{path}/removal"),
        Some(json!({"expected_revision": revision})),
        StatusCode::OK,
    )
    .await;
    assert_eq!(outcome["removed"], true);
    let replay = request(
        &fixture.app,
        "POST",
        &format!("{path}/removal"),
        Some(json!({"expected_revision": revision})),
        StatusCode::OK,
    )
    .await;
    assert_eq!(outcome, replay);
    outcome
}

async fn verify_removed_candidate(fixture: &TestStorageApi) {
    let page = request(
        &fixture.app,
        "GET",
        "/api/nodes/2/removed-disks?limit=1",
        None,
        StatusCode::OK,
    )
    .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        page["items"][0]["disk_id"],
        fixture.disk.disk_id.to_display_string()
    );
    assert_eq!(page["items"][0]["disk_group_id"], 3);
    assert!(page["items"][0]["capacity_units"].is_string());
    request(
        &fixture.app,
        "GET",
        "/api/nodes/2/removed-disks?limit=65",
        None,
        StatusCode::BAD_REQUEST,
    )
    .await;
    request(
        &fixture.app,
        "GET",
        "/api/nodes/2/removed-disks?after=foreign-node",
        None,
        StatusCode::BAD_REQUEST,
    )
    .await;
}

async fn verify_partial_zone_rejected(fixture: &TestStorageApi) {
    let response = request(
        &fixture.app,
        "POST",
        "/api/nodes/2/disk-files",
        Some(
            json!({"operation_id":"12345678-1234-4234-8234-123456789abc", "disk_group_id":3,
            "capacity_bytes":48_u64 * 1024 * 1024 * 1024}),
        ),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(response["error"].as_str().unwrap().contains("whole 32 GiB zones"));
}

async fn verify_rejoin_requires_original_medium(fixture: &TestStorageApi, path: &str, outcome: &Value) {
    request(
        &fixture.app,
        "POST",
        &format!("{path}/rejoin"),
        Some(json!({"removal_operation_id":"wrong-operation"})),
        StatusCode::CONFLICT,
    )
    .await;
    let response = request(
        &fixture.app,
        "POST",
        &format!("{path}/rejoin"),
        Some(json!({"removal_operation_id":outcome["operation_id"]})),
        StatusCode::BAD_GATEWAY,
    )
    .await;
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("membership was not changed"));
    assert!(fixture
        .hardware
        .get_disk_with_revision(&fixture.disk)
        .await
        .unwrap()
        .is_none());
    assert!(
        fixture
            .hardware
            .disk_storage(&fixture.disk)
            .await
            .unwrap()
            .unwrap()
            .removed
    );
}

async fn verify_online_requires_original_medium(fixture: &TestStorageApi, path: &str, revision: &str) {
    let rejected = request(
        &fixture.app,
        "POST",
        &format!("{path}/online"),
        Some(json!({"expected_revision": revision})),
        StatusCode::BAD_GATEWAY,
    )
    .await;
    assert!(rejected["error"]
        .as_str()
        .unwrap()
        .contains("Original medium check failed"));
    assert!(fixture
        .hardware
        .disk_online_operation(&fixture.disk)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        fixture
            .hardware
            .get_disk_with_revision(&fixture.disk)
            .await
            .unwrap()
            .unwrap()
            .1
            .to_string(),
        revision
    );
}

#[tokio::test]
async fn compatibility_delete_cannot_drop_an_unverified_legacy_physical_disk() {
    use crowdb_protocol::{
        common::DiskId,
        key::{DiskGroupKey, DiskKey, TextKey},
    };
    let fixture = TestStorageApi::start().await;
    let key = DiskKey {
        disk_id: DiskId { high: 90, low: 91 },
        ..fixture.disk
    };
    let original = fixture
        .hardware
        .get_disk_with_revision(&fixture.disk)
        .await
        .unwrap()
        .unwrap()
        .0;
    let mut group = fixture
        .hardware
        .get_disk_group(1, 2, 3)
        .await
        .unwrap()
        .unwrap()
        .value;
    group.disk_ids.push(key.disk_id);
    fixture
        .hardware
        .kv()
        .put(
            0,
            0,
            key.to_path().as_bytes(),
            &serde_json::to_vec(&original).unwrap(),
            None,
        )
        .await
        .unwrap();
    fixture
        .hardware
        .kv()
        .put(
            0,
            0,
            DiskGroupKey {
                rack_id: 1,
                node_id: 2,
                disk_group_id: 3,
            }
            .to_path()
            .as_bytes(),
            &serde_json::to_vec(&group).unwrap(),
            None,
        )
        .await
        .unwrap();
    let path = format!(
        "/api/nodes/2/disk-groups/3/disks/{}",
        key.disk_id.to_display_string()
    );
    let response = request(&fixture.app, "DELETE", &path, None, StatusCode::CONFLICT).await;
    assert!(response["error"].as_str().unwrap().contains("Offline/Remove"));
    assert_eq!(
        fixture
            .hardware
            .get_disk_with_revision(&key)
            .await
            .unwrap()
            .unwrap()
            .0,
        original
    );
    assert_eq!(
        fixture
            .hardware
            .get_disk_group(1, 2, 3)
            .await
            .unwrap()
            .unwrap()
            .value
            .disk_ids,
        group.disk_ids
    );
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `/ready` (and `/health` alias) endpoint tests.

use std::sync::Arc;

use axum::body::to_bytes;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crowdb_diskdb::health;
use crowdb_diskdb::liveness::lifecycle::StartupPhase;
use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;

#[tokio::test]
async fn uncertain_metadata_write_closes_readiness_and_requests_fenced_process_recovery() {
    use crowdb_diskdb::{
        liveness::recovery,
        model::{disk::DdbDisk, disk_group::DdbDiskGroup},
    };
    use crowdb_protocol::{common::DiskId, diskdb::rpc::DiskValue};

    let container = Arc::new(DdbDiskGroupContainer::new(1));
    container.set_lifecycle_phase(StartupPhase::Up);
    let group = Arc::new(DdbDiskGroup::new(7, 1, 1));
    let disk = Arc::new(DdbDisk::new(
        DiskId { high: 2, low: 3 },
        7,
        1,
        1,
        DiskValue::default(),
    ));
    group.add_disk(disk.clone());
    container.replace_disk_group(&group);
    drop(disk.write_gate.enter().unwrap());

    let response = health::router(container.clone())
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_string(response).await;
    assert!(body.contains("metadata-write-outcome-unknown"), "{body}");
    assert!(body.contains("\"disk_group_id\":7"), "{body}");
    assert!(
        !container.is_degraded(),
        "the health read must not initiate recovery"
    );
    let writes = recovery::wait_required(&container).await;
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].disk_id, disk.disk_id);
    assert!(container.is_degraded());
    assert!(disk.write_gate.closed());
    assert!(
        !disk.write_gate.quiescent(),
        "unknown writes cannot acknowledge disk removal"
    );
}

#[test]
fn administrative_exclusion_does_not_request_process_recovery() {
    use crowdb_diskdb::{
        liveness::recovery,
        model::{disk::DdbDisk, disk_group::DdbDiskGroup},
    };
    use crowdb_protocol::{common::DiskId, diskdb::rpc::DiskValue};
    let container = DdbDiskGroupContainer::new(1);
    let group = Arc::new(DdbDiskGroup::new(7, 1, 1));
    let disk = Arc::new(DdbDisk::new(DiskId::default(), 7, 1, 1, DiskValue::default()));
    group.add_disk(disk.clone());
    container.replace_disk_group(&group);
    disk.exclude();
    assert!(recovery::uncertain_writes(&container).is_empty());
    assert!(disk.write_gate.closed() && disk.write_gate.quiescent());
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.expect("body");
    String::from_utf8(bytes.to_vec()).expect("utf8")
}

#[tokio::test]
async fn ready_returns_503_when_init() {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    let app = health::router(container);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_string(resp).await;
    assert!(body.contains("\"phase\":\"init\""), "body: {body}");
    assert!(body.contains("\"ready\":false"), "body: {body}");
}

#[tokio::test]
async fn ready_returns_503_when_loading() {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    container.set_lifecycle_phase(StartupPhase::Loading);
    let app = health::router(container);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_string(resp).await;
    assert!(body.contains("\"phase\":\"loading\""), "body: {body}");
}

#[tokio::test]
async fn ready_returns_200_when_up() {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    container.set_lifecycle_phase(StartupPhase::Up);
    let app = health::router(container);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("\"phase\":\"up\""), "body: {body}");
    assert!(body.contains("\"ready\":true"), "body: {body}");
    assert!(body.contains("\"degraded\":false"), "body: {body}");
}

#[tokio::test]
async fn health_alias_returns_same_as_ready() {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    container.set_lifecycle_phase(StartupPhase::Up);
    let app = health::router(container);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("\"phase\":\"up\""), "body: {body}");
}

#[tokio::test]
async fn ready_returns_503_when_degraded() {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    container.set_lifecycle_phase(StartupPhase::Up);
    container.enter_degraded_mode();
    let app = health::router(container);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_string(resp).await;
    assert!(body.contains("\"degraded\":true"), "body: {body}");
    assert!(body.contains("\"ready\":false"), "body: {body}");
}

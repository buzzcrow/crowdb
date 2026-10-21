// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/cluster_target.rs"]
mod cluster_target;
#[path = "common/memory_kv.rs"]
mod memory_kv;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cluster_target::TestClusterTarget;
use crowdb_console_shared::deployment::CLUSTER_KEY;
use crowdb_kv_client::{GetOutcome, ReadMode};
use serde_json::Value;
use tower::ServiceExt;

async fn get(state: crowdb_web::AppState) -> (StatusCode, Value) {
    let response = crowdb_web::router(state)
        .oneshot(
            Request::builder()
                .uri("/api/cluster/target")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 65536).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn committed(fixture: &TestClusterTarget) -> (Vec<u8>, u64) {
    match fixture
        .kv
        .get(0, 0, CLUSTER_KEY, ReadMode::Linearizable, None)
        .await
        .unwrap()
    {
        GetOutcome::Found { value, revision, .. } => (value.to_vec(), revision),
        GetOutcome::NotFound => panic!("missing cluster identity"),
    }
}

#[tokio::test]
async fn readonly_target_observes_committed_identity_without_publishing_or_repairing_state() {
    let fixture = TestClusterTarget::start().await;
    let before = committed(&fixture).await;
    let (status, value) = get(fixture.state.clone()).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["cluster_id"], fixture.prepared.identity.cluster_id);
    assert!(value["group0_management_endpoints"][0]
        .as_str()
        .unwrap()
        .starts_with("http://"));
    assert!(value["group0_rpc_endpoints"][0].as_str().unwrap().contains(':'));
    assert_eq!(committed(&fixture).await, before);
    assert_eq!(
        std::fs::read_dir(fixture.root.path().join("console"))
            .unwrap()
            .count(),
        0
    );
    assert!(value.get("runtime").is_none());
    let mut target = crowdb_console_shared::deployment::target::ResolvedClusterTarget {
        identity: serde_json::from_value(value).unwrap(),
        management_endpoints: vec![],
        runtime: None,
    };
    target.verify_native_authority(&fixture.kv).await.unwrap();
    target.identity.cluster_id = uuid::Uuid::new_v4().to_string();
    assert!(matches!(
        target.verify_native_authority(&fixture.kv).await,
        Err(crowdb_console_shared::deployment::target::TargetError::IdentityConflict)
    ));
    assert_eq!(committed(&fixture).await, before);
}

#[tokio::test]
async fn conflicting_binding_fails_without_rebinding_or_overwriting_authority() {
    let mut fixture = TestClusterTarget::start().await;
    fixture.binding.bootstrap.cluster_id = uuid::Uuid::new_v4().to_string();
    let path = fixture.root.path().join("node-binding.json");
    let bytes = serde_json::to_vec(&fixture.binding).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    let before = committed(&fixture).await;
    let (status, error) = get(fixture.state.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    assert_eq!(committed(&fixture).await, before);
}

#[tokio::test]
async fn absent_or_corrupt_publication_cannot_report_an_accepted_local_uuid_as_confirmed() {
    let fixture = TestClusterTarget::start().await;
    for bytes in [b"{}".as_slice(), b"not-json".as_slice()] {
        fixture.kv.put(0, 0, CLUSTER_KEY, bytes, None).await.unwrap();
        let before = committed(&fixture).await;
        let (status, error) = get(fixture.state.clone()).await;
        assert_eq!(status, StatusCode::CONFLICT, "{error}");
        assert_eq!(committed(&fixture).await, before);
    }
    fixture.kv.delete(0, 0, CLUSTER_KEY, None).await.unwrap();
    let (status, error) = get(fixture.state.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
}

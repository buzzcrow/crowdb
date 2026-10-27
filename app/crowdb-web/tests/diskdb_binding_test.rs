// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/cluster_target.rs"]
mod cluster_target;
#[path = "common/memory_kv.rs"]
mod memory_kv;

use axum::{body::Body, http::Request};
use cluster_target::TestClusterTarget;
use tower::ServiceExt;

#[tokio::test]
async fn disk_drilldown_uses_persisted_binding_without_local_process_observation() {
    let fixture = TestClusterTarget::start().await;
    assert_eq!(fixture.prepared.identity, fixture.binding.bootstrap);
    assert!(fixture.root.path().join("node-binding.json").exists());
    assert!(fixture
        .kv
        .get(
            0,
            0,
            b"/deployment/cluster",
            crowdb_kv_client::ReadMode::Linearizable,
            None
        )
        .await
        .is_ok());
    assert!(fixture.state.monitor_cache.snapshot().await.is_empty());
    let response = crowdb_web::router(fixture.state.clone())
        .oneshot(
            Request::builder()
                .uri("/api/diskdb/usage?dg=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // This fixture has no DiskDB owner. Authority resolution still succeeds;
    // the request must reach DiskDB routing rather than claim no cluster exists.
    let body = axum::body::to_bytes(response.into_body(), 8192).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(!text.contains("cluster not initialized"), "{text}");
    assert!(fixture.state.diskdb_client.read().await.is_some());
}

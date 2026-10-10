// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::get,
    Json, Router,
};
use crowdb_protocol::mgmt::node::{
    CandidateSnapshot, NodeAdvertisement, NodeHandshake, NODE_PROTOCOL_VERSION,
};
use crowdb_web::{router, AppState};
use tower::ServiceExt;

#[tokio::test]
async fn local_candidate_is_shown_before_any_kv_server_exists() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let handshake = NodeHandshake {
        advertisement: NodeAdvertisement {
            discovery_id: "12345678-1234-4234-8234-123456789abc".into(),
            protocol_version: NODE_PROTOCOL_VERSION,
            monitor_endpoints: vec![endpoint.clone()],
            cluster_id: None,
        },
        physical_host_id: "one-host".into(),
        rack_hint: None,
    };
    let expected = handshake.clone();
    let monitor = Router::new()
        .route("/node", get(move || async move { Json(handshake) }))
        .route(
            "/candidates",
            get(|| async { Json(CandidateSnapshot::default()) }),
        );
    let task = tokio::spawn(async move {
        axum::serve(listener, monitor).await.unwrap();
    });
    let state = AppState::default().with_node_monitor(&endpoint).unwrap();
    let app = router(state);
    let reply = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/node/candidates")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reply.status(), StatusCode::OK);
    let body = axum::body::to_bytes(reply.into_body(), 256 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["local"]["physical_host_id"], expected.physical_host_id);
    assert_eq!(
        body["candidates"][0]["advertisement"]["discovery_id"],
        expected.advertisement.discovery_id
    );
    assert_eq!(body["candidates"][0]["state"], "unbound");
    let mode = app
        .oneshot(Request::builder().uri("/api/mode").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let mode = axum::body::to_bytes(mode.into_body(), 1024).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&mode).unwrap()["mode"],
        "node"
    );
    task.abort();
}

#[test]
fn discovery_proxy_requires_local_configured_monitor() {
    for endpoint in [
        "http://192.0.2.1:9093",
        "http://user:password@127.0.0.1:9093",
        "http://127.0.0.1:9093/path",
        "http://127.0.0.1:9093?query",
    ] {
        assert!(AppState::default().with_node_monitor(endpoint).is_err());
    }
}

#[tokio::test]
async fn absent_monitor_does_not_invent_candidates() {
    let response = router(AppState::default())
        .oneshot(
            Request::builder()
                .uri("/api/node/candidates")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

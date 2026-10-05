// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crowdb_console_shared::{
    config::{NodeEntry, RackEntry, ServerEntry},
    ConsoleConfig,
};
use crowdb_web::{router, AppState};
use tower::ServiceExt;

#[tokio::test]
async fn unresponsive_authority_bounds_creation_and_fences_duplicate_unknown_outcomes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let upstream = axum::Router::new().fallback(|| async { std::future::pending::<StatusCode>().await });
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    let mut config = ConsoleConfig::default();
    config
        .add_rack(RackEntry {
            id: 1,
            name: String::new(),
        })
        .unwrap();
    config
        .add_node(NodeEntry {
            id: 1,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
            ssh_credential_ref: None,
        })
        .unwrap();
    let mut server = ServerEntry::new("1", origin);
    server.node_id = Some(1);
    config.servers.push(server);
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("disk-group-deadline");
    let state = AppState::with_runtime_root(config, root.path().to_owned());
    let app = router(state.clone());
    let request = || {
        Request::builder()
            .method("POST")
            .uri("/api/nodes/1/disk-groups")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id":1,"name":"storage"}"#))
            .unwrap()
    };
    let started = std::time::Instant::now();
    let response = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let diagnostic = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        diagnostic.contains("outcome unknown after 8 seconds"),
        "{diagnostic}"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(9));
    let repeated = app.oneshot(request()).await.unwrap();
    assert_eq!(repeated.status(), StatusCode::CONFLICT);
    assert!(state.config.read().unwrap().disk_groups.is_empty());
    task.abort();
}

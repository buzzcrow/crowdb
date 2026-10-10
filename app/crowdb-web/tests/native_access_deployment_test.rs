// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::os::unix::fs::PermissionsExt;

use axum::{body::Body, http::Request};
use crowdb_console_shared::config::{NodeEntry, RackEntry};
use crowdb_web::{router, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn call(app: &axum::Router, method: &str, path: &str, body: Value) -> (u16, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

struct TestProcess(u32);
impl Drop for TestProcess {
    fn drop(&mut self) {
        if crowdb_console_shared::lifecycle::process_is_alive(self.0) {
            crowdb_console_shared::lifecycle::stop_pid(self.0).unwrap();
        }
    }
}

struct TestCluster(std::path::PathBuf);

impl Drop for TestCluster {
    fn drop(&mut self) {
        crowdb_console_shared::ops::s3::stop(&self.0).unwrap();
    }
}

#[tokio::test]
#[cfg_attr(target_os = "macos", ignore = "Requires a complete native storage chain")]
async fn native_access_deployment_provisions_private_credentials_and_reuses_them() {
    let fixture = crowdb_test_harness::test_dirs::tempdir_in_test_data("native-access-storage");
    let source = fixture.path().to_owned();
    let _cluster = TestCluster(source.clone());
    crowdb_console_shared::ops::s3::start(&source).await.unwrap();
    let (cluster, _) = crowdb_console_shared::ops::s3::load(&source).unwrap();
    let workspace = crowdb_test_harness::test_dirs::tempdir_in_test_data("native-access-deploy");
    let mut config = cluster;
    config
        .servers
        .retain(|server| server.service_type != crowdb_console_shared::config::ServiceType::AccessServer);
    config
        .add_rack(RackEntry {
            id: 1,
            name: "native fixture".into(),
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
    let seeds = config
        .servers
        .iter()
        .filter(|server| server.service_type == crowdb_console_shared::config::ServiceType::PaxosKv)
        .map(|server| server.url.clone())
        .collect();
    let mut state = AppState::with_runtime_root(config, workspace.path().to_owned());
    state.authority_seeds = std::sync::Arc::new(seeds);
    let app = router(state.clone());
    let http = crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::Web);
    let s3 = crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::Web);
    let health = crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::Web);
    let (status, body) = call(
        &app,
        "POST",
        "/api/nodes/1/services/deploy",
        json!({
        "kind":"access-server", "instance_id":"902", "http_port":http, "s3_port":s3, "health_port":health,
        "test_single_node":true }),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let deployed: Value = serde_json::from_str(&body).unwrap();
    let mut child = TestProcess(deployed["pid"].as_u64().unwrap().try_into().unwrap());
    let credentials = workspace.path().join("secrets/client.env");
    let before = std::fs::read(&credentials).unwrap();
    assert!(!before.is_empty());
    assert_eq!(
        std::fs::metadata(&credentials).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let (status, body) = call(&app, "GET", "/api/access/s3/", Value::Null).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("ListAllMyBucketsResult"));
    let (status, body) = call(&app, "GET", "/api/access/iceberg/v1/namespaces", Value::Null).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = call(
        &app,
        "POST",
        "/api/services/access-server-902/restart",
        Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    child.0 = serde_json::from_str::<Value>(&body).unwrap()["pid"]
        .as_u64()
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(std::fs::read(&credentials).unwrap(), before);
    let (status, body) = call(&app, "GET", "/api/access/s3/", Value::Null).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = call(&app, "DELETE", "/api/services/access-server-902", Value::Null).await;
    assert_eq!(status, 200, "{body}");
    assert!(!crowdb_console_shared::lifecycle::process_is_alive(child.0));
}

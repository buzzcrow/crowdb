// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Real child ownership when a management request is cancelled before readiness.

use std::{collections::BTreeSet, path::Path, time::Duration};

use axum::{body::Body, http::Request};
use crowdb_console_shared::{lifecycle, ConsoleConfig};
use crowdb_web::{router, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

struct TestChildren(BTreeSet<u32>);
impl Drop for TestChildren {
    fn drop(&mut self) {
        for pid in &self.0 {
            lifecycle::stop_pid_with_timeout(*pid, Duration::from_secs(1)).unwrap();
        }
    }
}

async fn request(app: axum::Router, path: &str, body: Value) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn success(app: &axum::Router, path: &str, body: Value) -> Value {
    let response = request(app.clone(), path, body).await;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert!(
        status.is_success(),
        "{path}: {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

fn spawned_pid(root: &Path, service: &str, excluded: &BTreeSet<u32>) -> Option<u32> {
    std::fs::read_dir(root.join("N-1/log"))
        .ok()?
        .filter_map(Result::ok)
        .find_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            let pid = name
                .strip_prefix(service)?
                .strip_prefix('-')?
                .strip_suffix(".out.log")?
                .parse()
                .ok()?;
            (!excluded.contains(&pid) && lifecycle::process_is_alive(pid)).then_some(pid)
        })
}

async fn cancel_and_reset(kind: &str, restart: bool) {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("cancel-reset");
    let state =
        AppState::with_runtime_root(ConsoleConfig::default(), root.path().to_owned()).with_test_mode(true);
    let app = router(state.clone());
    let mut children = TestChildren(BTreeSet::new());
    success(&app, "/api/racks", json!({"id": 1})).await;
    success(
        &app,
        "/api/nodes",
        json!({"id": 1, "rack_id": 1, "host": "127.0.0.1", "ssh_user": ""}),
    )
    .await;
    let binary = lifecycle::crowdb_kv_server_bin().expect("build the actual KV server");
    let kv = json!({"rest_port": free_port(), "rpc_port": free_port(), "binary": binary, "election_profile": "test"});
    let (mut path, mut body, service) = if kind == "diskdb" {
        let deployed = success(&app, "/api/nodes/1/server/deploy", kv).await;
        children
            .0
            .insert(u32::try_from(deployed["pid"].as_u64().unwrap()).unwrap());
        success(&app, "/api/cluster/init", json!({"nodes": [1]})).await;
        let port = crowdb_protocol::port::alloc::alloc_test_port_range(
            crowdb_protocol::ServicePort::DiskdbListen,
            3,
        )[0];
        (
            "/api/nodes/1/diskdb/deploy",
            json!({"rpc_port": port}),
            "crowdb-diskdb",
        )
    } else {
        ("/api/nodes/1/server/deploy", kv, "crowdb-kv-server")
    };
    if restart {
        let deployed = success(&app, path, body).await;
        children
            .0
            .insert(u32::try_from(deployed["pid"].as_u64().unwrap()).unwrap());
        path = if kind == "diskdb" {
            "/api/nodes/1/diskdb/restart"
        } else {
            "/api/nodes/1/server/restart"
        };
        body = json!({});
    }
    let started = std::time::Instant::now();
    let deployment = tokio::spawn(request(app.clone(), path, body));
    let pid = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(pid) = spawned_pid(root.path(), service, &children.0) {
                break pid;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual child starts before cancellation");
    children.0.insert(pid);
    assert!(
        !deployment.is_finished(),
        "request must still be awaiting readiness"
    );
    assert!(
        !state
            .config
            .read()
            .unwrap()
            .servers
            .iter()
            .any(|entry| entry.pid == Some(pid)),
        "child must not already be registered"
    );
    deployment.abort();
    assert!(deployment.await.unwrap_err().is_cancelled());
    success(&app, "/internal/reset", json!({})).await;
    eprintln!(
        "[PHASE] {kind} cancelled deployment and Reset: {}ms",
        started.elapsed().as_millis()
    );
    for pid in &children.0 {
        assert!(!lifecycle::process_is_alive(*pid), "Reset left child {pid} alive");
    }
    let config = state.config.read().unwrap();
    assert!(config.servers.is_empty());
    assert!(config.nodes.is_empty());
    assert!(config.racks.is_empty());
    assert!(
        !root.path().join("N-1").exists(),
        "Reset must remove the workspace after child exit"
    );
    children.0.clear();
}

fn free_port() -> u16 {
    crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::Web)
}

#[tokio::test]
async fn cancelled_kv_deployment_is_registered_before_reset_removes_its_workspace() {
    cancel_and_reset("kv", false).await;
}

#[tokio::test]
async fn cancelled_diskdb_deployment_is_registered_before_reset_removes_its_workspace() {
    cancel_and_reset("diskdb", false).await;
}

#[tokio::test]
async fn cancelled_kv_restart_is_registered_before_reset_removes_its_workspace() {
    cancel_and_reset("kv", true).await;
}

#[tokio::test]
async fn cancelled_diskdb_restart_is_registered_before_reset_removes_its_workspace() {
    cancel_and_reset("diskdb", true).await;
}

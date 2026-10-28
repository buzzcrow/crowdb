// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Integration coverage for the physical tree (A3) routes: rack + node
//! CRUD, rack detail, rack-scoped node add, node detail, node-addressed
//! server lifecycle. The CRUD path runs without any upstream server.
//! Workspace/reset tests use a fake child exposing only a health stub. Packaged KV
//! deployment/restart/stop acceptance belongs to the release container tests.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use crowdb_console_shared::lifecycle::stop_pid_with_timeout;
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use serde_json::{json, Value};

struct ProcessGuard {
    pids: BTreeMap<String, u32>,
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        for pid in self.pids.values() {
            let _ = stop_pid_with_timeout(*pid, Duration::from_secs(1));
        }
    }
}

fn pick_free_port() -> u16 {
    crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::Web)
}

async fn spawn_web_with_path(path: std::path::PathBuf) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let cfg = ConsoleConfig::default();
    let state = AppState::with_config(cfg, Some(path)).with_test_mode(true);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn tempdir(tag: &str) -> std::path::PathBuf {
    let base = crowdb_test_harness::test_dirs::test_data_dir();
    let unique = format!(
        "crowdb-web-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = base.join(unique);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn json_get(client: &reqwest::Client, url: &str) -> (reqwest::StatusCode, Value) {
    let r = client.get(url).send().await.unwrap();
    let status = r.status();
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    (status, v)
}

async fn json_post(client: &reqwest::Client, url: &str, body: Value) -> (reqwest::StatusCode, Value) {
    let r = client.post(url).json(&body).send().await.unwrap();
    let status = r.status();
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    (status, v)
}

async fn delete_status(client: &reqwest::Client, url: &str) -> reqwest::StatusCode {
    client.delete(url).send().await.unwrap().status()
}

fn assert_node_workspace(dir: &std::path::Path, node_id: &str) {
    let base = dir.join(format!("N-{node_id}"));
    assert!(
        base.is_dir(),
        "missing workspace for {node_id}: {}",
        base.display()
    );
    assert!(
        base.join("bin").is_dir(),
        "missing bin for {node_id}: {}",
        base.display()
    );
    assert!(
        base.join("log").is_dir(),
        "missing log for {node_id}: {}",
        base.display()
    );
    assert!(
        base.join("waldata").is_dir(),
        "missing waldata for {node_id}: {}",
        base.display()
    );
}

#[tokio::test]
async fn prepare_node_workspace_creates_base_directory_when_missing() {
    // Test the specific regression: prepare_node_workspace should create
    // the base directory (N-{node_id}) before creating subdirectories.
    let dir = tempdir("workspace-missing-base");
    let cfg_path = dir.join("console.toml");
    let state = AppState::with_config(ConsoleConfig::default(), Some(cfg_path.clone()));

    // The runtime_root is the parent of cfg_path (dir), which exists.
    // But N-test-node doesn't exist yet.
    let result = state.prepare_node_workspace("test-node");
    assert!(result.is_ok(), "prepare_node_workspace should succeed");

    let base = dir.join("N-test-node");
    assert!(base.is_dir(), "base directory should be created");
    assert!(base.join("bin").is_dir(), "bin directory should be created");
    assert!(base.join("log").is_dir(), "log directory should be created");
    assert!(
        base.join("waldata").is_dir(),
        "waldata directory should be created"
    );
}

#[tokio::test]
async fn rack_node_crud_through_web_routes() {
    let dir = tempdir("rack-node");
    let cfg_path = dir.join("console.toml");
    let addr = spawn_web_with_path(cfg_path.clone()).await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Empty registry.
    let (s, v) = json_get(&client, &format!("{base}/api/racks")).await;
    assert!(s.is_success(), "list racks empty: {s}");
    assert_eq!(v.as_array().unwrap().len(), 0);

    // Add rack.
    let (s, v) = json_post(
        &client,
        &format!("{base}/api/racks"),
        json!({ "id": 1, "name": "rack-1" }),
    )
    .await;
    assert_eq!(s.as_u16(), 201, "add rack: {s} {v}");
    assert_eq!(v["id"], 1);

    // Duplicate rack -> 409.
    let (s, _) = json_post(&client, &format!("{base}/api/racks"), json!({ "id": 1 })).await;
    assert_eq!(s.as_u16(), 409);

    // GET /api/racks/:rack_id — rack detail.
    let (s, v) = json_get(&client, &format!("{base}/api/racks/1")).await;
    assert!(s.is_success(), "get rack: {s} {v}");
    assert_eq!(v["id"], 1);
    assert_eq!(v["name"], "rack-1");
    assert_eq!(v["nodes"].as_array().unwrap().len(), 0);

    // Add node referencing missing rack -> 400 (Validation).
    let (s, _) = json_post(
        &client,
        &format!("{base}/api/nodes"),
        json!({ "id": 1, "rack_id": 999, "host": "127.0.0.1", "ssh_user": "" }),
    )
    .await;
    assert_eq!(s.as_u16(), 400);

    // Add node via rack-scoped route.
    let (s, v) = json_post(
        &client,
        &format!("{base}/api/racks/1/nodes"),
        json!({ "id": 1, "rack_id": 999, "host": "127.0.0.1", "ssh_user": "" }),
    )
    .await;
    assert_eq!(s.as_u16(), 201, "add rack node: {s} {v}");
    assert_eq!(v["rack_id"], 1);
    assert_node_workspace(&dir, "1");

    // GET /api/racks/:rack_id/nodes.
    let (_, v) = json_get(&client, &format!("{base}/api/racks/1/nodes")).await;
    assert_eq!(v.as_array().unwrap().len(), 1);

    // List nodes filtered by rack.
    let (_, v) = json_get(&client, &format!("{base}/api/nodes?rack_id=1")).await;
    assert_eq!(v.as_array().unwrap().len(), 1);
    let (_, v) = json_get(&client, &format!("{base}/api/nodes?rack_id=999")).await;
    assert_eq!(v.as_array().unwrap().len(), 0);

    // GET /api/nodes/:id — node detail.
    let (s, v) = json_get(&client, &format!("{base}/api/nodes/1")).await;
    assert!(s.is_success(), "get node: {s} {v}");
    assert_eq!(v["id"], 1);
    assert_eq!(v["rack_id"], 1);
    assert_eq!(v["has_server"], false);

    // Rack detail now shows the node.
    let (_, v) = json_get(&client, &format!("{base}/api/racks/1")).await;
    assert_eq!(v["nodes"].as_array().unwrap().len(), 1);

    // Cannot remove rack while node references it.
    let s = delete_status(&client, &format!("{base}/api/racks/1")).await;
    assert_eq!(s.as_u16(), 409);

    // Ping a local-fork node returns ok=true (no SSH attempted).
    let (s, v) = json_post(&client, &format!("{base}/api/nodes/1/ping"), json!({})).await;
    assert!(s.is_success(), "ping local: {s} {v}");
    assert_eq!(v["ok"], true);

    // GET /api/nodes/:id/server — 404 since nothing is deployed.
    let (s, _) = json_get(&client, &format!("{base}/api/nodes/1/server")).await;
    assert_eq!(s.as_u16(), 404);

    // Remove node, then rack.
    let s = delete_status(&client, &format!("{base}/api/nodes/1")).await;
    assert_eq!(s.as_u16(), 204);
    let s = delete_status(&client, &format!("{base}/api/racks/1")).await;
    assert_eq!(s.as_u16(), 204);

    assert!(!cfg_path.exists(), "Web must not write local topology");
}

#[tokio::test]
async fn multiple_racks_and_nodes_create_expected_workspaces() {
    let dir = tempdir("multi-workspace");
    let cfg_path = dir.join("console.toml");
    let addr = spawn_web_with_path(cfg_path.clone()).await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let stub = TestChildStub::new(&dir);
    let bin = &stub.binary;

    let (s, _) = json_post(
        &client,
        &format!("{base}/api/racks"),
        json!({ "id": 1, "name": "rack-1" }),
    )
    .await;
    assert_eq!(s.as_u16(), 201);
    let (s, _) = json_post(
        &client,
        &format!("{base}/api/racks"),
        json!({ "id": 2, "name": "rack-2" }),
    )
    .await;
    assert_eq!(s.as_u16(), 201);

    let (s, _) = json_post(
        &client,
        &format!("{base}/api/racks/1/nodes"),
        json!({ "id": 1, "rack_id": 999, "host": "127.0.0.1", "ssh_user": "" }),
    )
    .await;
    assert_eq!(s.as_u16(), 201);
    let (s, _) = json_post(
        &client,
        &format!("{base}/api/racks/1/nodes"),
        json!({ "id": 2, "rack_id": 999, "host": "127.0.0.1", "ssh_user": "" }),
    )
    .await;
    assert_eq!(s.as_u16(), 201);
    let (s, _) = json_post(
        &client,
        &format!("{base}/api/racks/2/nodes"),
        json!({ "id": 10, "rack_id": 999, "host": "127.0.0.1", "ssh_user": "" }),
    )
    .await;
    assert_eq!(s.as_u16(), 201);

    assert_node_workspace(&dir, "1");
    assert_node_workspace(&dir, "2");
    assert_node_workspace(&dir, "10");

    let (s, v) = json_post(
        &client,
        &format!("{base}/api/nodes/1/server/deploy"),
        json!({
            "rest_port": pick_free_port(),
            "rpc_port": pick_free_port(),
            "binary": bin.to_string_lossy().to_string(),
            "election_profile": "test",
        }),
    )
    .await;
    assert!(s.is_success(), "deploy node 1: {s} {v}");
    let pid1 = u32::try_from(v["pid"].as_u64().unwrap()).unwrap();

    let (s, v) = json_post(
        &client,
        &format!("{base}/api/nodes/10/server/deploy"),
        json!({
            "rest_port": pick_free_port(),
            "rpc_port": pick_free_port(),
            "binary": bin.to_string_lossy().to_string(),
            "election_profile": "test",
        }),
    )
    .await;
    assert!(s.is_success(), "deploy node 10: {s} {v}");
    let pid10 = u32::try_from(v["pid"].as_u64().unwrap()).unwrap();

    assert!(std::fs::read_dir(dir.join("N-1/bin")).unwrap().next().is_some());
    assert!(dir
        .join(format!("N-1/log/crowdb-paxos-kv-server-{pid1}.out.log"))
        .exists());
    assert!(std::fs::read_dir(dir.join("N-10/bin")).unwrap().next().is_some());
    assert!(dir
        .join(format!("N-10/log/crowdb-paxos-kv-server-{pid10}.out.log"))
        .exists());
    assert!(std::fs::read_dir(dir.join("N-2/bin")).unwrap().next().is_none());

    tokio::task::spawn_blocking(move || {
        let _ = stop_pid_with_timeout(pid1, Duration::from_millis(100));
        let _ = stop_pid_with_timeout(pid10, Duration::from_millis(100));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn reset_waits_for_owned_children_before_removing_workspaces() {
    let dir = tempdir("reset-waits");
    let stub = TestChildStub::new(&dir);
    let bin = &stub.binary;
    let addr = spawn_web_with_path(dir.join("console.toml")).await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let mut guard = ProcessGuard {
        pids: BTreeMap::new(),
    };
    let (status, body) = json_post(&client, &format!("{base}/api/racks"), json!({"id": 1})).await;
    assert_eq!(status.as_u16(), 201, "{body}");
    for node in [1, 2] {
        let (status, body) = json_post(
            &client,
            &format!("{base}/api/nodes"),
            json!({"id": node, "rack_id": 1, "host": "127.0.0.1", "ssh_user": ""}),
        )
        .await;
        assert_eq!(status.as_u16(), 201, "{body}");
        let (status, body) = json_post(
            &client,
            &format!("{base}/api/nodes/{node}/server/deploy"),
            json!({"rest_port": pick_free_port(), "rpc_port": pick_free_port(),
                "binary": bin, "election_profile": "test"}),
        )
        .await;
        assert!(status.is_success(), "{status}: {body}");
        let pid = u32::try_from(body["pid"].as_u64().unwrap()).unwrap();
        guard.pids.insert(node.to_string(), pid);
        assert!(dir.join(format!("N-{node}")).exists());
    }
    // Stop clears the in-memory PID while process teardown runs in the
    // background. Reset must also wait for that retained launch PID.
    let (status, body) = json_post(&client, &format!("{base}/api/nodes/1/server/stop"), json!({})).await;
    assert!(status.is_success(), "{status}: {body}");
    let (status, body) = json_post(&client, &format!("{base}/internal/reset"), json!({})).await;
    assert!(status.is_success(), "{status}: {body}");
    for (node, pid) in &guard.pids {
        assert!(
            !crowdb_console_shared::lifecycle::process_is_alive(*pid),
            "child {pid} survived reset"
        );
        assert!(
            !dir.join(format!("N-{node}")).exists(),
            "workspace survived reset"
        );
    }
    guard.pids.clear();
}

struct TestChildStub {
    binary: std::path::PathBuf,
}

impl TestChildStub {
    fn new(dir: &std::path::Path) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let binary = dir.join("fake-child");
        std::fs::write(
            &binary,
            r#"#!/usr/bin/env python3
import http.server
import sys
port = int(sys.argv[sys.argv.index("--management-port") + 1])
class Health(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"status":"ok"}'
        self.send_response(200 if self.path == "/health" else 404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
http.server.HTTPServer(("127.0.0.1", port), Health).serve_forever()
"#,
        )
        .unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self { binary }
    }
}

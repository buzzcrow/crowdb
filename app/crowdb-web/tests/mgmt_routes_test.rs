// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A5/A6 web e2e: spin up a real `crowdb-kv-server` plus the console web
//! backend, then exercise the orchestrated `/api/stores` and
//! `/api/stores/:sid/groups` routes through HTTP. Skips silently when
//! the `crowdb-kv-server` binary is not built.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crowdb_console_shared::bootstrap_intent::BootstrapIntent;
use crowdb_console_shared::cluster::{NodeHealth, NodeStore};
use crowdb_console_shared::config::web::{WebMode, WebProcessConfig};
use crowdb_console_shared::config::{NodeEntry, RackEntry, ServerEntry, ServiceType};
use crowdb_console_shared::lifecycle::{self, crowdb_kv_server_bin, stop_pid_with_timeout, DeployRequest};
use crowdb_console_shared::monitor::NodeRecord;
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use serde_json::json;

fn pick_free_port() -> u16 {
    crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::Web)
}

struct Upstream {
    pid: u32,
    mgmt_url: String,
    rpc_url: String,
    workspace: PathBuf,
}

impl Drop for Upstream {
    fn drop(&mut self) {
        let _ = stop_pid_with_timeout(self.pid, Duration::from_secs(5));
        let _ = std::fs::remove_dir_all(&self.workspace);
    }
}

async fn spawn_upstream() -> Option<Upstream> {
    let bin = crowdb_kv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    let workspace = crowdb_test_harness::test_dirs::test_data_dir().join(format!(
        "crowdb_kv-mgmt-routes-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        pick_free_port()
    ));
    std::fs::create_dir_all(&workspace).ok()?;
    std::fs::create_dir_all(workspace.join("bin")).ok()?;
    std::fs::create_dir_all(workspace.join("log")).ok()?;
    let node = NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
        ssh_credential_ref: None,
    };
    let req = DeployRequest {
        server_id: "1".to_string(),
        rest_port: pick_free_port(),
        rpc_port: pick_free_port(),
        election_profile: Some("e2e".into()),
        binary: Some(bin),
        ..Default::default()
    };
    let deployed = lifecycle::deploy_local_in_dir(&req, &node, &workspace)
        .await
        .expect("deploy_local_in_dir");
    Some(Upstream {
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        rpc_url: deployed.rpc_url,
        workspace,
    })
}

async fn spawn_web(upstream: &Upstream) -> SocketAddr {
    spawn_web_with_config_path(upstream, None).await
}

async fn spawn_web_with_config_path(
    upstream: &Upstream,
    config_path: Option<std::path::PathBuf>,
) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let cfg = config_for_upstream(upstream);
    let state = AppState::with_config(cfg, config_path);
    // Register the upstream's pid so `refresh_node_cache` (which skips
    // nodes with no tracked runtime pid) refreshes after mutations.
    state.set_runtime_pid(1, upstream.pid);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn config_for_upstream(upstream: &Upstream) -> ConsoleConfig {
    let mut cfg = ConsoleConfig::default();
    cfg.racks.push(RackEntry {
        id: 1,
        name: "r1".into(),
    });
    cfg.nodes.push(NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
        ssh_credential_ref: None,
    });
    cfg.add_server(ServerEntry {
        id: "n1".to_string(),
        url: upstream.mgmt_url.clone(),
        node_id: Some(1),
        rpc_url: Some(upstream.rpc_url.clone()),
        rest_port: None,
        rpc_port: None,
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: None,
        service_type: ServiceType::Kv,
        rpc_workers: None,
        no_fsync: false,
    })
    .unwrap();
    cfg
}

#[tokio::test]
async fn persistent_web_bootstrap_clears_verified_intent() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let config_path = upstream.workspace.join("console.toml");
    let web = spawn_web_with_config_path(&upstream, Some(config_path.clone())).await;
    let response = reqwest::Client::new()
        .post(format!("http://{web}/api/cluster/init"))
        .json(&json!({"nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());
    assert!(config_path.exists());
    assert!(!upstream.workspace.join("bootstrap-intent.toml").exists());
}

#[tokio::test]
async fn bare_metal_web_bootstrap_consumes_sealed_topology_input() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let source = upstream.workspace.join("bootstrap-source.toml");
    BootstrapIntent::capture(&config_for_upstream(&upstream), &[1])
        .unwrap()
        .seal(&source)
        .unwrap();
    let config = WebProcessConfig {
        version: 1,
        mode: WebMode::BareMetal,
        bind: "127.0.0.1".into(),
        port: 14000,
        group0_management_seeds: vec![upstream.mgmt_url.clone()],
        ui_root: "/tmp".into(),
        monitor_status: None,
        log_dir: "/tmp".into(),
        log_max_file_mb: 30,
        log_max_files: 5,
        request_timeout_ms: Some(500),
    };
    let state = AppState::with_config_engine(ConsoleConfig::default(), None, upstream.workspace.clone())
        .with_process_config(&config)
        .with_management_token("bare-metal-bootstrap-test-token-12345".into())
        .unwrap();
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/api/cluster/init"))
        .bearer_auth("bare-metal-bootstrap-test-token-12345")
        .json(&json!({"nodes": [1], "bootstrap_file": source}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());
    assert!(!upstream.workspace.join("bootstrap-intent.toml").exists());
    assert!(!upstream.workspace.join("console.toml").exists());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn full_mgmt_cycle_through_web_routes() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    // 1. Initialize the system group so non-zero stores can be created.
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "cluster init: {:?}", resp.text().await.ok());

    // 2. POST /api/stores → 201 (orchestrated create on node n1).
    let store_id: u64 = 7;
    let resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": store_id, "nodes": [1]}))
        .send()
        .await
        .expect("POST /api/stores");
    assert_eq!(resp.status(), 201, "{:?}", resp.text().await.ok());

    // 3. GET /api/stores → list contains the store (from cache).
    let stores: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        stores
            .iter()
            .any(|s| s.get("store_id").and_then(serde_json::Value::as_u64) == Some(store_id)),
        "store_id={store_id} not found in {stores:?}"
    );

    // 4. GET /api/stores/:sid → store detail.
    let resp = http
        .get(format!("{base}/api/stores/{store_id}"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let detail: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(detail["store_id"], store_id);
    let groups_before = detail["groups"].as_array().expect("groups array");
    assert!(
        groups_before.is_empty(),
        "new store should start empty: {detail:?}"
    );

    // 5. POST /api/stores/:sid/groups → 201 (orchestrated group create).
    let group_id: u64 = 70;
    let replica_id: u64 = 700;
    let resp = http
        .post(format!("{base}/api/stores/{store_id}/groups"))
        .json(&json!({"group_id": group_id, "replica_id": replica_id, "nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{:?}", resp.text().await.ok());

    // 6. POST /api/stores/:sid/groups → 201 (second group).
    let group_id_2: u64 = 80;
    let replica_id_2: u64 = 800;
    let resp = http
        .post(format!("{base}/api/stores/{store_id}/groups"))
        .json(&json!({"group_id": group_id_2, "replica_id": replica_id_2, "nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{:?}", resp.text().await.ok());

    // 7. GET /api/stores/:sid/groups → 2 explicitly created groups.
    let groups: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores/{store_id}/groups"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(groups.len(), 2, "expected 2 groups, got {groups:?}");

    // 8. GET /api/stores/:sid/groups/:gid → group detail.
    let resp = http
        .get(format!("{base}/api/stores/{store_id}/groups/{group_id_2}"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let gv: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(gv["group_id"], group_id_2);

    // 9. DELETE /api/stores/:sid/groups/:gid → removes the second group.
    let resp = http
        .delete(format!("{base}/api/stores/{store_id}/groups/{group_id_2}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // 10. DELETE /api/stores/:sid → removes the store.
    let resp = http
        .delete(format!("{base}/api/stores/{store_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // 11. GET /api/stores → store 7 should be gone (default store 1 may
    //    remain because crowdb-kv-server creates it on startup).
    let stores: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !stores
            .iter()
            .any(|s| s.get("store_id").and_then(serde_json::Value::as_u64) == Some(store_id)),
        "store {store_id} should be gone, got {stores:?}"
    );

    // Cleanup.
    let _ = lifecycle::stop_pid(upstream.pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn store_reads_reject_cached_topology_without_group0() {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let state = AppState::with_config(ConsoleConfig::default(), None);
    let mut stores = BTreeMap::new();
    stores.insert(
        7,
        NodeStore {
            node_id: 1,
            store_id: 7,
            listen_addr: None,
            groups: Vec::new(),
        },
    );
    state
        .monitor_cache
        .set_node_report(
            1,
            NodeRecord {
                health: NodeHealth::Up,
                last_seen_ms: 1,
                stores,
                last_error: None,
                recovering: false,
            },
        )
        .await;
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });

    let client = reqwest::Client::new();
    for path in [
        "/api/stores",
        "/api/stores/7",
        "/api/stores/7/groups",
        "/api/stores/7/groups/70",
        "/api/stores/7/groups/70/replicas",
    ] {
        let response = client
            .get(format!("http://{address}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 502, "{path}");
    }
}

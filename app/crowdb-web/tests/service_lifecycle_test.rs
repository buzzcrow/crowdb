// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crowdb_console_shared::{
    config::{LocalLaunchSpec, NodeEntry, RackEntry, ServerEntry, ServiceType},
    ConsoleConfig,
};
use crowdb_web::{router, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

fn config() -> ConsoleConfig {
    let mut config = ConsoleConfig::default();
    config
        .add_rack(RackEntry {
            id: 1,
            name: "r1".into(),
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
    let mut kv = ServerEntry::new("kv-1", "http://127.0.0.1:19191");
    kv.node_id = Some(1);
    config.servers.push(kv);
    config
}

async fn request(app: &axum::Router, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
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
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes))),
    )
}

#[tokio::test]
async fn typed_deployment_rejects_invalid_scope_before_starting_processes() {
    let state = AppState::with_config(config(), None);
    let app = router(state.clone());
    let path = "/api/nodes/1/services/deploy";
    for body in [
        json!({"kind":"chunkdb","instance_id":"0","http_port":43000,"rpc_port":43001}),
        json!({"kind":"chunkdb","instance_id":"9223372036854775808","http_port":43000,"rpc_port":43001}),
        json!({"kind":"chunkdb","instance_id":"1","http_port":43000,"rpc_port":43000}),
        json!({"kind":"access-server","instance_id":"1","http_port":43000,"s3_port":43001,"rpc_port":43002}),
        json!({"kind":"diskio","instance_id":"1","rpc_port":43000,"http_port":43001}),
        json!({"kind":"chunk-kv","instance_id":"1","http_port":43000,"rpc_port":43001,"metadata_store_id":1,"bootstrap_group_id":0}),
        json!({"kind":"chunkdb","instance_id":"1","http_port":43000,"rpc_port":43001,"metadata_store_id":1}),
    ] {
        let (status, message) = request(&app, "POST", path, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{message}");
    }
    let (status, _) = request(
        &app,
        "POST",
        path,
        json!({"kind":"chunkdb","instance_id":"1","http_port":19191,"rpc_port":43001}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = request(&app, "POST", "/api/services/kv-1/stop", Value::Null).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(state.config.read().unwrap().servers.len(), 1);
}

#[tokio::test]
async fn auxiliary_lifecycle_keeps_exact_identity_and_preserves_data() {
    let workdir = crowdb_test_harness::test_dirs::tempdir_in_test_data("typed-service");
    std::fs::write(workdir.path().join("data"), "preserve").unwrap();
    let mut child = std::process::Command::new("/bin/sleep")
        .arg("600")
        .current_dir(workdir.path())
        .spawn()
        .unwrap();
    let original_pid = child.id();
    let mut config = config();
    let mut entry = ServerEntry::new("chunkdb-9007199254740993", "http://127.0.0.1:43000");
    entry.service_type = ServiceType::Chunkdb;
    entry.node_id = Some(1);
    entry.pid = Some(original_pid);
    config.servers.push(entry.clone());
    config.local_launches.insert(
        entry.id.clone(),
        LocalLaunchSpec {
            program: "/bin/sleep".into(),
            args: vec!["600".into()],
            workdir: workdir.path().to_string_lossy().into_owned(),
            env: std::collections::BTreeMap::new(),
            env_file: None,
            readiness_url: None,
        },
    );
    let state = AppState::with_runtime_root(config, workdir.path().join("runtime"));
    let app = router(state.clone());
    let (status, _) = request(&app, "DELETE", "/api/nodes/1", Value::Null).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(crowdb_console_shared::lifecycle::process_is_alive(original_pid));
    let path = format!("/api/services/{}", entry.id);
    let (status, result) = request(&app, "POST", &format!("{path}/restart"), Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    let replacement = u32::try_from(result["pid"].as_u64().unwrap()).unwrap();
    assert_ne!(replacement, original_pid);
    child.wait().unwrap();
    let (_, rows) = request(&app, "GET", "/api/servers", Value::Null).await;
    let row = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == entry.id)
        .unwrap();
    assert_eq!(row["pid"], replacement);
    assert_eq!(row["service_type"], "chunkdb");
    assert_eq!(row["health"], "up");
    let (status, _) = request(&app, "POST", &format!("{path}/stop"), Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!crowdb_console_shared::lifecycle::process_is_alive(replacement));
    let (status, _) = request(&app, "DELETE", &path, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        std::fs::read_to_string(workdir.path().join("data")).unwrap(),
        "preserve"
    );
    assert_eq!(state.config.read().unwrap().servers.len(), 1);
}

#[tokio::test]
async fn upgraded_executable_can_restart_without_relaxing_pid_identity() {
    let workdir = crowdb_test_harness::test_dirs::tempdir_in_test_data("upgraded-service");
    let program = workdir.path().join("service");
    stage_sleep_executable(&program);
    let mut child = std::process::Command::new(&program)
        .arg("600")
        .current_dir(workdir.path())
        .spawn()
        .unwrap();
    let old_pid = child.id();
    std::fs::remove_file(&program).unwrap();
    stage_sleep_executable(&program);
    let mut cfg = config();
    let mut entry = ServerEntry::new("chunkdb-1", "http://127.0.0.1:43000");
    entry.service_type = ServiceType::Chunkdb;
    entry.node_id = Some(1);
    entry.pid = Some(old_pid);
    cfg.servers.push(entry);
    cfg.local_launches.insert(
        "chunkdb-1".into(),
        LocalLaunchSpec {
            program: program.to_string_lossy().into_owned(),
            args: vec!["600".into()],
            workdir: workdir.path().to_string_lossy().into_owned(),
            env: std::collections::BTreeMap::new(),
            env_file: None,
            readiness_url: None,
        },
    );
    let state = AppState::with_runtime_root(cfg, workdir.path().join("runtime"));
    let app = router(state.clone());
    // Matching executable path alone must not permit a different command.
    state
        .config
        .write()
        .unwrap()
        .local_launches
        .get_mut("chunkdb-1")
        .unwrap()
        .args = vec!["601".into()];
    let (status, _) = request(&app, "POST", "/api/services/chunkdb-1/stop", Value::Null).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(crowdb_console_shared::lifecycle::process_is_alive(old_pid));
    state
        .config
        .write()
        .unwrap()
        .local_launches
        .get_mut("chunkdb-1")
        .unwrap()
        .args = vec!["600".into()];
    let (status, result) = request(&app, "POST", "/api/services/chunkdb-1/restart", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_ne!(result["pid"].as_u64().unwrap(), u64::from(old_pid));
    child.wait().unwrap();
    let (status, result) = request(&app, "POST", "/api/services/chunkdb-1/stop", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{result}");
}

fn stage_sleep_executable(program: &std::path::Path) {
    // Keep writable executable handles out of the multithreaded test parent:
    // concurrent forks inherit even CLOEXEC handles until their exec completes.
    let output = std::process::Command::new("cp")
        .arg("/bin/sleep")
        .arg(program)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
}

#[tokio::test]
async fn stale_pid_cannot_signal_an_unrelated_process() {
    let workspace = crowdb_test_harness::test_dirs::tempdir_in_test_data("typed-service");
    let mut config = config();
    let mut entry = ServerEntry::new("diskio-1", "http://127.0.0.1:43000");
    entry.service_type = ServiceType::Diskio;
    entry.node_id = Some(1);
    entry.pid = Some(std::process::id());
    config.servers.push(entry);
    config.local_launches.insert(
        "diskio-1".into(),
        LocalLaunchSpec {
            program: "/bin/sleep".into(),
            args: vec!["600".into()],
            workdir: workspace.path().to_string_lossy().into_owned(),
            env: std::collections::BTreeMap::new(),
            env_file: None,
            readiness_url: None,
        },
    );
    let app = router(AppState::with_config(config, None));
    let (status, message) = request(&app, "POST", "/api/services/diskio-1/stop", Value::Null).await;
    assert_eq!(status, StatusCode::CONFLICT, "{message}");
}

#[tokio::test]
async fn deploys_real_chunkdb_with_fixed_cluster_seeds_and_retained_launch() {
    let cluster = crowdb_test_harness::cluster::KvCluster::start().await;
    let kv = std::sync::Arc::new(crowdb_kv_client::CrowdbKvClient::new(
        crowdb_kv_client::ClientConfig::new(cluster.mgmt_endpoints.clone()),
    ));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    kv.seed_leader(0, 1, cluster.group1_leader_endpoint.clone());
    let sysmd = crowdb_kv_client::CrowdbSysmdClient::from_shared(kv.clone());
    sysmd.add_store(0, &[1]).await.unwrap();
    sysmd.add_group(0, 0).await.unwrap();
    sysmd.add_group(0, 1).await.unwrap();
    let hardware = crowdb_kv_client::HardwareClient::from_shared(kv.clone());
    hardware
        .add_rack(
            1,
            &crowdb_protocol::common::RackValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                node_ids: vec![1],
                name: "test rack".into(),
            },
        )
        .await
        .unwrap();
    hardware
        .add_node(
            1,
            1,
            &crowdb_protocol::common::NodeValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                management_host: "127.0.0.1".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let workspace = crowdb_test_harness::test_dirs::tempdir_in_test_data("typed-service");
    let mut config = config();
    config.servers.clear();
    for (index, endpoint) in cluster.mgmt_endpoints.iter().enumerate() {
        let mut server = ServerEntry::new(format!("kv-{}", index + 1), endpoint.clone());
        server.node_id = Some(u64::try_from(index + 1).unwrap());
        config.servers.push(server);
    }
    let state = AppState::with_runtime_root(config, workspace.path().to_owned());
    *state.kv_client.write().await = Some(kv);
    let app = router(state.clone());
    let http = crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::Web);
    let rpc = crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::Web);
    let (status, deployed) = request(
        &app,
        "POST",
        "/api/nodes/1/services/deploy",
        json!({
            "kind":"chunkdb", "instance_id":"901", "http_port":http, "rpc_port":rpc,
            "test_single_node":true,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{deployed}");
    let pid = u32::try_from(deployed["pid"].as_u64().unwrap()).unwrap();
    let _process = TestProcess(pid);
    let launch = state.config.read().unwrap().local_launches["chunkdb-901"].clone();
    let body = std::fs::read_to_string(
        std::path::Path::new(&launch.workdir).join("conf/crowdb_chunkdb_config.toml"),
    )
    .unwrap();
    assert!(body.contains(&cluster.mgmt_endpoints[0]));
    let (status, removed) = request(&app, "DELETE", "/api/services/chunkdb-901", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{removed}");
    assert!(!crowdb_console_shared::lifecycle::process_is_alive(pid));
    assert!(std::path::Path::new(&launch.workdir).is_dir());
}

struct TestProcess(u32);
impl Drop for TestProcess {
    fn drop(&mut self) {
        if crowdb_console_shared::lifecycle::process_is_alive(self.0) {
            crowdb_console_shared::lifecycle::stop_pid(self.0).unwrap();
        }
    }
}

#[tokio::test]
async fn reset_removes_auxiliary_launches_before_nodes_and_racks() {
    let mut config = config();
    for (id, kind) in [
        ("cdb", ServiceType::Chunkdb),
        ("io", ServiceType::Diskio),
        ("ckv", ServiceType::ChunkKv),
        ("access", ServiceType::AccessServer),
    ] {
        let mut server = ServerEntry::new(id, "http://127.0.0.1:19999");
        server.node_id = Some(1);
        server.service_type = kind;
        config.servers.push(server);
        config.local_launches.insert(
            id.into(),
            LocalLaunchSpec {
                program: "/nonexistent/stopped-service".into(),
                args: vec![],
                workdir: "/nonexistent/stopped-workspace".into(),
                ..Default::default()
            },
        );
    }
    let state = AppState::with_config(config, None);
    let (status, body) = request(
        &router(state.clone()),
        "POST",
        "/api/cluster/destroy",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let config = state.config.read().unwrap();
    assert!(config.servers.is_empty());
    assert!(config.local_launches.is_empty());
    assert!(config.nodes.is_empty());
    assert!(config.racks.is_empty());
}

#[tokio::test]
async fn deployment_defaults_avoid_cross_service_and_live_listener_conflicts() {
    let mut config = config();
    let occupied = std::net::TcpListener::bind("0.0.0.0:12010").ok();
    let mut access = ServerEntry::new("access-server-7", "http://127.0.0.1:9092");
    access.service_type = ServiceType::AccessServer;
    config.servers.push(access);
    config.local_launches.insert(
        "access-server-7".into(),
        LocalLaunchSpec {
            env: [("CROWDB_S3_PUBLIC_URI".into(), "http://127.0.0.1:9091".into())].into(),
            ..Default::default()
        },
    );
    let app = router(AppState::with_config(config, None));
    let (status, body) = request(&app, "GET", "/api/deployment-defaults", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["access-server"]["instance_id"], "8");
    let mut ports = std::collections::HashSet::new();
    for (kind, values) in body.as_object().unwrap() {
        for name in ["http_port", "rpc_port", "s3_port", "health_port"] {
            if let Some(port) = values[name].as_u64() {
                assert!(![9091, 9092, 19191].contains(&port));
                if occupied.is_some() {
                    assert_ne!(port, 12010);
                }
                assert!(ports.insert(port));
                if kind == "diskdb" {
                    assert!(ports.insert(port + 1));
                    assert!(ports.insert(port + 2));
                }
            }
        }
    }
    let (status, _) = request(
        &app,
        "POST",
        "/api/nodes/1/services/deploy",
        json!({"kind":"chunkdb","instance_id":"1","http_port":9091,"rpc_port":12110}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

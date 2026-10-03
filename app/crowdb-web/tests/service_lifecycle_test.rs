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
        json!({"kind":"diskio","instance_id":"1","rpc_port":43000}),
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
    assert_eq!(row["health"], "unknown");
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
    let hardware = crowdb_kv_client::HardwareClient::from_shared(kv);
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
        config
            .servers
            .push(ServerEntry::new(format!("kv-{index}"), endpoint.clone()));
    }
    let state = AppState::with_runtime_root(config, workspace.path().to_owned());
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

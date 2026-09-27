use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use crowdb_console_shared::config::web::{WebMode, WebProcessConfig};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, CrowdbSysmdClient};
use crowdb_monitor::{MonitorPhase, MonitorStatus, ServiceStatus, StatusStore};
use crowdb_protocol::common::{HwStatus, NodeValue, RackValue, ServiceExtra};
use crowdb_web::{router, AppState};
use tower::ServiceExt;
use uuid::Uuid;

async fn get_json(app: axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn managed_mode_does_not_expose_local_topology_or_mutations() {
    let app = router(AppState::default().with_managed_ui(PathBuf::from("/tmp/crowdb-ui")));
    for (method, path) in [
        (Method::GET, "/api/racks"),
        (Method::GET, "/api/stores"),
        (Method::POST, "/api/racks"),
        (Method::DELETE, "/api/nodes/1"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/authority")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
        .await
        .unwrap();
    let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status["source"], "group0");
    assert_eq!(status["available"], false);
    assert_eq!(status["reason"], "monitor_unavailable");
}

#[tokio::test]
async fn managed_snapshot_uses_group0_and_monitor_without_local_fallback() {
    if crowdb_test_harness::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping real Group 0 test: crowdb-kv-server is unavailable");
        return;
    }
    let cluster = crowdb_test_harness::cluster::KvCluster::start().await;
    let kv = CrowdbKvClient::new(ClientConfig::new(cluster.mgmt_endpoints.clone()));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    let sysmd = CrowdbSysmdClient::new(kv);
    sysmd
        .add_rack(
            1,
            &RackValue {
                status: HwStatus::Up as i32,
                node_ids: vec![1],
            },
        )
        .await
        .unwrap();
    sysmd
        .add_node(
            1,
            1,
            &NodeValue {
                status: HwStatus::Up as i32,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    sysmd.add_store(0, &[1]).await.unwrap();
    sysmd
        .register_service("diskio", 7, &cluster.mgmt_endpoints[0], &ServiceExtra::default())
        .await
        .unwrap();

    let run_root =
        crowdb_test_harness::test_dirs::test_data_dir().join(format!("managed-web-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&run_root).unwrap();
    let store = StatusStore::new(&run_root).unwrap();
    let mut status = MonitorStatus::new(Uuid::new_v4(), MonitorPhase::Initializing);
    status.services.insert(
        "diskio".into(),
        ServiceStatus {
            pid: Some(123),
            generation: 2,
            healthy: true,
            restart_attempts: 1,
        },
    );
    store.publish(&mut status).unwrap();
    let config = WebProcessConfig {
        version: 1,
        mode: WebMode::Docker,
        bind: "127.0.0.1".into(),
        port: 8080,
        group0_management_seeds: cluster.mgmt_endpoints.clone(),
        ui_root: run_root.clone(),
        monitor_status: Some(run_root.join("status/monitor.json")),
        log_dir: run_root.join("log"),
        log_max_file_mb: 30,
        log_max_files: 5,
        request_timeout_ms: Some(5_000),
    };
    let app = router(AppState::default().with_process_config(&config));
    let (code, authority) = get_json(app.clone(), "/api/authority").await;
    assert_eq!(code, StatusCode::OK, "{authority}");
    assert_eq!(authority["source"], "group0");
    assert_eq!(authority["available"], true);
    let (code, snapshot) = get_json(app.clone(), "/api/preview").await;
    assert_eq!(code, StatusCode::OK, "{snapshot}");
    assert_eq!(snapshot["racks"][0]["id"], 1);
    assert_eq!(snapshot["nodes"][0]["id"], 1);
    assert_eq!(snapshot["stores"][0]["store_id"], 0);
    let diskio = snapshot["services"]
        .as_array()
        .unwrap()
        .iter()
        .find(|service| service["kind"] == "diskio")
        .unwrap();
    assert_eq!(diskio["monitor"]["pid"], 123, "{snapshot}");
    assert_eq!(diskio["monitor"]["generation"], 2);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/stores")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    drop(cluster);
    let (code, unavailable) = get_json(app, "/api/preview").await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{unavailable}");
    assert_eq!(unavailable["reason"], "group0_unavailable");
    std::fs::remove_dir_all(run_root).unwrap();
}

#[test]
fn old_mixed_config_is_rejected_before_startup() {
    let path = std::env::temp_dir().join(format!(
        "crowdb-web-old-config-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, "[[rack]]\nid = 1\n").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_crowdb-web"))
        .args(["--config", path.to_str().unwrap()])
        .output()
        .unwrap();
    std::fs::remove_file(path).unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("version") || error.contains("unknown field"),
        "{error}"
    );
}

#[test]
fn managed_process_rejects_standalone_registry() {
    let directory = std::env::temp_dir().join(format!(
        "crowdb-web-registry-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let config = directory.join("crowdb-web.toml");
    let registry = directory.join("registry.toml");
    std::fs::write(
        &config,
        "version = 1\nmode = 'docker'\nbind = '127.0.0.1'\nport = 14000\ngroup0_management_seeds = ['http://127.0.0.1:10000']\nui_root = '/tmp'\nmonitor_status = '/tmp/monitor.json'\nlog_dir = '/tmp'\nlog_max_file_mb = 30\nlog_max_files = 5\n",
    )
    .unwrap();
    std::fs::write(&registry, "version = 1\n").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_crowdb-web"))
        .args([
            "--config",
            config.to_str().unwrap(),
            "--registry",
            registry.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("docker web does not accept --registry"));
}

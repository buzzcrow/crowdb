// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, start_node_auto_join, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

struct TestWorkspace(std::path::PathBuf);
impl TestWorkspace {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("crowdb-auto-join-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("console")).unwrap();
        Self(root)
    }
    fn state(&self) -> AppState {
        AppState::with_runtime_root(ConsoleConfig::default(), self.0.join("console"))
            .with_node_monitor("http://127.0.0.1:1")
            .unwrap()
    }
}
impl Drop for TestWorkspace {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
async fn request(app: Router, body: Option<Value>) -> (StatusCode, Value) {
    let mut request = Request::builder().uri("/api/node/auto-join");
    let body = body.map_or_else(Body::empty, |body| {
        request = std::mem::take(&mut request)
            .method("POST")
            .header("content-type", "application/json");
        Body::from(body.to_string())
    });
    let reply = app.oneshot(request.body(body).unwrap()).await.unwrap();
    let status = reply.status();
    let bytes = axum::body::to_bytes(reply.into_body(), 65536).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn default_enabled_policy_survives_disable_and_restart_without_touching_accepted_work() {
    let workspace = TestWorkspace::new();
    let accepted = workspace.0.join("initialization.json");
    std::fs::write(&accepted, b"monitor-owned intent").unwrap();
    let (status, policy) = request(router(workspace.state()), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(policy["enabled"], true);
    assert_eq!(policy["required_voters"], 3);
    let (status, _) = request(router(workspace.state()), Some(json!({"enabled": false}))).await;
    assert_eq!(status, StatusCode::OK);
    let state = workspace.state();
    let task = start_node_auto_join(state.clone());
    let path = state.runtime_root.join("auto-join-status.json");
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    let (_, policy) = request(router(workspace.state()), None).await;
    assert_eq!(policy["enabled"], false);
    assert_eq!(policy["observation"]["phase"], "disabled");
    assert_eq!(std::fs::read(accepted).unwrap(), b"monitor-owned intent");
    assert!(!workspace.0.join("console/prepared-bootstrap.json").exists());
}

#[tokio::test]
async fn readonly_rejects_policy_mutation_and_invalid_policy_never_reenables_defaults() {
    let workspace = TestWorkspace::new();
    let mut state = workspace.state();
    state.readonly = true;
    let (status, _) = request(router(state), Some(json!({"enabled": false}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(!workspace.0.join("console/auto-join.json").exists());
    std::fs::write(
        workspace.0.join("console/auto-join.json"),
        br#"{"version":2,"enabled":true,"target_cluster_id":null}"#,
    )
    .unwrap();
    let (status, body) = request(router(workspace.state()), None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "Unsupported Auto join policy version");
}

#[tokio::test]
async fn observation_rejects_symlinks_and_oversized_state() {
    let workspace = TestWorkspace::new();
    let path = workspace.0.join("console/auto-join-status.json");
    std::fs::write(&path, vec![b' '; 65537]).unwrap();
    assert_eq!(
        request(router(workspace.state()), None).await.0,
        StatusCode::CONFLICT
    );
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(workspace.0.join("missing"), &path).unwrap();
    assert_eq!(
        request(router(workspace.state()), None).await.0,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn pinned_target_without_binding_blocks_replacement_bootstrap() {
    let workspace = TestWorkspace::new();
    std::fs::write(
        workspace.0.join("console/auto-join.json"),
        br#"{"version":1,"enabled":true,"target_cluster_id":"accepted-cluster"}"#,
    )
    .unwrap();
    let state = workspace.state();
    let task = start_node_auto_join(state.clone());
    let path = state.runtime_root.join("auto-join-status.json");
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    let (_, policy) = request(router(state), None).await;
    assert_eq!(policy["observation"]["phase"], "failed");
    assert_eq!(policy["target_cluster_id"], "accepted-cluster");
    assert!(!workspace.0.join("console/prepared-bootstrap.json").exists());
}

#[tokio::test]
async fn two_authenticated_nodes_wait_in_uuid_order_without_initializing() {
    use axum::{routing::get, Json};
    use crowdb_console_shared::config::{NodeEntry, RackEntry};
    use crowdb_console_shared::deployment::registry::NodeRecord;
    use crowdb_protocol::mgmt::node::{
        CandidateNode, CandidateSnapshot, CandidateState, NodeAdvertisement, NodeHandshake, NodeHardware,
        NODE_PROTOCOL_VERSION,
    };
    let workspace = TestWorkspace::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mut config = ConsoleConfig::default();
    config.racks.push(RackEntry {
        id: 1,
        name: "default".into(),
    });
    let mut candidates = Vec::new();
    for (node_id, discovery_id) in [
        (9, "00000000-0000-4000-8000-000000000002"),
        (2, "00000000-0000-4000-8000-000000000001"),
    ] {
        config.nodes.push(NodeEntry {
            id: node_id,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 2222,
            ssh_user: "crowdb".into(),
            ssh_key: None,
            ssh_password: None,
            ssh_credential_ref: Some("id_ed25519".into()),
        });
        let record = NodeRecord {
            discovery_id: discovery_id.into(),
            node_id,
            physical_host_id: "host".into(),
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 2222,
            ssh_user: "crowdb".into(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            confirmed: false,
            cancelled: false,
        };
        std::fs::write(
            workspace.0.join(format!("console/admission-{discovery_id}.json")),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        candidates.push(CandidateNode {
            advertisement: NodeAdvertisement {
                discovery_id: discovery_id.into(),
                protocol_version: NODE_PROTOCOL_VERSION,
                monitor_endpoints: vec![endpoint.clone()],
                cluster_id: None,
            },
            state: CandidateState::Unbound,
        });
    }
    let local = NodeHandshake {
        advertisement: candidates[0].advertisement.clone(),
        physical_host_id: "host".into(),
        rack_hint: None,
        hardware: NodeHardware::default(),
    };
    let monitor = Router::new()
        .route("/node", get(move || async move { Json(local) }))
        .route(
            "/candidates",
            get(move || async move {
                Json(CandidateSnapshot {
                    nodes: candidates,
                    discovery_diagnostics: vec![],
                })
            }),
        );
    let server = tokio::spawn(async move {
        axum::serve(listener, monitor).await.unwrap();
    });
    let state = AppState::with_runtime_root(config, workspace.0.join("console"))
        .with_node_monitor(&endpoint)
        .unwrap();
    let task = start_node_auto_join(state.clone());
    let path = state.runtime_root.join("auto-join-status.json");
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    server.abort();
    let (_, policy) = request(router(state), None).await;
    assert_eq!(policy["observation"]["phase"], "waiting");
    assert_eq!(policy["observation"]["members"], json!([2, 9]));
    assert!(!workspace.0.join("console/prepared-bootstrap.json").exists());
}

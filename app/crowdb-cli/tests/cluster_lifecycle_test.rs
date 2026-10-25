// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{routing::get, Json, Router};
use crowdb_protocol::mgmt::{
    cluster::{ClusterObservation, DependencyKind, DependencyObservation, DependencyState},
    node::deployment::NodeDeploymentPolicy,
};
use serde_json::{json, Value};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

struct TestDocker {
    root: crowdb_test_harness::test_dirs::TestDir,
    program: PathBuf,
}

impl TestDocker {
    fn new() -> Self {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("cli-lifecycle");
        let program = root.path().join("docker-fixture");
        fs::write(&program, include_str!("common/local_docker.py")).unwrap();
        fs::set_permissions(&program, fs::Permissions::from_mode(0o700)).unwrap();
        let test = Self { root, program };
        test.save(&json!({"runtime":{"daemon":"test-daemon", "context":"default", "endpoint":"unix:///test-docker.sock"},
            "calls":[], "container":{}, "volume":{}, "network":{}}));
        test
    }
    fn save(&self, state: &Value) {
        fs::write(
            self.root.path().join("docker.json"),
            serde_json::to_vec(state).unwrap(),
        )
        .unwrap();
    }
    fn state(&self) -> Value {
        serde_json::from_slice(&fs::read(self.root.path().join("docker.json")).unwrap()).unwrap()
    }
    fn run(&self, cluster: Option<&str>, args: &[&str], code: i32) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"));
        command
            .env("CROWDB_RUNTIME_ROOT", self.root.path())
            .env("CROWDB_CONTAINER_RUNTIME", &self.program)
            .env_remove("DOCKER_HOST")
            .env_remove("CROWDB_CLUSTER")
            .env_remove("CROWDB_SYSTEM_IP")
            .env_remove("CROWDB_SYSTEM_PORT");
        if let Some(cluster) = cluster {
            command.args(["--cluster", cluster]);
        }
        let output = command.arg("cluster").args(args).output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(code),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }
    fn receipt(&self, name: &str) -> PathBuf {
        self.root
            .path()
            .join(format!("persistent/oci-clusters/{name}.json"))
    }
}

fn report(cluster: &str, nodes: u8, ready: bool) -> Value {
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap();
    let policy = NodeDeploymentPolicy {
        cluster: cluster.into(),
        initial_voters: nodes,
        ..NodeDeploymentPolicy::default()
    };
    let observation = ClusterObservation {
        version: 1,
        cluster: cluster.into(),
        cluster_id: Some(uuid::Uuid::from_u128(1).to_string()),
        bound: true,
        initial_voters: nodes,
        policy: Some(policy),
        observed_at_unix_ms: now,
        members: vec![],
        operation_id: None,
        work_accepted: true,
        setup_completed: true,
        runtime: None,
        dependencies: [
            DependencyKind::Authority,
            DependencyKind::Metadata,
            DependencyKind::Storage,
            DependencyKind::Member,
            DependencyKind::Service,
            DependencyKind::Protocol,
        ]
        .into_iter()
        .map(|kind| DependencyObservation {
            id: format!("{kind:?}"),
            kind,
            required: true,
            state: if ready {
                DependencyState::Ready
            } else {
                DependencyState::Unknown
            },
            observed_at_unix_ms: now,
            max_age_ms: 60_000,
            revision: None,
            operation_id: None,
            current_operation_id: None,
            reason: None,
            detail: "current fixture evidence".into(),
        })
        .collect(),
    };
    serde_json::to_value(crowdb_console_shared::ops::cluster::readiness::reduce(
        observation,
        now,
    ))
    .unwrap()
}

async fn serve(cluster: &str, nodes: u8, ready: bool) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let value = report(cluster, nodes, ready);
    let app = Router::new().route(
        "/api/cluster/status",
        get(move || {
            let value = value.clone();
            async move { Json(value) }
        }),
    );
    (
        port,
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_single_node_start_seals_identity_and_resumes_without_duplicate_resources() {
    let test = TestDocker::new();
    let (port, server) = serve("local", 1, true).await;
    test.run(
        None,
        &["start", "--port-base", &port.to_string(), "--wait", "0", "--json"],
        0,
    );
    let original = fs::read(test.receipt("local")).unwrap();
    let binding = test.root.path().join("persistent/clusters/local/target.json");
    let target: Value = serde_json::from_slice(&fs::read(binding).unwrap()).unwrap();
    assert_eq!(target["cluster_id"], uuid::Uuid::from_u128(1).to_string());
    assert_eq!(test.state()["container"].as_object().unwrap().len(), 1);
    test.run(None, &["stop"], 0);
    assert_eq!(
        test.state()["container"]["crowdb-local-1"]["State"]["Running"],
        false
    );
    test.run(None, &["start", "--wait", "0"], 0);
    assert_eq!(
        test.state()["container"]["crowdb-local-1"]["State"]["Running"],
        true
    );
    assert_eq!(fs::read(test.receipt("local")).unwrap(), original);
    assert_eq!(
        test.state()["calls"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|args| args[0] == "run")
            .count(),
        1
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_node_partial_readiness_keeps_resumable_inputs_and_stop_is_scoped() {
    let test = TestDocker::new();
    let (port, server) = serve("three", 3, false).await;
    let mut partial = test.state();
    partial["fail_run_once"] = json!("crowdb-three-2");
    test.save(&partial);
    test.run(
        Some("three"),
        &[
            "start",
            "--nodes",
            "3",
            "--port-base",
            &port.to_string(),
            "--wait",
            "0",
        ],
        2,
    );
    assert_eq!(test.state()["container"].as_object().unwrap().len(), 1);
    let receipt = fs::read(test.receipt("three")).unwrap();
    let calls = test.state()["calls"].clone();
    test.run(Some("three"), &["status", "--json"], 0);
    assert_eq!(test.state()["calls"], calls);
    test.run(
        Some("three"),
        &[
            "start",
            "--nodes",
            "3",
            "--port-base",
            &port.to_string(),
            "--wait",
            "0",
            "--json",
        ],
        1,
    );
    let mut state = test.state();
    assert_eq!(state["container"].as_object().unwrap().len(), 3);
    let mut foreign = state["container"]["crowdb-three-1"].clone();
    foreign["Name"] = json!("/crowdb-other-1");
    foreign["Id"] = json!("crowdb-other-1");
    state["container"]["crowdb-other-1"] = foreign;
    test.save(&state);
    test.run(Some("three"), &["stop"], 0);
    let state = test.state();
    for index in 1..=3 {
        assert_eq!(
            state["container"][format!("crowdb-three-{index}")]["State"]["Running"],
            false
        );
    }
    assert_eq!(state["container"]["crowdb-other-1"]["State"]["Running"], true);
    test.run(Some("three"), &["start", "--wait", "0"], 1);
    assert_eq!(fs::read(test.receipt("three")).unwrap(), receipt);
    assert_eq!(
        test.state()["calls"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|args| args[0] == "run")
            .count(),
        4
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_daemon_or_inputs_fail_without_effects_and_preserve_receipt() {
    let test = TestDocker::new();
    let (port, server) = serve("local", 1, true).await;
    test.run(
        None,
        &["start", "--port-base", &port.to_string(), "--wait", "0"],
        0,
    );
    let original = fs::read(test.receipt("local")).unwrap();
    let before = test.state()["container"].clone();
    test.run(None, &["start", "--nodes", "3", "--wait", "0"], 2);
    let mut state = test.state();
    state["calls"] = json!([]);
    state["runtime"]["daemon"] = json!("foreign-daemon");
    test.save(&state);
    test.run(None, &["stop"], 2);
    test.run(None, &["start", "--wait", "0"], 2);
    assert_eq!(test.state()["container"], before);
    assert_eq!(fs::read(test.receipt("local")).unwrap(), original);
    assert!(test.state()["calls"]
        .as_array()
        .unwrap()
        .iter()
        .all(|args| args[0] != "stop" && args[0] != "run" && args[0] != "start"));
    server.abort();
}

#[test]
fn invalid_topology_and_symlinked_receipts_fail_before_docker() {
    let test = TestDocker::new();
    test.run(None, &["start", "--nodes", "2"], 2);
    let directory = test.root.path().join("persistent/oci-clusters");
    fs::create_dir_all(&directory).unwrap();
    std::os::unix::fs::symlink(Path::new("/nonexistent"), test.receipt("local")).unwrap();
    test.run(None, &["start", "--wait", "0"], 2);
    assert_eq!(test.state()["calls"], json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_network_and_changed_container_configuration_are_never_adopted() {
    let test = TestDocker::new();
    let (port, server) = serve("local", 1, true).await;
    test.run(
        None,
        &["start", "--port-base", &port.to_string(), "--wait", "0"],
        0,
    );
    test.run(None, &["stop"], 0);
    let original = test.state();
    for foreign_network in [true, false] {
        let mut state = original.clone();
        state["calls"] = json!([]);
        if foreign_network {
            state["network"]["crowdb-local"]["Labels"]["org.crowdb.cluster.owner"] = json!("foreign");
        } else {
            state["container"]["crowdb-local-1"]["HostConfig"]["Memory"] = json!(512);
        }
        test.save(&state);
        test.run(None, &["start", "--wait", "0"], 2);
        assert_eq!(
            test.state()["container"]["crowdb-local-1"]["State"]["Running"],
            false
        );
        assert!(test.state()["calls"]
            .as_array()
            .unwrap()
            .iter()
            .all(|args| args[0] != "start"
                && args[0] != "run"
                && args.get(1).and_then(Value::as_str) != Some("create")));
    }
    server.abort();
}

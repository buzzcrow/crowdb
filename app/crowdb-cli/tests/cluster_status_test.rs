// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use crowdb_protocol::mgmt::{
    cluster::{ClusterObservation, ClusterStatus, DependencyKind, DependencyObservation, DependencyState},
    node::deployment::NodeDeploymentPolicy,
};
use std::{
    process::{Command, Output},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

struct TestStatusServer {
    origin: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for TestStatusServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl TestStatusServer {
    async fn start(cluster_id: String, ready_after: usize, invalid: bool) -> Self {
        Self::with_failures(cluster_id, ready_after, invalid, None, false).await
    }
    async fn with_failures(
        cluster_id: String,
        ready_after: usize,
        invalid: bool,
        unavailable_after: Option<usize>,
        stalled: bool,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(AtomicUsize::new(0));
        let router = Router::new().route(
            "/api/cluster/status",
            get(move || {
                let seen = Arc::clone(&seen);
                let cluster_id = cluster_id.clone();
                async move {
                    let now =
                        u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis())
                            .unwrap();
                    let count = seen.fetch_add(1, Ordering::Relaxed);
                    if unavailable_after.is_some_and(|limit| count >= limit) {
                        return StatusCode::SERVICE_UNAVAILABLE.into_response();
                    }
                    if stalled {
                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    }
                    let ready = count >= ready_after;
                    let observation = ClusterObservation {
                        version: if invalid { 2 } else { 1 },
                        cluster: NodeDeploymentPolicy::default().cluster,
                        cluster_id: Some(cluster_id),
                        bound: true,
                        initial_voters: 1,
                        policy: Some(NodeDeploymentPolicy::default()),
                        observed_at_unix_ms: now,
                        members: Vec::new(),
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
                            state: if ready || kind == DependencyKind::Authority {
                                DependencyState::Ready
                            } else {
                                DependencyState::Unknown
                            },
                            observed_at_unix_ms: now,
                            max_age_ms: 3000,
                            revision: None,
                            operation_id: None,
                            current_operation_id: None,
                            reason: None,
                        })
                        .collect(),
                    };
                    Json(crowdb_console_shared::ops::cluster::readiness::reduce(
                        observation,
                        now,
                    ))
                    .into_response()
                }
            }),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { origin, task }
    }
}
fn run(origins: &[&str], args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"));
    command
        .env_remove("CROWDB_CLUSTER")
        .env_remove("CROWDB_SYSTEM_IP")
        .env_remove("CROWDB_SYSTEM_PORT")
        .env("DOCKER_HOST", "unix:///nonexistent-crowdb-test.sock");
    for origin in origins {
        command.args(["--management-endpoint", origin]);
    }
    command.args(["cluster"]).args(args).output().unwrap()
}
fn report(output: &Output, code: i32) -> ClusterStatus {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_returns_valid_unhealthy_json_and_check_returns_one_without_docker() {
    let server = TestStatusServer::start(uuid::Uuid::new_v4().to_string(), usize::MAX, false).await;
    let status = report(&run(&[&server.origin], &["status", "--json"]), 0);
    assert_ne!(status.phase, crowdb_protocol::mgmt::cluster::ClusterPhase::Ready);
    report(&run(&[&server.origin], &["check", "--json", "--wait", "1"]), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_waits_for_current_application_evidence_then_returns_zero() {
    let server = TestStatusServer::start(uuid::Uuid::new_v4().to_string(), 2, false).await;
    let status = report(&run(&[&server.origin], &["check", "--json", "--wait", "2"]), 0);
    assert_eq!(status.phase, crowdb_protocol::mgmt::cluster::ClusterPhase::Ready);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_reports_and_mixed_cluster_origins_return_two_and_no_json() {
    let invalid = TestStatusServer::start(uuid::Uuid::new_v4().to_string(), 0, true).await;
    let output = run(&[&invalid.origin], &["status", "--json"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let left = TestStatusServer::start(uuid::Uuid::new_v4().to_string(), 0, false).await;
    let right = TestStatusServer::start(uuid::Uuid::new_v4().to_string(), 0, false).await;
    let output = run(&[&left.origin, &right.origin], &["check", "--json"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("disagree on identity"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_endpoint_after_a_valid_observation_remains_an_unmet_readiness_report() {
    let server = TestStatusServer::with_failures(
        uuid::Uuid::new_v4().to_string(),
        usize::MAX,
        false,
        Some(1),
        false,
    )
    .await;
    let output = run(&[&server.origin], &["check", "--json", "--wait", "1"]);
    let report = report(&output, 1);
    assert_eq!(
        report.phase,
        crowdb_protocol::mgmt::cluster::ClusterPhase::Unavailable
    );
    assert!(report
        .reasons
        .iter()
        .any(|reason| reason.code == "management-origin-unavailable"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_selected_origins_share_the_caller_deadline_and_never_multiply_it() {
    let left = TestStatusServer::with_failures(uuid::Uuid::new_v4().to_string(), 0, false, None, true).await;
    let right = TestStatusServer::with_failures(uuid::Uuid::new_v4().to_string(), 0, false, None, true).await;
    let started = std::time::Instant::now();
    let output = run(
        &[&left.origin, &right.origin],
        &["check", "--json", "--wait", "1", "--timeout", "20"],
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(started.elapsed() < std::time::Duration::from_secs(3));
}

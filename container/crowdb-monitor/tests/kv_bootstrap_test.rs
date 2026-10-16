// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::path::{Path, PathBuf};

use crowdb_monitor::{kv_step_names, BootstrapSession, DeploymentProfile, KvBootstrap, MonitorLog};
use crowdb_protocol::mgmt::GroupSummary;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use uuid::Uuid;

struct TestDataRoot(PathBuf);

impl TestDataRoot {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-kv-bootstrap-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root.canonicalize().unwrap())
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDataRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn profile() -> DeploymentProfile {
    DeploymentProfile::load(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/profile.toml"),
    )
    .unwrap()
}

fn session(root: &TestDataRoot, profile: &DeploymentProfile) -> BootstrapSession {
    let names = kv_step_names(profile).unwrap();
    let steps = names.iter().map(String::as_str).collect::<Vec<_>>();
    BootstrapSession::open(root.path(), b"profile", b"config", &steps).unwrap()
}

async fn monitor_log(root: &TestDataRoot, profile: &DeploymentProfile) -> MonitorLog {
    let log_root = root.path().join("log");
    fs::create_dir_all(&log_root).unwrap();
    MonitorLog::open(&log_root, profile.logs.clone()).await.unwrap()
}

#[derive(Default)]
struct MockState {
    groups: Vec<GroupSummary>,
    system_posts: u32,
    data_posts: u32,
    lose_system_response: bool,
    accepted: Option<crowdb_protocol::mgmt::SystemPrepareRequest>,
}

struct MockKvServer {
    base_url: String,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<MockState>,
}

impl MockKvServer {
    async fn start(state: MockState) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut state = state;
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        handle(stream, &mut state).await;
                    }
                    _ = &mut stop_rx => break,
                }
            }
            state
        });
        Self { base_url, stop, task }
    }

    async fn finish(self) -> MockState {
        self.stop.send(()).unwrap();
        self.task.await.unwrap()
    }
}

async fn handle(mut stream: TcpStream, state: &mut MockState) {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let count = stream.read(&mut buffer).await.unwrap();
        if count == 0 {
            return;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&bytes[..end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if bytes.len() >= end + 4 + content_length {
                let request = headers.lines().next().unwrap();
                let mut words = request.split_whitespace();
                let method = words.next().unwrap();
                let path = words.next().unwrap();
                let body = &bytes[end + 4..end + 4 + content_length];
                respond(&mut stream, state, method, path, body).await;
                return;
            }
        }
    }
}

async fn respond(
    stream: &mut TcpStream,
    state: &mut MockState,
    method: &str,
    path: &str,
    request_body: &[u8],
) {
    let (status, body) = match (method, path) {
        ("GET", "/health") => (200, serde_json::json!({"status":"ok"}).to_string()),
        ("POST", "/system/prepare") => {
            let requested: crowdb_protocol::mgmt::SystemPrepareRequest =
                serde_json::from_slice(request_body).unwrap();
            if state.accepted.as_ref().is_some_and(|accepted| {
                accepted.replica_id != requested.replica_id || accepted.bootstrap != requested.bootstrap
            }) {
                (
                    409,
                    serde_json::json!({"error":"conflicting bootstrap"}).to_string(),
                )
            } else {
                state.accepted = Some(requested);
                (200, String::from_utf8(request_body.to_vec()).unwrap())
            }
        }
        ("GET", "/stores") => {
            let stores = if state.groups.is_empty() {
                Vec::new()
            } else {
                vec![
                    serde_json::json!({"store_id":0,"listen_addr":"127.0.0.1:10100","group_count":state.groups.len()}),
                ]
            };
            (200, serde_json::json!({"stores":stores}).to_string())
        }
        ("GET", "/stores/0/groups") if state.groups.is_empty() => (404, "{}".into()),
        ("GET", "/stores/0/groups") => (200, serde_json::to_string(&state.groups).unwrap()),
        ("GET", "/stores/0/groups/0/ready" | "/stores/0/groups/1/ready") => {
            let group_id = path.split('/').nth(4).unwrap().parse::<u64>().unwrap();
            let group = state
                .groups
                .iter()
                .find(|group| group.group_id == group_id)
                .unwrap();
            (200, serde_json::json!({"ready":true,"leader_id":group.local_replica_id,"voting_replicas":1,"reachable_replicas":1}).to_string())
        }
        ("POST", "/system/init") => {
            let requested: crowdb_protocol::mgmt::SystemInitRequest =
                serde_json::from_slice(request_body).unwrap();
            assert_eq!(
                requested.bootstrap.as_ref(),
                state.accepted.as_ref().map(|accepted| &accepted.bootstrap)
            );
            assert!(
                state.accepted.is_some(),
                "shared initialization must prepare before creating the system group"
            );
            state.system_posts += 1;
            state.groups.push(GroupSummary {
                group_id: 0,
                local_replica_id: 1,
                leader_id: 1,
                remote_count: 0,
            });
            if state.lose_system_response {
                state.lose_system_response = false;
                return;
            }
            (
                201,
                serde_json::json!({"store_id":0,"group_id":0,"replica_id":1}).to_string(),
            )
        }
        ("POST", "/stores/0/groups") => {
            state.data_posts += 1;
            state.groups.push(GroupSummary {
                group_id: 1,
                local_replica_id: 2,
                leader_id: 2,
                remote_count: 0,
            });
            (201, "{}".into())
        }
        _ => (404, "{}".into()),
    };
    let response = format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    stream.write_all(response.as_bytes()).await.unwrap();
}

#[tokio::test]
async fn creates_once_then_ready_restart_only_validates() {
    let root = TestDataRoot::new();
    let profile = profile();
    let server = MockKvServer::start(MockState::default()).await;
    let bootstrap = KvBootstrap::new(&server.base_url).unwrap();
    let mut initial = session(&root, &profile);
    let mut events = monitor_log(&root, &profile).await;
    bootstrap
        .reconcile(&mut initial, &profile, &mut events)
        .await
        .unwrap();
    assert_eq!(initial.manifest().next_step(), None);
    initial.mark_ready().unwrap();
    drop(initial);
    let mut ready = session(&root, &profile);
    bootstrap
        .reconcile(&mut ready, &profile, &mut events)
        .await
        .unwrap();
    let state = server.finish().await;
    assert_eq!((state.system_posts, state.data_posts), (1, 1));
    assert_eq!(
        state.accepted.unwrap().bootstrap,
        ready.manifest().system_bootstrap_identity()
    );
    let body = fs::read_to_string(root.path().join("log/monitor/monitor.log")).unwrap();
    let entries = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0]["kind"], "bootstrap_step_started");
    assert_eq!(entries[0]["service"], "kv-group-0-0");
    assert_eq!(entries[1]["kind"], "bootstrap_step_completed");
    assert_eq!(entries[2]["service"], "kv-group-0-1");
    assert_eq!(entries[3]["kind"], "bootstrap_step_completed");
}

#[tokio::test]
async fn lost_create_response_is_proven_without_replaying_post() {
    let root = TestDataRoot::new();
    let profile = profile();
    let server = MockKvServer::start(MockState {
        lose_system_response: true,
        ..MockState::default()
    })
    .await;
    let bootstrap = KvBootstrap::new(&server.base_url).unwrap();
    let mut session = session(&root, &profile);
    let mut events = monitor_log(&root, &profile).await;
    bootstrap
        .reconcile(&mut session, &profile, &mut events)
        .await
        .unwrap();
    let state = server.finish().await;
    assert_eq!((state.system_posts, state.data_posts), (1, 1));
}

#[tokio::test]
async fn conflicting_existing_group_fails_without_mutation() {
    let root = TestDataRoot::new();
    let profile = profile();
    let server = MockKvServer::start(MockState {
        groups: vec![GroupSummary {
            group_id: 0,
            local_replica_id: 99,
            leader_id: 99,
            remote_count: 0,
        }],
        ..MockState::default()
    })
    .await;
    let bootstrap = KvBootstrap::new(&server.base_url).unwrap();
    let mut session = session(&root, &profile);
    let mut events = monitor_log(&root, &profile).await;
    assert!(bootstrap
        .reconcile(&mut session, &profile, &mut events)
        .await
        .is_err());
    assert_eq!(session.manifest().next_step(), Some("kv-group-0-0"));
    let state = server.finish().await;
    assert_eq!((state.system_posts, state.data_posts), (0, 0));
    let body = fs::read_to_string(root.path().join("log/monitor/monitor.log")).unwrap();
    let entries = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(entries.last().unwrap()["kind"], "bootstrap_failed");
    assert_eq!(entries.last().unwrap()["service"], "kv-group-0-0");
}

#[tokio::test]
async fn matching_replica_with_foreign_bootstrap_identity_is_not_adopted() {
    let root = TestDataRoot::new();
    let profile = profile();
    let server = MockKvServer::start(MockState {
        groups: vec![GroupSummary {
            group_id: 0,
            local_replica_id: 1,
            leader_id: 1,
            remote_count: 0,
        }],
        accepted: Some(crowdb_protocol::mgmt::SystemPrepareRequest {
            replica_id: 1,
            bootstrap: crowdb_protocol::mgmt::SystemBootstrapIdentity {
                cluster_id: Uuid::new_v4().to_string(),
                operation_id: Uuid::new_v4().to_string(),
                configuration_digest: "a".repeat(64),
            },
        }),
        ..MockState::default()
    })
    .await;
    let bootstrap = KvBootstrap::new(&server.base_url).unwrap();
    let mut session = session(&root, &profile);
    let mut events = monitor_log(&root, &profile).await;
    assert!(bootstrap
        .reconcile(&mut session, &profile, &mut events)
        .await
        .is_err());
    assert_eq!(session.manifest().step_complete("kv-group-0-0"), Some(false));
    let state = server.finish().await;
    assert_eq!((state.system_posts, state.data_posts), (0, 0));
}

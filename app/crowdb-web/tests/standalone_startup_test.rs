// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crowdb_console_shared::lifecycle::stop_pid_with_timeout;
use crowdb_protocol::port::alloc::alloc_test_port;
use crowdb_protocol::ServicePort;
use crowdb_test_harness::test_dirs::tempdir_in_test_data;
use reqwest::Client;
use serde_json::{json, Value};

struct TestWeb {
    child: Child,
    base: String,
}

impl Drop for TestWeb {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct TestServers(Vec<u32>);

impl Drop for TestServers {
    fn drop(&mut self) {
        for pid in &self.0 {
            let _ = stop_pid_with_timeout(*pid, Duration::from_secs(2));
        }
    }
}

async fn post(web: &TestWeb, http: &Client, path: &str, body: Value) -> Value {
    let response = http
        .post(format!("{}{path}", web.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "{status}: {body}");
    serde_json::from_str(&body).unwrap()
}

#[test]
fn failed_standalone_publication_preserves_complete_previous_config() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempdir_in_test_data("standalone-write-failure");
    let state = crowdb_web::AppState::open_standalone(root.path().to_path_buf()).unwrap();
    let path = root.path().join("config.json");
    let before = std::fs::read(&path).unwrap();
    state
        .config
        .write()
        .unwrap()
        .racks
        .push(crowdb_console_shared::config::RackEntry {
            id: 42,
            name: "new-rack".into(),
        });
    let permissions = std::fs::metadata(root.path()).unwrap().permissions();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
    let result = state.persist();
    std::fs::set_permissions(root.path(), permissions).unwrap();
    assert!(result.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    state.persist().unwrap();
    let reopened = crowdb_web::AppState::open_standalone(root.path().to_path_buf()).unwrap();
    assert_eq!(reopened.config.read().unwrap().racks[0].id, 42);
}

#[cfg(unix)]
#[tokio::test]
async fn test_mode_termination_reaps_children_and_preserves_persistent_runtime() {
    test_disposable_termination(false).await;
}

#[tokio::test]
async fn test_mode_forced_exit_recovers_owned_children_without_removing_persistent_data() {
    test_disposable_termination(true).await;
}

async fn test_disposable_termination(forced: bool) {
    let root = tempdir_in_test_data("console-test-shutdown");
    let sentinel = root.path().join("persistent/console/N-1/operator-data");
    std::fs::create_dir_all(sentinel.parent().unwrap()).unwrap();
    std::fs::write(&sentinel, "preserve").unwrap();
    let port = alloc_test_port(ServicePort::Web);
    let child = Command::new(env!("CARGO_BIN_EXE_crowdb-web"))
        .args(["--bind", "127.0.0.1", "--port", &port.to_string(), "--test-mode"])
        .env("CROWDB_RUNTIME_ROOT", root.path())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut web = TestWeb {
        child,
        base: format!("http://127.0.0.1:{port}"),
    };
    let http = Client::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while http.get(format!("{}/healthz", web.base)).send().await.is_err() {
            assert!(web.child.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    post(&web, &http, "/api/racks", json!({"id": 1})).await;
    post(
        &web,
        &http,
        "/api/nodes",
        json!({"id": 1, "rack_id": 1, "host": "127.0.0.1", "ssh_user": ""}),
    )
    .await;
    let deployed = post(
        &web,
        &http,
        "/api/nodes/1/server/deploy",
        json!({
            "rest_port": alloc_test_port(ServicePort::KvServerMgmt),
            "rpc_port": alloc_test_port(ServicePort::KvServerListen),
            "election_profile": "test"
        }),
    )
    .await;
    let pid = u32::try_from(deployed["pid"].as_u64().unwrap()).unwrap();
    let mut servers = TestServers(vec![pid]);
    assert!(root.path().join("ephemeral").read_dir().unwrap().next().is_some());
    assert!(Command::new("kill")
        .args([
            if forced { "-KILL" } else { "-TERM" },
            &web.child.id().to_string()
        ])
        .status()
        .unwrap()
        .success());
    let exited = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = web.child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    if forced {
        assert!(!exited.success());
        assert!(crowdb_console_shared::lifecycle::process_is_alive(pid));
        let cleanup =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tools/pixi-tasks/clean-env.sh");
        assert!(Command::new("bash")
            .arg(cleanup)
            .env("CROWDB_RUNTIME_ROOT", root.path())
            .status()
            .unwrap()
            .success());
    } else {
        assert!(exited.success());
    }
    assert!(!crowdb_console_shared::lifecycle::process_is_alive(pid));
    assert!(
        !root.path().join("ephemeral").exists()
            || root.path().join("ephemeral").read_dir().unwrap().next().is_none()
    );
    assert_eq!(std::fs::read_to_string(sentinel).unwrap(), "preserve");
    servers.0.clear();
}

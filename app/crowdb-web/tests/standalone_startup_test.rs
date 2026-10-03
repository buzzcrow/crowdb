// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::Path;
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

async fn start(root: &Path, http: &Client) -> TestWeb {
    let port = alloc_test_port(ServicePort::Web);
    let output = std::fs::File::create(root.join(format!("web-{port}.log"))).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_crowdb-web"))
        .args(["--bind", "127.0.0.1", "--port", &port.to_string()])
        .env("CROWDB_RUNTIME_ROOT", root)
        .env_remove("CROWDB_ICEBERG_MANAGE_TOKEN")
        .stdout(Stdio::from(output.try_clone().unwrap()))
        .stderr(Stdio::from(output))
        .spawn()
        .unwrap();
    let mut web = TestWeb {
        child,
        base: format!("http://127.0.0.1:{port}"),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if http
            .get(format!("{}/healthz", web.base))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return web;
        }
        let exited = web.child.try_wait().unwrap().is_some();
        assert!(
            !exited && tokio::time::Instant::now() < deadline,
            "Web failed to start: {}",
            std::fs::read_to_string(root.join(format!("web-{port}.log"))).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn get(web: &TestWeb, http: &Client, path: &str) -> Value {
    let response = http.get(format!("{}{path}", web.base)).send().await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "{status}: {body}");
    serde_json::from_str(&body).unwrap()
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

#[tokio::test]
async fn empty_ui_persists_rack_and_node_across_restart() {
    let root = tempdir_in_test_data("standalone-empty");
    let http = Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
    let web = start(root.path(), &http).await;
    assert_eq!(get(&web, &http, "/api/mode").await["mode"], "legacy");
    assert_eq!(get(&web, &http, "/api/racks").await, json!([]));
    assert_eq!(get(&web, &http, "/api/stores").await, json!([]));
    assert!(http.get(&web.base).send().await.unwrap().status().is_success());
    post(&web, &http, "/api/racks", json!({"id":1,"name":"saved-rack"})).await;
    post(
        &web,
        &http,
        "/api/nodes",
        json!({"id":1,"rack_id":1,"host":"127.0.0.1"}),
    )
    .await;
    drop(web);
    let web = start(root.path(), &http).await;
    assert_eq!(get(&web, &http, "/api/racks").await[0]["name"], "saved-rack");
    assert_eq!(get(&web, &http, "/api/nodes").await[0]["id"], 1);
    assert!(root
        .path()
        .join("persistent/console/default/config.json")
        .is_file());
}

#[tokio::test]
async fn standalone_recovers_kv_before_and_after_group0_init() {
    let root = tempdir_in_test_data("standalone-recovery");
    let http = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let mut servers = TestServers(Vec::new());
    let web = start(root.path(), &http).await;
    post(&web, &http, "/api/racks", json!({"id":1,"name":"rack"})).await;
    post(
        &web,
        &http,
        "/api/nodes",
        json!({"id":1,"rack_id":1,"host":"127.0.0.1"}),
    )
    .await;
    let management = alloc_test_port(ServicePort::KvServerMgmt);
    let rpc = alloc_test_port(ServicePort::KvServerListen);
    let deployed = post(
        &web,
        &http,
        "/api/nodes/1/server/deploy",
        json!({"rest_port":management,"rpc_port":rpc}),
    )
    .await;
    servers
        .0
        .push(u32::try_from(deployed["pid"].as_u64().unwrap()).unwrap());
    assert_eq!(get(&web, &http, "/api/stores").await, json!([]));
    drop(web);
    let web = start(root.path(), &http).await;
    assert_eq!(get(&web, &http, "/api/servers").await[0]["health"], "up");
    let directory = root.path().join("persistent/console/default");
    let config: crowdb_console_shared::ConsoleConfig =
        serde_json::from_slice(&std::fs::read(directory.join("config.json")).unwrap()).unwrap();
    crowdb_console_shared::bootstrap_intent::BootstrapIntent::capture(&config, &[1])
        .unwrap()
        .seal(&directory.join("bootstrap-intent.toml"))
        .unwrap();
    drop(web);
    let web = start(root.path(), &http).await;
    assert!(!directory.join("bootstrap-intent.toml").exists());
    assert_eq!(get(&web, &http, "/api/stores").await[0]["store_id"], 0);
    post(&web, &http, "/api/stores", json!({"store_id":7,"nodes":[1]})).await;
    post(
        &web,
        &http,
        "/api/stores/7/groups",
        json!({"group_id":1,"replica_id":1,"nodes":[1]}),
    )
    .await;
    post(
        &web,
        &http,
        "/api/stores/7/groups/1/kv/put",
        json!({"key":"restart-key","value":"saved-value"}),
    )
    .await;
    drop(web);
    stop_pid_with_timeout(servers.0[0], Duration::from_secs(5)).unwrap();
    let web = start(root.path(), &http).await;
    let saved: Value = serde_json::from_slice(
        &std::fs::read(root.path().join("persistent/console/default/config.json")).unwrap(),
    )
    .unwrap();
    servers
        .0
        .push(u32::try_from(saved["server"][0]["pid"].as_u64().unwrap()).unwrap());
    assert_eq!(get(&web, &http, "/api/racks").await[0]["name"], "rack");
    assert_eq!(
        get(&web, &http, "/api/stores/7/groups/1/kv/get?key=restart-key").await["value_utf8"],
        "saved-value"
    );
}

#[test]
fn malformed_standalone_config_is_preserved_and_rejected() {
    let root = tempdir_in_test_data("standalone-malformed");
    let directory = root.path().join("persistent/console/default");
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("config.json");
    std::fs::write(&path, "broken config").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-web"))
        .env("CROWDB_RUNTIME_ROOT", root.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("config.json"));
    assert_eq!(std::fs::read_to_string(path).unwrap(), "broken config");
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

#[tokio::test]
async fn standalone_recovers_three_member_group0_and_diskdb() {
    let root = tempdir_in_test_data("standalone-three-member");
    let http = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let mut servers = TestServers(Vec::new());
    let web = start(root.path(), &http).await;
    post(&web, &http, "/api/racks", json!({"id":1,"name":"authority-rack"})).await;
    for id in 1..=3 {
        post(
            &web,
            &http,
            "/api/nodes",
            json!({"id":id,"rack_id":1,"host":"127.0.0.1"}),
        )
        .await;
        let deployed = post(
            &web,
            &http,
            &format!("/api/nodes/{id}/server/deploy"),
            json!({
                "rest_port":alloc_test_port(ServicePort::KvServerMgmt),
                "rpc_port":alloc_test_port(ServicePort::KvServerListen),
                "election_profile":"e2e"
            }),
        )
        .await;
        servers
            .0
            .push(u32::try_from(deployed["pid"].as_u64().unwrap()).unwrap());
    }
    post(&web, &http, "/api/cluster/init", json!({"nodes":[1,2,3]})).await;
    let deployed = post(
        &web,
        &http,
        "/api/nodes/1/diskdb/deploy",
        json!({
            "rpc_port":alloc_test_port(ServicePort::DiskdbRpc),
            "listen_port":alloc_test_port(ServicePort::DiskdbListen),
            "http_port":alloc_test_port(ServicePort::DiskdbHttp)
        }),
    )
    .await;
    servers
        .0
        .push(u32::try_from(deployed["pid"].as_u64().unwrap()).unwrap());
    drop(web);
    for pid in &servers.0 {
        stop_pid_with_timeout(*pid, Duration::from_secs(5)).unwrap();
    }
    let config_path = root.path().join("persistent/console/default/config.json");
    let mut saved: Value = serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
    saved["rack"][0]["name"] = json!("stale-cache");
    std::fs::write(&config_path, serde_json::to_vec(&saved).unwrap()).unwrap();
    let web = start(root.path(), &http).await;
    let saved: Value = serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
    for entry in saved["server"].as_array().unwrap() {
        servers
            .0
            .push(u32::try_from(entry["pid"].as_u64().unwrap()).unwrap());
    }
    assert_eq!(get(&web, &http, "/api/racks").await[0]["name"], "authority-rack");
    assert_eq!(
        get(&web, &http, "/api/stores/0/groups/0?recursive=2").await["replicas"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        get(&web, &http, "/api/servers").await.as_array().unwrap().len(),
        4
    );
    let launch = &saved["local_launches"]["diskdb-1"];
    assert!(!launch.is_null(), "DiskDB launch inputs must survive Web restart");
}

#[cfg(unix)]
#[tokio::test]
async fn test_mode_termination_reaps_children_and_preserves_persistent_runtime() {
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
        .args(["-TERM", &web.child.id().to_string()])
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
    assert!(exited.success());
    assert!(!crowdb_console_shared::lifecycle::process_is_alive(pid));
    assert!(root.path().join("ephemeral").read_dir().unwrap().next().is_none());
    assert_eq!(std::fs::read_to_string(sentinel).unwrap(), "preserve");
    servers.0.clear();
}

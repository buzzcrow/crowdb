// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crowdb_console_shared::config::web::{LaunchRecord, LaunchRegistry};
use crowdb_console_shared::launch::LaunchRuntime;
use crowdb_console_shared::lifecycle;
use crowdb_test_harness::test_dirs::tempdir_in_test_data;
use reqwest::StatusCode;

struct TestProcesses {
    web: Child,
    services: Vec<u32>,
}
impl Drop for TestProcesses {
    fn drop(&mut self) {
        for pid in &self.services {
            if lifecycle::process_is_alive(*pid) {
                let _ = lifecycle::stop_pid_with_timeout(*pid, Duration::from_secs(2));
            }
        }
        let _ = self.web.kill();
        let _ = self.web.wait();
    }
}

async fn start_web(
    directory: &std::path::Path,
    registry_path: &std::path::Path,
) -> (TestProcesses, String, String, reqwest::Client) {
    let port = crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::Web);
    let config = directory.join("web.toml");
    std::fs::write(&config, format!(
        "version = 1\nmode = 'bare-metal'\nbind = '127.0.0.1'\nport = {port}\ngroup0_management_seeds = ['http://127.0.0.1:9']\nui_root = {}\nlog_dir = {}\nlog_max_file_mb = 30\nlog_max_files = 5\nrequest_timeout_ms = 200\n",
        serde_json::to_string(directory).unwrap(), serde_json::to_string(&directory.join("log")).unwrap())).unwrap();
    let token = "t".repeat(40);
    let output = std::fs::File::create(directory.join("web-output.log")).unwrap();
    let web = Command::new(env!("CARGO_BIN_EXE_crowdb-web"))
        .arg("--config")
        .arg(&config)
        .arg("--registry")
        .arg(registry_path)
        .env("CROWDB_ICEBERG_MANAGE_TOKEN", &token)
        .stdout(Stdio::from(output.try_clone().unwrap()))
        .stderr(Stdio::from(output))
        .spawn()
        .unwrap();
    let guard = TestProcesses {
        web,
        services: Vec::new(),
    };
    let base = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if http
            .get(format!("{base}/healthz"))
            .send()
            .await
            .is_ok_and(|reply| reply.status().is_success())
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Web startup failed: {}",
            std::fs::read_to_string(directory.join("web-output.log")).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (guard, base, token, http)
}

#[tokio::test]
async fn bare_metal_web_loads_launch_policy_without_restoring_topology() {
    let dir = tempdir_in_test_data("web-launch-registry");
    let binary = dir.path().join("service");
    std::fs::write(&binary, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let service_config = dir.path().join("service.toml");
    std::fs::write(&service_config, "").unwrap();
    let record = LaunchRecord {
        node_id: 701,
        service_id: "kv".into(),
        host: "localhost".into(),
        ssh_credential_ref: None,
        ssh_user: None,
        ssh_port: 22,
        binary_path: binary,
        service_config_path: service_config,
        workspace: dir.path().to_owned(),
        auto_start: true,
        args: Vec::new(),
        readiness_url: None,
    };
    let mut registry = LaunchRegistry {
        version: 1,
        launches: vec![record.clone()],
    };
    let registry_path = dir.path().join("launches.toml");
    registry.save(&registry_path).unwrap();
    let (mut guard, base, token, http) = start_web(dir.path(), &registry_path).await;
    let runtime = LaunchRuntime::for_registry(&registry_path).unwrap();
    let first = runtime
        .status(&record)
        .await
        .unwrap()
        .expect("auto-start process");
    guard.services.push(first.pid);
    assert_eq!(
        http.get(format!("{base}/api/launches"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let view: serde_json::Value = http
        .get(format!("{base}/api/launches"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(view[0]["process"]["pid"], first.pid);
    assert_eq!(
        http.get(format!("{base}/api/stores"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let restarted: serde_json::Value = http
        .post(format!("{base}/api/launches/701/kv/restart"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let second = u32::try_from(restarted["pid"].as_u64().unwrap()).unwrap();
    guard.services.push(second);
    assert_ne!(second, first.pid);
    assert_eq!(
        http.post(format!("{base}/api/launches/701/kv/stop"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert!(runtime.status(&record).await.unwrap().is_none());
    registry.launches[0].auto_start = false;
    registry.save(&registry_path).unwrap();
    let updated: serde_json::Value = http
        .get(format!("{base}/api/launches"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(updated[0]["auto_start"], false);
    assert!(!std::fs::read_to_string(registry_path).unwrap().contains("pid"));
}

#[test]
fn docker_web_refuses_launch_policy() {
    let state = crowdb_web::AppState::default().with_managed_ui("/tmp/ui".into());
    assert!(state
        .with_launch_registry("/tmp/unused-registry.toml".into())
        .is_err());
}

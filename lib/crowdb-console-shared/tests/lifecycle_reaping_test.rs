// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

use crowdb_console_shared::{
    config::NodeEntry,
    lifecycle::{self, DeployRequest},
};
use crowdb_test_harness::test_dirs::tempdir_in_test_data;
use std::{os::unix::fs::PermissionsExt, time::Duration};

struct TestChild(u32);
impl Drop for TestChild {
    fn drop(&mut self) {
        if lifecycle::process_is_alive(self.0) {
            let _ = lifecycle::stop_pid_with_timeout(self.0, Duration::from_secs(2));
        }
    }
}

#[cfg(target_os = "linux")]
async fn assert_reaped(pid: u32) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"));
        if stat.is_err() {
            return;
        }
        let state = stat
            .unwrap()
            .rsplit(')')
            .next()
            .and_then(|rest| rest.trim_start().chars().next());
        assert_ne!(state, Some('Z'), "deployed child {pid} became a zombie");
        assert!(
            tokio::time::Instant::now() < deadline,
            "deployed child {pid} was not reaped"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn deployed_child_is_reaped_after_stop() {
    let root = tempdir_in_test_data("lifecycle-reaping");
    std::fs::create_dir_all(root.path().join("log")).unwrap();
    let binary = root.path().join("child");
    std::fs::write(&binary, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_port = listener.local_addr().unwrap().port();
    let health = axum::Router::new().route(
        "/health",
        axum::routing::get(|| async { axum::Json(serde_json::json!({"status": "ok"})) }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, health).await.unwrap();
    });
    let node = NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
        ssh_credential_ref: None,
    };
    let request = DeployRequest {
        server_id: "s1".into(),
        rest_port,
        rpc_port: if rest_port == 7001 { 7002 } else { 7001 },
        binary: Some(std::fs::canonicalize(binary).unwrap()),
        ..Default::default()
    };
    let deployed = lifecycle::deploy_local_in_dir(&request, &node, root.path())
        .await
        .unwrap();
    let child = TestChild(deployed.pid);
    assert_eq!(deployed.server_id, "s1");
    assert!(lifecycle::process_is_alive(deployed.pid));
    assert!(lifecycle::stop_pid(deployed.pid).unwrap());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_reaped(deployed.pid).await;
    drop(child);
    server.abort();
}

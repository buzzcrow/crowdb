// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{cleanup_node_for_tests, DeploymentProfile, ProcessManager};
use crowdb_protocol::mgmt::SystemBootstrapIdentity;
use crowdb_test_harness::test_dirs::tempdir_in_test_data;
use std::{os::unix::fs::PermissionsExt, path::Path};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn retired_cleanup_restarts_its_local_worker_after_a_failed_deletion() {
    let directory = tempdir_in_test_data("monitor-retired-cleanup");
    let root = directory.path();
    let bootstrap = SystemBootstrapIdentity {
        cluster_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        configuration_digest: "a".repeat(64),
    };
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile.paths.log_root = root.join("logs");
    let program = root.join("test-worker");
    std::fs::write(&program, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    profile
        .services
        .iter_mut()
        .find(|service| service.id == "kv")
        .unwrap()
        .program = program;
    let accepted = serde_json::json!({"node_id":1,"bootstrap":bootstrap,"prepared":true});
    std::fs::write(
        root.join("accepted-node.json"),
        serde_json::to_vec(&accepted).unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(
        root.join("accepted-node.json"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    std::fs::create_dir_all(&profile.paths.log_root).unwrap();
    let mut manager = ProcessManager::new(profile.paths.log_root.clone(), profile.logs.clone())
        .await
        .unwrap();
    let retired = root.join(format!("retired-bootstrap-{}.json", bootstrap.operation_id));
    assert!(
        cleanup_node_for_tests(&profile, &mut manager, bootstrap.clone(), false)
            .await
            .is_err()
    );
    let mut wrong = bootstrap.clone();
    wrong.cluster_id = uuid::Uuid::new_v4().to_string();
    assert!(cleanup_node_for_tests(&profile, &mut manager, wrong, true)
        .await
        .is_err());
    assert!(!retired.exists());
    assert!(!manager.owns("kv"));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:10000").await.unwrap();
    let endpoint = tokio::spawn(serve_deletions(listener, bootstrap.clone(), retired.clone()));
    assert!(
        cleanup_node_for_tests(&profile, &mut manager, bootstrap.clone(), true)
            .await
            .is_err()
    );
    assert!(retired.exists());
    assert!(root.join("accepted-node.json").exists());
    assert!(!manager.owns("kv"));
    cleanup_node_for_tests(&profile, &mut manager, bootstrap.clone(), true)
        .await
        .unwrap();
    endpoint.await.unwrap();
    assert!(retired.exists());
    assert!(!root.join("accepted-node.json").exists());
    assert!(root.join("node-cleaned.json").exists());
    assert!(!manager.owns("kv"));
    cleanup_node_for_tests(&profile, &mut manager, bootstrap, true)
        .await
        .unwrap();
}

async fn serve_deletions(
    listener: tokio::net::TcpListener,
    expected: SystemBootstrapIdentity,
    witness: std::path::PathBuf,
) {
    let mut deletions = 0;
    while deletions < 2 {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0; 4096];
        let body = loop {
            let count = stream.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(index) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..index]);
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")?
                            .parse()
                            .ok()
                    })
                    .unwrap_or(0);
                if bytes.len() >= index + 4 + length {
                    break bytes[index + 4..].to_vec();
                }
            }
        };
        let deletion = bytes.starts_with(b"POST /system/cleanup ");
        let status = if deletion {
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["bootstrap"], serde_json::to_value(&expected).unwrap());
            assert_eq!(value["confirm_delete_system_store"], true);
            assert!(witness.exists());
            deletions += 1;
            if deletions == 1 {
                "503 Service Unavailable"
            } else {
                "200 OK"
            }
        } else {
            assert!(bytes.starts_with(b"GET /health "));
            "200 OK"
        };
        stream
            .write_all(
                format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes(),
            )
            .await
            .unwrap();
    }
}

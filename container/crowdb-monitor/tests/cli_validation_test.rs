// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[test]
fn rejected_profile_reports_the_operator_error_and_exits_unsuccessfully() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("monitor-cli-validation");
    let profile = root.path().join("invalid.toml");
    std::fs::write(&profile, "invalid = true\n").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
        .arg("validate")
        .arg(profile)
        .env("TOKIO_WORKER_THREADS", "service-only-setting")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("failed to decode deployment profile"), "{error}");
    assert!(error.contains("unknown field `invalid`"), "{error}");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn control_client_does_not_create_a_service_worker_pool() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut observations = Vec::new();
    for workers in ["1", "64"] {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("monitor-control-cli");
        let socket = root.path().join("control.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
            .args(["control", "--socket"])
            .arg(&socket)
            .args([
                "--json",
                &serde_json::to_string(&crowdb_protocol::mgmt::node::NodeControl::NodeSetup).unwrap(),
            ])
            .env("TOKIO_WORKER_THREADS", workers)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        let (mut stream, _) = tokio::time::timeout(std::time::Duration::from_secs(3), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        let request: crowdb_protocol::mgmt::node::NodeControl = serde_json::from_slice(&request).unwrap();
        assert!(matches!(
            request,
            crowdb_protocol::mgmt::node::NodeControl::NodeSetup
        ));
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
        let threads: usize = status
            .lines()
            .find_map(|line| line.strip_prefix("Threads:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        stream
            .write_all(br#"{"ok":true,"value":{"probe":true}}"#)
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
        let output = child.wait_with_output().await.unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
            serde_json::json!({"probe":true})
        );
        observations.push(threads);
    }
    assert_eq!(
        observations[0], observations[1],
        "socket control must be independent of service worker sizing"
    );
    assert!(
        observations.iter().all(|threads| *threads <= 2),
        "control process thread budget: {observations:?}"
    );
}

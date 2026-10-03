// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::process::Command;
use std::time::Duration;

use crowdb_console_shared::config::LocalLaunchSpec;
use crowdb_console_shared::lifecycle;
use crowdb_test_harness::test_dirs;

#[tokio::test]
async fn early_readiness_exit_reports_actual_service_and_cause() {
    let workdir = test_dirs::tempdir_in_test_data("readiness-exit");
    let spec = LocalLaunchSpec {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), "echo 'journal owner unavailable' >&2; exit 7".into()],
        workdir: workdir.path().to_string_lossy().into_owned(),
        env: BTreeMap::new(),
        env_file: None,
        readiness_url: Some("http://127.0.0.1:1/ready".into()),
    };
    let error = lifecycle::restart_local_service("chunk-kv-17", 0, &spec)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("chunk-kv-17 child exited before readiness"),
        "{error}"
    );
    assert!(error.contains("journal owner unavailable"), "{error}");
    assert!(error.contains('7'), "{error}");
    assert!(!error.contains("DiskDB"), "{error}");
}

#[tokio::test]
async fn retained_launch_restarts_with_a_new_pid() {
    let workdir = test_dirs::test_data_dir().join(format!("lifecycle-restart-{}", std::process::id()));
    std::fs::create_dir_all(&workdir).unwrap();
    let mut child = Command::new("/bin/sh")
        .args(["-c", "trap 'exit 0' TERM; while :; do sleep 1; done"])
        .spawn()
        .unwrap();
    let old_pid = child.id();
    let spec = LocalLaunchSpec {
        env_file: None,
        program: "/bin/sh".into(),
        args: vec![
            "-c".into(),
            "trap 'exit 0' TERM; while :; do sleep 1; done".into(),
        ],
        workdir: workdir.to_string_lossy().into_owned(),
        env: BTreeMap::new(),
        readiness_url: None,
    };

    let new_pid = lifecycle::restart_local_service("test-service", old_pid, &spec)
        .await
        .unwrap();
    assert_ne!(old_pid, new_pid);
    assert!(!lifecycle::process_is_alive(old_pid));
    assert!(lifecycle::process_is_alive(new_pid));

    lifecycle::stop_pid_with_timeout(new_pid, Duration::from_secs(5)).unwrap();
    let _ = child.wait();
}

#[tokio::test]
async fn private_environment_is_loaded_before_stopping_the_old_process() {
    use std::os::unix::fs::PermissionsExt;
    let workdir = test_dirs::test_data_dir().join(format!("private-environment-{}", std::process::id()));
    std::fs::create_dir_all(&workdir).unwrap();
    let env = workdir.join("server.env");
    std::fs::write(&env, "CROWDB_TEST_CREDENTIAL=private-value\n").unwrap();
    std::fs::set_permissions(&env, std::fs::Permissions::from_mode(0o644)).unwrap();
    let mut old = Command::new("/bin/sleep").arg("600").spawn().unwrap();
    let spec = LocalLaunchSpec {
        program: "/bin/sh".into(),
        args: vec![
            "-c".into(),
            "printf '%s' \"$CROWDB_TEST_CREDENTIAL\" > observed; exec sleep 600".into(),
        ],
        workdir: workdir.to_string_lossy().into_owned(),
        env: BTreeMap::new(),
        env_file: Some(env.to_string_lossy().into_owned()),
        readiness_url: None,
    };
    let result = lifecycle::restart_local_service("secret-test", old.id(), &spec).await;
    assert!(result.is_err());
    assert!(lifecycle::process_is_alive(old.id()));
    assert!(!serde_json::to_string(&spec).unwrap().contains("private-value"));
    std::fs::set_permissions(&env, std::fs::Permissions::from_mode(0o600)).unwrap();
    let pid = lifecycle::restart_local_service("secret-test", old.id(), &spec)
        .await
        .unwrap();
    old.wait().unwrap();
    assert_eq!(
        std::fs::read_to_string(workdir.join("observed")).unwrap(),
        "private-value"
    );
    lifecycle::stop_pid_with_timeout(pid, Duration::from_secs(3)).unwrap();
    std::fs::remove_file(env).unwrap();
}

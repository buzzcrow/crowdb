// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::process::Command;
use std::time::Duration;

use crowdb_console_shared::config::LocalLaunchSpec;
use crowdb_console_shared::lifecycle;
use crowdb_test_harness::test_dirs;

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

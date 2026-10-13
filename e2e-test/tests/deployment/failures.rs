// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[tokio::test]
#[ignore = "requires the owned complete release monitor"]
async fn restart_exhaustion_exits_the_container() {
    super::witness_with_write(true, true).await;
    let prepared = crate::common::control::monitor_failure("prepare-lifecycle").await;
    assert_eq!(prepared["phase"], "ready");
    super::witness_with_write(true, false).await;
    let result = crate::common::control::monitor_failure("exhaust-restarts").await;
    assert_eq!(result["state"]["Status"], "exited");
    assert_ne!(result["state"]["ExitCode"], 0);
    assert!(result["log"]
        .as_str()
        .unwrap()
        .contains("\"kind\":\"restart_exhausted\""));
    assert_eq!(result["secret_free"], true);
    for event in [
        "starting",
        "ready",
        "bootstrap_step_started",
        "bootstrap_step_completed",
        "child_started",
        "child_stopped",
        "child_exited",
        "probe_failed",
        "restarting",
        "draining",
        "stopped",
        "restart_exhausted",
    ] {
        assert!(
            result["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == event),
            "missing {event}"
        );
    }
}

#[tokio::test]
#[ignore = "requires the owned complete release monitor"]
async fn recovery_rejects_changed_durable_credentials() {
    super::witness_with_write(true, true).await;
    let result = crate::common::control::monitor_failure("reject-changed-credentials").await;
    assert_eq!(result["state"]["Status"], "exited");
    assert_ne!(result["state"]["ExitCode"], 0);
    assert_eq!(result["ready_before"], result["ready_after"]);
    assert!(result["log"]
        .as_str()
        .unwrap()
        .contains("server credentials are incomplete"));
}

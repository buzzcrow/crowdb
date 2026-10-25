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
        .contains("web exceeded monitor restart budget"));
    assert_eq!(result["secret_free"], true);
    assert_eq!(result["after"]["phase"], "failed");
    assert_eq!(result["after"]["services"]["web"]["restart_attempts"], 5);
}

#[tokio::test]
#[ignore = "requires the owned complete release monitor"]
async fn recovery_rejects_changed_durable_credentials() {
    super::witness_with_write(true, true).await;
    let result = crate::common::control::monitor_failure("reject-changed-credentials").await;
    assert_eq!(result["state"]["Status"], "exited");
    assert_ne!(result["state"]["ExitCode"], 0);
    assert_eq!(result["credential_rejection"], true);
    assert_eq!(result["secret_free"], true);
}

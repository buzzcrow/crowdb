// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{IcebergBootstrap, IcebergBootstrapError, ServerCredentials};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

async fn failure(root: &Path, script: &str, credentials: &ServerCredentials) -> IcebergBootstrapError {
    let program = root.join("management");
    fs::write(&program, script).unwrap();
    fs::set_permissions(&program, fs::Permissions::from_mode(0o700)).unwrap();
    IcebergBootstrap::ensure_console_catalog(root, &program, "127.0.0.1:1", credentials)
        .await
        .unwrap_err()
}

#[tokio::test]
async fn command_failure_reports_action_exit_status_and_redacts_every_server_credential() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("iceberg-command-failure");
    let credentials = ServerCredentials::load_or_create(root.path()).unwrap();
    let script = r#"#!/bin/sh
printf 'catalog owner unavailable\n%s\n%s\n%s\n%s\n%s\n' \
  "$CROWDB_S3_MASTER_KEY" "$CROWDB_ICEBERG_READ_TOKEN" "$CROWDB_ICEBERG_WRITE_TOKEN" \
  "$CROWDB_ICEBERG_MANAGE_TOKEN" "$CROWDB_ICEBERG_CLEAR_TOKEN" >&2
exit 17
"#;
    let error = failure(root.path(), script, &credentials).await;
    let message = error.to_string();
    let IcebergBootstrapError::CommandExit {
        action,
        status,
        diagnostic,
    } = error
    else {
        panic!("expected a command exit diagnostic: {message}");
    };
    assert_eq!(action, "inspect");
    assert_eq!(status, "exit status: 17");
    assert!(diagnostic.contains("catalog owner unavailable"));
    assert!(diagnostic.contains("<redacted>"));
    assert!(!diagnostic.chars().any(char::is_control));
    for line in credentials.server_env().lines() {
        let (_, value) = line.split_once('=').unwrap();
        assert!(!message.contains(value), "a server credential escaped redaction");
    }
}

#[tokio::test]
async fn oversized_multibyte_stderr_is_bounded_without_splitting_utf8() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("iceberg-command-bound");
    let credentials = ServerCredentials::load_or_create(root.path()).unwrap();
    let error = failure(
        root.path(),
        "#!/usr/bin/env python3\nimport sys\nsys.stderr.write('界' * 4096)\nsys.exit(3)\n",
        &credentials,
    )
    .await;
    let IcebergBootstrapError::CommandExit { diagnostic, .. } = error else {
        panic!("expected a command exit diagnostic");
    };
    assert_eq!(diagnostic, "界".repeat(1024));
}

#[tokio::test]
async fn a_signal_exit_remains_distinguishable_from_an_application_error() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("iceberg-command-signal");
    let credentials = ServerCredentials::load_or_create(root.path()).unwrap();
    let error = failure(root.path(), "#!/bin/sh\nkill -TERM $$\n", &credentials).await;
    let IcebergBootstrapError::CommandExit {
        status, diagnostic, ..
    } = error
    else {
        panic!("expected a command exit diagnostic");
    };
    assert!(status.contains("signal: 15"), "{status}");
    assert_eq!(diagnostic, "no stderr was produced");
}

#[tokio::test]
async fn a_credential_crossing_the_diagnostic_bound_is_redacted_before_truncation() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("iceberg-command-boundary");
    let credentials = ServerCredentials::load_or_create(root.path()).unwrap();
    let padding = "x".repeat(1020);
    let script =
        format!("#!/bin/sh\nprintf '%s%s' '{padding}' \"$CROWDB_ICEBERG_MANAGE_TOKEN\" >&2\nexit 1\n");
    let error = failure(root.path(), &script, &credentials).await;
    let IcebergBootstrapError::CommandExit { diagnostic, .. } = error else {
        panic!("expected a command exit diagnostic");
    };
    assert_eq!(diagnostic, format!("{padding}<red"));
}

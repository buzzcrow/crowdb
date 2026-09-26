// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crowdb_monitor::{
    s3_step_names, show_client_credentials, BootstrapSession, DeploymentProfile, MonitorLog, S3Bootstrap,
    ServerCredentials,
};
use uuid::Uuid;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-access-{}", Uuid::new_v4()));
        fs::create_dir_all(path.join("data")).unwrap();
        fs::create_dir_all(path.join("log")).unwrap();
        Self(path.canonicalize().unwrap())
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn profile(root: &TestRoot) -> DeploymentProfile {
    let mut profile = DeploymentProfile::load(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-preview/profile.toml"),
    )
    .unwrap();
    let program = root.0.join("credential-command");
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$1\" >> '{}'\nprintf 'rpc initialization log\\nAWS_ACCESS_KEY_ID=CROW123\\nAWS_SECRET_ACCESS_KEY=secret_123\\n'\n",
        root.0.join("calls").display()
    );
    fs::write(&program, script).unwrap();
    fs::set_permissions(&program, fs::Permissions::from_mode(0o700)).unwrap();
    profile
        .services
        .iter_mut()
        .find(|service| service.id == "s3")
        .unwrap()
        .program = program;
    profile
}

#[tokio::test]
async fn s3_bootstrap_reuses_user_and_validates_ready_without_creation() {
    let root = TestRoot::new();
    let profile = profile(&root);
    let data_root = root.0.join("data");
    let mut session = BootstrapSession::open(&data_root, b"profile", b"config", &s3_step_names()).unwrap();
    let credentials = ServerCredentials::load_or_create(&data_root).unwrap();
    let mut events = MonitorLog::open(&root.0.join("log"), profile.logs.clone())
        .await
        .unwrap();

    S3Bootstrap::reconcile(&mut session, &profile, &credentials, &mut events)
        .await
        .unwrap();
    assert_eq!(session.manifest().step_complete("s3-user"), Some(true));
    let client = show_client_credentials(&data_root).unwrap();
    assert!(client.contains("AWS_ENDPOINT_URL=http://localhost:16000\n"));
    assert!(client.contains("ICEBERG_URI=http://localhost\n"));
    session.mark_ready().unwrap();

    let mut restarted = BootstrapSession::open(&data_root, b"profile", b"config", &s3_step_names()).unwrap();
    S3Bootstrap::reconcile(&mut restarted, &profile, &credentials, &mut events)
        .await
        .unwrap();
    assert_eq!(show_client_credentials(&data_root).unwrap(), client);
    assert_eq!(
        fs::read_to_string(root.0.join("calls")).unwrap(),
        "ensure-user\nlookup-user\n"
    );
}

#[tokio::test]
async fn ready_s3_bootstrap_rejects_client_file_conflict() {
    let root = TestRoot::new();
    let profile = profile(&root);
    let data_root = root.0.join("data");
    let mut session = BootstrapSession::open(&data_root, b"profile", b"config", &s3_step_names()).unwrap();
    let credentials = ServerCredentials::load_or_create(&data_root).unwrap();
    let mut events = MonitorLog::open(&root.0.join("log"), profile.logs.clone())
        .await
        .unwrap();
    S3Bootstrap::reconcile(&mut session, &profile, &credentials, &mut events)
        .await
        .unwrap();
    session.mark_ready().unwrap();
    fs::write(data_root.join("secrets/client.env"), b"conflict\n").unwrap();

    assert!(
        S3Bootstrap::reconcile(&mut session, &profile, &credentials, &mut events)
            .await
            .is_err()
    );
    assert_eq!(
        fs::read_to_string(root.0.join("calls")).unwrap(),
        "ensure-user\nlookup-user\n"
    );
}

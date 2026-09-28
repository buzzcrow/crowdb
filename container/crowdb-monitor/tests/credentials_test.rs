// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

use crowdb_monitor::{show_client_credentials, ClientCredentials, ServerCredentials};
use uuid::Uuid;

struct TestDataRoot(PathBuf);

impl TestDataRoot {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-credentials-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root.canonicalize().unwrap())
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDataRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn client() -> ClientCredentials {
    ClientCredentials {
        s3_endpoint: "http://localhost:16000".into(),
        iceberg_endpoint: "http://localhost:8181".into(),
        region: "us-east-1".into(),
        access_key_id: "CROW123".into(),
        secret_access_key: "secret_123".into(),
    }
}

#[test]
fn server_secrets_are_distinct_private_and_stable() {
    let root = TestDataRoot::new();
    let first = ServerCredentials::load_or_create(root.path()).unwrap();
    let body = first.server_env();
    assert_eq!(body.lines().count(), 5);
    let tokens = body
        .lines()
        .skip(1)
        .map(|line| line.split_once('=').unwrap().1)
        .collect::<Vec<_>>();
    for (index, token) in tokens.iter().enumerate() {
        assert_eq!(token.len(), 64);
        assert!(!tokens[..index].contains(token));
    }
    let server_path = root.path().join("secrets/server.env");
    assert_eq!(
        fs::metadata(&server_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(root.path().join("secrets"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let second = ServerCredentials::load_or_create(root.path()).unwrap();
    assert_eq!(second.server_env(), body);
    assert!(show_client_credentials(root.path()).is_err());
}

#[test]
fn client_output_excludes_server_only_values_and_cannot_be_replaced() {
    let root = TestDataRoot::new();
    let server = ServerCredentials::load_or_create(root.path()).unwrap();
    server.persist_client(&client()).unwrap();
    server.persist_client(&client()).unwrap();
    let output = show_client_credentials(root.path()).unwrap();
    assert!(output.contains("AWS_ACCESS_KEY_ID=CROW123"));
    assert!(output.contains("ICEBERG_TOKEN="));
    assert!(!output.contains("CROWDB_S3_MASTER_KEY"));
    assert!(!output.contains("CROWDB_ICEBERG_MANAGE_TOKEN"));
    let mut conflicting = client();
    conflicting.access_key_id = "CROW456".into();
    assert!(server.persist_client(&conflicting).is_err());
    assert_eq!(show_client_credentials(root.path()).unwrap(), output);
    assert_eq!(
        fs::metadata(root.path().join("secrets/client.env"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[test]
fn malformed_or_exposed_secret_files_fail_closed() {
    let root = TestDataRoot::new();
    let server = ServerCredentials::load_or_create(root.path()).unwrap();
    server.persist_client(&client()).unwrap();
    let client_path = root.path().join("secrets/client.env");
    fs::set_permissions(&client_path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(show_client_credentials(root.path()).is_err());
    fs::set_permissions(&client_path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(
        &client_path,
        b"AWS_ENDPOINT_URL=http://localhost:16000\nCROWDB_S3_MASTER_KEY=leak\n",
    )
    .unwrap();
    assert!(show_client_credentials(root.path()).is_err());
    let server_path = root.path().join("secrets/server.env");
    fs::remove_file(&server_path).unwrap();
    symlink(&client_path, &server_path).unwrap();
    assert!(ServerCredentials::load_or_create(root.path()).is_err());
}

#[test]
fn invalid_client_values_are_not_persisted() {
    let root = TestDataRoot::new();
    let server = ServerCredentials::load_or_create(root.path()).unwrap();
    let mut value = client();
    value.secret_access_key = "secret\nINJECTED=yes".into();
    assert!(server.persist_client(&value).is_err());
    assert!(!root.path().join("secrets/client.env").exists());
}

#[test]
fn explicit_cli_prints_only_client_file() {
    let root = TestDataRoot::new();
    let server = ServerCredentials::load_or_create(root.path()).unwrap();
    server.persist_client(&client()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
        .args(["credentials", "show", "--format", "env", "--data-root"])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        output.stdout,
        show_client_credentials(root.path()).unwrap().as_bytes()
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("CROWDB_S3_MASTER_KEY"));
}

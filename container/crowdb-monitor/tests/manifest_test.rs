// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};

use crowdb_monitor::{BootstrapSession, ManifestState};
use uuid::Uuid;

const STEPS: &[&str] = &["kv", "storage", "catalog"];

struct TestDataRoot(PathBuf);

impl TestDataRoot {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-manifest-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root.canonicalize().unwrap())
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn manifest_path(&self) -> PathBuf {
        self.0.join("bootstrap/manifest.json")
    }
}

impl Drop for TestDataRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn open(root: &TestDataRoot) -> BootstrapSession {
    BootstrapSession::open(root.path(), b"profile", b"configuration", STEPS).unwrap()
}

#[test]
fn interrupted_steps_resume_with_stable_identity_and_order() {
    let root = TestDataRoot::new();
    let mut session = open(&root);
    let identity = session.manifest().deployment_id();
    let operation_id = session.manifest().operation_id("kv").unwrap();
    assert_eq!(session.manifest().state(), ManifestState::Initializing);
    assert_eq!(session.manifest().next_step(), Some("kv"));
    assert_eq!(
        fs::metadata(root.manifest_path()).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(session.complete_step("catalog").is_err());
    assert!(session.mark_ready().is_err());
    session.complete_step("kv").unwrap();
    drop(session);

    let mut resumed = open(&root);
    assert_eq!(resumed.manifest().deployment_id(), identity);
    assert_eq!(resumed.manifest().operation_id("kv").unwrap(), operation_id);
    assert_eq!(resumed.manifest().next_step(), Some("storage"));
    assert!(resumed.complete_step("kv").is_err());
    resumed.complete_step("storage").unwrap();
    resumed.complete_step("catalog").unwrap();
    resumed.mark_ready().unwrap();
    drop(resumed);

    let ready = open(&root);
    assert_eq!(ready.manifest().state(), ManifestState::Ready);
    assert_eq!(ready.manifest().deployment_id(), identity);
    assert_eq!(ready.manifest().next_step(), None);
    assert!(ready.manifest().operation_id("unknown").is_err());
}

#[test]
fn changed_inputs_and_step_plan_fail_without_mutation() {
    let root = TestDataRoot::new();
    open(&root);
    let original = fs::read(root.manifest_path()).unwrap();
    assert!(BootstrapSession::open(root.path(), b"different", b"configuration", STEPS).is_err());
    assert!(BootstrapSession::open(root.path(), b"profile", b"different", STEPS).is_err());
    assert!(BootstrapSession::open(root.path(), b"profile", b"configuration", &["kv", "catalog"]).is_err());
    assert_eq!(fs::read(root.manifest_path()).unwrap(), original);
}

#[test]
fn nonempty_and_corrupt_roots_fail_closed() {
    let nonempty = TestDataRoot::new();
    fs::write(nonempty.path().join("orphan"), b"data").unwrap();
    assert!(BootstrapSession::open(nonempty.path(), b"profile", b"configuration", STEPS).is_err());
    assert!(!nonempty.path().join("bootstrap").exists());

    let corrupt = TestDataRoot::new();
    open(&corrupt);
    fs::write(corrupt.manifest_path(), b"not json").unwrap();
    assert!(BootstrapSession::open(corrupt.path(), b"profile", b"configuration", STEPS).is_err());
    assert_eq!(fs::read(corrupt.manifest_path()).unwrap(), b"not json");
}

#[test]
fn symlinked_or_wrong_mode_manifest_is_rejected() {
    let root = TestDataRoot::new();
    open(&root);
    fs::set_permissions(root.manifest_path(), fs::Permissions::from_mode(0o644)).unwrap();
    assert!(BootstrapSession::open(root.path(), b"profile", b"configuration", STEPS).is_err());
    fs::set_permissions(root.manifest_path(), fs::Permissions::from_mode(0o600)).unwrap();
    let original = root.path().join("original.json");
    fs::rename(root.manifest_path(), &original).unwrap();
    symlink(&original, root.manifest_path()).unwrap();
    assert!(BootstrapSession::open(root.path(), b"profile", b"configuration", STEPS).is_err());
}

#[test]
fn invalid_plan_is_rejected_before_root_mutation() {
    let root = TestDataRoot::new();
    assert!(BootstrapSession::open(root.path(), b"profile", b"configuration", &[]).is_err());
    assert!(BootstrapSession::open(root.path(), b"profile", b"configuration", &["kv", "kv"]).is_err());
    assert!(BootstrapSession::open(root.path(), b"profile", b"configuration", &["bad/name"]).is_err());
    assert!(fs::read_dir(root.path()).unwrap().next().is_none());
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::os::unix::fs::{symlink, FileExt, MetadataExt};
use std::path::PathBuf;

use crowdb_monitor::{accept_file_for_tests, prepare_file_for_tests};
use crowdb_protocol::mgmt::node::storage::{FilePreparationPhase, PrepareDiskFile};

struct TestRoot(PathBuf);
impl TestRoot {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("file-preparation-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }
}
impl Drop for TestRoot {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn request() -> PrepareDiskFile {
    PrepareDiskFile {
        operation_id: uuid::Uuid::new_v4().to_string(),
        cluster_id: "test-cluster".into(),
        node_id: 1,
        capacity_bytes: 1024 * 1024,
        sparse: false,
        reserve_bytes: 1024 * 1024,
    }
}

#[test]
fn preallocated_file_has_durable_identity_and_retries_preserve_written_bytes() {
    let root = TestRoot::new();
    let request = request();
    let accepted = accept_file_for_tests(&root.0, &request).unwrap();
    assert_eq!(accepted.phase, FilePreparationPhase::Accepted);
    assert!(!PathBuf::from(&accepted.path).exists());
    let state = prepare_file_for_tests(&root.0, &request.operation_id).unwrap();
    assert_eq!(state.phase, FilePreparationPhase::Prepared, "{:?}", state.error);
    assert!(state.preparation_complete);
    let metadata = fs::metadata(&state.path).unwrap();
    assert_eq!(metadata.len(), request.capacity_bytes);
    assert!(metadata.blocks() * 512 >= request.capacity_bytes);
    fs::OpenOptions::new()
        .write(true)
        .open(&state.path)
        .unwrap()
        .write_all_at(b"acknowledged data", 0)
        .unwrap();
    assert_eq!(accept_file_for_tests(&root.0, &request).unwrap(), state);
    assert_eq!(
        prepare_file_for_tests(&root.0, &request.operation_id).unwrap(),
        state
    );
    assert!(fs::read(&state.path).unwrap().starts_with(b"acknowledged data"));
    let mut different = request.clone();
    different.sparse = true;
    assert!(accept_file_for_tests(&root.0, &different).is_err());
}

#[test]
fn missing_or_truncated_completed_file_is_never_recreated_or_regrown() {
    let root = TestRoot::new();
    let mut request = request();
    request.sparse = true;
    accept_file_for_tests(&root.0, &request).unwrap();
    let state = prepare_file_for_tests(&root.0, &request.operation_id).unwrap();
    assert_eq!(state.phase, FilePreparationPhase::Prepared);
    assert!(fs::metadata(&state.path).unwrap().blocks() * 512 < request.capacity_bytes);
    fs::OpenOptions::new()
        .write(true)
        .open(&state.path)
        .unwrap()
        .set_len(4096)
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            prepare_file_for_tests(&root.0, &request.operation_id)
                .unwrap()
                .phase,
            FilePreparationPhase::Failed
        );
        assert_eq!(fs::metadata(&state.path).unwrap().len(), 4096);
    }
    fs::remove_file(&state.path).unwrap();
    assert_eq!(
        prepare_file_for_tests(&root.0, &request.operation_id)
            .unwrap()
            .phase,
        FilePreparationPhase::Failed
    );
    assert!(!PathBuf::from(&state.path).exists());
}

#[test]
fn unowned_existing_file_and_symlink_are_never_adopted() {
    let root = TestRoot::new();
    fs::create_dir(root.0.join("disks")).unwrap();
    for link in [false, true] {
        let request = request();
        let state = accept_file_for_tests(&root.0, &request).unwrap();
        let original = root.0.join("original");
        fs::write(&original, b"original content").unwrap();
        if link {
            symlink(&original, &state.path).unwrap();
        } else {
            fs::write(&state.path, b"original content").unwrap();
        }
        let failed = prepare_file_for_tests(&root.0, &request.operation_id).unwrap();
        assert_eq!(failed.phase, FilePreparationPhase::Failed);
        assert!(failed.backing_identity.is_none());
        assert_eq!(fs::read(&state.path).unwrap(), b"original content");
    }
}

#[test]
fn insufficient_reserve_does_not_create_a_file_or_fall_back_to_sparse() {
    let root = TestRoot::new();
    let mut request = request();
    request.reserve_bytes = u64::MAX;
    let accepted = accept_file_for_tests(&root.0, &request).unwrap();
    let failed = prepare_file_for_tests(&root.0, &request.operation_id).unwrap();
    assert_eq!(failed.phase, FilePreparationPhase::Failed);
    assert!(!failed.request.sparse);
    assert!(!PathBuf::from(&accepted.path).exists());
}

#[test]
fn replaced_backing_file_and_aliased_owned_file_are_rejected() {
    let root = TestRoot::new();
    let request = request();
    accept_file_for_tests(&root.0, &request).unwrap();
    let state = prepare_file_for_tests(&root.0, &request.operation_id).unwrap();
    fs::hard_link(&state.path, root.0.join("alias")).unwrap();
    assert_eq!(
        prepare_file_for_tests(&root.0, &request.operation_id)
            .unwrap()
            .phase,
        FilePreparationPhase::Failed
    );
    fs::remove_file(&state.path).unwrap();
    fs::write(&state.path, b"replacement content").unwrap();
    assert_eq!(
        prepare_file_for_tests(&root.0, &request.operation_id)
            .unwrap()
            .phase,
        FilePreparationPhase::Failed
    );
    assert_eq!(fs::read(&state.path).unwrap(), b"replacement content");
}

#[test]
fn invalid_geometry_or_operation_path_never_accepts_intent() {
    let root = TestRoot::new();
    let mut request = request();
    request.operation_id = "../escape".into();
    assert!(accept_file_for_tests(&root.0, &request).is_err());
    request.operation_id = uuid::Uuid::new_v4().to_string();
    for capacity in [0, 4097, u64::MAX] {
        request.capacity_bytes = capacity;
        assert!(accept_file_for_tests(&root.0, &request).is_err());
    }
    assert_eq!(fs::read_dir(&root.0).unwrap().count(), 0);
}

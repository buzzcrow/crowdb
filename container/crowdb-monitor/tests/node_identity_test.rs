// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::PathBuf;

use crowdb_monitor::NodeIdentity;
use uuid::Uuid;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("crowdb-node-identity-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn restart_preserves_identity_with_unrelated_state() {
    let root = TestRoot::new();
    fs::write(root.0.join("existing-state"), "preserve").unwrap();
    let identity = NodeIdentity::load_or_create(&root.0).unwrap();
    assert_eq!(identity.uuid().get_version_num(), 4);
    assert_eq!(identity, NodeIdentity::load_or_create(&root.0).unwrap());
    assert_eq!(
        fs::read_to_string(root.0.join("existing-state")).unwrap(),
        "preserve"
    );
}

#[test]
fn concurrent_initialization_selects_one_identity() {
    let root = TestRoot::new();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let path = root.0.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                NodeIdentity::load_or_create(&path).unwrap()
            })
        })
        .collect();
    let identities: Vec<_> = workers.into_iter().map(|worker| worker.join().unwrap()).collect();
    assert!(identities.iter().all(|identity| *identity == identities[0]));
}

#[test]
fn corruption_and_symlinks_do_not_regenerate_identity() {
    let root = TestRoot::new();
    let path = root.0.join("node-identity");
    fs::write(&path, "corrupt").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(NodeIdentity::load_or_create(&root.0).is_err());
    assert_eq!(fs::read_to_string(&path).unwrap(), "corrupt");
    fs::remove_file(&path).unwrap();
    let target = root.0.join("target");
    fs::write(&target, format!("{}\n", Uuid::new_v4())).unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
    symlink(&target, &path).unwrap();
    assert!(NodeIdentity::load_or_create(&root.0).is_err());
}

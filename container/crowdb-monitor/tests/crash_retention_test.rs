// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs::{self, File, FileTimes};
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crowdb_monitor::CrashRetention;
use uuid::Uuid;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-crash-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn core(root: &Path, name: &str, seconds: u64) {
    let file = File::create(root.join(name)).unwrap();
    file.set_times(FileTimes::new().set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)))
        .unwrap();
}

#[test]
fn latest_core_survives_startup_and_recovery_without_touching_other_files() {
    let test = TestRoot::new();
    let root = test.0.join("crash");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    core(&root, "core.10", 10);
    core(&root, "core.11", 11);
    core(&root, "notes", 12);
    symlink(root.join("notes"), root.join("core.link")).unwrap();

    let retention = CrashRetention::open(root.clone()).unwrap();
    assert_eq!(fs::metadata(&root).unwrap().permissions().mode() & 0o777, 0o700);
    assert!(!root.join("core.10").exists());
    assert!(root.join("core.11").exists());
    assert!(root.join("notes").exists());
    assert!(root.join("core.link").exists());

    core(&root, "core.12", 12);
    retention.prune().unwrap();
    assert!(!root.join("core.11").exists());
    assert!(root.join("core.12").exists());
}

#[test]
fn symlinked_core_directory_is_rejected() {
    let test = TestRoot::new();
    let real = test.0.join("real");
    fs::create_dir(&real).unwrap();
    let alias = test.0.join("alias");
    symlink(real, &alias).unwrap();
    assert!(CrashRetention::open(alias).is_err());
}

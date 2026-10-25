// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_test_harness::test_dirs::{tempdir_in_test_data, TestDir};
use std::{
    fs,
    path::{Path, PathBuf},
};

/// Retain test state in the workspace while bounding Unix socket addresses.
pub struct TestRunRoot {
    _data: TestDir,
    alias: PathBuf,
    root: PathBuf,
}

impl TestRunRoot {
    pub fn new(tag: &str) -> Self {
        let data = tempdir_in_test_data(tag);
        // /tmp also avoids macOS's long per-user temporary directory prefix.
        let alias = PathBuf::from("/tmp").join(format!("cm-run-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir(data.path().join("run")).unwrap();
        std::os::unix::fs::symlink(data.path(), &alias).unwrap();
        // Keep the root itself a real directory for node identity validation.
        let root = alias.join("run");
        Self {
            _data: data,
            alias,
            root,
        }
    }

    pub fn path(&self) -> &Path {
        &self.root
    }
}

impl Drop for TestRunRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_file(&self.alias).unwrap();
        }
    }
}

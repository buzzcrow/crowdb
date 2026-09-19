// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;

use crowdb_console_shared::ops::s3;

struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "crowdb-s3-mini-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir(&path).expect("create test directory");
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn foreign_nonempty_directory_is_not_a_cluster() {
    let dir = TestDir::new("foreign");
    std::fs::write(dir.0.join("owned-by-user"), b"keep").expect("write sentinel");

    let error = s3::status(&dir.0).expect_err("foreign directory must be rejected");

    assert!(error.to_string().contains("is not a CROWDB S3 mini-cluster"));
    assert_eq!(std::fs::read(dir.0.join("owned-by-user")).unwrap(), b"keep");
}

#[test]
fn incomplete_marker_fails_closed() {
    let dir = TestDir::new("incomplete");
    std::fs::write(dir.0.join("s3-mini-cluster.json"), b"{}").expect("write marker");

    let error = s3::status(&dir.0).expect_err("incomplete marker must be rejected");

    assert!(error.to_string().contains("config error"));
}

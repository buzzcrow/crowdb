// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;

use crowdb_console_shared::ops::s3;
use reqwest::Method;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "starts the complete local storage and S3 process stack"]
async fn persistent_cluster_survives_stop_restart_and_range_read() {
    let dir = TestDir::new("persistent-e2e");
    s3::start(&dir.0).await.expect("start persistent cluster");
    let client = s3::S3HttpClient::from_data_dir(&dir.0).expect("S3 client");
    client
        .request(Method::PUT, Some("durable-bucket"), None, &[], None, None)
        .await
        .expect("create bucket");
    client
        .request(
            Method::PUT,
            Some("durable-bucket"),
            Some("nested/object"),
            &[],
            Some(b"durable-object-bytes".to_vec()),
            None,
        )
        .await
        .expect("put object");
    let marker = std::fs::read_to_string(dir.0.join("s3-mini-cluster.json")).expect("marker");
    let config = std::fs::read_to_string(dir.0.join("console.toml")).expect("config");
    assert!(!marker.contains("1111111111111111"));
    assert!(!config.contains("1111111111111111"));

    let stopped = s3::stop(&dir.0).expect("stop cluster");
    assert_eq!(stopped.running_services, 0);
    let restarted = s3::start(&dir.0).await.expect("restart cluster");
    assert_eq!(restarted.running_services, restarted.total_services);
    let client = s3::S3HttpClient::from_data_dir(&dir.0).expect("restarted S3 client");
    let (_, body) = client
        .request(
            Method::GET,
            Some("durable-bucket"),
            Some("nested/object"),
            &[],
            None,
            None,
        )
        .await
        .expect("read after restart");
    assert_eq!(body, b"durable-object-bytes");
    let (_, range) = client
        .request(
            Method::GET,
            Some("durable-bucket"),
            Some("nested/object"),
            &[],
            None,
            Some((8, 13)),
        )
        .await
        .expect("range read");
    assert_eq!(range, b"object");
    s3::stop(&dir.0).expect("final stop");
}

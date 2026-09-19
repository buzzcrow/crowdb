// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::ops::s3;
use crowdb_test_harness::test_dirs::TestDir;
use reqwest::Method;

#[test]
fn foreign_nonempty_directory_is_not_a_cluster() {
    let dir = TestDir::new("s3-mini-foreign").expect("create test directory");
    std::fs::write(dir.path().join("owned-by-user"), b"keep").expect("write sentinel");

    let error = s3::status(dir.path()).expect_err("foreign directory must be rejected");

    assert!(error.to_string().contains("is not a CROWDB S3 mini-cluster"));
    assert_eq!(std::fs::read(dir.path().join("owned-by-user")).unwrap(), b"keep");
}

#[test]
fn incomplete_marker_fails_closed() {
    let dir = TestDir::new("s3-mini-incomplete").expect("create test directory");
    std::fs::write(dir.path().join("s3-mini-cluster.json"), b"{}").expect("write marker");

    let error = s3::status(dir.path()).expect_err("incomplete marker must be rejected");

    assert!(error.to_string().contains("config error"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "starts the complete local storage and S3 process stack"]
async fn persistent_cluster_survives_stop_restart_and_range_read() {
    let dir = TestDir::new("s3-mini-persistent-e2e").expect("create test directory");
    s3::start(dir.path()).await.expect("start persistent cluster");
    let client = s3::S3HttpClient::from_data_dir(dir.path()).expect("S3 client");
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
    let marker = std::fs::read_to_string(dir.path().join("s3-mini-cluster.json")).expect("marker");
    let config = std::fs::read_to_string(dir.path().join("console.toml")).expect("config");
    assert!(!marker.contains("1111111111111111"));
    assert!(!config.contains("1111111111111111"));

    let stopped = s3::stop(dir.path()).expect("stop cluster");
    assert_eq!(stopped.running_services, 0);
    let restarted = s3::start(dir.path()).await.expect("restart cluster");
    assert_eq!(restarted.running_services, restarted.total_services);
    let client = s3::S3HttpClient::from_data_dir(dir.path()).expect("restarted S3 client");
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
    s3::stop(dir.path()).expect("final stop");
}

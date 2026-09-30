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

#[test]
fn local_launch_state_rejects_topology_and_legacy_console_file() {
    let dir = TestDir::new("s3-mini-local-only").expect("create test directory");
    std::fs::write(
        dir.path().join("s3-mini-cluster.json"),
        r#"{"version":1,"endpoint":"http://127.0.0.1:16000","tenant":"local"}"#,
    )
    .unwrap();
    std::fs::write(dir.path().join("console.toml"), "[[rack]]\nid = 1\n").unwrap();
    assert!(
        s3::status(dir.path()).is_err(),
        "legacy topology must not be loaded"
    );
    std::fs::write(
        dir.path().join("s3-local-state.toml"),
        "version = 1\ngroup0_seeds = ['http://127.0.0.1:10000']\n[[rack]]\nid = 1\n",
    )
    .unwrap();
    let error = s3::status(dir.path()).expect_err("local state cannot contain topology");
    assert!(error.to_string().contains("unknown field"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "starts the complete local storage and S3 process stack"]
async fn persistent_cluster_survives_stop_restart_and_range_read() {
    let dir = TestDir::new("s3-mini-persistent-e2e").expect("create test directory");
    let started = s3::start(dir.path()).await.expect("start persistent cluster");
    assert!(started.web_endpoint.starts_with("http://127.0.0.1:"));
    let health = reqwest::get(format!("{}/healthz", started.web_endpoint))
        .await
        .expect("web health request");
    assert!(health.status().is_success());
    let preview = reqwest::get(format!("{}/api/preview", started.web_endpoint))
        .await
        .expect("web preview request")
        .text()
        .await
        .expect("web preview body");
    assert!(preview.contains("group0"));
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
    let config = std::fs::read_to_string(dir.path().join("s3-local-state.toml")).expect("local state");
    assert!(!marker.contains("1111111111111111"));
    assert!(!config.contains("1111111111111111"));
    assert!(!config.contains("[[rack]]"));
    assert!(!config.contains("[[node]]"));
    assert!(!config.contains("[[store]]"));
    assert!(!dir.path().join("console.toml").exists());

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
    s3::delete(dir.path()).expect("delete cluster");
    assert!(!dir.path().exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "starts a complete simulated three-rack process stack"]
async fn protected_cluster_starts_and_reads_after_restart() {
    let dir = TestDir::new("s3-mini-protected-e2e").expect("create test directory");
    let started = s3::start_protected_test_cluster(dir.path())
        .await
        .expect("start protected cluster");
    let (_, record) = s3::load(dir.path()).expect("load protected cluster");
    assert!(record.protected_test);
    for node_id in 1..=3 {
        assert!(dir.path().join(format!("rack{node_id}/node{node_id}")).is_dir());
    }
    let client = s3::S3HttpClient::from_data_dir(dir.path()).expect("S3 client");
    client
        .request(Method::PUT, Some("protected-bucket"), None, &[], None, None)
        .await
        .expect("create bucket");
    client
        .request(
            Method::PUT,
            Some("protected-bucket"),
            Some("protected-object"),
            &[],
            Some(b"protected-object-bytes".to_vec()),
            None,
        )
        .await
        .expect("put protected object");
    assert_eq!(
        s3::stop(dir.path())
            .expect("stop protected cluster")
            .running_services,
        0
    );
    let restarted = s3::start_protected_test_cluster(dir.path())
        .await
        .expect("restart protected cluster");
    assert_eq!(restarted.endpoint, started.endpoint);
    let client = s3::S3HttpClient::from_data_dir(dir.path()).expect("restarted S3 client");
    let (_, body) = client
        .request(
            Method::GET,
            Some("protected-bucket"),
            Some("protected-object"),
            &[],
            None,
            None,
        )
        .await
        .expect("read protected object");
    assert_eq!(body, b"protected-object-bytes");
    s3::delete(dir.path()).expect("delete protected cluster");
}

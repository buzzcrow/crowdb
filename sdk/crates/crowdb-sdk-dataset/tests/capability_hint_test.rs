// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[allow(dead_code)]
#[path = "../../crowdb-sdk-core/tests/common/peer.rs"]
mod peer;

use crowdb_sdk_core::{CredentialProvider, HttpConfig};
use crowdb_sdk_dataset::{DatasetClient, Error};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

const SUPPORTED: &str = r#"{"api_version":2,"capabilities":["head-inspection","bounded-inspection"],"latest":null,"stable":null,"generation":null}"#;
const EMPTY: &str = r#"{"api_version":2,"capabilities":[]}"#;
const UNKNOWN: &str = r#"{"api_version":3,"capabilities":["head-inspection"]}"#;
const DENIED: &str =
    r#"{"code":"unauthorized","message":"unauthorized","request_id":"test","retryable":false}"#;

struct TestCredentials(AtomicUsize);
impl CredentialProvider for TestCredentials {
    fn bearer(&self) -> Result<Option<String>, Error> {
        Ok(Some(format!("token-{}", self.0.fetch_add(1, Ordering::Relaxed))))
    }
}

#[tokio::test]
async fn warm_clones_reuse_discovery_and_each_request_gets_current_credentials() {
    let mut peer = peer::TestPeer::start(SUPPORTED, false).await;
    let credentials = Arc::new(TestCredentials(AtomicUsize::new(0)));
    let client = DatasetClient::with_credential_provider(
        &peer.endpoint,
        "test",
        credentials.clone(),
        HttpConfig::default(),
    )
    .unwrap();
    let credential_checks = credentials.0.load(Ordering::Relaxed);
    client.head().await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let clone = client.clone();
        tasks.spawn(async move { clone.control().head().await });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap().unwrap();
    }
    assert_eq!(credentials.0.load(Ordering::Relaxed) - credential_checks, 6);
    let mut discovery = 0;
    let mut head = 0;
    for _ in 0..6 {
        let request = peer.requests.recv().await.unwrap();
        assert!(request
            .to_ascii_lowercase()
            .contains("authorization: bearer token-"));
        if request.starts_with("GET /api/dataset/v2/capabilities ") {
            discovery += 1;
        } else {
            assert!(request.contains("/datasets/test/head "));
            head += 1;
        }
    }
    assert_eq!((discovery, head), (1, 5));
    assert!(peer.requests.try_recv().is_err());
    client.capabilities().await.unwrap();
    client.control().head().await.unwrap();
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .contains("/datasets/test/head "));
    assert!(peer.requests.try_recv().is_err());
}

#[tokio::test]
async fn refresh_removes_lost_capabilities_and_missing_features_are_not_cached() {
    let mut peer = peer::TestPeer::start_responses(
        vec![
            (200, SUPPORTED),
            (200, SUPPORTED),
            (200, EMPTY),
            (200, EMPTY),
            (200, SUPPORTED),
            (200, SUPPORTED),
        ],
        false,
    )
    .await;
    let client = DatasetClient::new(&peer.endpoint, "test", None).unwrap();
    client.head().await.unwrap();
    assert!(client.capabilities().await.unwrap().capabilities.is_empty());
    assert!(matches!(client.control().head().await, Err(Error::Invalid(_))));
    client.control().head().await.unwrap();
    let mut paths = Vec::new();
    for _ in 0..6 {
        paths.push(peer.requests.recv().await.unwrap());
    }
    assert!(paths[0].starts_with("GET /api/dataset/v2/capabilities "));
    assert!(paths[1].contains("/datasets/test/head "));
    for request in &paths[2..5] {
        assert!(request.starts_with("GET /api/dataset/v2/capabilities "));
    }
    assert!(paths[5].contains("/datasets/test/head "));
    assert!(peer.requests.try_recv().is_err());
}

#[tokio::test]
async fn authentication_and_unsupported_versions_still_fail_closed_after_discovery() {
    let mut peer = peer::TestPeer::start_responses(
        vec![(200, SUPPORTED), (401, DENIED), (200, UNKNOWN), (200, UNKNOWN)],
        false,
    )
    .await;
    let client = DatasetClient::new(&peer.endpoint, "test", None).unwrap();
    client.capabilities().await.unwrap();
    assert!(matches!(
        client.control().head().await,
        Err(Error::Service { status: 401, .. })
    ));
    assert!(matches!(client.capabilities().await, Err(Error::Invalid(_))));
    assert!(matches!(client.control().head().await, Err(Error::Invalid(_))));
    let first = peer.requests.recv().await.unwrap();
    let denied = peer.requests.recv().await.unwrap();
    assert!(first.starts_with("GET /api/dataset/v2/capabilities "));
    assert!(denied.contains("/datasets/test/head "));
    for _ in 0..2 {
        assert!(peer
            .requests
            .recv()
            .await
            .unwrap()
            .starts_with("GET /api/dataset/v2/capabilities "));
    }
    assert!(peer.requests.try_recv().is_err());
}

#[tokio::test]
async fn cold_concurrent_calls_complete_without_global_serialization() {
    let mut peer = peer::TestPeer::start(SUPPORTED, true).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None).unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let clone = client.clone();
        tasks.spawn(async move { clone.control().head().await });
    }
    let request = tokio::time::timeout(Duration::from_secs(2), peer.requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(request.starts_with("GET /api/dataset/v2/capabilities "));
    peer.release.send(true).unwrap();
    while let Some(result) = tokio::time::timeout(Duration::from_secs(2), tasks.join_next())
        .await
        .unwrap()
    {
        result.unwrap().unwrap();
    }
    let mut discovery = 1;
    let mut head = 0;
    while let Ok(request) = peer.requests.try_recv() {
        if request.starts_with("GET /api/dataset/v2/capabilities ") {
            discovery += 1;
        } else {
            assert!(request.contains("/datasets/test/head "));
            head += 1;
        }
    }
    assert!((1..=4).contains(&discovery));
    assert_eq!(head, 4);
    client.control().head().await.unwrap();
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .contains("/datasets/test/head "));
    assert!(peer.requests.try_recv().is_err());
}

#[tokio::test]
async fn cancelling_discovery_does_not_publish_a_hint_or_leave_waiting_state() {
    let mut peer = peer::TestPeer::start_responses(vec![(200, SUPPORTED), (200, EMPTY)], true).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None).unwrap();
    let clone = client.clone();
    let task = tokio::spawn(async move { clone.control().head().await });
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    peer.release.send(true).unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), client.control().head())
            .await
            .unwrap(),
        Err(Error::Invalid(_))
    ));
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    assert!(peer.requests.try_recv().is_err());
}

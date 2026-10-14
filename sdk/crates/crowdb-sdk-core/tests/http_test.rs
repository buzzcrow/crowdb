// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/peer.rs"]
mod peer;

use bytes::Bytes;
use crowdb_sdk_core::{Error, HttpClient, HttpConfig};
use peer::TestPeer;
use std::sync::atomic::Ordering;

fn config() -> HttpConfig {
    HttpConfig {
        max_request_bytes: 1024,
        max_response_bytes: 1024,
        max_in_flight_bytes: 4096,
        max_requests: 2,
        ..HttpConfig::default()
    }
}

#[tokio::test]
async fn clones_reuse_a_connection_and_escape_each_path_segment() {
    let mut peer = TestPeer::start("{}", false).await;
    let client = HttpClient::with_config(&peer.endpoint, Some("secret".into()), config()).unwrap();
    for http in [&client, &client.clone()] {
        let value: serde_json::Value = http
            .json::<(), _>("GET", &["datasets", "a/b ?#"], None)
            .await
            .unwrap();
        assert_eq!(value, serde_json::json!({}));
        let header = peer.requests.recv().await.unwrap();
        assert!(
            header.starts_with("GET /datasets/a%2Fb%20%3F%23 HTTP/1.1\r\n"),
            "{header}"
        );
    }
    assert_eq!(peer.connections.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn admission_is_shared_and_cancellation_releases_capacity() {
    let mut peer = TestPeer::start("{}", true).await;
    let client = HttpClient::with_config(
        &peer.endpoint,
        None,
        HttpConfig {
            max_requests: 1,
            ..config()
        },
    )
    .unwrap();
    let copy = client.clone();
    let blocked = tokio::spawn(async move {
        copy.request("GET", &["blocked"], Bytes::new(), "application/json")
            .await
    });
    peer.requests.recv().await.unwrap();
    assert!(matches!(
        client
            .request("GET", &["second"], Bytes::new(), "application/json")
            .await,
        Err(Error::Capacity)
    ));
    blocked.abort();
    assert!(blocked.await.unwrap_err().is_cancelled());
    peer.release.send(true).unwrap();
    client
        .request("GET", &["after-cancel"], Bytes::new(), "application/json")
        .await
        .unwrap();
}

#[tokio::test]
async fn independent_requests_progress_without_one_global_request_lock() {
    let mut peer = TestPeer::start("{}", true).await;
    let client = HttpClient::with_config(&peer.endpoint, None, config()).unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for path in ["first", "second"] {
        let copy = client.clone();
        tasks.spawn(async move {
            copy.request("GET", &[path], Bytes::new(), "application/json")
                .await
        });
    }
    peer.requests.recv().await.unwrap();
    peer.requests.recv().await.unwrap();
    assert_eq!(peer.connections.load(Ordering::Acquire), 2);
    peer.release.send(true).unwrap();
    while let Some(result) = tasks.join_next().await {
        result.unwrap().unwrap();
    }
}

#[tokio::test]
async fn request_encoding_and_response_limits_are_enforced() {
    let peer = TestPeer::start("{\"value\":12345}", false).await;
    let client = HttpClient::with_config(
        &peer.endpoint,
        None,
        HttpConfig {
            max_response_bytes: 4,
            ..config()
        },
    )
    .unwrap();
    assert!(matches!(
        client
            .request("GET", &["value"], Bytes::new(), "application/json")
            .await,
        Err(Error::Limit)
    ));
    let huge = "X".repeat(2048);
    let result: Result<serde_json::Value, _> = client.json("POST", &["value"], Some(&huge)).await;
    assert!(matches!(result, Err(Error::Limit)));
}

#[test]
fn invalid_credentials_and_endpoints_never_echo_secrets() {
    for (endpoint, token) in [
        ("http://secret@example.com", None),
        ("http://example.com?secret", None),
        ("http://example.com", Some("secret\n".into())),
    ] {
        let Err(error) = HttpClient::with_config(endpoint, token, config()) else {
            panic!("configuration unexpectedly accepted")
        };
        assert!(!format!("{error:?}").contains("secret"));
    }
}

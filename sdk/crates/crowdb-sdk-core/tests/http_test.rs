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

#[tokio::test]
async fn safe_retry_preserves_body_and_refreshes_credentials_per_attempt() {
    struct TestCredential(std::sync::atomic::AtomicUsize);
    impl crowdb_sdk_core::CredentialProvider for TestCredential {
        fn bearer(&self) -> Result<Option<String>, Error> {
            Ok(Some(
                if self.0.fetch_add(1, Ordering::AcqRel) < 2 {
                    "old"
                } else {
                    "new"
                }
                .into(),
            ))
        }
    }
    let mut peer = TestPeer::start_statuses(
        "{\"code\":\"unavailable\",\"message\":\"busy\",\"request_id\":\"fixture\",\"retryable\":true}",
        false,
        vec![503, 200],
    )
    .await;
    let client = HttpClient::with_provider(
        &peer.endpoint,
        std::sync::Arc::new(TestCredential(std::sync::atomic::AtomicUsize::new(0))),
        HttpConfig {
            retry_backoff: std::time::Duration::ZERO,
            ..config()
        },
    )
    .unwrap();
    let body = serde_json::json!({"operation":"same-token"});
    let _: serde_json::Value = client
        .json_idempotent("POST", &["publish"], Some(&body))
        .await
        .unwrap();
    let first = peer.requests.recv().await.unwrap();
    let second = peer.requests.recv().await.unwrap();
    assert!(first.ends_with("{\"operation\":\"same-token\"}"));
    assert!(second.ends_with("{\"operation\":\"same-token\"}"));
    assert!(first.to_lowercase().contains("authorization: bearer old"));
    assert!(second.to_lowercase().contains("authorization: bearer new"));
}
#[tokio::test]
async fn auth_is_not_retried_and_retry_sleep_obeys_the_overall_deadline() {
    let mut denied = TestPeer::start_statuses(
        "{\"code\":\"unauthorized\",\"message\":\"denied\",\"request_id\":\"fixture\",\"retryable\":true}",
        false,
        vec![401],
    )
    .await;
    let client = HttpClient::with_config(&denied.endpoint, None, config()).unwrap();
    assert!(matches!(
        client
            .json_idempotent::<(), serde_json::Value>("GET", &["read"], None)
            .await,
        Err(Error::Service { status: 401, .. })
    ));
    denied.requests.recv().await.unwrap();
    assert!(denied.requests.try_recv().is_err());
    let mut busy = TestPeer::start_statuses(
        "{\"code\":\"unavailable\",\"message\":\"busy\",\"request_id\":\"fixture\",\"retryable\":true}",
        false,
        vec![503, 200],
    )
    .await;
    let client = HttpClient::with_config(
        &busy.endpoint,
        None,
        HttpConfig {
            overall_timeout: std::time::Duration::from_millis(20),
            retry_backoff: std::time::Duration::from_secs(1),
            ..config()
        },
    )
    .unwrap();
    assert!(matches!(
        client
            .json_idempotent::<(), serde_json::Value>("GET", &["read"], None)
            .await,
        Err(Error::Deadline)
    ));
    busy.requests.recv().await.unwrap();
    assert!(busy.requests.try_recv().is_err());
    let _: serde_json::Value = client.json::<(), _>("GET", &["after"], None).await.unwrap();
}
#[test]
fn provider_errors_and_oversize_endpoint_diagnostics_do_not_echo_sensitive_input() {
    struct TestBadCredential;
    impl crowdb_sdk_core::CredentialProvider for TestBadCredential {
        fn bearer(&self) -> Result<Option<String>, Error> {
            Err(Error::Invalid("PRIVATE-CREDENTIAL".into()))
        }
    }
    let result = HttpClient::with_provider(
        "http://localhost",
        std::sync::Arc::new(TestBadCredential),
        config(),
    );
    let error = result.err().unwrap();
    assert!(!format!("{error:?}").contains("PRIVATE-CREDENTIAL"));
    let endpoint = format!("http://localhost/{}", "PRIVATE-CREDENTIAL".repeat(1000));
    assert!(!format!(
        "{:?}",
        HttpClient::with_config(&endpoint, None, config()).err().unwrap()
    )
    .contains("PRIVATE-CREDENTIAL"));
}

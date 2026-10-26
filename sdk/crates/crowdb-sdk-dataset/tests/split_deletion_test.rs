// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[allow(dead_code)] // Shares the transport peer and response injection helpers.
#[path = "../../crowdb-sdk-core/tests/common/peer.rs"]
mod peer;

use crowdb_sdk_dataset::{
    v2::{Decimal, OpaqueId, SplitRevisionDeletion},
    DatasetClient, Error,
};
use serde_json::{json, Value};

const CAPABILITY: &str = r#"{"api_version":2,"capabilities":["split-revision-delete"]}"#;
const PENDING: &str = r#"{"snapshot":"01010101010101010101010101010101","revision":"02020202020202020202020202020202","reclaim_after":"2200","complete":false}"#;

fn ids() -> (OpaqueId, OpaqueId) {
    ("01".repeat(16).parse().unwrap(), "02".repeat(16).parse().unwrap())
}

#[test]
fn deletion_status_preserves_exact_deadlines_and_requires_explicit_null() {
    let (snapshot, revision) = ids();
    let mut value = json!({"snapshot":snapshot,"revision":revision,"reclaim_after":"18446744073709551615","complete":false});
    let status: SplitRevisionDeletion = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(status.reclaim_after, Some(Decimal(u64::MAX)));
    assert_eq!(serde_json::to_value(&status).unwrap(), value);
    for malformed in [json!(2200), json!("02200"), json!("18446744073709551616")] {
        value["reclaim_after"] = malformed;
        assert!(serde_json::from_value::<SplitRevisionDeletion>(value.clone()).is_err());
    }
    value["reclaim_after"] = Value::Null;
    value["complete"] = json!(true);
    let terminal: SplitRevisionDeletion = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(terminal.reclaim_after, None);
    assert!(terminal.complete);
    value.as_object_mut().unwrap().remove("reclaim_after");
    assert!(serde_json::from_value::<SplitRevisionDeletion>(value).is_err());
}

#[tokio::test]
async fn reading_capability_cannot_authorize_delete_or_downgrade_to_abandon() {
    let mut peer =
        peer::TestPeer::start(r#"{"api_version":2,"capabilities":["split-revisions"]}"#, false).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    let (snapshot, revision) = ids();
    let error = client
        .delete_split_revision(&snapshot, &revision)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Invalid(message) if message.contains("split-revision-delete")));
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    assert!(peer.requests.try_recv().is_err());
}

#[tokio::test]
async fn missing_server_route_fails_without_status_reads_or_legacy_cleanup() {
    let mut peer = peer::TestPeer::start_responses(
        vec![
            (200, CAPABILITY),
            (
                501,
                r#"{"code":"unsupported","message":"unsupported","request_id":"test","retryable":false}"#,
            ),
        ],
        false,
    )
    .await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    let (snapshot, revision) = ids();
    assert!(matches!(
        client.delete_split_revision(&snapshot, &revision).await,
        Err(Error::Service { status: 501, .. })
    ));
    peer.requests.recv().await.unwrap();
    let request = peer.requests.recv().await.unwrap();
    assert!(request.starts_with(&format!("DELETE /api/dataset/v2/namespaces/dataset-ns/datasets/test/snapshots/{snapshot}/split-revisions/{revision} ")));
    assert!(request.ends_with("\r\n\r\n"));
    assert!(peer.requests.try_recv().is_err());
}

#[tokio::test]
async fn transient_retry_uses_the_same_two_ids_and_warm_calls_reuse_capability() {
    let mut peer = peer::TestPeer::start_responses(
        vec![
            (200, CAPABILITY),
            (
                503,
                r#"{"code":"unavailable","message":"unknown","request_id":"test","retryable":true}"#,
            ),
            (200, PENDING),
        ],
        false,
    )
    .await;
    let client = DatasetClient::new(&peer.endpoint, "a/b", None).unwrap();
    let (snapshot, revision) = ids();
    let status = client
        .control()
        .delete_split_revision(&snapshot, &revision)
        .await
        .unwrap();
    assert_eq!(status.reclaim_after, Some(Decimal(2200)));
    assert_eq!(
        client
            .clone()
            .control()
            .delete_split_revision(&snapshot, &revision)
            .await
            .unwrap(),
        status
    );
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    for _ in 0..3 {
        let request = peer.requests.recv().await.unwrap();
        assert!(request.starts_with(&format!("DELETE /api/dataset/v2/namespaces/dataset-ns/datasets/a%2Fb/snapshots/{snapshot}/split-revisions/{revision} ")));
        assert!(request.ends_with("\r\n\r\n"));
    }
    assert!(peer.requests.try_recv().is_err());
}

#[tokio::test]
async fn protected_and_missing_revisions_are_reported_without_automatic_retry() {
    let mut peer = peer::TestPeer::start_responses(
        vec![
            (200, CAPABILITY),
            (
                409,
                r#"{"code":"conflict","message":"current","request_id":"test","retryable":false}"#,
            ),
            (
                404,
                r#"{"code":"not_found","message":"not_found","request_id":"test","retryable":false}"#,
            ),
        ],
        false,
    )
    .await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    let (snapshot, revision) = ids();
    for expected in [409, 404] {
        assert!(
            matches!(client.delete_split_revision(&snapshot, &revision).await, Err(Error::Service { status, .. }) if status == expected)
        );
    }
    for _ in 0..3 {
        peer.requests.recv().await.unwrap();
    }
    assert!(peer.requests.try_recv().is_err());
}

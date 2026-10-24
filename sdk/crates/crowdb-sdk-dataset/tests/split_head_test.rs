// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[allow(dead_code)]
#[path = "../../crowdb-sdk-core/tests/common/peer.rs"]
mod peer;

use crowdb_sdk_dataset::{
    v2::{OpaqueId, SplitHead, UpdateSplitHead},
    DatasetClient,
};
use serde_json::json;

#[test]
fn conditions_are_required_nullable_values_and_older_errors_have_no_observation() {
    let request = json!({"revision":"02".repeat(16),"expected_revision":null,"expected_head":null});
    serde_json::from_value::<UpdateSplitHead>(request.clone()).unwrap();
    for field in ["expected_revision", "expected_head"] {
        let mut invalid = request.clone();
        invalid.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<UpdateSplitHead>(invalid).is_err());
    }
    let error: crowdb_sdk_core::ServiceError = serde_json::from_value(
        json!({"code":"conflict","message":"conflict","request_id":"test","retryable":false}),
    )
    .unwrap();
    assert!(error.observed_head.is_none());
}

#[tokio::test]
async fn unsupported_head_updates_do_not_fall_back_to_draft_publication() {
    let mut peer =
        peer::TestPeer::start(r#"{"api_version":2,"capabilities":["split-revisions"]}"#, false).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    assert!(client
        .update_split_head(
            &OpaqueId::random(),
            &UpdateSplitHead {
                revision: OpaqueId::random(),
                expected_revision: None,
                expected_head: None
            }
        )
        .await
        .is_err());
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    assert!(peer.requests.try_recv().is_err());
}

#[tokio::test]
async fn a_conflict_can_supply_the_next_explicit_request_without_another_head_get() {
    let mut peer = peer::TestPeer::start_statuses(r#"{"api_version":2,"capabilities":["split-head-update"],"revision":"02020202020202020202020202020202","generation":"0303030303030303030303030303030303030303030303030303030303030303","code":"conflict","message":"conflict","request_id":"test","retryable":false,"observed_head":{"snapshot":"01010101010101010101010101010101","revision":"04040404040404040404040404040404","generation":"0505050505050505050505050505050505050505050505050505050505050505"}}"#, false, vec![200, 409, 200]).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    let snapshot = "01".repeat(16).parse().unwrap();
    let mut request = UpdateSplitHead {
        revision: "02".repeat(16).parse().unwrap(),
        expected_revision: None,
        expected_head: None,
    };
    let error = client.update_split_head(&snapshot, &request).await.unwrap_err();
    let current = SplitHead::from_conflict(&error).unwrap();
    assert_eq!(current.revision.as_ref().unwrap().as_str(), "04".repeat(16));
    request.expected_revision = current.revision;
    request.expected_head = current.generation;
    assert_eq!(
        client
            .update_split_head(&snapshot, &request)
            .await
            .unwrap()
            .revision,
        Some(request.revision)
    );
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    let first = peer.requests.recv().await.unwrap();
    let next = peer.requests.recv().await.unwrap();
    assert!(first.starts_with("PUT /api/dataset/v2/namespaces/dataset-ns/datasets/test/snapshots/"));
    assert!(first.contains("\"expected_head\":null"));
    assert!(next.contains(&format!("\"expected_head\":\"{}\"", "05".repeat(32))));
    assert!(next.contains(&format!("\"expected_revision\":\"{}\"", "04".repeat(16))));
    assert!(peer.requests.try_recv().is_err());
}

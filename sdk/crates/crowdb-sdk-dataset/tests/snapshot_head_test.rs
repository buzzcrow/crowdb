// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[allow(dead_code)]
#[path = "../../crowdb-sdk-core/tests/common/peer.rs"]
mod peer;
use crowdb_sdk_dataset::{
    v2::{Head, OpaqueId, UpdateSnapshotHead},
    DatasetClient,
};
use serde_json::json;

#[test]
fn expected_head_is_required_and_nullable_and_legacy_conflicts_remain_decodable() {
    let input = json!({"snapshot":"01".repeat(16),"expected_head":null});
    serde_json::from_value::<UpdateSnapshotHead>(input).unwrap();
    assert!(serde_json::from_value::<UpdateSnapshotHead>(json!({"snapshot":"01".repeat(16)})).is_err());
    assert!(serde_json::from_value::<UpdateSnapshotHead>(
        json!({"snapshot":"01".repeat(16),"expected_head":null,"attempt":"02".repeat(16)})
    )
    .is_err());
    let error = crowdb_sdk_dataset::Error::Service {
        status: 409,
        error: serde_json::from_value(
            json!({"code":"conflict","message":"conflict","request_id":"test","retryable":false}),
        )
        .unwrap(),
    };
    assert!(Head::from_conflict(&error).is_none());
}

#[tokio::test]
async fn unsupported_updates_do_not_fall_back_to_a_composition_or_v1_write() {
    let mut peer = peer::TestPeer::start(r#"{"api_version":2,"capabilities":["composition"]}"#, false).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    assert!(client
        .update_snapshot_head(&UpdateSnapshotHead {
            snapshot: OpaqueId::random(),
            expected_head: None
        })
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
async fn conflict_observations_supply_the_next_explicit_update_without_a_head_get() {
    let mut peer = peer::TestPeer::start_statuses(r#"{"api_version":2,"capabilities":["snapshot-head-update"],"latest":"01010101010101010101010101010101","stable":null,"generation":"0202020202020202020202020202020202020202020202020202020202020202","code":"conflict","message":"conflict","request_id":"test","retryable":false,"observed_head":{"latest":"03030303030303030303030303030303","stable":null,"generation":"0404040404040404040404040404040404040404040404040404040404040404"}}"#, false, vec![200,409,200]).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    let mut update = UpdateSnapshotHead {
        snapshot: "01".repeat(16).parse().unwrap(),
        expected_head: None,
    };
    let error = client.update_snapshot_head(&update).await.unwrap_err();
    let observed = Head::from_conflict(&error).unwrap();
    assert_eq!(observed.latest.unwrap().as_str(), "03".repeat(16));
    update.expected_head = observed.generation;
    assert_eq!(
        client.update_snapshot_head(&update).await.unwrap().latest,
        Some(update.snapshot.clone())
    );
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    for expected in [None, update.expected_head.clone()] {
        let request = peer.requests.recv().await.unwrap();
        assert!(request.starts_with("PUT /api/dataset/v2/namespaces/dataset-ns/datasets/test/head "));
        let body: serde_json::Value =
            serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body, json!({"snapshot":update.snapshot,"expected_head":expected}));
    }
    assert!(peer.requests.try_recv().is_err());
}

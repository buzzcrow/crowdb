// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[allow(dead_code)]
#[path = "../../crowdb-sdk-core/tests/common/peer.rs"]
mod peer;
use crowdb_sdk_dataset::{
    v2::{CompositionSource, OpaqueId, PrepareSnapshot, SplitPolicy},
    DatasetClient,
};
use serde_json::json;

fn input() -> PrepareSnapshot {
    PrepareSnapshot {
        parent: None,
        source: CompositionSource::All,
        schema: None,
        samples: Vec::new(),
        splits: Vec::new(),
        policy: SplitPolicy::default(),
    }
}

#[test]
fn input_requires_explicit_parent_and_schema_without_process_identifiers() {
    let original = serde_json::to_value(input()).unwrap();
    for field in ["parent", "schema"] {
        let mut invalid = original.clone();
        invalid.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<PrepareSnapshot>(invalid).is_err());
    }
    for field in [
        "operation",
        "edit",
        "expected_revision",
        "expected_page",
        "inherit_parent_splits",
    ] {
        let mut invalid = original.clone();
        invalid[field] = json!("1");
        assert!(serde_json::from_value::<PrepareSnapshot>(invalid).is_err());
    }
}

#[tokio::test]
async fn unavailable_preparation_cannot_fall_back_to_a_composition_workflow() {
    let mut peer = peer::TestPeer::start(r#"{"api_version":2,"capabilities":["composition"]}"#, false).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    assert!(client
        .prepare_snapshot(&OpaqueId::random(), &input())
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
async fn an_unknown_reply_retries_the_same_snapshot_and_full_input_without_status_queries() {
    let mut peer = peer::TestPeer::start_responses(
        vec![
            (
                200,
                r#"{"api_version":2,"capabilities":["composition-preparation"]}"#,
            ),
            (
                503,
                r#"{"code":"unavailable","message":"unknown outcome","request_id":"test","retryable":true}"#,
            ),
            (
                200,
                r#"{"snapshot":"01010101010101010101010101010101","initial_split":null,"sample_count":"1"}"#,
            ),
        ],
        false,
    )
    .await;
    let snapshot: OpaqueId = "01".repeat(16).parse().unwrap();
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    let input = input();
    assert_eq!(
        client.prepare_snapshot(&snapshot, &input).await.unwrap().snapshot,
        snapshot
    );
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    for _ in 0..2 {
        let request = peer.requests.recv().await.unwrap();
        assert!(request.starts_with(&format!(
            "PUT /api/dataset/v2/namespaces/dataset-ns/datasets/test/snapshots/{snapshot} "
        )));
        let body: serde_json::Value =
            serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body, serde_json::to_value(&input).unwrap());
    }
    assert!(peer.requests.try_recv().is_err());
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[allow(dead_code)]
#[path = "../../crowdb-sdk-core/tests/common/peer.rs"]
mod peer;
use crowdb_sdk_dataset::{
    v2::{OpaqueId, PrepareSplitRevision, SplitPolicy},
    DatasetClient,
};
use serde_json::json;

fn input() -> PrepareSplitRevision {
    PrepareSplitRevision {
        policy: SplitPolicy::default(),
        splits: Vec::new(),
    }
}
#[test]
fn preparation_has_no_draft_identity_or_progress_fields() {
    let mut value = serde_json::to_value(input()).unwrap();
    assert_eq!(
        value,
        json!({"policy":{"overlap":"disjoint", "coverage":"allow_unassigned"}, "splits":[]})
    );
    for field in ["operation", "expected_page", "expected_revision"] {
        value[field] = json!("1");
        assert!(serde_json::from_value::<PrepareSplitRevision>(value.clone()).is_err());
        value.as_object_mut().unwrap().remove(field);
    }
}

#[tokio::test]
async fn unavailable_preparation_never_downgrades_to_the_old_draft_protocol() {
    let mut peer =
        peer::TestPeer::start(r#"{"api_version":2,"capabilities":["split-revisions"]}"#, false).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    let revision = OpaqueId::random();
    assert!(client
        .prepare_split_revision(&OpaqueId::random(), &revision, &input())
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
async fn transport_retry_preserves_the_resource_id_and_full_input_without_discovery_or_status_queries() {
    let mut peer = peer::TestPeer::start_responses(vec![
        (200, r#"{"api_version":2,"capabilities":["split-preparation"]}"#),
        (503, r#"{"code":"unavailable","message":"unknown outcome","request_id":"test","retryable":true}"#),
        (200, r#"{"snapshot":"01010101010101010101010101010101","revision":"02020202020202020202020202020202","operation":"02020202020202020202020202020202","evaluator_version":1,"policy":{"overlap":"disjoint","coverage":"allow_unassigned"},"splits":[]}"#),
    ], false).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    let snapshot: OpaqueId = "01".repeat(16).parse().unwrap();
    let revision: OpaqueId = "02".repeat(16).parse().unwrap();
    let request = input();
    assert_eq!(
        client
            .prepare_split_revision(&snapshot, &revision, &request)
            .await
            .unwrap()
            .revision,
        revision
    );
    assert!(peer
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    for _ in 0..2 {
        let call = peer.requests.recv().await.unwrap();
        assert!(call.starts_with(&format!(
            "PUT /api/dataset/v2/namespaces/dataset-ns/datasets/test/snapshots/{}/split-revisions/{} ",
            snapshot.as_str(),
            revision.as_str()
        )));
        let body: serde_json::Value = serde_json::from_str(call.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body, serde_json::to_value(&request).unwrap());
    }
    assert!(peer.requests.try_recv().is_err());
}

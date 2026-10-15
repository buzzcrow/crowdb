// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_sdk_dataset::DatasetCapabilities;

#[allow(dead_code)]
#[path = "../../crowdb-sdk-core/tests/common/peer.rs"]
mod peer;

#[test]
fn capabilities_fail_closed_for_missing_features_and_unknown_versions() {
    let mut capabilities: DatasetCapabilities = serde_json::from_str(
        r#"{"api_version":2,"capabilities":["split-revisions"],"future_optional":true}"#,
    )
    .unwrap();
    capabilities.require(&["split-revisions"]).unwrap();
    assert!(capabilities.require(&["composition"]).is_err());
    capabilities.api_version = 3;
    assert!(capabilities.require(&[]).is_err());
    for malformed in [r#"{"capabilities":[]}"#, r#"{"api_version":2}"#] {
        assert!(serde_json::from_str::<DatasetCapabilities>(malformed).is_err());
    }
}

#[tokio::test]
async fn discovery_uses_v2_and_old_servers_cannot_trigger_a_downgraded_write() {
    use crowdb_sdk_dataset::{DatasetClient, Error};

    let mut server = peer::TestPeer::start(r#"{"api_version":2,"capabilities":[]}"#, false).await;
    let client = DatasetClient::new(&server.endpoint, "training/a b", None).unwrap();
    let capabilities = client.capabilities().await.unwrap();
    assert!(capabilities.require(&["split-revisions"]).is_err());
    let request = server.requests.recv().await.unwrap();
    assert!(request.starts_with("GET /api/dataset/v2/capabilities HTTP/1.1\r\n"));
    assert!(server.requests.try_recv().is_err());

    let mut old = peer::TestPeer::start_statuses(
        r#"{"code":"not_found","message":"not_found","request_id":"test","retryable":false}"#,
        false,
        vec![404],
    )
    .await;
    let client = DatasetClient::new(&old.endpoint, "example", None).unwrap();
    assert!(matches!(
        client.capabilities().await,
        Err(Error::Service { status: 404, .. })
    ));
    assert!(old
        .requests
        .recv()
        .await
        .unwrap()
        .starts_with("GET /api/dataset/v2/capabilities "));
    assert!(old.requests.try_recv().is_err());
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_sdk_dataset::{
    v2::{Decimal, OpaqueId, PublishComposition, SampleId},
    DatasetClient,
};
use serde_json::json;
#[allow(dead_code)]
#[path = "../../crowdb-sdk-core/tests/common/peer.rs"]
mod peer;

#[test]
fn v2_stream_accepts_long_binary_ids_without_changing_v1_bounds() {
    use bytes::BytesMut;
    use crowdb_sdk_dataset::wire::{BatchDecoder, BATCH_MAGIC};
    let id = SampleId::new(vec![255; 16384]).unwrap().base64();
    let header = serde_json::to_vec(&json!({"snapshot":"01".repeat(16),"sample_ids":[id.clone()],"fields":[{"sample":id,"field":"x".repeat(1024),"state":"missing","length":0,"md5":null}],"next":null,"end":true})).unwrap();
    let mut bytes = Vec::from(BATCH_MAGIC.as_slice());
    bytes.push(1);
    bytes.extend_from_slice(&u32::try_from(header.len()).unwrap().to_be_bytes());
    bytes.extend(header);
    bytes.extend([0, 0, 0, 0, 0]);
    assert!(BatchDecoder::new()
        .decode(&mut BytesMut::from(bytes.as_slice()))
        .is_err());
    let mut decoder = BatchDecoder::for_control_v2();
    let mut buffer = BytesMut::from(bytes.as_slice());
    while decoder.decode(&mut buffer).unwrap().is_some() {}
    decoder.finish().unwrap();
    assert!(buffer.is_empty());
}

#[test]
fn exact_id_and_integer_contracts_reject_lossy_or_ambiguous_input() {
    assert_eq!(
        serde_json::to_value(Decimal(u64::MAX)).unwrap(),
        json!("18446744073709551615")
    );
    for value in [json!(1), json!("01"), json!("+1"), json!("18446744073709551616")] {
        assert!(serde_json::from_value::<Decimal>(value).is_err());
    }
    for value in ["0".repeat(32), "A".repeat(32), "1".repeat(31)] {
        assert!(value.parse::<OpaqueId>().is_err());
    }
    let id = SampleId::new(vec![0, 255, 128]).unwrap();
    assert_eq!(serde_json::to_value(&id).unwrap(), json!("AP+A"));
    assert_eq!(serde_json::from_value::<SampleId>(json!("AP+A")).unwrap(), id);
    for value in ["", "YQ", "YR==", "YQ==\n"] {
        assert!(serde_json::from_value::<SampleId>(json!(value)).is_err());
    }
    let mut publish = json!({"attempt":"01".repeat(16),"snapshot":"02".repeat(16),"expected_head":null});
    serde_json::from_value::<PublishComposition>(publish.clone()).unwrap();
    publish.as_object_mut().unwrap().remove("expected_head");
    assert!(serde_json::from_value::<PublishComposition>(publish).is_err());
}

#[tokio::test]
async fn unsupported_composition_cannot_issue_a_write() {
    let mut peer = peer::TestPeer::start(r#"{"api_version":2,"capabilities":[]}"#, false).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    let id = OpaqueId::random();
    assert!(client
        .publish_composition(
            &id,
            &PublishComposition {
                attempt: OpaqueId::random(),
                snapshot: OpaqueId::random(),
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
async fn pages_are_lazy_bounded_and_reject_a_repeated_continuation() {
    let mut peer = peer::TestPeer::start(
        r#"{"api_version":2,"capabilities":["bounded-inspection"],"entries":["AAH/"],"after":"YQ=="}"#,
        false,
    )
    .await;
    let client = DatasetClient::new(&peer.endpoint, "a/b", None).unwrap().control();
    let mut pages = client.samples(OpaqueId::random(), 1).unwrap();
    assert!(peer.requests.try_recv().is_err());
    let page = pages.next_page().await.unwrap().unwrap();
    assert_eq!(page.entries[0].as_bytes(), &[0, 1, 255]);
    assert_eq!(pages.continuation(), Some("YQ=="));
    peer.requests.recv().await.unwrap();
    let request = peer.requests.recv().await.unwrap();
    assert!(request.contains("/datasets/a%2Fb/snapshots/"));
    assert!(request.contains("?limit=1 "));
    assert!(peer.requests.try_recv().is_err());
    assert!(pages.next_page().await.is_err());
    assert_eq!(pages.continuation(), Some("YQ=="));
    peer.requests.recv().await.unwrap();
    assert!(peer.requests.recv().await.unwrap().contains("after=YQ%3D%3D"));
}

#[tokio::test]
async fn publication_transport_retry_preserves_exact_attempt_and_body() {
    let mut peer = peer::TestPeer::start_statuses(r#"{"api_version":2,"capabilities":["composition"],"code":"busy","message":"busy","request_id":"retry","retryable":true,"attempt":"01010101010101010101010101010101","target":{"kind":"snapshot","snapshot":"02020202020202020202020202020202"},"expected_head":null,"committed_head":"0303030303030303030303030303030303030303030303030303030303030303"}"#,false,vec![200,503,200]).await;
    let client = DatasetClient::new(&peer.endpoint, "test", None)
        .unwrap()
        .control();
    let request = PublishComposition {
        attempt: "01".repeat(16).parse().unwrap(),
        snapshot: "02".repeat(16).parse().unwrap(),
        expected_head: None,
    };
    let receipt = client
        .publish_composition(&OpaqueId::random(), &request)
        .await
        .unwrap();
    assert_eq!(receipt.attempt, request.attempt);
    peer.requests.recv().await.unwrap();
    let first = peer.requests.recv().await.unwrap();
    let retry = peer.requests.recv().await.unwrap();
    assert_eq!(first, retry);
    assert!(first.contains("\"expected_head\":null"));
}

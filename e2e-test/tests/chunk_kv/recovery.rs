// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource, MultiScanRequest,
};
use crowdb_protocol::chunk_kv::{OperationResult, RpcCompareCondition, ScanDirection, SeekKind};

fn client() -> ChunkKvClient {
    let addresses = crate::common::management::addresses(12);
    let seeds = addresses[..3]
        .iter()
        .map(|ip| crate::common::management::origin(ip))
        .collect();
    let kv = Arc::new(crowdb_kv_client::CrowdbKvClient::new(
        crowdb_kv_client::ClientConfig::new(seeds),
    ));
    ChunkKvClient::new(
        ClientConfig::default(),
        Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(kv)),
        Arc::new(ChunkKvRpcTransport::new(4, 1, 1)),
    )
    .unwrap()
}

async fn verify(client: &ChunkKvClient) {
    for (key, expected) in [(b"a".as_slice(), Some(b"updated".as_slice())), (b"b", None)] {
        let result = client.get(key.to_vec(), None).await.unwrap().result.unwrap();
        let OperationResult::Value(value) = result else {
            panic!("get returned a non-value response: {result:?}");
        };
        assert_eq!(value.as_ref().map(|value| value.value.as_slice()), expected);
    }
    let seek = client.seek(b"a".to_vec(), SeekKind::Ceiling).await.unwrap();
    let OperationResult::Value(Some(value)) = seek.result.unwrap() else {
        panic!("ordered seek lost the inclusive start record");
    };
    assert_eq!(value.key, b"a");
    assert_eq!(value.value, b"updated");
    for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
        let page = client
            .scan(MultiScanRequest {
                start: Some(b"a".to_vec()),
                end: Some(b"c".to_vec()),
                direction,
                max_items: 10,
                max_bytes: 4096,
                continuation: None,
            })
            .await
            .unwrap();
        assert!(page.terminal_failure.is_none());
        assert!(page.continuation.is_none());
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].key, b"a");
        assert_eq!(page.items[0].value, b"updated");
    }
}

#[tokio::test]
#[ignore = "requires the owned persistent release Chunk-KV containers"]
async fn routed_native_records_and_tombstones_survive_complete_service_restart() {
    let client = client();
    if std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("verify") {
        verify(&client).await;
        return;
    }
    let first = client.put(b"a".to_vec(), b"original".to_vec()).await.unwrap();
    let OperationResult::Mutation {
        applied: true,
        revision: Some(revision),
        ..
    } = first.result.unwrap()
    else {
        panic!("native put was not acknowledged");
    };
    let conditional = client
        .put_if_absent(b"a".to_vec(), b"forbidden".to_vec())
        .await
        .unwrap();
    assert!(matches!(
        conditional.result.unwrap(),
        OperationResult::Mutation { applied: false, .. }
    ));
    let exchanged = client
        .compare_exchange(
            b"a".to_vec(),
            RpcCompareCondition::Revision(revision),
            b"updated".to_vec(),
        )
        .await
        .unwrap();
    assert!(matches!(
        exchanged.result.unwrap(),
        OperationResult::Mutation { applied: true, .. }
    ));
    let stale = client
        .compare_exchange(
            b"a".to_vec(),
            RpcCompareCondition::Revision(revision),
            b"stale".to_vec(),
        )
        .await
        .unwrap();
    assert!(matches!(
        stale.result.unwrap(),
        OperationResult::Mutation { applied: false, .. }
    ));
    let put = client.put(b"b".to_vec(), b"deleted".to_vec()).await.unwrap();
    assert!(matches!(
        put.result.unwrap(),
        OperationResult::Mutation { applied: true, .. }
    ));
    let delete = client.delete(b"b".to_vec()).await.unwrap();
    assert!(matches!(
        delete.result.unwrap(),
        OperationResult::Mutation { applied: true, .. }
    ));
    verify(&client).await;
}

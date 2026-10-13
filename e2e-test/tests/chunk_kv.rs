// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_client::{ChunkKvRpcTransport, ChunkKvTransport};
use crowdb_protocol::chunk_kv::{
    BatchMutationItem, BatchMutationRequest, ChunkKvRpcErrorCode, ClientRequestId, Id128, MultiGetRequest,
    PartitionRouting, PointOperation, PointRequest, RequestRouting,
};

mod recovery;

#[tokio::test]
#[ignore = "requires the owned release Chunk-KV containers"]
async fn production_transport_calls_direct_owner_rpc() {
    let ips = crate::common::management::addresses(12);
    let endpoint = format!("{}:7402", ips[11]);

    let transport = ChunkKvRpcTransport::new(4, 1, 1);
    let response = transport
        .point(
            &endpoint,
            &PointRequest {
                routing: RequestRouting {
                    request_id: ClientRequestId {
                        client_instance_id: Id128 { high: 1, low: 2 },
                        client_sequence: 1,
                    },
                    map_revision: 1,
                    partition_id: Id128 { high: 3, low: 4 },
                    owner_epoch: 1,
                    min_journal_position: None,
                    deadline_ms: None,
                },
                operation: PointOperation::Get { key: b"key".to_vec() },
            },
        )
        .await
        .unwrap();
    assert_eq!(response.result.unwrap_err().code, ChunkKvRpcErrorCode::NotMyRange);

    let request_id = ClientRequestId {
        client_instance_id: Id128 { high: 1, low: 2 },
        client_sequence: 2,
    };
    let multi_get = transport
        .multi_get(
            &endpoint,
            &MultiGetRequest {
                routing: RequestRouting {
                    request_id,
                    map_revision: 1,
                    partition_id: Id128 { high: 3, low: 4 },
                    owner_epoch: 1,
                    min_journal_position: None,
                    deadline_ms: None,
                },
                keys: vec![b"key".to_vec()],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        multi_get.result.unwrap_err().code,
        ChunkKvRpcErrorCode::NotMyRange
    );

    let batch = transport
        .batch_mutate(
            &endpoint,
            &BatchMutationRequest {
                routing: PartitionRouting {
                    map_revision: 1,
                    partition_id: Id128 { high: 3, low: 4 },
                    owner_epoch: 1,
                    deadline_ms: None,
                },
                operations: vec![BatchMutationItem {
                    request_id,
                    operation: PointOperation::Delete { key: b"key".to_vec() },
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(batch.result.unwrap_err().code, ChunkKvRpcErrorCode::NotMyRange);
}

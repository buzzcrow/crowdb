// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crowdb_chunk_kv_client::{ChunkKvRpcTransport, ChunkKvRpcTransportConfig, ChunkKvTransport};
use crowdb_protocol::chunk_kv::{
    ChunkKvResponse, ChunkKvRpcErrorCode, ClientRequestId, Id128, PointOperation, PointRequest,
    RequestRouting, RpcFailure, ScanDirection, ScanRequest,
};
use crowdb_protocol::chunk_kv_wire::encode_point_response;
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{RdmaConfig, RpcServer};
use serde_json::Value;

#[test]
fn explicit_class_overrides_validate_before_connect_and_keep_service_defaults() {
    let transport = ChunkKvRpcTransport::new(2, 1, 1);
    let status: Value = serde_json::from_slice(&transport.transport_status_json().unwrap()).unwrap();
    assert_eq!(status["metadata"]["data_capacity"], 65_536);
    assert_eq!(status["aggregate"]["data_capacity"], 262_144);
    let invalid = ChunkKvRpcTransportConfig {
        metadata_rdma: Some(RdmaConfig {
            enabled: true,
            ..RdmaConfig::default()
        }),
        ..ChunkKvRpcTransportConfig::default()
    };
    assert!(invalid.validate().is_err());
    assert!(ChunkKvRpcTransport::with_config(2, 1, 1, &invalid).is_err());
    let aggregate = RdmaConfig::default()
        .with_control_data_capacity(1_048_576)
        .unwrap();
    let config = ChunkKvRpcTransportConfig {
        metadata_rdma: None,
        aggregate_rdma: Some(aggregate),
    };
    let encoded = serde_json::to_vec(&config).unwrap();
    assert_eq!(
        serde_json::from_slice::<ChunkKvRpcTransportConfig>(&encoded).unwrap(),
        config
    );
    let transport = ChunkKvRpcTransport::with_config(2, 1, 1, &config).unwrap();
    let status: Value = serde_json::from_slice(&transport.transport_status_json().unwrap()).unwrap();
    assert_eq!(status["aggregate"]["data_capacity"], 1_048_576);
    assert!(status["aggregate"]["transport"]["rdma"].is_null());
}

#[tokio::test]
async fn point_and_scan_use_distinct_pools_and_one_correlated_rpc_identity_sequence() {
    let server = RpcServer::new(None);
    server.configure_rdma(&RdmaConfig::default()).unwrap();
    server.listen("127.0.0.1", 0).unwrap();
    let observed = Arc::new(AtomicU64::new(0));
    for kind in [FBMsgType::EChunkKvPointRequest, FBMsgType::EChunkKvScanRequest] {
        let observed = observed.clone();
        server.register_reply_for_tests(u16::try_from(kind.0).unwrap(), move |request| {
            observed.fetch_or(1 << request.request_id, Ordering::Relaxed);
            let response = ChunkKvResponse {
                map_revision: 4,
                journal_position: None,
                result: Err(RpcFailure {
                    code: ChunkKvRpcErrorCode::NotMyRange,
                    message: "test owner".into(),
                    retry_after_ms: None,
                    latest_map_revision: None,
                    owner_hint: None,
                }),
            };
            let (bytes, offset) =
                encode_point_response(request.request_id, request.rpc_create_nano, &response);
            (bytes[offset..].to_vec(), Vec::new())
        });
    }
    server.start();
    let config = ChunkKvRpcTransportConfig {
        metadata_rdma: Some(RdmaConfig::default().with_control_data_capacity(65_536).unwrap()),
        aggregate_rdma: Some(RdmaConfig::default().with_control_data_capacity(262_144).unwrap()),
    };
    let transport = ChunkKvRpcTransport::with_config(2, 1, 1, &config).unwrap();
    let endpoint = format!("http://0.0.0.0:{}", server.port());
    let routing = RequestRouting {
        request_id: ClientRequestId {
            client_instance_id: Id128 { high: 1, low: 2 },
            client_sequence: 3,
        },
        map_revision: 4,
        partition_id: Id128 { high: 5, low: 6 },
        owner_epoch: 7,
        min_journal_position: None,
        deadline_ms: None,
    };
    let point = PointRequest {
        routing: routing.clone(),
        operation: PointOperation::Get {
            key: b"object".to_vec(),
        },
    };
    let scan = ScanRequest {
        routing,
        start: Some(Vec::new()),
        end: None,
        direction: ScanDirection::Forward,
        limit: 10,
        continuation: None,
    };
    let (point, scan) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            transport.point(&endpoint, &point),
            transport.scan(&endpoint, &scan)
        )
    })
    .await
    .expect("class RPC responses did not complete");
    for response in [point.unwrap(), scan.unwrap()] {
        assert_eq!(response.result.unwrap_err().code, ChunkKvRpcErrorCode::NotMyRange);
    }
    assert_eq!(observed.load(Ordering::Relaxed), (1 << 1) | (1 << 2));
    let status: Value = serde_json::from_slice(&transport.transport_status_json().unwrap()).unwrap();
    assert_eq!(status["metadata"]["connection_count"], 1);
    assert_eq!(status["aggregate"]["connection_count"], 1);
    let connections: Value =
        serde_json::from_slice(&transport.connection_status_json(&endpoint).unwrap()).unwrap();
    assert_eq!(connections["metadata"]["path"], "tcp");
    assert_eq!(connections["aggregate"]["path"], "tcp");
    server.stop();
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs};
use crowdb_rpc_ffi::{Buffer, RdmaConfig, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;
use serde_json::Value;

#[test]
fn explicit_tcp_configuration_is_typed_and_cold() {
    let server = RpcServer::new(None);
    let config = RdmaConfig::default();
    assert_eq!(config.sq_depth, 1024);
    server.configure_rdma(&config).unwrap();
    let status: Value = serde_json::from_slice(&server.transport_status_json().unwrap()).unwrap();
    assert_eq!(status["schema_version"], 1);
    assert_eq!(status["allow_tcp_fallback"], true);
    assert!(status["rdma"].is_null());
    assert!(status["cpu_user_ns"].is_u64());
    server.listen("127.0.0.1", 0).unwrap();
    assert!(server.configure_rdma(&config).is_err());
    server.start();
    assert!(server.configure_rdma(&config).is_err());
    server.stop();
}

#[test]
fn invalid_queue_bounds_fail_before_start() {
    let server = RpcServer::new(None);
    let config = RdmaConfig {
        cq_count: 0,
        ..RdmaConfig::default()
    };
    assert!(server.configure_rdma(&config).is_err());
    let config = RdmaConfig {
        send_queue_capacity: 3,
        ..RdmaConfig::default()
    };
    assert!(server.configure_rdma(&config).is_err());
}

#[tokio::test]
async fn status_control_rpc_returns_owned_versioned_json() {
    let server = RpcServer::new(None);
    server.configure_rdma(&RdmaConfig::default()).unwrap();
    server.listen("127.0.0.1", 0).unwrap();
    server.register_status_handler(101);
    server.start();
    let connection = server.connect("127.0.0.1", server.port()).unwrap();
    let client = RpcClient::new();
    client.attach(&connection);
    let mut builder = FlatBufferBuilder::new();
    let request = ConnectionPingRequest::create(
        &mut builder,
        &ConnectionPingRequestArgs {
            id: 137,
            rpc_create_nano: 0,
        },
    );
    builder.finish(request, None);
    let reply = client
        .call(
            &server,
            &connection,
            137,
            Buffer::from_bytes(builder.finished_data()),
            None,
            101,
        )
        .unwrap()
        .await
        .unwrap();
    let status: Value = serde_json::from_slice(reply.data.as_ref().unwrap().as_slice()).unwrap();
    assert_eq!(reply.request_id, 137);
    assert_eq!(status["schema_version"], 1);
    assert!(status["rdma"].is_null());
    let connection_status: Value =
        serde_json::from_slice(&connection.transport_status_json().unwrap()).unwrap();
    assert_eq!(connection_status["path"], "tcp");
    server.stop();
    let closed: Value = serde_json::from_slice(&connection.transport_status_json().unwrap()).unwrap();
    assert_eq!(closed["open"], false);
}

#[test]
fn hardware_qpair_status_retains_immutable_provider_values_after_close() {
    if std::env::var_os("CROWDB_RDMA_E2E").is_none() {
        return;
    }
    let address = std::env::var("CROWDB_RDMA_E2E_LISTEN_ADDR").unwrap();
    let config = RdmaConfig {
        enabled: true,
        allow_tcp_fallback: false,
        listen_enabled: true,
        device: std::env::var("CROWDB_RDMA_DEVICE").unwrap_or_default(),
        source_address: address.clone(),
        wire_slot_capacity: 1024,
        sq_depth: 8,
        rq_depth: 8,
        posted_receive_target: 8,
        cq_depth: 16,
        send_arena_bytes: 8192,
        receive_arena_bytes: 8192,
        ..RdmaConfig::default()
    };
    let server = RpcServer::new(None);
    server.configure_rdma(&config).unwrap();
    server.listen(&address, 0).unwrap();
    server.start();
    let client = RpcServer::new(None);
    client
        .configure_rdma(&RdmaConfig {
            listen_enabled: false,
            ..config
        })
        .unwrap();
    client.listen("127.0.0.1", 0).unwrap();
    client.start();
    let connection = client.connect(&address, server.port()).unwrap();
    let info: Value = serde_json::from_slice(&connection.transport_status_json().unwrap()).unwrap();
    assert_eq!(info["path"], "rdma");
    assert!(info["info"]["qpair_number"].as_u64().unwrap() > 0);
    assert!(info["info"]["effective_sq_depth"].as_u64().unwrap() >= 8);
    assert_eq!(info["info"]["peer_receive_slot_capacity"], 1024);
    client.stop();
    server.stop();
    let closed: Value = serde_json::from_slice(&connection.transport_status_json().unwrap()).unwrap();
    assert_eq!(closed["open"], false);
    assert_eq!(closed["info"], info["info"]);
}

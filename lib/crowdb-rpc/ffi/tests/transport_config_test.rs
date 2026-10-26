// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs};
use crowdb_rpc_ffi::{Buffer, RdmaConfig, RdmaWireLayout, RpcClient, RpcServer};
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

#[test]
fn send_post_batch_and_software_window_are_independent_bounded_settings() {
    let server = RpcServer::new(None);
    let mut config = RdmaConfig {
        send_batch_size: 16,
        receive_batch_size: 8,
        send_window_size: 4,
        ..RdmaConfig::default()
    };
    config.send_window_bytes = u64::from(config.wire_slot_capacity);
    server.configure_rdma(&config).unwrap();
    let encoded = serde_json::to_value(&config).unwrap();
    assert_eq!(serde_json::from_value::<RdmaConfig>(encoded).unwrap(), config);
    config.send_window_size = config.sq_depth + 1;
    assert!(server.configure_rdma(&config).is_err());
    config.send_window_size = 4;
    config.send_window_bytes -= 1;
    assert!(server.configure_rdma(&config).is_err());
    config.send_window_bytes = 0;
    config.receive_batch_size = 0;
    assert!(server.configure_rdma(&config).is_err());
    config.receive_batch_size = 8;
    config.send_batch_size = 0;
    assert!(server.configure_rdma(&config).is_err());
}

#[test]
fn logical_control_limits_are_typed_and_bounded_in_both_directions() {
    let server = RpcServer::new(None);
    let mut config = RdmaConfig::default();
    assert_eq!(config.max_control_message_bytes, 32 << 20);
    assert_eq!(config.control_send_budget_bytes, 64 << 20);
    assert_eq!(config.control_receive_budget_bytes, 64 << 20);
    config.max_control_message_bytes = 65536;
    config.control_send_budget_bytes = 65535;
    assert!(server.configure_rdma(&config).is_err());
    config.control_send_budget_bytes = 65536;
    config.control_receive_budget_bytes = 65535;
    assert!(server.configure_rdma(&config).is_err());
    config.control_receive_budget_bytes = 65536;
    config.control_assembly_timeout_ms = 0;
    assert!(server.configure_rdma(&config).is_err());
    config.control_assembly_timeout_ms = 125;
    server.configure_rdma(&config).unwrap();
}

#[test]
fn serialized_control_data_settings_derive_the_selected_sq_and_rq_windows() {
    for capacity in [65_536, 262_144, 1_048_576] {
        let config = RdmaConfig {
            sq_depth: 8,
            rq_depth: 4,
            posted_receive_target: 4,
            cq_depth: 16,
            ..RdmaConfig::default()
        }
        .with_control_data_capacity(capacity)
        .unwrap();
        assert_eq!(config.control_data_capacity(), Some(capacity));
        assert_eq!(config.wire_slot_capacity, capacity + 1024);
        assert_eq!(config.send_arena_bytes, u64::from(capacity) * 8);
        assert_eq!(config.receive_arena_bytes, u64::from(capacity + 1024) * 4);
        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(encoded["wire_layout"], "control_data");
        assert_eq!(serde_json::from_value::<RdmaConfig>(encoded).unwrap(), config);
        RpcServer::new(None).configure_rdma(&config).unwrap();
    }
    assert!(RdmaConfig::default().with_control_data_capacity(4096).is_err());
    assert!(RdmaConfig {
        aggregation_enabled: true,
        ..RdmaConfig::default()
    }
    .with_control_data_capacity(65_536)
    .is_ok());
    assert!(serde_json::from_str::<RdmaConfig>(r#"{"receive_queue_length": 1024}"#).is_err());
}

#[test]
fn service_layout_defaults_validate_capacity_and_preserve_explicit_tcp() {
    assert!(RpcServer::with_rdma_layout(None, 1, 1, RdmaWireLayout::ControlData, 4096).is_err());
    let server = RpcServer::with_rdma_layout(None, 1, 1, RdmaWireLayout::ControlData, 1 << 20).unwrap();
    server.configure_rdma(&RdmaConfig::default()).unwrap();
    let status: Value = serde_json::from_slice(&server.transport_status_json().unwrap()).unwrap();
    assert!(status["rdma"].is_null());
}

#[test]
fn typed_control_data_layout_accounts_for_both_regions_with_optional_packing() {
    let server = RpcServer::new(None);
    let mut config = RdmaConfig {
        wire_layout: RdmaWireLayout::ControlData,
        wire_slot_capacity: (1 << 20) + 1024,
        receive_arena_bytes: 1024 * ((1 << 20) + 1024),
        ..RdmaConfig::default()
    };
    server.configure_rdma(&config).unwrap();
    config.aggregation_enabled = true;
    server.configure_rdma(&config).unwrap();
    config.aggregation_enabled = false;
    config.receive_arena_bytes -= 1;
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
    assert!(!connection.is_rdma());
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
    assert!(connection.is_rdma());
    assert!(info["info"]["qpair_number"].as_u64().unwrap() > 0);
    assert!(info["info"]["effective_sq_depth"].as_u64().unwrap() >= 8);
    assert_eq!(info["info"]["peer_receive_slot_capacity"], 1024);
    client.stop();
    server.stop();
    let closed: Value = serde_json::from_slice(&connection.transport_status_json().unwrap()).unwrap();
    assert_eq!(closed["open"], false);
    assert_eq!(closed["info"], info["info"]);
    assert!(connection.is_rdma());
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use crowdb_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs};
use crowdb_rpc_ffi::{Buffer, OwnedPayload, RdmaConfig, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;

fn control(id: u64) -> Buffer {
    let mut builder = FlatBufferBuilder::new();
    let request = ConnectionPingRequest::create(
        &mut builder,
        &ConnectionPingRequestArgs {
            id,
            rpc_create_nano: 0,
        },
    );
    builder.finish(request, None);
    Buffer::from_bytes(builder.finished_data())
}

#[tokio::test]
async fn retained_rdma_payload_applies_receive_backpressure_and_survives_shutdown() {
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
        sq_depth: 4,
        rq_depth: 2,
        posted_receive_target: 2,
        cq_depth: 16,
        ..RdmaConfig::default()
    }
    .with_control_data_capacity(65_536)
    .unwrap();
    let server = RpcServer::new(None);
    server.configure_rdma(&config).unwrap();
    server.register_echo_handler(100);
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
    assert!(connection.is_rdma());
    let caller = RpcClient::new();
    caller.attach(&connection);
    let mut held = Vec::new();
    for id in 1..=2 {
        let reply = tokio::time::timeout(
            Duration::from_secs(2),
            caller
                .call(
                    &client,
                    &connection,
                    id,
                    control(id),
                    Some(Buffer::from_vec(vec![id as u8; 8192])),
                    100,
                )
                .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        let data = OwnedPayload::from_buffer(reply.data.unwrap());
        assert!(data.allocation().unwrap().capacity() >= 65_536);
        assert_ne!(data.allocation().unwrap().native_identity(), Some(0));
        assert_eq!(data.native_metadata().unwrap().request_id, id);
        held.push(data);
    }
    let mut pending = caller
        .call(
            &client,
            &connection,
            3,
            control(3),
            Some(Buffer::from_vec(vec![3; 8192])),
            100,
        )
        .unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut pending)
        .await
        .is_err());
    drop(held.remove(0));
    let reply = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap();
    let data = OwnedPayload::from_buffer(reply.data.unwrap());
    drop(connection);
    client.stop();
    server.stop();
    drop(client);
    drop(server);
    assert_eq!(held[0].as_ref(), &[2; 8192]);
    assert_eq!(data.as_ref(), &[3; 8192]);
    assert!(!data.allocation().unwrap().bytes().is_empty());
}

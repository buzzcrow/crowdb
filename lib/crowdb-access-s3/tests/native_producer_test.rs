// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::poll_fn;
use std::sync::Arc;
use std::time::Duration;

use crowdb_access_s3::native_buffer::NativeBodyAllocator;
use crowdb_chunk_client::FramedWriteBuffer;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs};
use crowdb_protocol::frame::{parse_frame, FrameMagic, MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES};
use crowdb_rpc_ffi::{
    Buffer, BufferChain, BufferPool, ProducerBufferAllocator, RdmaConfig, RpcClient, RpcServer,
};
use flatbuffers::FlatBufferBuilder;
use hyper::body::Http1BodyReceiveProvider;

#[tokio::test]
async fn native_credit_returned_by_another_producer_wakes_http_intake() {
    let pool = BufferPool::new(1);
    let server = Arc::new(RpcServer::new(Some(&pool)));
    server.configure_rdma(&RdmaConfig::default()).unwrap();
    let source = ProducerBufferAllocator::new(server, false);
    let held = source.allocate(64 * 1024).unwrap();
    let allocator = NativeBodyAllocator::new(2 * MAX_FRAME_BYTES, MAX_FRAME_BYTES)
        .unwrap()
        .with_producer_allocator(source)
        .unwrap();
    let receiver = allocator.object_receiver();
    let mut waiting = Box::pin(poll_fn(|cx| receiver.poll_next_buffer(cx, 13)));
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut waiting)
        .await
        .is_err());
    assert_eq!(allocator.retained_bytes(), 0);
    assert_eq!(allocator.allocation_count(), 0);
    drop(held);
    let mut region = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    for byte in &mut region.spare_capacity_mut()[..13] {
        byte.write(7);
    }
    region.advance(13).unwrap();
    let payload = receiver.on_data_ready(region).unwrap();
    assert!(payload.iter().all(|byte| *byte == 7));
    assert!(allocator.metrics_snapshot().backpressure_events > 0);
    drop(payload);
    drop(receiver);
    assert_eq!(allocator.retained_bytes(), 0);
}

#[tokio::test]
async fn framed_native_views_retain_storage_and_keep_original_payload_addresses() {
    let pool = BufferPool::new(1);
    let server = Arc::new(RpcServer::new(Some(&pool)));
    server.configure_rdma(&RdmaConfig::default()).unwrap();
    let source = ProducerBufferAllocator::new(server, false);
    let allocator = NativeBodyAllocator::new(MAX_FRAME_BYTES, MAX_FRAME_BYTES)
        .unwrap()
        .with_producer_allocator(source.clone())
        .unwrap();
    let receiver = allocator.object_receiver();
    receiver.enable_owner_handoff();
    let mut region = poll_fn(|cx| receiver.poll_next_buffer(cx, MAX_FRAME_PAYLOAD_BYTES))
        .await
        .unwrap();
    let pointer = region.spare_capacity_mut().as_ptr().cast::<u8>();
    for byte in &mut region.spare_capacity_mut()[..MAX_FRAME_PAYLOAD_BYTES] {
        byte.write(0x5a);
    }
    region.advance(MAX_FRAME_PAYLOAD_BYTES).unwrap();
    let payload = receiver.on_data_ready(region).unwrap();
    let mut owner = receiver.take_ready_owner().unwrap();
    owner.prepare_frames(FrameMagic::RepoLargeV1, 123).unwrap();
    let chunk = ChunkId { high: 17, low: 31 };
    let range = owner
        .finalize_frame(0, FrameMagic::RepoLargeV1, chunk, 123)
        .unwrap();
    let views = owner.views(range).unwrap();
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].as_ptr().wrapping_add(14), pointer);
    assert!(parse_frame(&views[0], chunk)
        .unwrap()
        .payload
        .iter()
        .all(|byte| *byte == 0x5a));
    assert!(source.allocate(32).is_none());
    let mirror = views[0].clone();
    drop(views);
    drop(owner);
    drop(receiver);
    drop(payload);
    assert!(source.allocate(32).is_none());
    assert_eq!(mirror.as_ptr().wrapping_add(14), pointer);
    drop(mirror);
    assert_eq!(allocator.retained_bytes(), 0);
    assert!(source.allocate(32).is_some());
}

#[tokio::test]
async fn hardware_http_framed_owner_uses_direct_payload_send() {
    if std::env::var_os("CROWDB_RDMA_E2E").is_none() {
        return;
    }
    let address = std::env::var("CROWDB_RDMA_E2E_LISTEN_ADDR").unwrap();
    let config = RdmaConfig {
        enabled: true,
        listen_enabled: true,
        allow_tcp_fallback: false,
        wire_layout: crowdb_rpc_ffi::RdmaWireLayout::ControlData,
        wire_slot_capacity: (1 << 20) + 1024,
        sq_depth: 4,
        rq_depth: 4,
        posted_receive_target: 4,
        cq_depth: 8,
        send_arena_bytes: 4 << 20,
        receive_arena_bytes: 4 * ((1 << 20) + 1024),
        device: std::env::var("CROWDB_RDMA_DEVICE").unwrap_or_default(),
        source_address: address.clone(),
        ..RdmaConfig::default()
    };
    let server = RpcServer::new(None);
    server.configure_rdma(&config).unwrap();
    server.listen(&address, 0).unwrap();
    server.register_echo_handler(100);
    server.start();
    let client = Arc::new(RpcServer::new(None));
    client
        .configure_rdma(&RdmaConfig {
            listen_enabled: false,
            ..config
        })
        .unwrap();
    client.start();
    let connection = client.connect(&address, server.port()).unwrap();
    let rpc = RpcClient::new();
    rpc.attach(&connection);
    let allocator = NativeBodyAllocator::new(1 << 20, 1 << 20)
        .unwrap()
        .with_producer_allocator(ProducerBufferAllocator::new(Arc::clone(&client), true))
        .unwrap();
    let receiver = allocator.object_receiver();
    receiver.enable_owner_handoff();
    for index in 0..16 {
        let mut region = poll_fn(|cx| receiver.poll_next_buffer(cx, MAX_FRAME_PAYLOAD_BYTES))
            .await
            .unwrap();
        for byte in &mut region.spare_capacity_mut()[..MAX_FRAME_PAYLOAD_BYTES] {
            byte.write(index);
        }
        region.advance(MAX_FRAME_PAYLOAD_BYTES).unwrap();
        drop(receiver.on_data_ready(region).unwrap());
    }
    let mut owner = receiver.take_ready_owner().unwrap();
    owner.prepare_frames(FrameMagic::RepoLargeV1, 123).unwrap();
    let chunk = ChunkId { high: 11, low: 19 };
    for index in 0..16 {
        owner
            .finalize_frame(index, FrameMagic::RepoLargeV1, chunk, 123)
            .unwrap();
    }
    let views = owner.views(0..(1 << 20)).unwrap();
    let expected = views[0].clone();
    let mut builder = FlatBufferBuilder::new();
    let request = ConnectionPingRequest::create(
        &mut builder,
        &ConnectionPingRequestArgs {
            id: 91,
            rpc_create_nano: 0,
        },
    );
    builder.finish(request, None);
    let response = rpc
        .call_chain(
            &client,
            &connection,
            91,
            Buffer::from_bytes(builder.finished_data()),
            BufferChain::from_owned_bytes(views).unwrap(),
            100,
        )
        .unwrap()
        .await
        .unwrap();
    assert_eq!(response.data.unwrap().as_slice(), expected.as_ref());
    let status: serde_json::Value = serde_json::from_slice(&client.transport_status_json().unwrap()).unwrap();
    assert_eq!(status["rdma"]["direct_payload_bytes"], 1 << 20);
    assert_eq!(status["rdma"]["copied_payload_bytes"], 0);
    assert_eq!(status["rdma"]["fallback_count"], 0);
    assert_eq!(status["rdma"]["protocol_errors"], 0);
    assert_eq!(allocator.prefix_copy_bytes(), 0);
    client.stop();
    server.stop();
}

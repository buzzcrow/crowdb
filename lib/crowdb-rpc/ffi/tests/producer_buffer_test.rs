// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_rpc_ffi::{BufferPool, RdmaConfig, RpcServer};

#[test]
fn producer_admission_stays_bounded_and_bytes_keep_the_original_allocation() {
    let pool = BufferPool::new(1);
    let server = RpcServer::new(Some(&pool));
    server.configure_rdma(&RdmaConfig::default()).unwrap();
    assert!(server.allocate_producer_buffer(512, true).is_none());
    assert!(server.allocate_producer_buffer(0, false).is_none());
    assert!(server.allocate_producer_buffer((1 << 20) + 1, false).is_none());
    let mut writable = server.allocate_producer_buffer(512, false).unwrap();
    assert_eq!(writable.registration_id(), 0);
    assert!(server.allocate_producer_buffer(1, false).is_none());
    let pointer = writable.uninit_bytes_mut().as_ptr().cast::<u8>();
    let buffer = writable.write(b"initialized producer data").unwrap();
    assert_eq!(buffer.bytes().as_ptr(), pointer);
    let bytes = bytes::Bytes::from_owner(buffer);
    let tail = bytes.slice(12..);
    drop(bytes);
    assert!(server.allocate_producer_buffer(1, false).is_none());
    drop(server);
    drop(pool);
    assert_eq!(tail, b"producer data"[..]);
    drop(tail);
}

#[test]
fn oversize_initialization_releases_producer_admission() {
    let pool = BufferPool::new(1);
    let server = RpcServer::new(Some(&pool));
    server.configure_rdma(&RdmaConfig::default()).unwrap();
    let buffer = server.allocate_producer_buffer(1, false).unwrap();
    assert!(buffer.write(b"too large").is_err());
    assert!(server.allocate_producer_buffer(1, false).is_some());
}

#[test]
fn generated_prefix_keeps_storage_and_freezes_only_initialized_bytes() {
    let pool = BufferPool::new(1);
    let server = RpcServer::new(Some(&pool));
    server.configure_rdma(&RdmaConfig::default()).unwrap();
    let mut writable = server.allocate_producer_buffer(512, false).unwrap();
    let pointer = writable.uninit_bytes_mut().as_ptr().cast::<u8>();
    let buffer = writable
        .initialize_with(127, 0xAB, |bytes| {
            assert!(bytes.iter().all(|byte| *byte == 0xAB));
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte = u8::try_from(index).unwrap();
            }
        })
        .unwrap();
    assert_eq!(buffer.bytes().as_ptr(), pointer);
    assert_eq!(buffer.bytes(), (0_u8..127).collect::<Vec<_>>());
    assert!(server.allocate_producer_buffer(1, false).is_none());
    drop(server);
    assert_eq!(buffer.bytes().len(), 127);
    drop(buffer);
    assert!(pool.alloc_buffer(1).is_some());
}

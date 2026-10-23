// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_protocol::frame::{MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES};
use crowdb_rpc_ffi::{ProducerBufferAllocator, RpcServer};
use crowdb_sdk_native::{Error, RuntimeLimits, RuntimeResources};

fn resources() -> Arc<RuntimeResources> {
    RuntimeResources::new(RuntimeLimits {
        producer_bytes: 2 * MAX_FRAME_BYTES,
        ..RuntimeLimits::default()
    })
    .unwrap()
}

#[test]
fn immutable_framed_payloads_keep_actual_capacity_until_the_final_external_view() {
    let resources = resources();
    let allocator = ProducerBufferAllocator::new(Arc::new(RpcServer::new(None)), false);
    let writable = resources
        .allocate(&allocator, u32::try_from(2 * MAX_FRAME_BYTES).unwrap())
        .unwrap();
    assert_eq!(writable.framed_capacity(), 2 * MAX_FRAME_PAYLOAD_BYTES);
    let mut addresses = Vec::new();
    let frozen = writable
        .initialize_framed(MAX_FRAME_PAYLOAD_BYTES + 7, 0x32, |offset, bytes| {
            addresses.push(bytes.as_ptr() as usize);
            bytes[0] = u8::from(offset != 0);
        })
        .unwrap();
    let mut views = frozen.payload_views();
    assert_eq!(views[0].as_ptr() as usize, addresses[0]);
    assert_eq!(views[1].as_ptr() as usize, addresses[1]);
    assert_eq!(views[0][0], 0);
    assert_eq!(views[1].as_ref(), &[1, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32]);
    assert_eq!(resources.status().producer_bytes, 2 * MAX_FRAME_BYTES);
    resources.shutdown();
    drop(allocator);
    drop(frozen);
    assert_eq!(views.pop().unwrap()[0], 1);
    assert_eq!(resources.status().producer_bytes, 2 * MAX_FRAME_BYTES);
    drop(views);
    assert_eq!(resources.status().producer_bytes, 0);
}

#[test]
fn invalid_lengths_and_initializer_panics_release_native_storage_before_returning_credit() {
    let resources = resources();
    let allocator = ProducerBufferAllocator::new(Arc::new(RpcServer::new(None)), false);
    for length in [0, MAX_FRAME_PAYLOAD_BYTES + 1] {
        let writable = resources
            .allocate(&allocator, u32::try_from(MAX_FRAME_BYTES).unwrap())
            .unwrap();
        assert!(matches!(
            writable.initialize_framed(length, 0, |_, _| {}),
            Err(Error::BufferRange)
        ));
        assert_eq!(resources.status().producer_bytes, 0);
    }
    let writable = resources
        .allocate(&allocator, u32::try_from(MAX_FRAME_BYTES).unwrap())
        .unwrap();
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = writable.initialize_framed(7, 0, |_, _| panic!("test initializer panic"));
    }));
    assert!(caught.is_err());
    assert_eq!(resources.status().producer_bytes, 0);
    assert!(resources
        .allocate(&allocator, u32::try_from(MAX_FRAME_BYTES).unwrap())
        .is_ok());
}

#[test]
fn multi_allocation_producers_release_partial_work_and_keep_external_views_charged() {
    let allocator = ProducerBufferAllocator::new(Arc::new(RpcServer::new(None)), false);
    let resources = RuntimeResources::new(RuntimeLimits {
        producer_bytes: (1 << 20) + MAX_FRAME_BYTES,
        ..RuntimeLimits::default()
    })
    .unwrap();
    let large = vec![0x57; 2 << 20];
    assert!(resources.prepare_framed(&allocator, &large).is_err());
    assert_eq!(resources.status().producer_bytes, 0);
    let length = 16 * MAX_FRAME_PAYLOAD_BYTES + 7;
    let frozen = resources.prepare_framed(&allocator, &large[..length]).unwrap();
    assert_eq!(frozen.length(), length as u64);
    let views = frozen.payload_views();
    assert_eq!(views.len(), 17);
    assert!(views.iter().flatten().all(|byte| *byte == 0x57));
    assert_eq!(views.last().unwrap().len(), 7);
    drop(frozen);
    assert_eq!(resources.status().producer_bytes, (1 << 20) + MAX_FRAME_BYTES);
    drop(views);
    assert_eq!(resources.status().producer_bytes, 0);
}

#[test]
fn concatenation_rejects_partial_interior_frames_and_foreign_budgets() {
    use crowdb_sdk_native::buffer::FrozenObjectBuffer;
    let allocator = ProducerBufferAllocator::new(Arc::new(RpcServer::new(None)), false);
    let first = resources();
    let second = resources();
    let full = vec![0; MAX_FRAME_PAYLOAD_BYTES];
    let partial = first.prepare_framed(&allocator, b"short").unwrap();
    let next = first.prepare_framed(&allocator, &full).unwrap();
    assert!(FrozenObjectBuffer::concatenate(vec![partial, next]).is_err());
    assert_eq!(first.status().producer_bytes, 0);
    let one = first.prepare_framed(&allocator, &full).unwrap();
    let two = second.prepare_framed(&allocator, b"tail").unwrap();
    assert!(FrozenObjectBuffer::concatenate(vec![one, two]).is_err());
    assert_eq!(first.status().producer_bytes, 0);
    assert_eq!(second.status().producer_bytes, 0);
}

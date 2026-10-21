// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_rpc_ffi::{Buffer, BufferChain, BufferChainError, BufferOwnership, BufferPool};

#[test]
fn lease_metadata_includes_registration_and_owner_state() {
    let pool = BufferPool::new(1);
    let mut buffer = pool.alloc_buffer(32).expect("pool allocation");
    buffer.set_lease_metadata(
        crowdb_rpc_ffi::BufferLeaseMetadata {
            request_id: 7,
            stream_id: 9,
            sequence: 11,
            logical_offset: 13,
            capacity: 0,
            registration_id: 0,
            ownership: BufferOwnership::Available,
        },
        42,
    );
    let metadata = buffer.lease_metadata();
    assert_eq!(metadata.request_id, 7);
    assert_eq!(metadata.stream_id, 9);
    assert_eq!(metadata.sequence, 11);
    assert_eq!(metadata.logical_offset, 13);
    assert_eq!(metadata.registration_id, 42);
    assert_eq!(metadata.ownership, BufferOwnership::Available);
    assert!(buffer.transition(BufferOwnership::Available, BufferOwnership::CallerOwned));
    assert_eq!(buffer.ownership(), BufferOwnership::CallerOwned);
}

#[test]
fn detached_read_buffer_survives_pool_and_keeps_pointer() {
    let pool = BufferPool::new(1);
    let mut buffer = pool.alloc_buffer(4).expect("pool allocation");
    buffer.write(b"read");
    let original = buffer.bytes().as_ptr();
    assert!(buffer.detach());
    drop(pool);

    let bytes = Bytes::from_owner(buffer);
    assert_eq!(bytes.as_ptr(), original);
    assert_eq!(&bytes[..], b"read");
}

#[test]
fn owned_bytes_buffer_keeps_the_original_allocation() {
    let bytes = Bytes::from(vec![0x5a; 4096]);
    let original = bytes.as_ptr();
    let buffer = Buffer::from_owned_bytes(bytes);

    assert_eq!(buffer.bytes().as_ptr(), original);
    assert_eq!(buffer.bytes(), &[0x5a; 4096]);
}

#[test]
fn vec_buffer_keeps_the_original_allocation() {
    let data = vec![0xa5; 4096];
    let original = data.as_ptr();
    let buffer = Buffer::from_vec(data);

    assert_eq!(buffer.bytes().as_ptr(), original);
    assert_eq!(buffer.bytes(), &[0xa5; 4096]);
}

#[test]
fn buffer_chain_retains_views_and_enforces_native_limit() {
    let owner = Bytes::from_static(b"abcdefghijklmnop");
    let first = owner.slice(1..5);
    let second = owner.slice(8..13);
    let first_ptr = first.as_ptr();
    let second_ptr = second.as_ptr();
    let chain = BufferChain::from_owned_bytes([first, second]).expect("valid chain");

    assert_eq!(chain.view_count(), 2);
    assert_eq!(chain.len(), 9);
    assert!(!chain.is_empty());
    assert_eq!(owner.slice(1..5).as_ptr(), first_ptr);
    assert_eq!(owner.slice(8..13).as_ptr(), second_ptr);

    assert_eq!(
        BufferChain::from_owned_bytes(Vec::<Bytes>::new()).unwrap_err(),
        BufferChainError::Empty
    );
    assert_eq!(
        BufferChain::from_owned_bytes([Bytes::new()]).unwrap_err(),
        BufferChainError::EmptyView
    );

    let too_many = BufferChain::from_owned_bytes((0..64).map(|_| Bytes::from_static(b"x")))
        .expect_err("native descriptor bound must be enforced");
    assert!(matches!(
        too_many,
        BufferChainError::TooManyViews {
            maximum: 16,
            actual: 64
        }
    ));
}

#[test]
fn discarded_completion_releases_both_response_owners() {
    use crowdb_rpc_ffi::{noop_completion, sys};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct TestOwner(Arc<AtomicUsize>);
    impl AsRef<[u8]> for TestOwner {
        fn as_ref(&self) -> &[u8] {
            b"response"
        }
    }
    impl Drop for TestOwner {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Release);
        }
    }
    for status in [sys::CROWDB_RPC_OK, sys::CROWDB_RPC_ERR_CONN_CLOSED] {
        let released = Arc::new(AtomicUsize::new(0));
        let make_buffer = || Buffer::from_owned_bytes(Bytes::from_owner(TestOwner(Arc::clone(&released))));
        let control = make_buffer().into_raw();
        let data = make_buffer().into_raw();
        // Both live handles transfer to the completion once; it does not use context.
        unsafe {
            noop_completion().unwrap()(1, control, data, status, std::ptr::null_mut());
        }
        assert_eq!(released.load(Ordering::Acquire), 2);
    }
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_rpc_ffi::{Buffer, OwnedPayload};

#[test]
fn payload_slices_keep_one_exact_capacity_owner_and_original_addresses() {
    let mut source = Vec::with_capacity(1024);
    source.extend_from_slice(b"0123456789");
    let pointer = source.as_ptr();
    let payload = OwnedPayload::from(source);
    assert_eq!(payload.as_ptr(), pointer);
    assert_eq!(payload.allocation().unwrap().capacity(), 1024);
    let first = payload.slice(2..8);
    let nested = first.slice(1..4);
    assert_eq!(nested.allocation_offset(), 3);
    assert!(Arc::ptr_eq(
        payload.allocation().unwrap(),
        nested.allocation().unwrap()
    ));
    assert_eq!(nested.as_ptr(), payload.as_ref()[3..].as_ptr());
    drop(payload);
    drop(first);
    assert_eq!(nested.as_ref(), b"345");
    assert_eq!(nested.allocation().unwrap().bytes().as_ref(), b"0123456789");
}

#[test]
fn external_bytes_keep_storage_but_do_not_invent_an_allocation_capacity() {
    let bytes = Bytes::from(vec![7; 16]);
    let pointer = bytes.as_ptr();
    let payload = OwnedPayload::from_buffer(Buffer::from_owned_bytes(bytes));
    assert_eq!(payload.as_ptr(), pointer);
    assert!(payload.allocation().is_none());
    assert!(payload.slice(4..).allocation().is_none());
    assert_eq!(payload.len(), 16);
}

#[test]
fn adding_an_owner_keeps_the_allocation_and_its_view_offset() {
    struct TestOwner(Bytes);
    impl AsRef<[u8]> for TestOwner {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }
    let payload = OwnedPayload::from(vec![9; 16]).slice(4..12);
    let allocation = Arc::clone(payload.allocation().unwrap());
    let payload = payload.with_owner(|bytes| Bytes::from_owner(TestOwner(bytes)));
    assert_eq!(payload.allocation_offset(), 4);
    assert!(Arc::ptr_eq(&allocation, payload.allocation().unwrap()));
    assert_eq!(payload.into_bytes().as_ref(), &[9; 8]);
}

// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_rpc_ffi::{Buffer, BufferChain, BufferChainError};

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

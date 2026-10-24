// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{atomic::Ordering, Arc};

use crowdb_chunk_kv_client::{ClientError, MultiScanContinuation, MultiScanOutcome, MultiScanRequest};
use crowdb_protocol::chunk_kv::{RpcValue, ScanDirection};

#[path = "common/observed_scan.rs"]
mod fixture;
use fixture::{client, TestTransport};

fn request(max_bytes: usize) -> MultiScanRequest {
    MultiScanRequest {
        start: Some(b"m".to_vec()),
        end: Some(b"z".to_vec()),
        direction: ScanDirection::Forward,
        max_items: 1,
        max_bytes,
        continuation: None,
    }
}

#[tokio::test]
async fn exact_aggregate_and_client_caps_return_the_complete_row_with_one_rpc() {
    let transport = Arc::new(TestTransport::row(b"n", 7));
    let client = client(transport.clone(), 8);
    let MultiScanOutcome::Page(page) = client.scan_with_oversized_key(request(8)).await.unwrap() else {
        panic!("an exact-cap row fits");
    };
    assert_eq!(page.items, transport.items);
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport.bounded_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn either_aggregate_or_client_cap_returns_only_a_validated_oversized_key() {
    for (aggregate, response_cap) in [(7, 8), (8, 7)] {
        let transport = Arc::new(TestTransport::row(b"n", 7));
        let client = client(transport.clone(), response_cap);
        assert_eq!(
            client.scan_with_oversized_key(request(aggregate)).await.unwrap(),
            MultiScanOutcome::OversizedRow { key: b"n".to_vec() }
        );
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport.bounded_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn binary_keys_are_retained_exactly_without_changing_the_requested_interval() {
    let key = b"n\0\xff";
    let transport = Arc::new(TestTransport::row(key, 8));
    let client = client(transport.clone(), 16);
    assert_eq!(
        client.scan_with_oversized_key(request(4)).await.unwrap(),
        MultiScanOutcome::OversizedRow { key: key.to_vec() }
    );
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legacy_and_strict_scan_keep_their_existing_too_large_errors() {
    let transport = Arc::new(TestTransport::row(b"n", 8));
    let client = client(transport.clone(), 8);
    assert_eq!(client.scan(request(8)).await, Err(ClientError::TooLarge));
    assert_eq!(client.scan_bounded(request(8)).await, Err(ClientError::TooLarge));
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport.bounded_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_bounds_never_dispatch_or_expose_a_key() {
    let transport = Arc::new(TestTransport::row(b"n", 32));
    let client = client(transport.clone(), 8);
    let mut invalid = request(8);
    invalid.start = Some(b"z".to_vec());
    assert!(matches!(
        client.scan_with_oversized_key(invalid).await,
        Err(ClientError::InvalidRequest(_))
    ));
    assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_response_range_order_and_continuation_never_expose_a_key() {
    for kind in 0..4 {
        let mut transport = TestTransport::row(b"n", 32);
        let mut query = request(8);
        match kind {
            0 => transport.items[0].key = b"a".to_vec(),
            1 => transport.items.push(RpcValue {
                key: b"m".to_vec(),
                value: vec![7; 32],
                revision: 1,
            }),
            2 => transport.continuation_key = Some(b"m".to_vec()),
            _ => {
                query.continuation = Some(MultiScanContinuation {
                    direction: query.direction,
                    original_start: query.start.clone(),
                    original_end: query.end.clone(),
                    last_key: b"n".to_vec(),
                    catalog_generation: 1,
                });
            }
        }
        let transport = Arc::new(transport);
        let client = client(transport.clone(), 8);
        assert!(matches!(
            client.scan_with_oversized_key(query).await,
            Err(ClientError::InvalidRequest(_))
        ));
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn a_transport_too_large_error_has_no_observed_key() {
    let mut transport = TestTransport::row(b"n", 32);
    transport.failure = Some(ClientError::TooLarge);
    let transport = Arc::new(transport);
    let client = client(transport.clone(), 8);
    assert_eq!(
        client.scan_with_oversized_key(request(8)).await,
        Err(ClientError::TooLarge)
    );
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
}
